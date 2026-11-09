#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
# shellcheck source=lib/common.sh
. "$(dirname "$0")/lib/common.sh"

usage() {
  cat <<'USAGE'
usage: build.sh HOSTS.json kv|stream|jepsen|all [--source DIR]

Builds Keep and the harness binaries from a checkout on the inventory's build host (targets.build) and
installs them in /opt/keep/bin on every host:
  kv      keep (keep-server), netprobe (RTT and control probes) and loadgen
  stream  keep, netprobe and mock-stalwart (every backend: --features mock-stalwart/all)
  jepsen  keep and keep-jepsen
  all     all of them
Each binary family builds in its own cargo invocation and target directory (target/server, target/bench,
target/jepsen). keep and netprobe build with the checkout's own cargo configuration and RUSTFLAGS left
unset, as a release build does. loadgen and mock-stalwart add --cfg keep_bench (keep-client's TLS config
hook) and keep-jepsen --cfg keep_observe (keep-client's server probe) through a cargo --config file whose
target rustflags cargo joins with the checkout's, so the keep binary and the load generators never carry
the probe.
The source is the keep checkout this harness belongs to (tracked and untracked, not ignored files), or
--source DIR (copied whole, without target/, work/ and .git/). The builds run as detached jobs
(build-UNIT, cargo build --release --locked) under ~/keep on the build host. The binaries and their sha256 sums are
kept in HARNESS_WORK/RUN/bin/ and streamed to every host over SSH (the VMs hold no keys to each other).
USAGE
}

units() {
  case "$1" in
    kv) echo "server probe loadgen" ;;
    stream) echo "server probe mock-stalwart" ;;
    jepsen) echo "server jepsen" ;;
    all) echo "server probe loadgen mock-stalwart jepsen" ;;
    *) die "unknown set $1 (kv, stream, jepsen or all)" ;;
  esac
}

unit_cargo() {
  case "$1" in
    server) echo "server - -p keep-server" ;;
    probe) echo "server - -p netprobe" ;;
    loadgen) echo "bench keep_bench -p loadgen" ;;
    mock-stalwart) echo "bench keep_bench -p mock-stalwart --features mock-stalwart/all" ;;
    jepsen) echo "jepsen keep_observe -p keep-jepsen" ;;
  esac
}

unit_binary() {
  case "$1" in
    server) echo keep ;;
    probe) echo netprobe ;;
    loadgen) echo loadgen ;;
    mock-stalwart) echo mock-stalwart ;;
    jepsen) echo keep-jepsen ;;
  esac
}

sync_source() {
  local host="$1" source="$2"
  hssh "$host" "mkdir -p keep" < /dev/null
  if [ -z "$source" ]; then
    source=$(cd "$HARNESS/.." && pwd)
    git -C "$source" ls-files -co --exclude-standard -z \
      | rsync_to "$host" "$source/" keep/ --from0 --files-from=-
  else
    rsync_to "$host" "$source/" keep/ --exclude /target --exclude /work --exclude /.git --exclude /harness/work
  fi
}

install_on() {
  local host="$1"
  hssh "$host" "sudo mkdir -p $BIN && sudo chown \$(id -un): $BIN && tar -xzf - -C $BIN" < "$STATE/bin/bin.tgz"
}

main() {
  local set source="" build unit dir cfg config cargo bins="" paths=""
  [ $# -ge 2 ] || { usage >&2; exit 2; }
  case "$1" in -h|--help) usage; exit 0 ;; esac
  load_inventory "$1"
  set="$2"
  shift 2
  while [ $# -gt 0 ]; do
    case "$1" in
      --source) source="$(cd "${2:?--source needs a directory}" && pwd)"; shift 2 ;;
      *) die "unknown argument $1" ;;
    esac
  done
  units "$set" > /dev/null
  build=$(target_host build)
  say "syncing the source to $build"
  sync_source "$build" "$source"
  for unit in $(units "$set"); do
    read -r dir cfg cargo <<< "$(unit_cargo "$unit")"
    config=""
    if [ "$cfg" != - ]; then
      printf '[target.%s]\nrustflags = ["--cfg", "%s"]\n' "'cfg(all())'" "$cfg" \
        | hssh "$build" "cat > keep-$cfg.toml"
      config="--config ../keep-$cfg.toml "
    fi
    say "building $unit on $build: cargo build --release --locked $config$cargo"
    job_run "$build" "build-$unit" --again -- bash -lc ". /etc/profile.d/keep-rust.sh && cd ~/keep && CARGO_TARGET_DIR=target/$dir cargo build --release --locked $config$cargo" < /dev/null
    bins="$bins $(unit_binary "$unit")"
    paths="$paths $dir/release/$(unit_binary "$unit")"
  done
  mkdir -p "$STATE/bin"
  # shellcheck disable=SC2086
  hssh "$build" "cd ~/keep/target && sha256sum $paths && tar -czf ~/keep-bin.tgz --transform='s|.*/||' $paths" > "$STATE/bin/sha256.txt" < /dev/null
  rsync_from "$build" keep-bin.tgz "$STATE/bin/bin.tgz"
  tar -xzf "$STATE/bin/bin.tgz" -C "$STATE/bin"
  # shellcheck disable=SC2046
  on_hosts install_on $(host_names)
  say "installed $bins in $BIN on every host"
  cat "$STATE/bin/sha256.txt"
}

main "$@"
