#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
# shellcheck source=lib/common.sh
. "$(dirname "$0")/lib/common.sh"

usage() {
  cat <<'USAGE'
usage: setup.sh HOSTS.json [--only NAME]...

Copies harness/provision, harness/remote and versions.env to /opt/keep-harness on every host (or the --only
ones), then runs the host's "provision" steps of the topology in order, each as a detached root job
(provision-STEP): os-tuning, idle-poll, docker, competitors, toolchain, jepsen-control, jepsen-node.
After idle-poll the host reboots once when /proc/cmdline lacks idle=poll, and setup fails unless it comes
back with it. jepsen-node runs last, with the root key of the control host (role control) that
jepsen-control printed. Rerunning skips the steps that already succeeded.
USAGE
}

ONLY=""

selected() {
  local name="$1" o
  [ -z "$ONLY" ] && return 0
  for o in $ONLY; do [ "$o" = "$name" ] && return 0; done
  return 1
}

steps() { inv --arg n "$1" '.hosts[] | select(.name == $n) | .provision[]'; }

upload() {
  local host="$1"
  tar -C "$HARNESS" -czf - provision remote versions.env \
    | hssh "$host" "sudo mkdir -p $REMOTE && sudo chown \$(id -un): $REMOTE && rm -rf $REMOTE/provision $REMOTE/remote && tar -xzf - -C $REMOTE"
}

reboot_for_idle_poll() {
  local host="$1"
  hssh "$host" "grep -qw idle=poll /proc/cmdline" < /dev/null && return 0
  say "$host: rebooting for idle=poll"
  hssh "$host" "sudo systemctl reboot" < /dev/null || true
  ssh_opts
  ssh "${SSH_OPTS[@]}" -O exit "$SSH_USER@$(host_field "$host" public_ip)" 2> /dev/null || true
  sleep 20
  for _ in $(seq 1 40); do
    if hssh "$host" true < /dev/null 2> /dev/null; then break; fi
    sleep 10
  done
  hssh "$host" "grep -qw idle=poll /proc/cmdline" < /dev/null || die "$host: no idle=poll in /proc/cmdline after the reboot"
  say "$host: idle=poll active"
}

provision_host() {
  local host="$1" step
  upload "$host"
  for step in $(steps "$host"); do
    [ "$step" != jepsen-node ] || continue
    [ -f "$HARNESS/provision/$step.sh" ] || die "$host: no provision step $step"
    say "$host: $step"
    job_run "$host" "provision-$step" --sudo -- bash "$REMOTE/provision/$step.sh" < /dev/null
    [ "$step" != idle-poll ] || reboot_for_idle_poll "$host"
  done
}

control_pubkey() {
  local control
  control=$(host_names control | head -1)
  [ -n "$control" ] || die "jepsen-node needs a host with role control"
  hssh "$control" "$JOB log provision-jepsen-control 200" < /dev/null | sed -n 's/^pubkey //p' | tail -1
}

node_host() {
  local host="$1"
  say "$host: jepsen-node"
  job_run "$host" provision-jepsen-node --sudo -- bash "$REMOTE/provision/jepsen-node.sh" "$PUBKEY" < /dev/null
}

main() {
  local hosts=() nodes=() host
  [ $# -ge 1 ] || { usage >&2; exit 2; }
  case "$1" in -h|--help) usage; exit 0 ;; esac
  load_inventory "$1"
  shift
  while [ $# -gt 0 ]; do
    case "$1" in
      --only) ONLY="$ONLY ${2:?--only needs a host name}"; shift 2 ;;
      *) die "unknown argument $1" ;;
    esac
  done
  for host in $(host_names); do
    selected "$host" || continue
    hosts+=("$host")
    if steps "$host" | grep -qx jepsen-node; then nodes+=("$host"); fi
  done
  [ ${#hosts[@]} -gt 0 ] || die "no host selected"
  on_hosts provision_host "${hosts[@]}"
  if [ ${#nodes[@]} -gt 0 ]; then
    PUBKEY=$(control_pubkey)
    [ -n "$PUBKEY" ] || die "no root key in the control host's provision-jepsen-control log"
    on_hosts node_host "${nodes[@]}"
  fi
  say "provisioned: ${hosts[*]}"
}

main "$@"
