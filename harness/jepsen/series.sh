#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
set -u

usage() {
  cat <<'USAGE'
usage: series.sh [--pressure] core|network|lease|readback|tls|arm|smoke|RUN...

Runs on the Jepsen control node as root, from the directory that holds run.sh, with the nodes in
/root/nodes.txt and the binaries in /opt/keep/bin. Each run goes to OUT (default /root/out)/RUN: args.txt
(route, TLS and options), run.log, proxy.log, rc.txt, the Jepsen store and all.tgz; progress lines go to
OUT/series.log. A run is named WORKLOAD-NEMESES, with -wide (kill and pause may target a majority),
-timesync (NTP left running in clock runs) and -unrouted (client route=none instead of shard routing);
tls- runs use mutual TLS with raw public keys. The series:
  core      lock, register, failover and counter runs under kill, pause, partition, clock and leave
  network   one-way cuts, packet faults, bridges and wide targets
  lease     lease runs
  readback  readback under heal-restart, both routes
  tls       the core, network and readback runs that cover TLS
  arm       the subset for an arm64 cluster
  smoke     one lock run with kills for 60 s
Any run can also be named alone; the run list is in preset() below. --pressure runs every one with a memory cap
and a writer of large values beside the workload (run.sh KEEP_PRESSURE=on). One series runs at a time on a
control node, and one run.sh: a second one refuses to start.
USAGE
}

OUT=${OUT:-/root/out}
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

preset() {
  case "$1" in
    lock-kill-pause-partition-clock-unrouted) echo "none off --workload lock --nemesis kill,pause,partition,clock --time-limit 600" ;;
    lock-kill-pause-partition-clock) echo "default off --workload lock --nemesis kill,pause,partition,clock --time-limit 600" ;;
    lock-pause-partition-unrouted) echo "none off --workload lock --nemesis pause,partition --time-limit 600" ;;
    register-leave) echo "default off --workload register --nemesis leave --time-limit 300" ;;
    failover-kill-partition-unrouted) echo "none off --workload register-failover --nemesis kill,partition --time-limit 300" ;;
    counter-leave) echo "default off --workload counter --nemesis leave --time-limit 300" ;;
    lock-kill-pause-partition-clock-timesync) echo "default off --workload lock --nemesis kill,pause,partition,clock --keep-time-sync --time-limit 600" ;;
    lock-one-way) echo "default off --workload lock --nemesis one-way --time-limit 600" ;;
    failover-one-way-packet-unrouted) echo "none off --workload register-failover --nemesis one-way,packet --time-limit 300" ;;
    lock-kill-pause-wide-unrouted) echo "none off --workload lock --nemesis kill,pause --wide-targets --time-limit 600" ;;
    failover-kill-pause-bridge-wide) echo "default off --workload register-failover --nemesis kill,pause,bridge --wide-targets --time-limit 300" ;;
    register-leave-unrouted) echo "none off --workload register --nemesis leave --time-limit 300" ;;
    lock-kill-pause-partition-bridge-one-way-packet-clock-wide) echo "default off --workload lock --nemesis kill,pause,partition,bridge,one-way,packet,clock --wide-targets --time-limit 900" ;;
    lock-kill-pause-partition-clock-unrouted-2) echo "none off --workload lock --nemesis kill,pause,partition,clock --time-limit 600" ;;
    lease-kill-pause-partition-clock) echo "default off --workload lease --nemesis kill,pause,partition,clock --time-limit 600" ;;
    lease-packet-bridge-unrouted) echo "none off --workload lease --nemesis packet,bridge --time-limit 600" ;;
    lease-kill-pause-partition-bridge-packet-clock-timesync) echo "default off --workload lease --nemesis kill,pause,partition,bridge,packet,clock --keep-time-sync --time-limit 600" ;;
    lease-one-way-unrouted) echo "none off --workload lease --nemesis one-way --time-limit 600" ;;
    lease-packet-bridge) echo "default off --workload lease --nemesis packet,bridge --time-limit 600" ;;
    smoke-lock-kill) echo "default off --workload lock --nemesis kill --time-limit 60" ;;
    readback-heal-restart) echo "default off --workload readback --nemesis heal-restart --time-limit 300" ;;
    readback-heal-restart-unrouted) echo "none off --workload readback --nemesis heal-restart --time-limit 300" ;;
    readback-heal-restart-2) echo "default off --workload readback --nemesis heal-restart --time-limit 300" ;;
    tls-lock-kill-pause-partition-clock) echo "default rpk --workload lock --nemesis kill,pause,partition,clock --time-limit 600" ;;
    tls-register-leave) echo "default rpk --workload register --nemesis leave --time-limit 300" ;;
    tls-failover-one-way-packet-unrouted) echo "none rpk --workload register-failover --nemesis one-way,packet --time-limit 300" ;;
    tls-readback-heal-restart) echo "default rpk --workload readback --nemesis heal-restart --time-limit 300" ;;
    tls-failover-kill-pause-bridge-wide) echo "default rpk --workload register-failover --nemesis kill,pause,bridge --wide-targets --time-limit 300" ;;
    tls-lease-packet-bridge-unrouted) echo "none rpk --workload lease --nemesis packet,bridge --time-limit 600" ;;
    tls-failover-kill-partition-unrouted) echo "none rpk --workload register-failover --nemesis kill,partition --time-limit 300" ;;
    tls-readback-heal-restart-2) echo "default rpk --workload readback --nemesis heal-restart --time-limit 300" ;;
    arm-lock-kill-pause-partition-clock) echo "default off --workload lock --nemesis kill,pause,partition,clock --time-limit 600" ;;
    arm-failover-kill-partition-unrouted) echo "none off --workload register-failover --nemesis kill,partition --time-limit 300" ;;
    arm-failover-kill-pause-bridge-wide) echo "default off --workload register-failover --nemesis kill,pause,bridge --wide-targets --time-limit 300" ;;
    arm-lease-packet-bridge-unrouted) echo "none off --workload lease --nemesis packet,bridge --time-limit 600" ;;
    arm-readback-heal-restart) echo "default off --workload readback --nemesis heal-restart --time-limit 300" ;;
    *) return 1 ;;
  esac
}

run() {
  local id=$1 spec route tls opts dir rc
  spec=$(preset "$id") || { echo "series.sh: unknown run $id" >&2; return 2; }
  read -r route tls opts <<< "$spec"
  dir=$OUT/$id
  rm -rf "$dir" "$HERE/store"
  mkdir -p "$dir"
  echo "KEEP_ROUTE=$route KEEP_TLS=$tls $opts" > "$dir/args.txt"
  echo "[$(date +%H:%M:%S)] $id start: KEEP_ROUTE=$route KEEP_TLS=$tls $opts" >> "$OUT/series.log"
  # shellcheck disable=SC2086
  KEEP_ROUTE=$route KEEP_TLS=$tls "$HERE/run.sh" /root/nodes.txt /opt/keep/bin $opts > "$dir/run.log" 2>&1
  rc=$?
  echo "rc=$rc" > "$dir/rc.txt"
  grep -m1 '^jepsen: client route' "$dir/run.log" >> "$dir/args.txt"
  grep -m1 '^jepsen: tls' "$dir/run.log" >> "$dir/args.txt"
  grep -m1 '^jepsen: pressure' "$dir/run.log" >> "$dir/args.txt"
  cp "$HERE/proxy.log" "$dir/proxy.log" 2> /dev/null
  cp "$HERE/large-writer.log" "$dir/large-writer.log" 2> /dev/null
  cp -a "$HERE/store" "$dir/store" 2> /dev/null
  tar -C "$dir" -czf "$dir/all.tgz" store run.log proxy.log args.txt rc.txt 2> /dev/null
  date -u +%FT%TZ > "$dir/done"
  echo "[$(date +%H:%M:%S)] $id end rc=$rc" >> "$OUT/series.log"
  return "$rc"
}

[ $# -ge 1 ] || { usage >&2; exit 2; }
case "$1" in -h|--help) usage; exit 0 ;; esac
if [ "$1" = --pressure ]; then
  export KEEP_PRESSURE=on
  shift
fi
exec 8> "$HERE/.series.lock"
flock -n 8 || { echo "series.sh: another series holds $HERE/.series.lock" >&2; exit 2; }
mkdir -p "$OUT"
cd "$HERE" || exit 2
ids=()
for arg in "$@"; do
  case "$arg" in
    core) ids+=(lock-kill-pause-partition-clock-unrouted lock-kill-pause-partition-clock lock-pause-partition-unrouted register-leave failover-kill-partition-unrouted counter-leave lock-kill-pause-partition-clock-timesync) ;;
    network) ids+=(lock-one-way failover-one-way-packet-unrouted lock-kill-pause-wide-unrouted failover-kill-pause-bridge-wide register-leave-unrouted lock-kill-pause-partition-bridge-one-way-packet-clock-wide lock-kill-pause-partition-clock-unrouted-2) ;;
    lease) ids+=(lease-kill-pause-partition-clock lease-packet-bridge-unrouted lease-kill-pause-partition-bridge-packet-clock-timesync lease-one-way-unrouted lease-packet-bridge) ;;
    readback) ids+=(readback-heal-restart readback-heal-restart-unrouted readback-heal-restart-2) ;;
    tls) ids+=(tls-lock-kill-pause-partition-clock tls-register-leave tls-failover-one-way-packet-unrouted tls-readback-heal-restart tls-failover-kill-pause-bridge-wide tls-lease-packet-bridge-unrouted tls-failover-kill-partition-unrouted tls-readback-heal-restart-2) ;;
    arm) ids+=(arm-lock-kill-pause-partition-clock arm-failover-kill-partition-unrouted arm-failover-kill-pause-bridge-wide arm-lease-packet-bridge-unrouted arm-readback-heal-restart) ;;
    smoke) ids+=(smoke-lock-kill) ;;
    *) ids+=("$arg") ;;
  esac
done
status=0
for id in "${ids[@]}"; do run "$id" || status=1; done
echo "[$(date +%H:%M:%S)] SERIES DONE status=$status" >> "$OUT/series.log"
exit "$status"
