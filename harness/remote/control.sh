#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
set -euo pipefail

usage() {
  cat <<'USAGE'
usage: control.sh server-start PORT
       control.sh server-cpu LABEL
       control.sh server-stop PORT
       control.sh client SERVER_IP PORT LABEL
       control.sh counters LABEL PHASE

The session control of every benchmark run (one JSON line per probe, on stdout), with
/opt/keep/bin/netprobe: server-start runs a TCP echo server on PORT; server-cpu runs a fixed CPU loop
on the last CPU; client runs the CPU loop, a closed-loop 64 B TCP ping-pong (50000 samples after 2000
warmup) and a paced one (10000/s for 20 s); counters prints steal ticks, TCP retransmits and receive
drops of the primary NIC.
USAGE
}

NP=/opt/keep/bin/netprobe

counters() {
  local label=$1 phase=$2 iface steal retrans drops
  iface=$(ip -o route show default | awk '{ for (i = 1; i < NF; i++) if ($i == "dev") { print $(i + 1); exit } }')
  steal=$(awk '$1 == "cpu" { print $9 }' /proc/stat)
  retrans=$(awk '$1 == "Tcp:" && $2 ~ /^[0-9]+$/ { print $13 }' /proc/net/snmp)
  drops=$(cat "/sys/class/net/$iface/statistics/rx_dropped")
  printf '{"probe":"counters","label":"%s","phase":"%s","host":"%s","steal_ticks":%s,"tcp_retrans":%s,"rx_dropped":%s}\n' \
    "$label" "$phase" "$(hostname)" "$steal" "$retrans" "$drops"
}

cpus() {
  if [ "$(nproc)" -ge 4 ]; then echo "--cpus 2,3"; fi
}

case "${1:-}" in
  server-start)
    pgrep -f "^$NP tcp-echo --port $2" > /dev/null \
      || { setsid nohup "$NP" tcp-echo --port "$2" > /dev/null 2>&1 < /dev/null & }
    sleep 0.3
    ;;
  server-cpu) "$NP" cpu-loop --cpu "$(( $(nproc) - 1 ))" --label "$2/server-cpu" ;;
  server-stop) pkill -f "^$NP tcp-echo --port $2" || true ;;
  client)
    # shellcheck disable=SC2046
    {
      "$NP" cpu-loop --cpu "$(( $(nproc) - 1 ))" --label "$4/cpu"
      "$NP" tcp-ping --target "$2:$3" --count 50000 --warmup 2000 $(cpus) --label "$4/closed64"
      "$NP" tcp-ping --target "$2:$3" --rate 10000 --duration 20 --warmup 2000 $(cpus) --label "$4/paced10k"
    }
    ;;
  counters) counters "$2" "$3" ;;
  -h|--help) usage ;;
  *) usage >&2; exit 2 ;;
esac
