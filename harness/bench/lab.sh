# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
# shellcheck shell=bash

PROBE_PORT=7101
NETPROBE=$BIN/netprobe

rtt_matrix() {
  local out=$1 threshold=$2 hosts=() src dst i j cmd
  for src in $(host_names); do hosts+=("$src"); done
  for src in "${hosts[@]}"; do
    hssh "$src" "pkill -f '^$NETPROBE tcp-echo --port $PROBE_PORT' || true; { setsid nohup $NETPROBE tcp-echo --port $PROBE_PORT > /dev/null 2>&1 < /dev/null & }; sleep 0.3; test -x $NETPROBE" < /dev/null \
      || die "rtt: cannot start the echo server on $src (is netprobe built? harness/build.sh)"
  done
  : > "$out.tmp"
  for ((i = 0; i < ${#hosts[@]} - 1; i++)); do
    src=${hosts[$i]} cmd=""
    for ((j = i + 1; j < ${#hosts[@]}; j++)); do
      dst=${hosts[$j]}
      cmd="$cmd $NETPROBE tcp-ping --target $(host_ip "$dst"):$PROBE_PORT --count 20000 --size 64 --warmup 2000 --label '$src>$dst';"
    done
    hssh "$src" "$cmd" < /dev/null >> "$out.tmp"
  done
  for src in "${hosts[@]}"; do hssh "$src" "pkill -f '^$NETPROBE tcp-echo --port $PROBE_PORT' || true" < /dev/null; done
  cat "$out.tmp" >> "$out"
  python3 -I - "$out.tmp" "$threshold" <<'PY'
import json, sys
path, threshold = sys.argv[1], float(sys.argv[2])
for line in open(path):
    if not line.startswith("{"):
        continue
    r = json.loads(line)
    p50 = r["rtt_ns"]["p50"] / 1e3
    flag = "  WARNING above %.0f us: reroll one of the pair" % threshold if p50 > threshold else ""
    print("%-16s p50 %7.1f us  p99 %7.1f us%s" % (r["label"], p50, r["rtt_ns"]["p99"] / 1e3, flag))
PY
  rm -f "$out.tmp"
}

session_control() {
  local label=$1 client=$2 server=$3 out=$4
  mkdir -p "$(dirname "$out")"
  hssh "$server" "bash $REMOTE/remote/control.sh server-start $PROBE_PORT && bash $REMOTE/remote/control.sh counters '$label' before && bash $REMOTE/remote/control.sh server-cpu '$label'" < /dev/null >> "$out"
  hssh "$client" "bash $REMOTE/remote/control.sh counters '$label' before && bash $REMOTE/remote/control.sh client $(host_ip "$server") $PROBE_PORT '$label' && bash $REMOTE/remote/control.sh counters '$label' after" < /dev/null >> "$out"
  hssh "$server" "bash $REMOTE/remote/control.sh counters '$label' after && bash $REMOTE/remote/control.sh server-stop $PROBE_PORT" < /dev/null >> "$out"
  jq -r --arg l "$label" 'select(.label != null and (.label | startswith($l))) |
    if .probe == "tcp-ping" then "\(.label)\tp50 \(.rtt_ns.p50 / 1000) us\tp99 \(.rtt_ns.p99 / 1000) us\tp99.9 \(.rtt_ns.p999 / 1000) us"
    elif .probe == "cpu-loop" then "\(.label)\talu \(.kernels[0].ns_per_op_median) ns/op"
    elif .probe == "counters" then "\(.label)/\(.host)/\(.phase)\tsteal \(.steal_ticks)\tretrans \(.tcp_retrans)\trx_dropped \(.rx_dropped)"
    else empty end' "$out" >&2
}

set_rto() {
  local us=$1 host
  [ "$us" != default ] || us=200000
  [[ "$us" =~ ^[0-9]+$ ]] || die "rto takes microseconds or default"
  for host in $(host_names); do
    printf '%s ' "$host"
    hssh "$host" "sudo sysctl -qw net.ipv4.tcp_rto_min_us=$us && sudo sysctl -n net.ipv4.tcp_rto_min_us" < /dev/null
  done
}

fill() {
  local host=$1 action=$2
  hssh "$host" "sudo bash $REMOTE/remote/fill.sh $action" < /dev/null > /dev/null
}
