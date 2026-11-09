#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../lib/common.sh
. "$HERE/../lib/common.sh"
# shellcheck source=lab.sh
. "$HERE/lab.sh"

SERVERS=$HERE/servers.sh
EVENTS=${EVENTS:-stalwart}
ACK_EVERY=${ACK_EVERY:-8}
VERIFY=${VERIFY:-full}
WARMUP=${WARMUP:-5}
DURATION=${DURATION:-30}
SAT_P99_US=${SAT_P99_US:-10000}
FILL=${FILL:-0}
FILL_BRK=${FILL_BRK:-0}
RTT_MAX_US=${RTT_MAX_US:-80}
ST_OUT=/opt/keep/out/stream
CLIENT_DIR=/opt/keep/clients
TARGETS_DEFAULT="keep-server valkey-pubsub:redis valkey-pubsub:redis-sharded nats kafka zenoh fluvio:stock fluvio:low-latency dragonfly-pubsub:redis-sharded"

usage() {
  cat <<'USAGE'
usage: harness/bench/stream.sh HOSTS.json STEP [ARGS]

Keep against the stream competitors: one broker VM (brk) and client VMs (c1..cN, role load) running
mock-stalwart nodes that each publish and receive every other node's stream. Outputs go to HARNESS_OUT
(default: the inventory's directory/results).
env: EVENTS (stalwart) ACK_EVERY (8) VERIFY (full|header) WARMUP DURATION (5, 30 s) SAT_P99_US (10000)
     FILL (0; 1: a SCHED_IDLE spinner on every vCPU of the client VMs during each point, for VMs without
     idle=poll) FILL_BRK (0; 1: the same on every broker vCPU, outside the broker's cgroup)
     RTT_MAX_US (80: the pair p50 above which `rtt` asks for a reroll)

  install                  versions into OUT/versions.txt; the node launcher on every client VM
  rtt                      64 B TCP ping-pong (netprobe) between every pair of VMs into OUT/rtt-stream.jsonl, with a
                           WARNING for each pair whose p50 is above RTT_MAX_US; the client-to-brk median is the
                           floor for event latency excess
  control LABEL            session control c1 to brk into OUT/controls.jsonl (start and end of every session)
  rto US|default           net.ipv4.tcp_rto_min_us on every VM
  point TARGET NODES RATE NAME
                           TARGET is SERVER or SERVER:SCHEME: keep-server, nats, kafka, zenoh,
                           valkey-pubsub:redis (PUBLISH), valkey-pubsub:redis-sharded and
                           dragonfly-pubsub:redis-sharded (SPUBLISH), fluvio:stock and fluvio:low-latency
                           (the Fluvio client's own sockets, or TCP_NODELAY plus TCP_QUICKACK); NODES mock-stalwart
                           processes spread over the client VMs, all starting together, each publishing RATE
                           events/s of EVENTS and receiving every other node's stream; writes
                           OUT/NAME/{node-*.json,accounting.json,server-stats.json,cgsample.jsonl,variant.json}
  matrix NODES RATES REPS [TARGET...]
                           REPS rounds over every TARGET (default: keep-server and every competitor) in
                           random order, for each comma-separated NODES count and RATES value
  saturate NODES RATES REPS [TARGET...]
                           per round and TARGET (random order) the RATES in ascending order until the first
                           rate where the target loses events, its slowest node's echo_last p99 exceeds
                           SAT_P99_US, or the clients themselves are the limit; the reason goes to OUT/saturation.jsonl
  collect                  copies the inventory into OUT
  table                    stream_table.py over OUT
USAGE
}

case "${1:-}" in -h|--help|"") usage; [ -n "${1:-}" ]; exit $? ;; esac
load_inventory "$1"
shift
servers() { "$SERVERS" "$HOSTS" "$@"; }
client_vms() { host_names load; }
client_count() { client_vms | wc -l | tr -d ' '; }

stream_url() {
  local server=$1 scheme=$2 ip
  ip=$(host_ip brk)
  case "$server:$scheme" in
    *:redis) echo "redis://$ip:6379" ;;
    *:redis-sharded) echo "redis-sharded://$ip:6379" ;;
    nats:) echo "nats://$ip:4222" ;;
    kafka:) echo "kafka://$ip:9092" ;;
    zenoh:) echo "zenoh-client://tcp/$ip:7447" ;;
    fluvio:stock | fluvio:low-latency) echo "fluvio://$ip:9003?socket=$scheme" ;;
    keep-server:) echo "keep://$ip:7000?secret=$(servers secret brk)&conns=1" ;;
    *) die "no stream URL for $server:$scheme" ;;
  esac
}

split_target() {
  SERVER=${1%%:*} SCHEME=${1#*:}
  [ "$SCHEME" != "$1" ] || SCHEME=""
}

check_targets() {
  local target
  for target in "$@"; do
    split_target "$target"
    stream_url "$SERVER" "$SCHEME" > /dev/null
  done
}

run_clients() {
  local vm=${1%%:*} rest=${1#*:} first count
  first=${rest%%:*} count=${rest#*:}
  job_run "$vm" "stream-$NAME" --again -- bash -c "rm -rf $ST_OUT/$NAME && mkdir -p $ST_OUT/$NAME && cd $CLIENT_DIR && env BIN=$BIN/mock-stalwart NODES=$count FIRST=$first EXPECTED=$NODES CPUS_PER_NODE=$CPUS OUT=$ST_OUT/$NAME START=$START bash run-nodes.sh --stream '$URL' --events $EVENTS --event-rate $RATE --ack-every $ACK_EVERY --verify $VERIFY --warmup $WARMUP --duration $DURATION --settle 3 --worker-threads $(( CPUS > 0 ? CPUS : 1 ))" < /dev/null
}

fetch_clients() {
  local vm=$1
  hssh "$vm" "test -d $ST_OUT/$NAME && tar -C $ST_OUT/$NAME -czf - . || true" < /dev/null | tar -xzf - -C "$OUT/$NAME"
  hssh "$vm" "$JOB log stream-$NAME 100000" > "$OUT/$NAME/$vm-run.log" < /dev/null || true
  [ ! -e "$OUT/$NAME/merged.json" ] || mv "$OUT/$NAME/merged.json" "$OUT/$NAME/$vm-merged.json"
}

point() {
  local target=$1 nodes=$2 rate=$3 name=$4 per_vm vm_cpus first count failed=0 effective items=() vm
  split_target "$target"
  URL=$(stream_url "$SERVER" "$SCHEME") || return 1
  NAME=$name NODES=$nodes RATE=$rate
  servers start brk "$SERVER" "$(hssh brk nproc < /dev/null)"
  servers sample brk start
  vm_cpus=$(hssh "$(client_vms | head -1)" nproc < /dev/null)
  per_vm=$(( (nodes + $(client_count) - 1) / $(client_count) ))
  CPUS=$(( vm_cpus / per_vm ))
  if [ "$FILL" = 1 ]; then
    for vm in $(client_vms); do fill "$vm" start || { say "$name: fill FAILED on $vm"; failed=1; }; done
  fi
  if [ "$FILL_BRK" = 1 ]; then fill brk start || { say "$name: fill FAILED on brk"; failed=1; }; fi
  START=$(python3 -I -c "import time; print(f'{time.time() + 25:.3f}')")
  first=1
  for vm in $(client_vms); do
    [ "$first" -le "$nodes" ] || break
    count=$(( nodes - first + 1 < per_vm ? nodes - first + 1 : per_vm ))
    items+=("$vm:$first:$count")
    first=$(( first + count ))
  done
  [ "$failed" = 0 ] && { on_hosts run_clients "${items[@]}" || say "$name: a node launcher FAILED (see OUT/$name/*-run.log)"; }
  if [ "$FILL" = 1 ]; then for vm in $(client_vms); do fill "$vm" stop || true; done; fi
  [ "$FILL_BRK" != 1 ] || fill brk stop || true
  servers sample brk stop
  mkdir -p "$OUT/$name"
  servers sample brk collect "$OUT/$name/cgsample.jsonl"
  hssh brk "rm -f /opt/keep/baselines/cgsample.jsonl" < /dev/null
  servers stats brk > "$OUT/$name/server-stats.json"
  [ "$SERVER" != keep-server ] || servers logs brk > "$OUT/$name/keep-server.log"
  effective='{}'
  [ "$SERVER" != keep-server ] || effective=$(python3 -I "$HERE/keep_coalesce.py" "$OUT/$name/keep-server.log") || effective='{}'
  python3 -I -c 'import json, sys; v = {"target": sys.argv[1]}; v.update(json.loads(sys.argv[2])); print(json.dumps(v))' \
    "$target" "$effective" > "$OUT/$name/variant.json"
  servers stop brk
  for item in "${items[@]}"; do fetch_clients "${item%%:*}"; done
  python3 -I "$HERE/stream_accounting.py" "$OUT/$name" > "$OUT/$name/accounting.json" || failed=1
  say "$name done"
  return "$failed"
}

classify() {
  local dir=$OUT/$1
  [ -d "$dir" ] && ls "$dir"/node-*.json > /dev/null 2>&1 \
    && python3 -I "$HERE/stream_accounting.py" "$dir" > "$dir/accounting.json.tmp" \
    && mv "$dir/accounting.json.tmp" "$dir/accounting.json"
  python3 -I -c "
import json, sys
try:
    a = json.load(open(sys.argv[1]))
except (OSError, ValueError):
    print('no-accounting'); sys.exit()
if (a.get('window_start_spread_ms') or 0) > 200:
    print('late-start')
elif a['gaps'] > 0 or a['end_markers'] < a['end_markers_expected'] or a['deliveries_incomplete'] > 0:
    print('lost')
elif a['echo_last_p99_us_max'] > float(sys.argv[2]):
    print('p99')
elif a['producer_stalls'] > 0 or a['client_cores_max'] > 0.7 * float(sys.argv[3]):
    print('client-limited')
else:
    print('ok')
" "$dir/accounting.json" "$SAT_P99_US" "$2"
}

step=${1:-}
[ -n "$step" ] || { usage >&2; exit 2; }
shift
case "$step" in
  install)
    mkdir -p "$OUT"
    servers versions brk | tee "$OUT/versions.txt"
    for vm in $(client_vms); do
      hssh "$vm" "mkdir -p $CLIENT_DIR && cat > $CLIENT_DIR/run-nodes.sh" < "$HERE/run-nodes.sh"
    done
    ;;
  rtt)
    mkdir -p "$OUT"
    rtt_matrix "$OUT/rtt-stream.jsonl" "$RTT_MAX_US"
    ;;
  control) session_control "stream/${1:-control}" "$(client_vms | head -1)" brk "$OUT/controls.jsonl" ;;
  rto) set_rto "${1:?rto US (e.g. 5000) or default}" ;;
  point)
    [ $# -ge 4 ] || die "point TARGET NODES RATE NAME"
    point "$@"
    ;;
  matrix)
    [ $# -ge 3 ] || die "matrix NODES RATES REPS [TARGET...]"
    nodes_list=$1 rates=$2 reps=$3
    shift 3
    targets=${*:-$TARGETS_DEFAULT}
    # shellcheck disable=SC2086
    check_targets $targets
    for rep in $(seq 1 "$reps"); do
      for nodes in ${nodes_list//,/ }; do
        for rate in ${rates//,/ }; do
          # shellcheck disable=SC2086
          for target in $(printf '%s\n' $targets | sort -R); do
            name=$(echo "$target" | tr ':' '-')-n$nodes-r$rate-$EVENTS-$rep
            [ -s "$OUT/$name/accounting.json" ] && { say "skip $name (done)"; continue; }
            say "$name"
            point "$target" "$nodes" "$rate" "$name" || say "$name FAILED"
          done
        done
      done
    done
    ;;
  saturate)
    [ $# -ge 3 ] || die "saturate NODES RATES REPS [TARGET...]"
    nodes=$1 rates=$2 reps=$3
    shift 3
    targets=${*:-$TARGETS_DEFAULT}
    # shellcheck disable=SC2086
    check_targets $targets
    cpus=$(( $(hssh "$(client_vms | head -1)" nproc < /dev/null) / ((nodes + $(client_count) - 1) / $(client_count)) ))
    [ "$cpus" -ge 1 ] || cpus=1
    mkdir -p "$OUT"
    for rep in $(seq 1 "$reps"); do
      # shellcheck disable=SC2086
      for target in $(printf '%s\n' $targets | sort -R); do
        for rate in $(printf '%s\n' ${rates//,/ } | sort -n); do
          name=sat-$(echo "$target" | tr ':' '-')-n$nodes-r$rate-$EVENTS-$rep
          if [ ! -s "$OUT/$name/accounting.json" ]; then
            say "$name"
            point "$target" "$nodes" "$rate" "$name" || say "$name FAILED"
          fi
          reason=$(classify "$name" "$cpus")
          if [ "$reason" = late-start ]; then
            say "$name: nodes started late, running the point again"
            rm -rf "$OUT/$name.late"
            mv "$OUT/$name" "$OUT/$name.late"
            point "$target" "$nodes" "$rate" "$name" || say "$name FAILED"
            reason=$(classify "$name" "$cpus")
          fi
          echo "{\"target\":\"$target\",\"nodes\":$nodes,\"rate\":$rate,\"rep\":$rep,\"name\":\"$name\",\"result\":\"$reason\"}" >> "$OUT/saturation.jsonl"
          say "$name: $reason"
          [ "$reason" = ok ] || break
        done
      done
    done
    ;;
  collect)
    mkdir -p "$OUT"
    cp "$HOSTS" "$OUT/hosts.json"
    [ ! -d "$STATE/bin" ] || cp "$STATE/bin/sha256.txt" "$OUT/bin-sha256.txt"
    say "collected into $OUT"
    ;;
  table)
    python3 -I "$HERE/stream_table.py" "$OUT"
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
