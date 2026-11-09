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
THREADS=${THREADS:-16}
IO=${IO:-epoll}
WARMUP=${WARMUP:-10}
SETTLE=${SETTLE:-5}
DURATION=${DURATION:-20}
LEAD=${LEAD:-30}
C_WIDE=${C_WIDE:-25000}
HEADROOM=${HEADROOM:-0.67}
KEEP_ROUTE=${KEEP_ROUTE:-least}
ANALYSIS=${ANALYSIS:-analysis}
FILL_SRV=${FILL_SRV:-0}
RTT_MAX_US=${RTT_MAX_US:-80}
ROUND_SEED=${ROUND_SEED:-}
FP_SETTLE=${FP_SETTLE:-30}
FP_RATE=${FP_RATE:-10100}
FP_CHURN=${FP_CHURN:-120}
FP_KEYS_SCALE=${FP_KEYS_SCALE:-}
KV_OUT=/opt/keep/out/kv
COMBOS_DEFAULT="keep-server:keep valkey-cluster:stalwart valkey-cluster:native valkey-io:stalwart valkey-io:native dragonfly:stalwart dragonfly:native garnet:stalwart garnet:native pogocache:native memcached:meta redis-cluster:stalwart redis-io:native"

usage() {
  cat <<'USAGE'
usage: harness/bench/kv.sh HOSTS.json STEP [ARGS]

Keep against the KV competitors: one server VM (srv) and generator VMs (gen1..genN, role load) running
loadgen. Outputs go to HARNESS_OUT (default: the inventory's directory/results).
env: THREADS (16 per generator) IO (epoll|uring) WARMUP SETTLE DURATION (10, 5, 20 s) LEAD (30 s)
     C_WIDE (25000 ops/s per generator thread) HEADROOM (0.67)
     KEEP_ROUTE (least: unrouted --select least with connections per server core; shards: --route shards
     with connections per shard) ANALYSIS (the analysis binary for `table`)
     FILL_SRV (0; 1 runs a SCHED_IDLE spinner on every srv vCPU during each sweep, for VMs without idle=poll)
     RTT_MAX_US (80: the pair p50 above which `rtt` asks for a reroll) ROUND_SEED (rounds order seed)
     FP_SETTLE (30 s) FP_RATE (10100 ops/s) FP_CHURN (120 s) FP_KEYS_SCALE (empty: the profile's key counts)

  install              selftest of every KV server on srv; versions into OUT/versions.txt
  rtt                  64 B TCP ping-pong (netprobe, 20000 samples after 2000 warmup) between every pair of VMs
                       into OUT/rtt-kv.jsonl, with a WARNING for each pair whose p50 is above RTT_MAX_US (reroll
                       one of its VMs); kv_table.py reports latency as measured and as excess over the
                       generator-to-srv median
  control LABEL        session control (netprobe CPU loop, closed and paced ping-pong, steal, retransmit and
                       drop counters) gen1 to srv into OUT/controls.jsonl; run it at the start and the end of a
                       session and void the session when the two differ beyond the noise floor
  rto US|default       net.ipv4.tcp_rto_min_us on every VM (5000 for the 5 ms RTO rows; default 200000)
  point SERVER MAPPING CORES PROFILE SHAPE RATES NAME
                       one sweep: start SERVER on CORES CPUs of srv, preload PROFILE from gen1, run the
                       RATES (total ops/s, split evenly over the generators used, which start together)
                       with SHAPE wide (32 connections per server core), narrow (4 per core) or x48, split
                       over as many generators as keep THREADS connections each; cluster servers (and
                       keep-server with KEEP_ROUTE=shards) get 32, 4 or 48 per node or shard. Rates above
                       the generator-safe ceiling (threads x HEADROOM x C_WIDE) are not run and go to
                       OUT/shortfall.jsonl. MAPPING is stalwart or native (RESP), meta (memcached), keep or
                       tls (keep-server, tls with mutual TLS on raw public keys). Writes
                       OUT/NAME/{genN,genN.log,server-cpu.jsonl,cgsample.jsonl,server-stats.json}, and
                       for keep-server keep-server.log and arm.json. NAME is TAG-cCORES-SERVER-MAPPING-SHAPE-rREP
  matrix CORES PROFILE RATES REPS [COMBO...]
                       REPS rounds; per round every SERVER:MAPPING combo (default: keep-server:keep and
                       every competitor) and both shapes in random order; done sweeps are skipped
  rounds CORES PROFILE RATES REPS SHAPE COMBO...
                       REPS rounds; in each round every combo runs one sweep of SHAPE over RATES in an order
                       drawn from ROUND_SEED (unset: the last seed in OUT/rounds-plan.txt, else the clock);
                       OUT/rounds-plan.txt records the seed and every round's order
  footprint SERVER MAPPING CORES PROFILE NAME
                       one memory sweep from gen1: start SERVER fresh, read "empty", preload PROFILE, settle
                       FP_SETTLE s, read "loaded" and read back every preloaded key, run PROFILE at FP_RATE
                       for FP_CHURN s, read "churned" and read back again; readings (servers.sh footprint) go to
                       OUT/NAME/footprint.jsonl and the loadgen outputs to OUT/NAME/{preload,check1,churn,check2}
  footprint-matrix CORES PROFILE REPS COMBO...
                       REPS rounds of one footprint sweep per combo in an order drawn from ROUND_SEED; NAME is
                       TAG-cCORES-SERVER-MAPPING-fp-rREP
  collect              copies the inventory into OUT
  table                kv_table.py over OUT (needs ANALYSIS)
USAGE
}

case "${1:-}" in -h|--help|"") usage; [ -n "${1:-}" ]; exit $? ;; esac
load_inventory "$1"
shift
case "$KEEP_ROUTE" in least | shards) ;; *) die "KEEP_ROUTE is shards or least, not $KEEP_ROUTE" ;; esac
servers() { "$SERVERS" "$HOSTS" "$@"; }
gens() { host_names load; }
gen_count() { gens | wc -l | tr -d ' '; }

mapping_args() {
  case "$1" in
    stalwart) echo "--resp-mapping stalwart" ;;
    native) echo "--resp-mapping native" ;;
    meta) echo "--drop-unsupported" ;;
    keep | tls) echo "" ;;
    *) die "unknown mapping $1" ;;
  esac
}

plan_rates() {
  python3 -I - "$@" <<'PY'
import json, sys
rates, used, threads, c_wide, headroom, name = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5]), sys.argv[6]
scale = {"k": 1e3, "K": 1e3, "m": 1e6, "M": 1e6}
total = [float(r[:-1]) * scale[r[-1]] if r[-1] in scale else float(r) for r in rates.split(",")]
ceiling = used * threads * headroom * c_wide
kept = [r for r in total if r <= ceiling]
skipped = [r for r in total if r > ceiling]
print(",".join(str(int(r / used)) for r in kept))
print(json.dumps({"name": name, "ceiling": ceiling, "generators": used, "threads_per_generator": threads,
                  "skipped": [{"rate": r, "client_vcpus_needed": round(r / (headroom * c_wide), 1)} for r in skipped]})
      if skipped else "")
PY
}

arm_record() {
  local mapping=$1 dir=$2
  python3 -I -c '
import json, re, sys
mapping, log = sys.argv[1], open(sys.argv[2], "rb").read()
found = re.search(rb"keep: listening on .*?, pinned (true|false), steering (\w+)", log)
arm = {"mapping": mapping, "pinned": found and found.group(1) == b"true", "steering": found and found.group(2).decode()}
print(json.dumps(arm))
if found is None or arm["pinned"] is not True or arm["steering"] != "Cpu":
    print("WARNING arm", mapping, "start line shows pinned", arm["pinned"], "steering", arm["steering"], file=sys.stderr)
' "$mapping" "$dir/keep-server.log" > "$dir/arm.json"
}

run_gen() {
  local gen=${1%%:*} conns=${1#*:} rc=0
  job_run "$gen" "kv-$NAME" --again -- bash -c "rm -rf $KV_OUT/$NAME/$gen && mkdir -p $KV_OUT/$NAME && $GEN_CMD --connections $conns --out $KV_OUT/$NAME/$gen" < /dev/null || rc=$?
  case "$rc" in
    0) ;;
    2) say "$NAME $gen: loadgen finished with INVALID points (exit 2; kv_table.py reports them)" ;;
    *) say "$NAME $gen FAILED: loadgen exit $rc (see $NAME/$gen.log)"; return 1 ;;
  esac
}

fetch_gen() {
  local gen=$1
  mkdir -p "$OUT/$NAME/$gen"
  hssh "$gen" "$JOB log kv-$NAME 100000" > "$OUT/$NAME/$gen.log" < /dev/null || true
  hssh "$gen" "tar -C $KV_OUT/$NAME/$gen -czf - ." < /dev/null | tar -xzf - -C "$OUT/$NAME/$gen" || true
}

point() {
  local server=$1 mapping=$2 cores=$3 profile=$4 shape=$5 rates=$6 name=$7
  local per_core total used threads plan host_rates shortfall endpoint args start_at i failed=0 per_node=0 tls="" gen
  local used_gens=() items=()
  case "$shape" in
    wide) per_core=32 ;;
    narrow) per_core=4 ;;
    x48) per_core=48 ;;
    *) die "shape is wide, narrow or x48" ;;
  esac
  threads=$THREADS
  case "$server" in
    *-cluster) per_node=1 ;;
    keep-server) [ "$KEEP_ROUTE" != shards ] || per_node=1 ;;
  esac
  if [ "$per_node" = 1 ]; then
    total=$per_core
    used=$(( total / THREADS ))
    if [ "$used" -lt 1 ]; then
      say "$name skipped: $shape on $server leaves fewer than THREADS=$THREADS connections per node or shard"
      return 0
    fi
  else
    total=$(( per_core * cores ))
    used=$(( total / THREADS ))
    [ "$used" -ge 1 ] || { used=1; threads=$total; }
  fi
  [ "$used" -le "$(gen_count)" ] || used=$(gen_count)
  plan=$(plan_rates "$rates" "$used" "$threads" "$C_WIDE" "$HEADROOM" "$name")
  host_rates=$(sed -n 1p <<< "$plan")
  shortfall=$(sed -n 2p <<< "$plan")
  mkdir -p "$OUT"
  [ -z "$shortfall" ] || { echo "$shortfall" >> "$OUT/shortfall.jsonl"; say "$name: rates above the generator-safe ceiling not run: $shortfall"; }
  if [ -z "$host_rates" ]; then
    say "$name skipped: every rate is above the generator-safe ceiling"
    return 0
  fi
  if [ "$mapping" = tls ]; then
    [ "$server" = keep-server ] || die "mapping tls is keep-server only"
    [ "$IO" = epoll ] || die "mapping tls needs IO=epoll (loadgen TLS runs on epoll only)"
    tls=rpk
  fi
  KEEP_ROUTE=$KEEP_ROUTE KEEP_TLS=$tls servers start srv "$server" "$cores"
  if [ "$FILL_SRV" = 1 ]; then
    fill srv start || { say "$name: fill FAILED on srv"; servers stop srv; return 1; }
  fi
  endpoint=$(KEEP_ROUTE=$KEEP_ROUTE KEEP_TLS=$tls servers endpoint srv "$server" "$cores")
  args="$endpoint $(mapping_args "$mapping") --io $IO --profile $profile"
  for gen in $(gens | head -n "$used"); do used_gens+=("$gen"); done
  if [ "$server" = keep-server ]; then
    for gen in "${used_gens[@]}"; do KEEP_TLS=$tls servers secret "$gen" push; done
  fi
  hssh "${used_gens[0]}" "mkdir -p $KV_OUT/$name && $BIN/loadgen run $args --threads 4 --connections 16 --rate 1 --preload --preload-only --out $KV_OUT/$name/preload > $KV_OUT/$name/preload.log 2>&1 || { tail -n 5 $KV_OUT/$name/preload.log; exit 1; }" < /dev/null \
    || { say "$name: preload FAILED"; [ "$FILL_SRV" != 1 ] || fill srv stop; servers stop srv; return 1; }
  servers sample srv start
  start_at=$(( $(date +%s) * 1000 + LEAD * 1000 ))
  NAME=$name
  GEN_CMD="$BIN/loadgen run $args --threads $threads --cpus 0-$((threads - 1)) --rate $host_rates --warmup $WARMUP --settle $SETTLE --duration $DURATION --start-at $start_at --label $name"
  i=0
  for gen in "${used_gens[@]}"; do
    i=$(( i + 1 ))
    items+=("$gen:$(( total / used + (i <= total % used ? 1 : 0) ))")
  done
  on_hosts run_gen "${items[@]}" || failed=1
  servers sample srv stop
  mkdir -p "$OUT/$name"
  servers sample srv collect "$OUT/$name/cgsample.jsonl"
  hssh srv "rm -f /opt/keep/baselines/cgsample.jsonl" < /dev/null
  servers stats srv > "$OUT/$name/server-stats.json"
  if [ "$server" = keep-server ]; then
    servers logs srv > "$OUT/$name/keep-server.log"
    arm_record "$mapping" "$OUT/$name" || say "$name: arm.json FAILED"
  fi
  servers stop srv
  [ "$FILL_SRV" != 1 ] || fill srv stop
  on_hosts fetch_gen "${used_gens[@]}" || true
  local gen_dirs=()
  for gen in "${used_gens[@]}"; do gen_dirs+=("$OUT/$name/$gen"); done
  python3 -I "$HERE/server_cpu.py" --samples "$OUT/$name/cgsample.jsonl" "${gen_dirs[@]}" > "$OUT/$name/server-cpu.jsonl" || failed=1
  say "$name done"
  return "$failed"
}

fp_read() {
  local server=$1 cores=$2 reading=$3 file=$4 line
  line=$(servers footprint srv "$server" "$cores") || { say "footprint $reading: servers.sh footprint FAILED"; return 1; }
  python3 -I -c 'import json, sys, time; d = json.loads(sys.argv[2]); print(json.dumps({"reading": sys.argv[1], "t": round(time.time(), 3), **d}))' "$reading" "$line" >> "$file"
}

fp_loadgen() {
  local name=$1 step=$2 args=$3 invalid=none
  [ "$step" != churn ] || invalid=2
  hssh gen1 "rm -rf $KV_OUT/$name/$step && { $args --out $KV_OUT/$name/$step > $KV_OUT/$name/$step.log 2>&1; rc=\$?; echo \$rc > $KV_OUT/$name/$step.rc; case \$rc in 0) ;; $invalid) test -s $KV_OUT/$name/$step/summary.json || { tail -n 5 $KV_OUT/$name/$step.log; exit 1; } ;; *) tail -n 5 $KV_OUT/$name/$step.log; exit 1 ;; esac; }" < /dev/null
}

footprint() {
  local server=$1 mapping=$2 cores=$3 profile=$4 name=$5
  local conns=$(( 32 * cores )) endpoint args file failed=0 step
  case "$server" in
    *-cluster) conns=32 ;;
    keep-server) [ "$KEEP_ROUTE" != shards ] || conns=32 ;;
  esac
  [ "$mapping" != tls ] || die "footprint runs plaintext only"
  file=$OUT/$name/footprint.jsonl
  [ ! -d "$OUT/$name" ] || mv "$OUT/$name" "$OUT/$name.failed-$(date +%s)"
  mkdir -p "$OUT/$name"
  KEEP_ROUTE=$KEEP_ROUTE servers start srv "$server" "$cores"
  endpoint=$(KEEP_ROUTE=$KEEP_ROUTE servers endpoint srv "$server" "$cores")
  args="$BIN/loadgen run $endpoint $(mapping_args "$mapping") --io $IO --profile $profile${FP_KEYS_SCALE:+ --keys-scale $FP_KEYS_SCALE} --threads $THREADS --cpus 0-$((THREADS - 1)) --connections $conns"
  [ "$server" != keep-server ] || servers secret gen1 push
  hssh gen1 "rm -rf $KV_OUT/$name && mkdir -p $KV_OUT/$name" < /dev/null
  fp_read "$server" "$cores" empty "$file" || failed=1
  fp_loadgen "$name" preload "$args --rate 1 --preload --preload-only" || { say "$name: preload FAILED"; failed=1; }
  if [ "$failed" = 0 ]; then
    sleep "$FP_SETTLE"
    fp_read "$server" "$cores" loaded "$file" || failed=1
    fp_loadgen "$name" check1 "$args --rate 1 --preload-only --preload-check" || { say "$name: check1 FAILED"; failed=1; }
    fp_loadgen "$name" churn "$args --rate $FP_RATE --warmup 0 --settle 0 --duration $FP_CHURN --label $name" || { say "$name: churn FAILED"; failed=1; }
    fp_read "$server" "$cores" churned "$file" || failed=1
    fp_loadgen "$name" check2 "$args --rate 1 --preload-only --preload-check" || { say "$name: check2 FAILED"; failed=1; }
  fi
  servers stats srv > "$OUT/$name/server-stats.json" || true
  [ "$server" != keep-server ] || servers logs srv > "$OUT/$name/keep-server.log" || true
  servers stop srv
  for step in preload check1 churn check2; do
    hssh gen1 "cat $KV_OUT/$name/$step.log 2> /dev/null" < /dev/null > "$OUT/$name/$step.log" || true
    hssh gen1 "cat $KV_OUT/$name/$step.rc 2> /dev/null" < /dev/null > "$OUT/$name/$step.rc" || true
    mkdir -p "$OUT/$name/$step"
    hssh gen1 "test -d $KV_OUT/$name/$step && tar -C $KV_OUT/$name/$step -czf - . || true" < /dev/null | tar -xzf - -C "$OUT/$name/$step" || true
  done
  [ "$failed" = 0 ] && touch "$OUT/$name/done"
  say "$name done (failed=$failed)"
  return "$failed"
}

round_order() {
  python3 -I -c '
import random, sys
seed, reps, combos = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3].split()
rng = random.Random(seed)
for rep in range(1, reps + 1):
    order = list(combos)
    rng.shuffle(order)
    for combo in order:
        print(rep, combo)
' "$@"
}

round_seed() {
  local plan=$1 seed=$ROUND_SEED
  [ -n "$seed" ] || [ ! -f "$plan" ] || seed=$(awk '$1 == "seed" { s = $2 } END { print s }' "$plan")
  echo "${seed:-$(date +%s)}"
}

step=${1:-}
[ -n "$step" ] || { usage >&2; exit 2; }
shift
case "$step" in
  install)
    mkdir -p "$OUT"
    servers versions srv | tee "$OUT/versions.txt"
    servers selftest srv "$(hssh srv nproc < /dev/null)"
    ;;
  rtt)
    mkdir -p "$OUT"
    rtt_matrix "$OUT/rtt-kv.jsonl" "$RTT_MAX_US"
    ;;
  control) session_control "kv/${1:-control}" gen1 srv "$OUT/controls.jsonl" ;;
  rto) set_rto "${1:?rto US (e.g. 5000) or default}" ;;
  point)
    [ $# -ge 7 ] || die "point SERVER MAPPING CORES PROFILE SHAPE RATES NAME"
    point "$@"
    ;;
  matrix)
    [ $# -ge 4 ] || die "matrix CORES PROFILE RATES REPS [COMBO...]"
    cores=$1 profile=$2 rates=$3 reps=$4
    shift 4
    combos=${*:-$COMBOS_DEFAULT}
    tag=$(echo "$profile" | tr ':@' '--')
    for rep in $(seq 1 "$reps"); do
      for item in $(for combo in $combos; do for shape in wide narrow; do echo "$combo:$shape"; done; done | sort -R); do
        IFS=: read -r server mapping shape <<< "$item"
        name=$tag-c$cores-$server-$mapping-$shape-r$rep
        [ -s "$OUT/$name/server-cpu.jsonl" ] && { say "skip $name (done)"; continue; }
        say "$name"
        point "$server" "$mapping" "$cores" "$profile" "$shape" "$rates" "$name" || { say "$name FAILED"; matrix_failed=1; }
      done
    done
    [ "${matrix_failed:-0}" = 0 ] || die "matrix: at least one sweep FAILED"
    ;;
  rounds)
    [ $# -ge 6 ] || die "rounds CORES PROFILE RATES REPS SHAPE COMBO..."
    cores=$1 profile=$2 rates=$3 reps=$4 shape=$5
    shift 5
    combos=$*
    tag=$(echo "$profile" | tr ':@' '--')
    mkdir -p "$OUT"
    seed=$(round_seed "$OUT/rounds-plan.txt")
    order=$(round_order "$seed" "$reps" "$combos")
    { echo "seed $seed cores $cores profile $profile rates $rates reps $reps shape $shape route $KEEP_ROUTE combos $combos"; awk '{ o[$1] = o[$1] " " $2 } END { for (r = 1; r in o; r++) print "round " r ":" o[r] }' <<< "$order"; } >> "$OUT/rounds-plan.txt"
    while read -r rep item <&3; do
      IFS=: read -r server mapping <<< "$item"
      name=$tag-c$cores-$server-$mapping-$shape-r$rep
      [ -s "$OUT/$name/server-cpu.jsonl" ] && { say "skip $name (done)"; continue; }
      say "$name"
      point "$server" "$mapping" "$cores" "$profile" "$shape" "$rates" "$name" || { say "$name FAILED"; matrix_failed=1; }
    done 3<<< "$order"
    [ "${matrix_failed:-0}" = 0 ] || die "rounds: at least one sweep FAILED"
    ;;
  footprint)
    [ $# -ge 5 ] || die "footprint SERVER MAPPING CORES PROFILE NAME"
    footprint "$@"
    ;;
  footprint-matrix)
    [ $# -ge 4 ] || die "footprint-matrix CORES PROFILE REPS COMBO..."
    cores=$1 profile=$2 reps=$3
    shift 3
    combos=$*
    tag=$(echo "$profile" | tr ':@' '--')
    mkdir -p "$OUT"
    seed=$(round_seed "$OUT/footprint-plan.txt")
    echo "seed $seed cores $cores profile $profile reps $reps route $KEEP_ROUTE combos $combos" >> "$OUT/footprint-plan.txt"
    while read -r rep item <&3; do
      IFS=: read -r server mapping <<< "$item"
      name=$tag-c$cores-$server-$mapping-fp-r$rep
      [ -f "$OUT/$name/done" ] && { say "skip $name (done)"; continue; }
      say "$name"
      footprint "$server" "$mapping" "$cores" "$profile" "$name" || { say "$name FAILED"; matrix_failed=1; }
    done 3< <(round_order "$seed" "$reps" "$combos")
    [ "${matrix_failed:-0}" = 0 ] || die "footprint-matrix: at least one sweep FAILED"
    ;;
  collect)
    mkdir -p "$OUT"
    cp "$HOSTS" "$OUT/hosts.json"
    [ ! -d "$STATE/bin" ] || cp "$STATE/bin/sha256.txt" "$OUT/bin-sha256.txt"
    say "collected into $OUT"
    ;;
  table)
    python3 -I "$HERE/kv_table.py" --analysis "$ANALYSIS" "$OUT"
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
