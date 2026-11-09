#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
set -euo pipefail
exec < /dev/null

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HARNESS=$(cd "$HERE/.." && pwd)
WORK=$(mktemp -d "${TMPDIR:-/tmp}/keep-harness-test.XXXXXX")
trap '[ -z "${KEEP_TEST_LOG:-}" ] || cp "$FAKE_LOG" "$KEEP_TEST_LOG"; rm -rf "$WORK"' EXIT
export HARNESS_WORK=$WORK/work HARNESS_SSH_KEY=$WORK/key FAKE_LOG=$WORK/ssh.log FAKE_EMPTY=$WORK/empty FAKE_BIN=$WORK/bin
export PATH="$HERE/stubs:$PATH" HARNESS_POLL=0
mkdir -p "$FAKE_EMPTY" "$FAKE_BIN"
echo '{"points": [{"point": 0, "completed": 10, "completed_rate": 1.0, "meas_start_unix_ns": 1, "meas_end_unix_ns": 2}], "args": []}' > "$FAKE_EMPTY/summary.json"
touch "$FAKE_BIN/keep" "$WORK/key" "$WORK/key.pub"
pass=0
fail=0

check() {
  local name=$1
  shift
  if "$@" > "$WORK/out.txt" 2>&1; then
    pass=$((pass + 1))
  else
    fail=$((fail + 1))
    echo "FAIL $name"
    tail -n 20 "$WORK/out.txt"
  fi
}

logged() {
  grep -qF -- "$1" "$FAKE_LOG"
}

for topo in "$HARNESS"/infra/topologies/*.json; do
  name=$(basename "$topo" .json)
  check "bash -n" bash -n "$HARNESS/infra/azure/up.sh"
  check "up $name" env HARNESS_DRY_RUN=1 "$HARNESS/infra/azure/up.sh" "$topo" --run "t-$name" --location main=westus
  check "inventory $name" jq -e --slurpfile t "$topo" '[.hosts[].name] == [$t[0].vms[].name] and (.hosts | all(.private_ip and .public_ip))' "$HARNESS_WORK/t-$name/hosts.json"
  check "down $name" env HARNESS_DRY_RUN=1 "$HARNESS/infra/azure/down.sh" "$topo" --run "t-$name"
done
check "up without location" bash -c "! env HARNESS_DRY_RUN=1 '$HARNESS/infra/azure/up.sh' '$HARNESS/infra/topologies/kv.json' --run t-x"
check "arm64 image" bash -c "env HARNESS_DRY_RUN=1 '$HARNESS/infra/azure/vms.sh' create '$HERE/arm.json' --run t-arm --location main=westus 2>&1 | grep -q 'debian-13:13-arm64'"

kv=$HARNESS_WORK/t-smoke-kv/hosts.json
check "setup kv" "$HARNESS/setup.sh" "$kv"
check "setup ran os-tuning" logged "provision-os-tuning  --sudo  -- bash /opt/keep-harness/provision/os-tuning.sh"
check "setup ran competitors on srv" grep -q "^keep@192.0.2.4"$'\t'".*provision-competitors" "$FAKE_LOG"
check "setup skipped toolchain on srv" bash -c "! grep -q '^keep@192.0.2.4.*provision-toolchain' '$FAKE_LOG'"
check "build kv" "$HARNESS/build.sh" "$kv" kv --source "$HARNESS/.."
check "build ran keep alone" logged "cd\\ ~/keep\\ \\&\\&\\ CARGO_TARGET_DIR=target/server\\ cargo\\ build\\ --release\\ --locked\\ -p\\ keep-server"
check "build ran netprobe" logged "CARGO_TARGET_DIR=target/server\\ cargo\\ build\\ --release\\ --locked\\ -p\\ netprobe"
check "build ran loadgen with keep_bench" logged "CARGO_TARGET_DIR=target/bench\\ cargo\\ build\\ --release\\ --locked\\ --config\\ ../keep-keep_bench.toml\\ -p\\ loadgen"
check "build set no RUSTFLAGS" bash -c "! grep -q RUSTFLAGS '$FAKE_LOG'"
check "kv point" "$HARNESS/bench/kv.sh" "$kv" point keep-server keep 4 stalwart-mix wide 20000 smoke-c4-keep-server-keep-wide-r1
check "kv started keep" logged "--shards 4 --cpus 0-3 --pin auto --steering auto --start bootstrap --secret-file /keep-secret --stats-interval 1 --tls off"
check "kv preload" logged "/opt/keep/bin/loadgen run --target 10.0.0.4:7000 --protocol keep --secret-file /opt/keep/keep-secret --select least"
check "kv generator job" logged "kv-smoke-c4-keep-server-keep-wide-r1  --again  -- bash -c"
check "kv valkey point" "$HARNESS/bench/kv.sh" "$kv" point valkey-cluster stalwart 4 stalwart-mix wide 20000 smoke-c4-valkey-cluster-stalwart-wide-r1
check "kv valkey cluster create" logged "valkey-cli --cluster create 10.0.0.4:7000 10.0.0.4:7001 10.0.0.4:7002 10.0.0.4:7003"
check "kv rtt" "$HARNESS/bench/kv.sh" "$kv" rtt
check "kv rtt record" jq -e '.label == "srv>gen1" and .rtt_ns.p50 == 60000' "$HARNESS_WORK/t-smoke-kv/results/rtt-kv.jsonl"
check "kv rtt method" logged "netprobe tcp-ping --target 10.0.0.5:7101 --count 20000 --size 64 --warmup 2000 --label 'srv>gen1'"
check "kv control" "$HARNESS/bench/kv.sh" "$kv" control start
check "kv control record" jq -e 'select(.probe == "tcp-ping") | .label == "kv/start/closed64"' "$HARNESS_WORK/t-smoke-kv/results/controls.jsonl"
check "kv rto" "$HARNESS/bench/kv.sh" "$kv" rto 5000
check "kv rto applied" logged "sudo sysctl -qw net.ipv4.tcp_rto_min_us=5000"
check "kv fill point" env FILL_SRV=1 "$HARNESS/bench/kv.sh" "$kv" point keep-server keep 4 stalwart-mix wide 20000 smoke-c4-keep-server-keep-wide-r2
check "kv fill on srv" grep -q "^keep@192.0.2.4"$'\t'"sudo bash /opt/keep-harness/remote/fill.sh start" "$FAKE_LOG"
check "kv rounds" env ROUND_SEED=7 "$HARNESS/bench/kv.sh" "$kv" rounds 4 stalwart-mix 20000 1 wide keep-server:keep memcached:meta
check "kv rounds plan" grep -q "^seed 7 " "$HARNESS_WORK/t-smoke-kv/results/rounds-plan.txt"
check "kv footprint" env FP_SETTLE=0 "$HARNESS/bench/kv.sh" "$kv" footprint memcached meta 4 footprint fp-c4-memcached-meta-fp-r1
check "kv footprint readings" bash -c "[ \$(grep -c '\"reading\"' '$HARNESS_WORK/t-smoke-kv/results/fp-c4-memcached-meta-fp-r1/footprint.jsonl') = 3 ]"

st=$HARNESS_WORK/t-smoke-stream/hosts.json
stream_mark=$(wc -l < "$FAKE_LOG")
check "stream install" "$HARNESS/bench/stream.sh" "$st" install
check "stream point keep" "$HARNESS/bench/stream.sh" "$st" point keep-server 2 1000 keep-server-n2-r1000-stalwart-1
check "stream url" logged "stream\\ \\'keep://10.0.0.4:7000\\?secret="
check "stream fill" logged "sudo bash /opt/keep-harness/remote/fill.sh start"
check "stream point nats" "$HARNESS/bench/stream.sh" "$st" point nats 2 1000 nats-n2-r1000-stalwart-1
check "stream nats url" logged "stream\\ \\'nats://10.0.0.4:4222\\'"
check "stream bad target" bash -c "! '$HARNESS/bench/stream.sh' '$st' matrix 2 1000 1 keep:first"
check "stream no fill by default" bash -c "! tail -n +$((stream_mark + 1)) '$FAKE_LOG' | grep -q 'fill.sh start'"
check "stream rtt" bash -c "'$HARNESS/bench/stream.sh' '$st' rtt 2>&1 | grep -q 'WARNING above 80 us'"
check "stream rtt file" test -s "$HARNESS_WORK/t-smoke-stream/results/rtt-stream.jsonl"

jp=$HARNESS_WORK/t-smoke-jepsen/hosts.json
check "setup jepsen" "$HARNESS/setup.sh" "$jp"
check "jepsen node got the key" logged "provision-jepsen-node  --sudo  -- bash /opt/keep-harness/provision/jepsen-node.sh ssh-rsa\\ AAAAfake\\ root@ctl"
check "jepsen install" "$HARNESS/jepsen/jepsen.sh" "$jp" install
check "jepsen nodes" logged "sudo tee /root/nodes.txt"
check "jepsen series" "$HARNESS/jepsen/jepsen.sh" "$jp" series smoke
check "jepsen series job" logged "jepsen-smoke-  --again --sudo  -- bash /root/jepsen/series.sh smoke"
check "series presets" bash -c "for id in lock-kill-pause-partition-clock-unrouted register-leave failover-one-way-packet-unrouted lease-packet-bridge readback-heal-restart-unrouted tls-readback-heal-restart arm-readback-heal-restart smoke-lock-kill; do grep -q \"^    \$id) echo\" '$HARNESS/jepsen/series.sh' || exit 1; done"
check "series tls presets" bash -c "[ \$(grep -c ' rpk --workload' '$HARNESS/jepsen/series.sh') = 8 ]"
check "series names are descriptive" bash -c "! grep -qE '^    [A-Z][0-9]+b?\\)' '$HARNESS/jepsen/series.sh'"

for script in "$HARNESS"/*.sh "$HARNESS"/infra/azure/*.sh "$HARNESS"/bench/*.sh "$HARNESS"/jepsen/*.sh "$HARNESS"/provision/*.sh "$HARNESS"/remote/*.sh; do
  check "bash -n $script" bash -n "$script"
done
if command -v shellcheck > /dev/null; then
  check shellcheck shellcheck -S warning -x "$HARNESS"/*.sh "$HARNESS"/infra/azure/*.sh "$HARNESS"/bench/*.sh "$HARNESS"/jepsen/*.sh "$HARNESS"/provision/*.sh "$HARNESS"/remote/*.sh
fi
echo "harness test: $pass passed, $fail failed"
[ "$fail" = 0 ]
