#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../lib/common.sh
. "$HERE/../lib/common.sh"
# shellcheck source=../versions.env
. "$HERE/../versions.env"

BASELINES=/opt/keep/baselines
KEEP_BIN=$BIN
KV_SERVERS="valkey-cluster valkey-io dragonfly garnet pogocache memcached redis-cluster redis-io"
KEEP_SECRET_FILE=/opt/keep/keep-secret
KEEP_TLS_DIR=/opt/keep/keep-tls
KEEP_PEER_PORT=7100
KEEP_STEERING=${KEEP_STEERING:-auto}
KEEP_PIN=${KEEP_PIN:-auto}
KEEP_EXTRA=${KEEP_EXTRA:-}

usage() {
  cat <<'USAGE'
usage: servers.sh HOSTS.json start     VM SERVER CORES
       servers.sh HOSTS.json cluster   VM SERVER CORES PEER...
       servers.sh HOSTS.json endpoint  VM SERVER CORES
       servers.sh HOSTS.json stats     VM
       servers.sh HOSTS.json footprint VM SERVER CORES
       servers.sh HOSTS.json logs      VM
       servers.sh HOSTS.json secret    VM [push|peer]
       servers.sh HOSTS.json stop      VM
       servers.sh HOSTS.json selftest  VM CORES
       servers.sh HOSTS.json sample    VM start|stop|collect [FILE]
       servers.sh HOSTS.json versions  VM

Keep and the competitor servers on a VM, in Docker with host networking, seccomp unconfined (so io_uring
servers can use it) and CPUs 0..CORES-1. Images are pinned in harness/versions.env and pulled by the
competitors provision step.
  start     starts SERVER on CPUs 0..CORES-1 and waits until it answers; KV servers: valkey-cluster,
            valkey-io, dragonfly, garnet, pogocache, memcached, redis-cluster, redis-io; stream servers:
            valkey-pubsub, nats, kafka, zenoh, fluvio (SC and one SPU, the SPU registered with the pinned
            CLI), dragonfly-pubsub; and keep-server (keep from /opt/keep/bin in container keep-server: one
            server, CORES shards on CPUs 0..CORES-1, --pin KEEP_PIN (auto or never), --steering
            KEEP_STEERING (auto), --start bootstrap, the run's 32-hex secret in a mounted file, stats every
            1 s; KEEP_TLS: empty or off adds --tls off, rpk runs mutual TLS with raw public keys made per
            run in the run state (the endpoint and `secret push` then give loadgen the client key and
            server key too); KEEP_ROUTE: least (default) gives loadgen --select least, shards gives it
            --route shards; KEEP_SHARDS, KEEP_CPUS and KEEP_EXTRA override the shards, the CPU list and add
            keep arguments)
  footprint prints one JSON line for the running SERVER: per container the VmRSS sum over every
            process in its cgroup, the cgroup's memory.current and memory.stat anon, the totals, and the
            server's own key count (DBSIZE summed over every RESP port, memcached curr_items, keep-server
            the last `keys N` of its stats lines), null when none answers
  cluster   stops every server container on VM and each PEER VM, then starts SERVER across them on
            CPUs 0..CORES-1 of each and waits until every VM answers: valkey-cluster (CORES
            valkey-server processes per VM, then one `valkey-cli --cluster create` over every endpoint,
            no replicas) or keep-server (one keep per VM, node ids 1.. in the order VM PEER..., --members
            over all of them with peer port 7100, every member --start bootstrap at once, the run's
            secret and peer secret in mounted files, stats every 1 s, then KEEP_EXTRA)
  endpoint  prints the loadgen or mock-stalwart arguments for SERVER (target, protocol, cluster flag)
  stats     prints one JSON line: per server process pid, CPU seconds (utime+stime) and RSS
  secret    prints the run's 32-hex Keep secret; with push, writes it to /opt/keep/keep-secret on VM
            instead (loadgen reads it with --secret-file); with peer, prints the run's peer secret
  logs      prints the output of every server container
  stop      stops every server container
  selftest  starts each KV server in turn on CORES CPUs, checks it answers, stops it
  sample    start: samples the summed CPU (cgroup usage_usec) and memory of every server container
            once a second into /opt/keep/baselines/cgsample.jsonl; stop; collect: copies it to FILE
  versions  prints /opt/keep/baselines/versions.txt plus the sha256 of /opt/keep/bin/keep
USAGE
}

die() { echo "servers.sh: $*" >&2; exit 1; }
case "${KEEP_TLS:-}" in "" | off | rpk) ;; *) die "KEEP_TLS is off or rpk, not $KEEP_TLS" ;; esac
case "${KEEP_ROUTE:-least}" in least | shards) ;; *) die "KEEP_ROUTE is least or shards, not $KEEP_ROUTE" ;; esac
case "$KEEP_PIN" in auto | never) ;; *) die "KEEP_PIN is auto or never, not $KEEP_PIN" ;; esac
say() { echo "[$(date +%H:%M:%S)] $*" >&2; }

case "${1:-}" in -h|--help) usage; exit 0 ;; esac
[ $# -ge 3 ] || { usage >&2; exit 2; }
load_inventory "$1"
action=$2 vm=$3
shift 3
on_at() { hssh "$1" "$2"; }
on() { on_at "$vm" "$1"; }
ip_at() { host_ip "$1"; }
private_ip() { ip_at "$vm"; }

run_secret() {
  local file=$STATE/$1
  [ -s "$file" ] || od -An -tx1 -N16 /dev/urandom | tr -d ' \n' > "$file"
  cat "$file"
}

keep_secret() { run_secret keep-secret; }

fluvio_cli() {
  echo "sudo docker run --rm --network host -v $BASELINES/fluvio:/cli:ro -e HOME=/tmp --entrypoint /bin/sh $FLUVIO -c '/cli/fluvio profile add local 127.0.0.1:9003 local > /dev/null && /cli/fluvio $1'"
}

docker_run() {
  local name=$1 cpus=$2 image=$3
  shift 3
  echo "sudo docker run -d --name keep-$name --network host --security-opt seccomp=unconfined --ulimit nofile=1048576:1048576 --ulimit memlock=-1 --cpuset-cpus $cpus $image $*"
}

io_threads() {
  local cores=$1
  if [ "$cores" -ge 16 ]; then echo 8; elif [ "$cores" -ge 8 ]; then echo 6; else echo $(( cores > 1 ? cores - 1 : 1 )); fi
}

cluster_nodes_cmd() {
  local image=$1 bin=$2 cores=$3 ip=$4 i
  for i in $(seq 0 $(( cores - 1 ))); do
    docker_run "kv$i" "$i" "$image" "$bin --port $((7000 + i)) --bind 0.0.0.0 --protected-mode no --cluster-enabled yes --cluster-config-file /tmp/nodes-$i.conf --cluster-announce-ip $ip --io-threads 1 --save '' --appendonly no"
    echo "&&"
  done
}

cluster_endpoints() {
  local ip=$1 cores=$2 i
  for i in $(seq 0 $(( cores - 1 ))); do printf ' %s:%s' "$ip" $((7000 + i)); done
}

tls_file() {
  local dir=$STATE/keep-tls
  if [ ! -s "$dir/client.pub" ]; then
    mkdir -p "$dir"
    openssl genpkey -algorithm ed25519 -out "$dir/server.key" 2> /dev/null
    openssl pkey -in "$dir/server.key" -pubout -out "$dir/server.pub"
    openssl genpkey -algorithm ed25519 -out "$dir/client.key" 2> /dev/null
    openssl pkey -in "$dir/client.key" -pubout -out "$dir/client.pub"
  fi
  base64 < "$dir/$1" | tr -d '\n'
}

push_tls() {
  local file
  echo "sudo mkdir -p $KEEP_TLS_DIR && sudo chown \$(id -u) $KEEP_TLS_DIR && chmod 700 $KEEP_TLS_DIR"
  for file in "$@"; do echo "&& echo $(tls_file "$file") | base64 -d > $KEEP_TLS_DIR/$file"; done
}

keep_tls_args() {
  case "${KEEP_TLS:-}" in
    "" | off) echo "--tls off" ;;
    rpk) echo "--tls rpk --tls-key $KEEP_TLS_DIR/server.key --tls-clients $KEEP_TLS_DIR/client.pub" ;;
  esac
}

keep_member_cmd() {
  local node=$1 cores=$2 ip=$3 members=$4 last=$(( $2 - 1 ))
  echo "sudo mkdir -p $BASELINES && echo $(keep_secret) | sudo tee $BASELINES/keep-secret > /dev/null && echo $(run_secret keep-peer-secret) | sudo tee $BASELINES/keep-peer-secret > /dev/null &&"
  docker_run server "0-$last" "-v $KEEP_BIN:$KEEP_BIN:ro -v $BASELINES/keep-secret:/keep-secret:ro -v $BASELINES/keep-peer-secret:/keep-peer-secret:ro $DEBIAN" "$KEEP_BIN/keep --listen 0.0.0.0:7000 --advertise $ip:7000 --node $node --members $members --steering $KEEP_STEERING --start bootstrap --secret-file /keep-secret --peer-secret-file /keep-peer-secret --stats-interval 1 --tls off${KEEP_EXTRA:+ $KEEP_EXTRA}"
}

start_cluster() {
  local server=$1 cores=$2 vms=("${@:3}") target ip node=0 members="" endpoints="" pids=() pid failed=0
  [ -z "${KEEP_TLS:-}" ] || die "KEEP_TLS applies to the single keep-server only"
  for target in "${vms[@]}"; do
    ip=$(ip_at "$target")
    [ -n "$ip" ] && [ "$ip" != null ] || die "no private IP for $target"
    node=$(( node + 1 ))
    members="$members$node@$ip:7000/$ip:$KEEP_PEER_PORT/$cores,"
    endpoints="$endpoints$(cluster_endpoints "$ip" "$cores")"
  done
  members=${members%,}
  for target in "${vms[@]}"; do on_at "$target" "sudo docker ps -aq --filter name=keep- | xargs -r sudo docker rm -f > /dev/null" & pids+=($!); done
  for pid in "${pids[@]}"; do wait "$pid" || failed=1; done
  [ "$failed" = 0 ] || die "stopping the containers failed on ${vms[*]}"
  [ "$server" != keep-server ] || { keep_secret > /dev/null; run_secret keep-peer-secret > /dev/null; }
  say "starting a $server cluster over ${vms[*]}, $cores CPUs each"
  pids=()
  node=0
  for target in "${vms[@]}"; do
    node=$(( node + 1 ))
    ip=$(ip_at "$target")
    case "$server" in
      valkey-cluster) on_at "$target" "$(cluster_nodes_cmd "$VALKEY" valkey-server "$cores" "$ip" | tr '\n' ' ') for p in \$(seq 7000 $(( 7000 + cores - 1 ))); do for i in \$(seq 1 60); do sudo docker exec keep-kv0 valkey-cli -p \$p ping 2>/dev/null | grep -q PONG && break; [ \$i = 60 ] && exit 1; sleep 1; done; done" > /dev/null & ;;
      keep-server) on_at "$target" "$(keep_member_cmd "$node" "$cores" "$ip" "$members" | tr '\n' ' ')" > /dev/null & ;;
      *) die "cluster runs valkey-cluster or keep-server, not $server" ;;
    esac
    pids+=($!)
  done
  for pid in "${pids[@]}"; do wait "$pid" || failed=1; done
  [ "$failed" = 0 ] || die "$server did not start on every VM of ${vms[*]}"
  if [ "$server" = valkey-cluster ]; then
    on_at "${vms[0]}" "sudo docker exec keep-kv0 valkey-cli --cluster create$endpoints --cluster-replicas 0 --cluster-yes > /dev/null" || die "valkey-cli --cluster create failed"
  fi
  for target in "${vms[@]}"; do
    if ! on_at "$target" "$(wait_cmd "$server" "$cores")"; then
      on_at "$target" "sudo docker ps -a --filter name=keep- --format '{{.Names}} {{.Status}}'; for c in \$(sudo docker ps -aq --filter name=keep-); do sudo docker logs --tail 20 \$c 2>&1; done" >&2
      die "$server did not answer on $target"
    fi
  done
  say "$server cluster ready over ${vms[*]}: members $members"
}

start_cmd() {
  local server=$1 cores=$2 ip=$3
  local last=$(( cores - 1 ))
  case "$server" in
    valkey-cluster | redis-cluster)
      local image=$VALKEY bin=valkey-server cli=valkey-cli
      if [ "$server" = redis-cluster ]; then image=$REDIS bin=redis-server cli=redis-cli; fi
      cluster_nodes_cmd "$image" "$bin" "$cores" "$ip"
      echo "sleep 2 && sudo docker exec keep-kv0 $cli --cluster create$(cluster_endpoints "$ip" "$cores") --cluster-replicas 0 --cluster-yes > /dev/null"
      ;;
    valkey-io | valkey-pubsub)
      docker_run kv "0-$last" "$VALKEY" "valkey-server --port 6379 --bind 0.0.0.0 --protected-mode no --io-threads $(io_threads "$cores") --save '' --appendonly no"
      ;;
    redis-io)
      docker_run kv "0-$last" "$REDIS" "redis-server --port 6379 --bind 0.0.0.0 --protected-mode no --io-threads $(io_threads "$cores") --save '' --appendonly no"
      ;;
    dragonfly | dragonfly-pubsub)
      docker_run kv "0-$last" "$DRAGONFLY" "--port 6379 --bind 0.0.0.0 --proactor_threads=$cores --conn_use_incoming_cpu --dbfilename= --logtostderr"
      ;;
    garnet)
      docker_run kv "0-$last" "$GARNET" "--bind 0.0.0.0 --port 6379 --lua --index 1g --memory 32g --no-obj"
      ;;
    pogocache)
      docker_run kv "0-$last" "$POGOCACHE" "-h 0.0.0.0 -p 6379 --threads $cores --evict no --maxconns 65536 --uring yes"
      ;;
    memcached)
      docker_run kv "0-$last" "$MEMCACHED" "memcached -p 11211 -U 0 -t $cores -m 32768 -c 65536"
      ;;
    nats)
      docker_run stream "0-$last" "$NATS" "--port 4222 --addr 0.0.0.0"
      ;;
    kafka)
      echo "sudo docker run -d --name keep-stream --network host --cpuset-cpus 0-$last -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://127.0.0.1:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$ip:9092 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS=1000 -e KAFKA_NUM_PARTITIONS=1 $KAFKA"
      ;;
    keep-server)
      local tls_mount=""
      if [ "${KEEP_TLS:-}" = rpk ]; then
        echo "$(push_tls server.key client.pub) &&"
        tls_mount=" -v $KEEP_TLS_DIR:$KEEP_TLS_DIR:ro"
      fi
      echo "sudo mkdir -p $BASELINES && echo $(keep_secret) | sudo tee $BASELINES/keep-secret > /dev/null &&"
      docker_run server "0-$last" "-v $KEEP_BIN:$KEEP_BIN:ro -v $BASELINES/keep-secret:/keep-secret:ro$tls_mount $DEBIAN" "$KEEP_BIN/keep --listen 0.0.0.0:7000 --advertise $ip:7000 --shards ${KEEP_SHARDS:-$cores} --cpus ${KEEP_CPUS:-0-$last} --pin $KEEP_PIN --steering $KEEP_STEERING --start bootstrap --secret-file /keep-secret --stats-interval 1 $(keep_tls_args)${KEEP_EXTRA:+ $KEEP_EXTRA}"
      ;;
    fluvio)
      echo "sudo docker run -d --name keep-stream --network host --cpuset-cpus 0-$last -e ASYNC_GLOBAL_EXECUTOR_THREADS=$cores --entrypoint /fluvio-run $FLUVIO sc --local /fluvio/metadata --bind-public 0.0.0.0:9003 --bind-private 0.0.0.0:9004"
      echo "&& for i in \$(seq 1 60); do timeout 1 bash -c '</dev/tcp/127.0.0.1/9003' 2>/dev/null && break; sleep 1; done"
      echo "&& $(fluvio_cli "cluster spu register --id 5001 -p $ip:9010 -l 127.0.0.1:9010 --private-server 127.0.0.1:9011") > /dev/null"
      echo "&& sudo docker run -d --name keep-spu --network host --cpuset-cpus 0-$last -e ASYNC_GLOBAL_EXECUTOR_THREADS=$cores --entrypoint /fluvio-run $FLUVIO spu -i 5001 -p 0.0.0.0:9010 -v 0.0.0.0:9011 --sc-addr 127.0.0.1:9004 --log-base-dir /fluvio/data"
      ;;
    zenoh)
      echo "sudo docker run -d --name keep-stream --network host --cpuset-cpus 0-$last -e ZENOH_RUNTIME='(rx:(worker_threads:$(( cores / 2 > 1 ? cores / 2 : 1 ))),tx:(worker_threads:$(( cores / 2 > 1 ? cores / 2 : 1 ))),net:(worker_threads:2))' $ZENOH --listen tcp/0.0.0.0:7447"
      ;;
    *) die "unknown server $server" ;;
  esac
}

wait_cmd() {
  local server=$1 cores=${2:-1}
  case "$server" in
    valkey-cluster) echo "for i in \$(seq 1 60); do sudo docker exec keep-kv0 valkey-cli -p 7000 cluster info 2>/dev/null | grep -q 'cluster_state:ok' && exit 0; sleep 1; done; exit 1" ;;
    redis-cluster) echo "for i in \$(seq 1 60); do sudo docker exec keep-kv0 redis-cli -p 7000 cluster info 2>/dev/null | grep -q 'cluster_state:ok' && exit 0; sleep 1; done; exit 1" ;;
    memcached) echo "for i in \$(seq 1 60); do printf 'mn\\r\\n' | timeout 1 nc -q1 127.0.0.1 11211 2>/dev/null | grep -q MN && exit 0; sleep 1; done; exit 1" ;;
    nats) echo "for i in \$(seq 1 60); do timeout 1 bash -c '</dev/tcp/127.0.0.1/4222' 2>/dev/null && exit 0; sleep 1; done; exit 1" ;;
    kafka) echo "for i in \$(seq 1 120); do timeout 1 bash -c '</dev/tcp/127.0.0.1/9092' 2>/dev/null && exit 0; sleep 1; done; exit 1" ;;
    zenoh) echo "for i in \$(seq 1 60); do timeout 1 bash -c '</dev/tcp/127.0.0.1/7447' 2>/dev/null && exit 0; sleep 1; done; exit 1" ;;
    fluvio) echo "for i in \$(seq 1 60); do $(fluvio_cli "cluster spu list") 2>/dev/null | grep -q Online && exit 0; sleep 1; done; exit 1" ;;
    keep-server) echo "test -x $KEEP_BIN/keep || { echo 'no $KEEP_BIN/keep: run harness/build.sh'; exit 1; }; for i in \$(seq 1 60); do timeout 1 bash -c '</dev/tcp/127.0.0.1/7000' 2>/dev/null && exit 0; sleep 1; done; exit 1" ;;
    *) echo "for i in \$(seq 1 60); do printf 'PING\\r\\n' | timeout 1 nc -q1 127.0.0.1 6379 2>/dev/null | grep -q PONG && exit 0; sleep 1; done; exit 1" ;;
  esac
}

endpoint() {
  local server=$1 ip=$2 cores=${3:-}
  case "$server" in
    valkey-cluster | redis-cluster) echo "--target $ip:7000 --protocol resp --cluster" ;;
    memcached) echo "--target $ip:11211 --protocol meta" ;;
    pogocache) echo "--target $ip:6379 --protocol resp" ;;
    valkey-pubsub) echo "--stream redis://$ip:6379" ;;
    dragonfly-pubsub) echo "--stream redis-sharded://$ip:6379" ;;
    nats) echo "--stream nats://$ip:4222" ;;
    kafka) echo "--stream kafka://$ip:9092 --kafka-consumer assign" ;;
    zenoh) echo "--stream zenoh-client://tcp/$ip:7447" ;;
    fluvio) echo "--stream fluvio://$ip:9003" ;;
    keep-server)
      local tls="" route="--select least"
      [ "${KEEP_TLS:-}" != rpk ] || tls=" --tls rpk --tls-key $KEEP_TLS_DIR/client.key --tls-servers $KEEP_TLS_DIR/server.pub"
      [ "${KEEP_ROUTE:-least}" != shards ] || route="--route shards"
      echo "--target $ip:7000 --protocol keep --secret-file $KEEP_SECRET_FILE $route$tls"
      ;;
    *) echo "--target $ip:6379 --protocol resp" ;;
  esac
}

stop_all() {
  on "sudo docker ps -aq --filter name=keep- | xargs -r sudo docker rm -f > /dev/null"
}

case "$action" in
  versions)
    on "cat $BASELINES/versions.txt; sha256sum $KEEP_BIN/keep"
    ;;
  start)
    [ $# -ge 2 ] || die "start needs SERVER CORES"
    server=$1 cores=$2
    stop_all
    say "starting $server on $cores CPUs of $vm"
    on "$(start_cmd "$server" "$cores" "$(private_ip)" | tr '\n' ' ')" > /dev/null
    if ! on "$(wait_cmd "$server" "$cores")"; then
      on "sudo docker ps -a --filter name=keep- --format '{{.Names}} {{.Status}}'; for c in \$(sudo docker ps -aq --filter name=keep-); do sudo docker logs --tail 20 \$c 2>&1; done" >&2
      die "$server did not answer"
    fi
    say "$server ready: $(endpoint "$server" "$(private_ip)" "$cores")"
    ;;
  cluster)
    [ $# -ge 3 ] || die "cluster needs SERVER CORES PEER..."
    start_cluster "$1" "$2" "$vm" "${@:3}"
    ;;
  endpoint)
    [ $# -ge 1 ] || die "endpoint needs SERVER"
    endpoint "$1" "$(private_ip)" "${2:-}"
    ;;
  stats)
    tck=$(on 'getconf CLK_TCK' < /dev/null)
    pg=$(on 'getconf PAGESIZE' < /dev/null)
    on "for c in \$(sudo docker ps -q --filter name=keep-); do pid=\$(sudo docker inspect --format '{{.State.Pid}}' \$c); for p in \$pid \$(pgrep -P \$pid); do read -r -a f < /proc/\$p/stat; echo \"\$c \$p \${f[13]} \${f[14]} \${f[23]}\"; done; done" \
      | awk -v tck="$tck" -v pg="$pg" 'BEGIN { printf "{\"processes\":[" } { printf "%s{\"container\":\"%s\",\"pid\":%s,\"cpu_s\":%.2f,\"rss_bytes\":%d}", (NR > 1 ? "," : ""), $1, $2, ($3 + $4) / tck, $5 * pg } END { print "]}" }'
    ;;
  secret)
    if [ "${1:-}" = peer ]; then
      run_secret keep-peer-secret
    elif [ "${1:-}" = push ]; then
      on "umask 077 && echo $(keep_secret) > $KEEP_SECRET_FILE"
      [ "${KEEP_TLS:-}" != rpk ] || on "$(push_tls client.key server.pub | tr '\n' ' ')"
    else
      keep_secret
    fi
    ;;
  logs)
    on "for c in \$(sudo docker ps -aq --filter name=keep-); do sudo docker logs \$c 2>&1; done"
    ;;
  stop)
    stop_all
    ;;
  footprint)
    [ $# -ge 2 ] || die "footprint needs SERVER CORES"
    server=$1 cores=$2 expected=1
    case "$server" in
      valkey-cluster | redis-cluster) expected=$cores keys="for p in \$(seq 7000 $(( 7000 + cores - 1 ))); do echo \"keys \$p \$(printf 'DBSIZE\\r\\n' | timeout 5 nc -q1 127.0.0.1 \$p | tr -d '\\r')\"; done" ;;
      memcached) keys="echo \"keys 11211 :\$(printf 'stats\\r\\nquit\\r\\n' | timeout 5 nc -q1 127.0.0.1 11211 | tr -d '\\r' | awk '\$2 == \"curr_items\" { print \$3 }')\"" ;;
      keep-server) keys="echo \"keys log :\$(docker logs keep-server 2>&1 | grep -o ' keys [0-9]*' | tail -n 1 | awk '{ print \$2 }')\"" ;;
      *) keys="echo \"keys 6379 \$(printf 'DBSIZE\\r\\n' | timeout 5 nc -q1 127.0.0.1 6379 | tr -d '\\r')\"" ;;
    esac
    mem="for c in \$(docker ps -q --filter name=keep-); do id=\$(docker inspect --format '{{.Id}}' \$c); cg=/sys/fs/cgroup/system.slice/docker-\$id.scope; rss=0; for p in \$(cat \$cg/cgroup.procs); do r=\$(awk '\$1 == \"VmRSS:\" { print \$2 }' /proc/\$p/status 2> /dev/null); rss=\$(( rss + \${r:-0} )); done; echo \"mem \$(docker inspect --format '{{.Name}}' \$c | tr -d /) \$rss \$(cat \$cg/memory.current) \$(awk '\$1 == \"anon\" { print \$2 }' \$cg/memory.stat)\"; done"
    script=$(printf '%s\n%s\n' "$mem" "$keys" | base64 | tr -d '\n')
    on "echo $script | base64 -d | sudo bash" | python3 -I -c '
import json, sys
expected = int(sys.argv[1])
containers, keys, raw = [], 0, {}
for line in sys.stdin:
    f = line.split()
    if f[:1] == ["mem"]:
        if len(f) != 5 or not all(x.isdigit() for x in f[2:]) or int(f[2]) == 0:
            sys.exit(f"footprint: bad memory reading {line.strip()!r}")
        containers.append({"name": f[1], "vmrss_bytes": int(f[2]) * 1024, "memory_current": int(f[3]), "anon": int(f[4])})
    elif f[:1] == ["keys"] and len(f) >= 2:
        value = f[2] if len(f) > 2 else ""
        raw[f[1]] = value
        keys = keys + int(value[1:]) if keys is not None and value[:1] == ":" and value[1:].isdigit() else None
if len(containers) != expected:
    sys.exit(f"footprint: {len(containers)} server containers, expected {expected}")
print(json.dumps({"containers": containers,
                  "vmrss_bytes": sum(c["vmrss_bytes"] for c in containers),
                  "memory_current": sum(c["memory_current"] for c in containers),
                  "anon": sum(c["anon"] for c in containers),
                  "server_keys": keys if raw else None, "server_keys_raw": raw}))
' "$expected"
    ;;
  sample)
    [ $# -ge 1 ] || die "sample needs start, stop or collect"
    case "$1" in
      start)
        on "sudo mkdir -p $BASELINES && sudo chown \$(id -u) $BASELINES && cat > $BASELINES/cgsample.sh" < "$HERE/cgsample.sh"
        on "pkill -f '^bash $BASELINES/cgsample.sh' || true; { setsid nohup bash $BASELINES/cgsample.sh 1 >> $BASELINES/cgsample.jsonl 2> /dev/null < /dev/null & }"
        ;;
      stop) on "pkill -f '^bash $BASELINES/cgsample.sh' || true" ;;
      collect)
        [ $# -ge 2 ] || die "sample collect needs FILE"
        mkdir -p "$(dirname "$2")"
        on "cat $BASELINES/cgsample.jsonl" > "$2"
        ;;
      *) die "sample start, stop or collect" ;;
    esac
    ;;
  selftest)
    [ $# -ge 1 ] || die "selftest needs CORES"
    cores=$1
    for server in $KV_SERVERS; do
      if "$HERE/servers.sh" "$HOSTS" start "$vm" "$server" "$cores"; then
        echo "selftest $server ok"
      else
        echo "selftest $server FAILED"
      fi
    done
    stop_all
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
