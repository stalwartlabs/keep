# Keep benchmark and Jepsen harness

Scripts that run Keep against its competitors (KV: Valkey, Redis, Dragonfly, Garnet, Pogocache, memcached; stream: NATS, Kafka, Fluvio, Zenoh, Valkey and Dragonfly pub/sub) and the Keep Jepsen suite, on Azure VMs you create with your own `az login`. The benchmark load is a mail-server cluster workload modelled on Stalwart; `mock-stalwart` is one simulated node of such a cluster. Nothing here holds credentials: the scripts use your Azure CLI session and an SSH key they generate under `harness/work/keys/`.

## Requirements

On the operator machine: bash, `az` (logged in, subscription selected), `jq`, `ssh`, `rsync`, `python3`, `git`, and Rust to build the `analysis` binary for the KV tables (`cargo build --release -p analysis`). The VMs run Debian 13 (x86_64 by default; arm64 sizes such as `Standard_D16ps_v6` get the arm64 image).

## Layout

| Path | Holds |
|---|---|
| `infra/azure/` | `up.sh` and `down.sh` (resource groups, NSG with SSH from your IP only, VNet, proximity placement group, VMs, inventory), `quota.sh`, `vms.sh`, `network.sh`, `inventory.sh` |
| `infra/topologies/` | `kv.json` (latency, throughput and CPU), `kv-memory.json` (memory per key), `stream.json`, `jepsen.json`, and `smoke-*.json` (smallest sizes) |
| `setup.sh` | provisions every VM with the steps its topology lists (`provision/`), rebooting once for `idle=poll` |
| `build.sh` | builds Keep and the harness binaries from this checkout on the topology's build host and installs them on every VM |
| `bench/` | `kv.sh`, `stream.sh`, `servers.sh` (Keep and competitors in Docker), `lab.sh` (RTT matrix, session control, RTO), the table scripts, and the `loadgen`, `mock-stalwart`, `workload` and `analysis` crates |
| `probe/` | `netprobe`: the TCP ping-pong and CPU loop behind the RTT floor and the session control |
| `jepsen/` | the Leiningen project, `run.sh` and `series.sh` (run on the control node), `jepsen.sh` (operator side) and `support/` (the `keep-jepsen` crate: client proxy, TLS keys, key owners, lock checker) |
| `versions.env` | competitor images pinned by tag and digest, the Fluvio CLI, Rust 1.99.1 and the rustup-init checksums |
| `test/test.sh` | dry-run and fake SSH tests of every script |

`HARNESS_WORK` (default `harness/work/`, ignored by git) keeps each run's state: `RUN/hosts.json` (the inventory every script takes), known hosts, the built binaries and `RUN/results/` (override with `HARNESS_OUT`).

## Always tear down

VMs bill until they are deleted. After every run, and after any failure of `up.sh`, `setup.sh`, `build.sh` or a benchmark step, run `down.sh` for that topology and run name. To find leftovers: `az group list --tag keep-harness-run=RUN -o table` (every resource group the harness creates carries that tag).

## Published conditions

| Table | Topology | Harness steps | Condition |
|---|---|---|---|
| Key-value latency | `kv.json`: server F4as_v7, generators F16as_v7 in one placement group | per block: `rto 5000`, then `rounds 4 stalwart:mixed@large 10100,122000 2 wide COMBO...`, plus one `point` per product at 80% of its own throughput limit (from the throughput sweeps); 5 blocks in 5 regions | `idle=poll` on every VM, no fill, 5 ms RTO |
| Throughput | `kv.json` | `point SERVER MAPPING 4 stalwart:mixed@large wide RATES NAME` with rising RATES until p99 crosses 1 ms, every product, 3 blocks; the Valkey cluster comparison three more repetitions with three generators | `idle=poll`, no fill, default RTO |
| CPU per request | `kv.json` | the throughput sweeps: server CPU per op at 122,000 ops/s (`server-cpu.jsonl`) | as throughput |
| Memory per key | `kv-memory.json`: server F16as_v7 | `footprint-matrix 16 footprint 3 COMBO...` | default idle (no `idle=poll`), no fill |
| TLS | `kv.json` | `point` sweeps as throughput with `keep-server keep` and `keep-server tls`: the cost is the drop in the 1 ms crossing rate; the p99 cost comes from the same sweeps' rate points | as throughput |
| Event streaming | `stream.json`: broker F16as_v7, two client VMs F16as_v7 | `matrix 3,8 104,1558,15580 REPS` and `ACK_EVERY=32 VERIFY=header matrix 32 104,1558 REPS` with 3 to 5 REPS, each run twice: at the default RTO, then after `rto 5000` with `HARNESS_OUT` set to a second results directory; Keep at its shipping defaults | `idle=poll`, no fill |
| Footprint in a mail-server cluster | not covered by this harness | 8 and 32 simulated nodes running the mixed peak phase against Keep, Redis, and Redis with NATS | |
| ARM servers | not covered by these topologies | the KV mixed block, the stream grid at 3 and 8 nodes x 104 and 1,558 events/s and the cluster footprint at 8 nodes, on Standard_D4ps_v6 and D16ps_v6 in place of F4as_v7 and F16as_v7 (set the sizes in a copy of the topology; arm64 sizes get the arm64 image) | as the x86 rows |

Each block starts and ends with `control` (below); a block whose two controls differ beyond the noise floor is void. `rto default` restores the 200 ms RTO.

## KV benchmark

```
harness/infra/azure/up.sh harness/infra/topologies/kv.json --run kv1 --location main=swedencentral
harness/setup.sh harness/work/kv1/hosts.json
harness/build.sh harness/work/kv1/hosts.json kv
harness/bench/kv.sh harness/work/kv1/hosts.json install
harness/bench/kv.sh harness/work/kv1/hosts.json rtt
harness/bench/kv.sh harness/work/kv1/hosts.json control start
harness/bench/kv.sh harness/work/kv1/hosts.json rounds 4 stalwart:mixed@large 10100,122000 2 wide keep-server:keep valkey-cluster:stalwart valkey-io:native dragonfly:native garnet:native pogocache:native memcached:meta redis-cluster:stalwart redis-io:native
harness/bench/kv.sh harness/work/kv1/hosts.json control end
harness/bench/kv.sh harness/work/kv1/hosts.json collect
ANALYSIS=target/release/analysis harness/bench/kv.sh harness/work/kv1/hosts.json table
harness/infra/azure/down.sh harness/infra/topologies/kv.json --run kv1
```

`rtt` measures a 64 B TCP ping-pong between every pair of VMs (20,000 samples after 2,000 warmup) and flags each pair above 80 us p50: recreate one of its VMs (`vms.sh delete --only NAME`, then `up.sh` again) before measuring. `rounds` runs every combo once per round in a seeded random order; `matrix` runs every server and op mapping at both connection shapes; `point` runs one sweep; `footprint` one memory sweep. `kv.sh -h` lists every step and setting.

## Stream benchmark

```
harness/infra/azure/up.sh harness/infra/topologies/stream.json --run st1 --location main=swedencentral
harness/setup.sh harness/work/st1/hosts.json
harness/build.sh harness/work/st1/hosts.json stream
harness/bench/stream.sh harness/work/st1/hosts.json install
harness/bench/stream.sh harness/work/st1/hosts.json rtt
harness/bench/stream.sh harness/work/st1/hosts.json control start
harness/bench/stream.sh harness/work/st1/hosts.json matrix 4 1000,5000 3
harness/bench/stream.sh harness/work/st1/hosts.json control end
harness/bench/stream.sh harness/work/st1/hosts.json table
harness/infra/azure/down.sh harness/infra/topologies/stream.json --run st1
```

Each point starts the broker, then NODES mock-stalwart processes over the client VMs; every node publishes at the given rate and receives every other node's events. `saturate` raises the rate until events are lost, the p99 bound is crossed or the clients are the limit.

## Jepsen

```
harness/infra/azure/up.sh harness/infra/topologies/jepsen.json --run jp1 --location main=swedencentral
harness/setup.sh harness/work/jp1/hosts.json
harness/build.sh harness/work/jp1/hosts.json jepsen
harness/jepsen/jepsen.sh harness/work/jp1/hosts.json install
harness/jepsen/jepsen.sh harness/work/jp1/hosts.json series core
harness/jepsen/jepsen.sh harness/work/jp1/hosts.json fetch
harness/jepsen/jepsen.sh harness/work/jp1/hosts.json finish
harness/infra/azure/down.sh harness/infra/topologies/jepsen.json --run jp1
```

The series are the ones Keep is tested with, each run with its own client route and TLS mode: `core` (locks, registers, failover reads and counters under kill, pause, partition, clock and leave), `network` (one-way cuts, packet faults, bridges, wide targets), `lease`, `readback` (readback under heal-restart), `tls` (mutual TLS with raw public keys), `arm` (the subset for an arm64 cluster) and `smoke` (one lock run with kills for 60 s). Runs are named after their workload and faults, for example `lock-kill-pause-partition-clock-unrouted`; `series.sh -h` explains the names. Each run's Jepsen store, logs and `args.txt` land in `RUN/results/jepsen/RUN-NAME/`; a lock or lease run is valid when `:valid? true` and its `:keep-check` line reads `0 violations`.

## Smoke tests

The `smoke-*.json` topologies run each harness on the smallest VMs, for example `THREADS=8 DURATION=20 kv.sh ... point keep-server keep 4 stalwart-mix wide 20000 smoke-c4-keep-server-keep-wide-r1`.

## Tests

The VM creation scripts (`infra/azure/up.sh`, `down.sh` and the scripts they call) were validated in dry-run mode only; the rest of the pipeline (`setup.sh` with the `idle=poll` reboot, `build.sh`, the KV, stream and Jepsen steps) was smoke-tested on Azure.

`harness/test/test.sh` creates every topology with `HARNESS_DRY_RUN=1` (the az commands are printed, not run) and drives setup, build, the KV and stream steps and the Jepsen steps against a fake `ssh` and `rsync`, then runs `bash -n` and shellcheck over every script.
