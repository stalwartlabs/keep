# Testing Keep

Keep hands out locks that client nodes rely on for mutual exclusion, and it does so while servers crash, restart, pause and lose touch with each other. A test that only shows Keep working on a healthy network says little about that. Most of Keep's testing therefore breaks things on purpose and then checks what clients saw against a written specification.

The specification is [docs/DESIGN.md](DESIGN.md) section 1. It covers the system model (which faults are allowed and which assumptions hold) and one claim per operation:

- **Claim L**: mutual exclusion for locks and leases.
- **The KV and counter claims**: no acknowledged write is lost on a member that stays up, and reads are valid.
- **The stream claims**: in-order delivery and reported loss.

It also lists what Keep does not claim. A check that fails against a claim is a product bug. Behaviour listed as not claimed is not.

## The levels

| Level | Runs | Checks |
|---|---|---|
| Unit and integration tests | every crate, x86_64 and aarch64, plaintext and TLS | functions, wire format, server startup and refusals, client behaviour |
| Model checking (loom) | the hand-off between the server's control thread and its shards after an OS suspend | every interleaving of the two threads |
| Cluster simulator | the real cluster code on a simulated network and clock | the claims on every simulated history |
| Fault suite | real `keep` processes on one Linux host | the claims under real kills, freezes, partitions and clock faults |
| Jepsen | a five-node cluster on real VMs | linearizability, mutual exclusion and lost writes under every nemesis |
| Mutation testing | hand-written mutants of the cluster, server, runtime, client and simulator | that the suites above catch each one |

### Unit and integration tests

Every crate has unit tests. The server has integration tests that start the `keep` binary and connect over the network. They cover TLS in both modes, cluster startup, handoff, streams, startup validation and the configuration file. The client has tests for routing, retries, reconnects and the busy gate (when a server refuses all its requests with `Busy`, for example while it is cut off from the majority, the client holds its calls to that server and sends one probe at a time until the server answers again). The protocol crate has round-trip tests for every frame, and differential tests of its optimised paths against simple reference versions. Every release passes about 950 tests on each of x86_64 and aarch64.

### Cluster simulator

The simulator runs the cluster code that ships, unchanged, against a simulated network and clock. Thousands of runs a minute fit on one machine, and every run replays from its seed. Each suite combines one workload with one fault plan. Every history it produces is checked against the claim the suite tests:

| Suite | Workload and faults |
|---|---|
| `kv`, `kv-failover`, `kv-lost-ack` | reads and writes through crashes, failovers and acknowledgements lost on the way back |
| `locks` | lock and lease traffic through crashes, restarts, partitions and pauses (claim L) |
| `stream` | publish and subscribe through drains and failovers |
| `rolling`, `rolling-hot` | rolling restarts of every member, idle and under load |
| `list-add`, `list-remove`, `list-kv` | member-list changes, alone and under KV traffic |
| `thaw` | members frozen with a stopped clock and resumed |

Every release passes all 11 suites with 1,000 seeds each on both architectures, and 14,000 randomized fault schedules (crashes, leaves, stopped clocks and lost acknowledgements drawn at random). With `--pressure`, any suite also runs with a low memory cap on every shard and a writer of large values on keys of its own, outside the history: the checked keys must hold their claims through the pressure.

### Fault suite

The fault suite runs real `keep` servers as separate processes on one Linux host. Each member sits in its own network namespace behind a fault-injecting proxy. It drives real clients through 39 cases:

- kills of single members, pairs and majorities;
- freezes, including freezes with the clock stopped;
- symmetric, one-way and bridge partitions;
- packet loss;
- clock steps and slews;
- member leaves and rolling restarts.

It checks every history with the simulator's checkers. Every release passes every case with three seeds on x86_64 and one on aarch64, in plaintext and with TLS on both architectures. `--pressure` adds the simulator's low memory cap and writer of large values.

### Jepsen

[Jepsen](https://jepsen.io) is a framework for testing distributed systems under faults. It is the tool behind most published analyses of databases and coordination services. A control node drives clients against a cluster on separate machines while a *nemesis* injects faults. Jepsen records every operation with its start and end time, then checks the whole history against a consistency model.

Keep's Jepsen suite runs a five-node cluster on Azure VMs. Its workloads:

| Workload | Checks |
|---|---|
| `register` | single-key reads and writes are linearizable (Knossos) |
| `register-failover` | reads stay valid while slots move between members |
| `counter` | increments are neither lost nor applied twice |
| `lock` | claim L: no two clients hold one lock at the same time |
| `lease` | claim L for the lease pattern (acquire, renew, release) |
| `readback` | acknowledged writes on surviving members are not lost across heal-and-restart cycles |

Its nemeses, freely combined:

| Nemesis | Fault |
|---|---|
| `kill` | `kill -9` of servers, restarted with the default `rejoin` |
| `pause` | `SIGSTOP` and `SIGCONT` |
| `partition` | majority and minority partitions |
| `bridge` | a member that sees both sides of a partition |
| `one-way` | a one-way partition |
| `packet` | packet loss, delay and reordering |
| `clock` | clock steps and slews |
| `leave` | a member that leaves and rejoins |
| `heal-restart` | heal and restart cycles; used alone, with `readback` |

Every release passes 35 Jepsen runs: x86_64 and aarch64 in plaintext, x86_64 with mutual TLS, and both the routed and the unrouted client, each with 5 to 15 minutes of faults. `series.sh --pressure` runs a series with a low memory cap and a writer of large values beside the workload.

### Mutation testing

A suite that passes proves little if it would also pass with a bug in place. Over 200 hand-written mutants each break one guard, ordering rule or timeout in the cluster, engine, server, runtime, client or simulator code. Each one is run against the suites to show that a test catches it. The few expected survivors are documented with the reason.

## Running the Jepsen suite

The Jepsen workloads and the scripts that create the cluster are in [`harness/`](../harness), with the details in [harness/README.md](../harness/README.md). Every step runs from the operator machine; `jp1` is the run name and `swedencentral` the Azure region.

1. **Prerequisites.**
   - An Azure subscription and the `az` CLI, signed in with your own account (`az login`). No credentials are stored in this repository; the scripts generate their own SSH key under `harness/work/keys/`.
   - bash, `jq`, `ssh`, `rsync`, `python3` and `git` on the operator machine, with this checkout.
2. **Create the cluster.** The `jepsen.json` topology is one control node and five database nodes, on Debian 13:

   ```
   harness/infra/azure/up.sh harness/infra/topologies/jepsen.json --run jp1 --location main=swedencentral
   harness/setup.sh harness/work/jp1/hosts.json
   harness/build.sh harness/work/jp1/hosts.json jepsen
   harness/jepsen/jepsen.sh harness/work/jp1/hosts.json install
   ```

   `up.sh` writes the inventory `harness/work/jp1/hosts.json`. `setup.sh` installs a JDK and Leiningen on the control node and sets up root SSH by key from it to the database nodes. `build.sh` builds `keep` and the `keep-jepsen` client proxy from the checkout on the control node and installs them on every node. `install` copies the Jepsen project to the control node, checks root SSH from it to every database node and resets the control node's clock rate, since the checker takes its timestamps there.
3. **Run.**

   ```
   harness/jepsen/jepsen.sh harness/work/jp1/hosts.json series core
   ```

   The series are the ones every release runs, each run with its own client route and TLS mode: `core` (locks, registers, failover reads and counters under kill, pause, partition, clock and leave), `network` (one-way cuts, packet faults, bridges and wide targets), `lease`, `readback` (readback under heal-restart), `tls` (mutual TLS with raw public keys), `arm` (the subset for an arm64 cluster), and `smoke`, one lock run with kills for 60 s. Runs are named after their workload and faults, for example `lock-kill-pause-partition-clock-unrouted`, and any run can be named alone; `series.sh -h` explains the names. Each run takes its time limit (5 to 15 minutes) plus a few minutes of setup and analysis. For a custom workload and nemesis, run `harness/jepsen/run.sh` in `/root/jepsen` on the control node as root, with `KEEP_TLS=rpk` for mutual TLS and `KEEP_ROUTE=none` for the unrouted client; without arguments it prints its options.
4. **Results.**

   ```
   harness/jepsen/jepsen.sh harness/work/jp1/hosts.json fetch
   ```

   copies every run to `harness/work/jp1/results/jepsen/RUN-NAME/`: Jepsen's store with the history, the checker's verdict and latency plots, the server and proxy logs, and `args.txt` with the run's options. A run passes when Jepsen reports `:valid? true`; for a lock or lease run, the `:keep-check` entry of its results reports the claim L check (`0 violations`).
5. **Clean up.** Restart the control node's clock service, then delete the cluster so the VMs stop costing money:

   ```
   harness/jepsen/jepsen.sh harness/work/jp1/hosts.json finish
   harness/infra/azure/down.sh harness/infra/topologies/jepsen.json --run jp1
   ```

The same harness has one-minute smoke runs of the key-value and stream benchmarks. They use the same prerequisites and are described in [harness/README.md](../harness/README.md).

