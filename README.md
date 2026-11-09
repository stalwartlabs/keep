<p align="center">
    <a href="https://stalw.art">
    <img src="./img/logo-red.svg" height="150">
    </a>
</p>

<h2 align="center">
  Stalwart Keep
</h3>
<h4 align="center">
  Ephemeral key-value store and event stream server for clustered applications
</h3>

<br>

<p align="center">
  <a href="https://github.com/stalwartlabs/keep/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/stalwartlabs/keep/ci.yml?style=flat-square" alt="continuous integration"></a>
  &nbsp;
  <a href="https://www.gnu.org/licenses/agpl-3.0"><img src="https://img.shields.io/badge/License-AGPL_v3-blue.svg?label=license&style=flat-square" alt="License: AGPL v3"></a>
  &nbsp;
  <a href="https://github.com/stalwartlabs/keep/releases"><img src="https://img.shields.io/github/downloads/stalwartlabs/keep/total?style=flat-square" alt="downloads"></a>
</p>

## Features

**Stalwart Keep** is an ephemeral state and cluster messaging server for clustered applications with a thread-per-core, share-nothing design. The nodes of a cluster often need two things besides their data store: a fast store for short-lived state (locks, leases, rate-limit counters, tokens and cached lookups) and a way to tell each other about changes. That commonly takes Redis or Valkey plus an event broker such as NATS, Kafka or Redis pub/sub. Keep does both in one server, with one protocol. It is written in Rust.

- **Ephemeral key-value store**: get, set, delete, counters, prefix deletes and TTLs, with a memory cap and eviction by key class. Nothing is written to disk.
- **Locks and leases** with a stated mutual exclusion guarantee that holds through crashes, restarts, pauses and network partitions (see [Architecture](#architecture)).
- **Event stream**: topics with publish and subscribe, per-subscription sequence numbers and loss detection, so a subscriber learns when events a server took for it were lost. A publisher that must not lose an event silently uses confirmed publish.
- **Clustering**: a static member list, heartbeat failure detection in about 225 ms, slot placement computed by every server and client, and handoff of a leaving server's keys. There is no consensus round on the request path.
- **Speed**: one shard per CPU, in-house event loop, no locks on the request path. 99% of requests finish within 59 microseconds at the typical load of the benchmark workload, round trip included (see [Benchmarks](#benchmarks)).
- **Security**: mutual TLS 1.3 by default, with raw public keys or certificates from a cluster CA, and a separate secret for the peer network.
- **Client**: [`keep-client`](clients/rust), an async Rust client that routes each request to the shard that owns its key.

## Goals

Keep is for clustered applications whose nodes share short-lived state and tell each other about changes, and that can rebuild that state when it is lost:

- **Coordination**: locks and leases that protect a task or a resource across nodes.
- **Short-lived shared state**: rate-limit counters, tokens and cached lookups.
- **Cluster events**: cache invalidations and wake-ups that every node has to hear, with a signal when an event a server accepted may have been missed.

[Stalwart](https://github.com/stalwartlabs/stalwart) mail server clusters are one such use case: queue and task locks, rate-limit counters and cache invalidation events. The [benchmarks](#benchmarks) run a mail-server cluster workload modelled on Stalwart, and [docs/DESIGN.md](docs/DESIGN.md) states what is guaranteed for any application. [docs/PROTOCOL.md](docs/PROTOCOL.md) section 11 shows, as an example integration, how Stalwart's needs map onto Keep.

Keep is not for:

- **Data that has to survive.** Keep is not a general Redis replacement. Nothing is written to disk and there are no replicas: each key lives on one server, and a server that crashes loses its keys. It has no rich data types (lists, hashes, sets, sorted sets), no scripting, no atomic operations across keys, and it does not speak the Redis protocol.
- **Durable messaging.** Keep is not a durable message queue. Topics are not retained or replayable: a subscription sees only the events published while it is live, there are no consumer groups, and delivery is not exactly-once. A subscriber is told when events a server accepted may have been missed, and reloads what it needs from its own source of truth. A plain `publish` is not covered: if it is lost with its connection, nobody is told. Only `publish_confirmed` gets an error back.

### Limits

- **Fan-out only.** Events cannot be shared out among subscribers: each subscription of a topic on a server is sent every event published there, so delivery work grows with the number of subscribers. Servers do not forward events to each other; cluster-wide delivery comes from the client, which by default subscribes on every member.
- **Nothing is persisted.** A cluster restarted as a whole starts empty.
- **Tested cluster sizes.** The consistency suites and Jepsen run clusters of up to five servers. The protocol allows 4,096 members, but larger clusters have not been tested. The key-value and event stream benchmarks measure one Keep server, with up to 32 subscribing nodes.
- **Keys and values.** Keys are at most 255 bytes, values at most 4 MiB by default, and TTLs are whole seconds.
- **Lock keys.** Under the memory cap, only keys whose first byte is 0, 1 or 14 to 17 are safe from eviction, and this set has no setting. Keys used as locks have to start with one of those bytes.
- **Clients.** `keep-client` is written in Rust. Other languages need their own implementation of [the protocol](docs/PROTOCOL.md).

## Install

### Requirements

| | |
|---|---|
| Operating system | Linux, kernel 6.12 or later |
| C library | glibc 2.34 or later (for example Debian 13, Ubuntu 24.04 with a 6.12 or later HWE kernel, RHEL 10, Amazon Linux 2023 with its 6.12 kernel) |
| CPU | x86_64 with the x86-64-v2 level (SSE4.2 and POPCNT: any x86 server CPU since 2009), or aarch64 |
| Clock | a clock source whose raw rate is within 833 ppm of real time; any server or cloud VM meets this |
| Network | the peer port reachable only from the other cluster members |

The complete list of conditions, with the reasons behind each, is in [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) section 1. Section 3 covers host tuning (TCP minimum retransmission timeout, NIC receive ring and IRQ affinity, huge pages, optional spinning with `--spin-us`).

### Binaries and Docker

- Binaries for `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu` are attached to every [release](https://github.com/stalwartlabs/keep/releases), with build attestations and Sigstore signatures.
- Docker images for `linux/amd64` and `linux/arm64`: `docker pull stalwartlabs/keep` (also on `ghcr.io/stalwartlabs/keep`). The image runs `keep` as an unprivileged user (uid 2000), reads its settings from `/etc/keep/keep.toml` (`KEEP_CONFIG`), and exposes the client port 7000 and the peer port 7100 (TCP and UDP). Arguments after the image name are added to the file's settings:

  ```
  docker run -d --name keep --network host --stop-timeout 16 -v /etc/keep:/etc/keep:ro stalwartlabs/keep --node 1
  ```

  Mount the config file, the secret files and the keys (readable by uid 2000) under `/etc/keep`. Cluster members address each other by IP, so they use `--network host`. A member leaves the cluster on SIGTERM and hands its keys over, which can take up to 16 s: give `docker run` `--stop-timeout 16` (Compose: `stop_grace_period: 16s`), since Docker's default of 10 s kills the server during the handoff.

### From source

```
cargo build --release -p keep-server
```

The binary is `target/release/keep`. The server runs on Linux only. The `Dockerfile`'s release build refuses any binary that needs a glibc newer than 2.34, so release binaries run on the systems above.

## Running

### Single server

Create a key pair for the server and one for each client node (Ed25519; one file of client public keys per server):

```
openssl genpkey -algorithm ed25519 -out keep.key
openssl pkey -in keep.key -pubout -out keep.pub
openssl genpkey -algorithm ed25519 -out client.key
openssl pkey -in client.key -pubout -out clients.pem
openssl rand -hex 16 > cluster.secret
```

Put the settings in a file:

```toml
# /etc/keep/keep.toml
listen = "0.0.0.0:7000"
secret-file = "/etc/keep/cluster.secret"
tls-key = "/etc/keep/keep.key"
tls-clients = "/etc/keep/clients.pem"
```

Start it the very first time with `--start bootstrap`, and every later time without it (the default `rejoin`, see [Restarts](#restarts-and-the-quarantine)):

```
keep --config /etc/keep/keep.toml --start bootstrap
keep --config /etc/keep/keep.toml
```

The same settings also work as flags only (`keep --listen 0.0.0.0:7000 --secret-file cluster.secret --tls-key keep.key --tls-clients clients.pem`). A client connects with `keep://10.0.0.1:7000?secret=HEX&tls=rpk&key=client.key&servers=keep.pub`.

### Cluster

Every member gets the same member list, cluster secret and peer secret, and its own node id:

```toml
members = [
  "1@10.0.0.1:7000/7100/4",
  "2@10.0.0.2:7000/7100/4",
  "3@10.0.0.3:7000/7100/4",
]
secret-file = "/etc/keep/cluster.secret"
peer-secret-file = "/etc/keep/peer.secret"
tls-key = "/etc/keep/keep.key"
tls-clients = "/etc/keep/clients.pem"
tls-servers = "/etc/keep/servers.pem"
ready-file = "/run/keep/settled"
```

```
keep --config /etc/keep/keep.toml --node 1
```

Each entry is `node@client_address/peer_address_or_port/shards`. `servers.pem` holds every member's public key and is also the clients' `servers=` file. Clients list one or more members as seeds and add `cluster=on`. A [Kubernetes StatefulSet](docs/DEPLOYMENT.md#41-example-statefulset) example and the rules for rolling restarts and member-list changes are in the deployment guide.

### Settings

Every setting is a command-line flag, a key in the TOML file named by `--config` (the flag name without the dashes) and an environment variable (`KEEP_` and the flag name in upper case, `-` as `_`). The command line wins over the environment, which wins over the file. `keep --help` lists them all; the main ones:

| Flag | Default | |
|---|---|---|
| `--listen` | `0.0.0.0:7000` | client address |
| `--members` | single server | every cluster member, this one included |
| `--node` | random | this member's node id (required in a cluster) |
| `--start` | `rejoin` | `bootstrap` only for a first start, when no client can hold a lock from an earlier run |
| `--secret-file` | `KEEP_SECRET` | the cluster secret, 32 hex digits, shared with clients |
| `--peer-secret-file` | `KEEP_PEER_SECRET` | the peer secret, 32 hex digits, members only |
| `--tls` | `rpk` | `rpk` (raw public keys), `ca` (certificates) or `off` (plaintext with a challenge) |
| `--shards` | the CPU budget | shard count (cpuset and cgroup `cpu.max`) |
| `--memory` | 75% of the memory budget | memory cap in bytes; the budget is the smaller of the cgroup's `memory.max` and the total memory |
| `--max-lock-ttl` | 600 | the longest lock TTL in seconds; it sets the restart quarantine |
| `--ready-file` | none | a file kept present while the member is ready, for a readiness probe |
| `--stats-interval` | 0 | seconds between stats lines on stderr |

Startup refuses unknown keys, invalid values and settings that do not fit together, naming the flag or key. [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) section 2 describes every flag and the rules behind it.

## Architecture

### One server

Keep uses a thread-per-core, share-nothing design. A server is one process with one shard per CPU in its budget (the cpuset and cgroup `cpu.max`). A shard is a thread that owns a part of the keys and everything about them: the hash table, the values, their expiry, its share of the memory cap and the connections it serves. Shards are pinned to their CPUs when the budget is exclusive; a `cpu.max` quota below the CPU set leaves them unpinned. Shards share no key data and take no locks on the steady-state request path: they pass work over single-producer rings and read a few atomics that publish the cluster view and the member's lease. Peer traffic and heartbeats run on a separate control thread.

- **Connections and I/O.** Each shard has its own listener on the shared port (`SO_REUSEPORT`) and its own level-triggered epoll loop, with no async runtime and no helper threads. Where every shard is pinned and owns one network receive queue, a socket filter hands each new connection to the shard on the CPU that took it, so a connection's packets, interrupts and requests stay on one core. Elsewhere connections are spread by hash. In each turn a shard reads up to 64 KiB from every ready connection, executes what it read, and then writes each connection's replies with one `send` (under TLS, sealed in one pass, in records of up to 16 KiB).
- **Requests for other shards.** A key's shard is a function of its hash. A request that arrives on the wrong shard is forwarded over a single-producer ring to the owner and answered on the connection it came in on, so every connection accepts every key. The client computes the same function and normally sends each request straight to the right shard, which saves the hop.
- **Storage.** The table is an open-addressing hash table laid out in cache lines (about 49 bytes per key at 10 million keys, metadata included). Values from 1 KiB go to a value arena of pre-faulted chunks, so large writes and deletes do not pay for mapping and unmapping memory. Keep's own memory regions request huge pages, and the heap uses them with glibc 2.35 or later, or with transparent huge pages set to `always`. Keys belong to key classes, each with its own eviction rule under the memory cap; lock keys are in pinned classes that are never evicted, and values from 8 KiB in the other classes leave first, oldest written first.
- **Protocol.** A small binary frame protocol over TCP (or TLS 1.3 in user space with rustls), with request ids for pipelining and out-of-order replies, explicit `Busy` and `Moved` answers, and server pushes for topology changes and stream events. It is specified in [docs/PROTOCOL.md](docs/PROTOCOL.md) and implemented once, in [`keep-proto`](crates/keep-proto), by both server and client.

### Cluster

A cluster is a fixed list of members. There is no leader and no consensus protocol: every server and every client computes the same placement from the same list.

- **Membership.** Every member sends every other member an authenticated UDP heartbeat every 25 ms and declares a member dead after 225 ms of silence. A member that cannot hear a majority of the list fences itself until it can, so a minority side of a partition never serves the locks it may have lost.
- **Placement.** A key hashes, with the cluster secret as the hash key, to one of 16,384 slots. Each slot belongs to the live member with the highest rendezvous score for it, so when a member joins or leaves only that member's slots move. Clients fetch the topology from the members, follow the view a majority of the list reports, and route each request to the owning member and shard; a server that does not own a key answers `Moved`.
- **Handoff.** When a member leaves (on SIGTERM, or in a rolling restart) the members that gain its slots pull its keys, locks included, over the TCP peer port before they serve them. A member that dies cannot hand anything over, so its slots open on their new owners empty.
- **Streams.** A publish reaches every subscription of its topic on the server it was sent to, across all its shards. Cluster-wide delivery is a client policy: the client publishes and subscribes on the topic's home member, or subscribes everywhere. Each subscription numbers its events, so a gap after a reconnect, a drain or a member change is reported to the caller as possible loss.

### Restarts and the quarantine

Keep keeps nothing on disk, but clients keep acting on the locks they were granted. A slot that changes owner without its keys (a member crashed, or a server restarted) is therefore quarantined: for a little longer than the longest lock TTL plus the longest clock stop (634 s with the defaults), a lock that is absent there cannot be taken, because its last holder may still be working. Ordinary keys are served at once. This is what makes the lock guarantee hold through every crash, restart and partition, and it is why `--start bootstrap` is only for a first start, when no lock can exist yet.

The guarantee itself, for locks, counters, keys and streams, with the assumptions it rests on and what is not claimed, is in [docs/DESIGN.md](docs/DESIGN.md) section 1.

## Benchmarks

We measured Keep against the products a clustered application would otherwise use for these jobs: Redis, Valkey and Dragonfly for key-value work, and Redis or NATS for cluster events. memcached, Pogocache and Garnet were also measured, as reference points. Every product got the same cloud servers, the same network and the same load: a mail-server cluster workload modelled on Stalwart, a mix of small requests (counters, locks, lookups and short-lived values), at a large deployment's typical rate (10,100 requests per second) and its peak (122,000). The runs were repeated across five cloud regions. An application with a different mix of requests should expect different numbers.

Across the 60 headline comparisons, Keep is better in 45, the other product in 5, 8 are about equal, and 2 are not separated by the measurement.

![How many times faster or leaner Keep is, per comparison](docs/benchmarks/factors.svg)

- **Latency.** At the typical load, 99% of Keep's requests finish within 59 microseconds, round trip included. On the slowest 0.1% of requests Keep is 2.0x faster than Valkey cluster, 2.2x faster than Redis cluster and 3.1x faster than Dragonfly; at the peak load it is 2.4x, 2.6x and 4.5x.
- **Throughput.** On one 4-vCPU server, Keep keeps 99% of requests under 1 ms up to at least 500,000 requests per second in the median of three runs. That is 1.39x Valkey cluster, the next best, and at least 2x Redis and Dragonfly.
- **Efficiency.** Keep uses 1.3x to 2.6x less CPU per request than Redis, Valkey and Dragonfly, and 1.4x to 2.1x less memory per key (48.7 bytes per key at 10 million keys).
- **In a mail-server cluster.** With Keep in place of Redis plus NATS, each simulated node uses about 1.2x less CPU and 1.4x less memory at 8 nodes. At 32 nodes Keep still uses less CPU than Redis plus NATS, while Redis on its own uses 1.6x less CPU than Keep.
- **Cluster events.** At 32 nodes the slowest 1% of Keep's event deliveries are 1.6x to 1.7x faster than the next best broker (Dragonfly), at 1.5x less CPU per event. At 3 and 8 nodes Keep is level with NATS on latency, at up to 1.4x less broker CPU per event.
- **TLS** costs Keep 4 to 10% of its throughput.

Keep does not win everywhere. memcached and Pogocache are 2x to 3x faster on the slowest 0.1% of requests on a small server; on a 16-vCPU server the gap shrinks to 1.07x to 1.21x. They are measured as reference points; for this workload, Keep does the job of Redis or Valkey together with an event broker. Redis alone uses less CPU at 32 nodes, as noted above. On ARM servers (Azure Cobalt 100) the results point the same way, except that Valkey's event fan-out uses less CPU than Keep's at 3 and 8 nodes.

Every table, the method, the caveats and how to reproduce the runs are in [BENCHMARK.md](docs/BENCHMARK.md).

## Testing

A store that hands out locks has to be right when things break, not only when they work. Keep is tested at four levels:

- **Unit and integration tests** for every crate, run on x86_64 and aarch64, with and without TLS, about 950 tests in all.
- **A deterministic cluster simulator** that runs the real cluster code on a simulated network and clock, with crashes, restarts, partitions, packet loss, pauses and member changes, and checks the guarantees of [docs/DESIGN.md](docs/DESIGN.md) on every history: 11 suites of 1,000 seeds each, plus randomized fault schedules.
- **A fault suite** that runs real `keep` processes on one Linux host behind a fault-injecting proxy and network namespaces: 39 cases of kills, freezes, partitions, clock faults and member changes, with and without TLS.
- **[Jepsen](https://jepsen.io)**, the standard tool for testing distributed systems under faults: a five-node cluster on real VMs, with process kills, pauses, partitions of every shape, packet loss, clock changes and member leaves, checked for linearizable registers, lock mutual exclusion and lost writes: every release passes 35 runs.

Every bug they catch becomes a regression test. The Jepsen workloads ship in [`harness/jepsen`](harness/jepsen), with the scripts to create the cluster on Azure and run them yourself; [TESTING.md](docs/TESTING.md) describes each level, what it checks and how to run the suite.

## License

The Keep server is dual-licensed under the **GNU Affero General Public License v3.0** (AGPL-3.0; as published by the Free Software Foundation) and the **Stalwart Enterprise License v2 (SELv2)**:

- The [GNU Affero General Public License v3.0](./LICENSES/AGPL-3.0-only.txt) is a free software license that ensures your freedom to use, modify, and distribute the software, with the condition that any modified versions of the software must also be distributed under the same license.
- The [Stalwart Enterprise License v2 (SELv2)](./LICENSES/LicenseRef-SEL.txt) is a proprietary license designed for commercial use. It offers greater flexibility for businesses that do not wish to comply with the AGPL-3.0 license requirements.

The protocol crate [`keep-proto`](crates/keep-proto) and the client [`keep-client`](clients/rust) are licensed under either of the [Apache License 2.0](./LICENSES/Apache-2.0.txt) or the [MIT license](./LICENSES/MIT.txt), at your option.

Each file in this project contains a license notice at the top, indicating the applicable license(s). The license notice follows the [REUSE guidelines](https://reuse.software/) to ensure clarity and consistency. The full text of each license is available in the [LICENSES](./LICENSES/) directory.

## Copyright

Copyright (C) 2020, Stalwart Labs LLC
