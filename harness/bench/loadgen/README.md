# loadgen

Open-loop KV load generator for the Keep benchmarks (harness/bench/kv.sh), and `nullserver`, a canned-reply server used to measure the generator's own ceiling.

## Design

- **Thread per core.** Each worker thread owns its connections, its I/O driver, its random streams and its histograms, from preload to the last point. Threads share nothing on the hot path; per-point results go to the main thread over a channel when a point has drained.
- **Two I/O drivers** (`--io`), a closed enum:
  - `epoll` (default): readiness through `mio` (epoll on Linux, kqueue on macOS), one `write` per flushed connection and `read` until `EAGAIN` per readable one.
  - `uring` (Linux): one io_uring per thread with `SINGLE_ISSUER`, `DEFER_TASKRUN` and `TASKRUN_FLAG`, registered sockets, one multishot receive per connection into a provided buffer ring (4 KiB buffers) and at most one send in flight per connection (requests issued meanwhile are sent together when it completes). A spinning thread enters the kernel only when it has queued submissions or the kernel flags pending completions, so one `io_uring_enter` carries every send and receive of a loop iteration.
- **Open loop.** Each thread draws its own arrival process (Poisson or constant) at `rate / threads`. A request is issued at its intended time whether or not earlier requests have completed, so a slow server builds a queue instead of slowing the generator (no coordinated omission).
- **Many requests in flight per connection.** Requests go round robin over the thread's connections (within the owning node's connections in Redis Cluster mode and the owning shard's with `--route shards`; by key hash when replaying a trace) and are written as soon as they are due. Replies are matched in order for RESP and memcached meta, and by request id for the Keep frame.
- **Latency from the intended send time.** `latency` = reply received minus intended send time. `service` = reply received minus the moment the request was handed to `write`, which is what `memtier_benchmark` and `rpc-perf` report. `lag` = handed to `write` minus intended time.
- **Busy polling.** By default the loop never blocks (`--idle spin`), so a due request is noticed within one loop iteration. Generator load is measured as the fraction of wall time spent in iterations that did work (`util`). `--idle sleep:<us>` blocks in the poller when the next send is further away than the threshold, rounded down to whole milliseconds.
- **Closed protocol set.** `Protocol` is an enum: RESP with Stalwart's mapping or the cheapest native mapping, memcached meta, and the Keep protocol (`keep-proto` frames).

## Build

```
RUSTFLAGS="--cfg keep_bench" cargo build --release -p loadgen
```

loadgen needs `--cfg keep_bench`, which exposes keep-client's TLS configuration to it. `RUSTFLAGS` replaces the rustflags of `.cargo/config.toml`; `harness/build.sh` passes the cfg through a `--config` file instead, which keeps them.

Linux is the measurement platform. On macOS the generator and null server work, but `kevent` with a zero timeout does not keep the worker on a core, so macOS numbers are functional only.

## Running

```
loadgen run --target HOST:PORT [--protocol resp|meta|keep] [options]
loadgen profile stalwart-mix            # print a built-in profile
nullserver --listen 0.0.0.0:7700 --protocol resp --threads 16 --cpus 0-15
```

Common cases:

```
# sweep stalwart-mix over RESP with Stalwart's EVALSHA mapping, 8 pinned threads, 64 connections
loadgen run --target 10.0.0.4:6379 --threads 8 --cpus 0-7 --connections 64 \
    --preload --rate 100k,200k,400k,800k --warmup 30 --settle 5 --duration 60 --out out/valkey

# the cheapest native RESP mapping, Zipf 0.99 keys
loadgen run --target 10.0.0.4:6379 --resp-mapping native --popularity zipf:0.99 --rate 300k --out out/native

# per-core Redis Cluster: discover slots from one node, one connection set per node
loadgen run --target 10.0.0.4:7000 --cluster --connections 2 --threads 8 --rate 500k --out out/cluster

# Keep v0 frame
loadgen run --target 10.0.0.4:7700 --protocol keep --rate 500k --out out/keep

# replay a Stalwart KV trace at its recorded timing, node 1 only, at twice the speed
loadgen run --target 10.0.0.4:7700 --protocol keep --trace <(zstd -dc mixed.tsv.zst) \
    --trace-node 1 --trace-speed 2 --out out/trace

# several generator hosts on one plan: same --start-at on every host, rates are per host
loadgen run ... --rate 400k --start-at $(( ($(date +%s) + 30) * 1000 ))
```

### Options

| Option | Meaning |
|---|---|
| `--target` | Server; with `--cluster`, the seed node for `CLUSTER SLOTS` |
| `--protocol` | `resp`, `meta`, `keep` (keep-proto) |
| `--resp-mapping` | `stalwart` (default): incr with TTL is `EVALSHA` of Stalwart's script, set is `SETEX`, renew is `SET EX`. `native`: `INCRBY` plus `EXPIRE NX` pipelined, `SET EX`, `EXPIRE` |
| `--cluster` | Redis Cluster slot routing (RESP only) |
| `--auth` | RESP `AUTH PASSWORD` or `AUTH USER PASSWORD` |
| `--io` | `epoll` (default) or `uring` (Linux 6.1 or later) |
| `--threads`, `--cpus` | Worker threads and the CPUs they are pinned to (Linux) |
| `--connections` | Connections per host, spread over threads; per node with `--cluster`, per shard with `--route shards` (every thread holds its share to every node or shard, so the total is connections x nodes) |
| `--profile` | Built-in profile (`stalwart-mix`, `getset`) or a TOML file |
| `--popularity` | Override every keyspace: `uniform` or `zipf:<s>` |
| `--keys-scale` | Multiply every keyspace's key count |
| `--rate` | Offered ops/s of this host, one value per load point (`k` and `M` suffixes) |
| `--arrival` | `poisson` (default) or `constant` |
| `--warmup`, `--settle`, `--duration` | Seconds before the first point, before every later point, and measured per point |
| `--min-samples` | Lengthen a point until it holds this many requests (p99.99 needs 10^6) |
| `--drain` | Seconds to wait for late replies after a point; unanswered requests count as timeouts |
| `--preload`, `--preload-depth`, `--preload-only` | Write every key of the keyspaces marked `preload` before measuring; with `--trace`, the profile's preload keyspaces stand in for the keys that existed before the trace window |
| `--start-at` | Unix time in ms at which the plan starts, to align several hosts |
| `--boundary-at` | Place a day boundary (and so a boundary of every window) this many seconds into the first measurement |
| `--trace`, `--trace-node`, `--trace-speed`, `--trace-repeat` | Replay a Stalwart KV trace; with `--rate` the trace is re-timed by the arrival process instead |
| `--trace-salt`, `--trace-salt-prefixes` | XOR a salt into key bytes 1..=8 (only for the listed hex prefixes) to make one trace act as another Stalwart node |
| `--idle` | `spin` (default) or `sleep:<us>` |
| `--max-util`, `--max-lag-p99-us`, `--max-lag-p999-us` | Validity thresholds (defaults 0.70, 10 us and 200 us) |
| `--drop-unsupported` | Drop ops the protocol cannot express (memcached meta and `delete_prefix`, Keep keys over 255 B) instead of failing |
| `--out`, `--label`, `--seed` | Output directory, free text, random seed |

## Workload profiles

A profile is a list of keyspaces and weighted ops on them. `stalwart-mix` is a small built-in mix (70% incr with TTL, 10% get/exists, 8% try_lock, 8% unlock, 3% set, 1% renew, 0.1% of ops on 1 KiB to 1 MiB values); key counts are placeholders; the `stalwart:` profiles of the workload crate replace it. `getset` is plain 1:1 GET/SET for cross-checks.

```toml
name = "example"

[[keyspace]]
name = "rate_60s"
key_size = [[13, 0.7], [25, 0.3]]   # or a single size
keys = 1000000
popularity = "zipf:0.99"            # or "uniform" (default)
ttl = "window:60"                   # "none", "<seconds>", or "window:<seconds>"
value_size = 8                      # or [[0, 0.5], [8, 0.5]] or "log-uniform:1024:1048576"
preload = "incr"                    # "none" (default), "set" or "incr"

[[op]]
kind = "incr"                       # get exists set incr try_lock unlock renew delete delete_prefix
keyspace = "rate_60s"
weight = 70
delta = 1
key = "random"                      # or "held": unlock pops a key this thread locked, renew reuses the latest
```

Keys are printable, fixed size per key index, and distinct per keyspace (one tag byte per keyspace). A `window:<p>` keyspace puts the window index in the key and sets the TTL to the end of the window, as Stalwart's rate limiters do, so keys roll over at window boundaries. Window time is a virtual Unix clock: the wall clock at startup (or a value chosen by `--boundary-at`) plus the time since the plan started. Preload uses the clock at the plan start, so it writes the first window's keys.

## Trace replay

The trace format is plain TSV:

```
t_ns	node	op	key_hex	value_len	ttl_s	delta
```

`#` lines are comments. The whole trace is loaded and split over threads by key hash, so every operation on a key goes through the same thread and connection in recorded order. Values are synthetic bytes of `value_len`. Records the chosen protocol cannot express are rejected at load time (or dropped with `--drop-unsupported`). Compressed traces are read through process substitution: `--trace <(zstd -dc file.tsv.zst)`.

## Hot-path cost

`RUSTFLAGS="--cfg keep_bench" cargo bench -p loadgen --bench hotpath` measures the user-space part of one request without any I/O: drawing it from `stalwart-mix`, encoding it per protocol, decoding the null server's reply and recording two histogram samples. The kernel I/O around it (one `write`, one `read` and part of an `epoll_wait` per request when nothing batches) costs an order of magnitude more.

## Outputs

With `--out DIR`:

- `summary.json`: arguments, host, protocol, workload, preload, and per point: offered, issued and completed rates, errors (error replies, timeouts, failed), percentiles of `latency` and `service` per op and for all ops, `lag` and `loop_gap` percentiles, generator use, TCP retransmits (Linux, host wide) and validity.
- `points.csv`: one row per point and op (plus an `all` row), the same numbers flattened.
- `hdr.hlog`: HdrHistogram interval log (V2, deflate), values in ns, one entry per point and metric with tags `p<point>.<op>.latency`, `p<point>.<op>.service`, `p<point>.lag` and `p<point>.loop_gap`. This is what `harness/bench/analysis` merges.

The console prints one line per point.

### Metrics

| Metric | Definition |
|---|---|
| `latency` | Reply received minus intended send time |
| `service` | Reply received minus actual send time |
| `lag` | Actual send time minus intended send time: the generator's schedule lag |
| `loop_gap` | Time between two event-loop iterations; a large gap with low `util` means the thread was not running (preemption, interrupts, steal) |
| `util` | Fraction of wall time spent in loop iterations that did work, per thread; `util_max` is the busiest thread |
| `busy_ns_per_op` | Busy wall time per completed op: the generator's CPU cost of the protocol, client side |
| `thread_cpu_ns_per_op` | Thread CPU time per op including busy polling (100% of a core in spin mode) |
| `process_cpu_util` | Process CPU over the point, per thread |
| `completed_rate` | Requests intended inside the measurement window and answered before the drain deadline, per second |
| `io_per_op` | Per completed op: `writes` (sends), `reads` (receives), `ready_polls`, `idle_polls`, `syscalls` (epoll: writes, reads and polls that returned events; io_uring: `io_uring_enter` calls), bytes sent and received, and `recv_rearms` (io_uring multishot receives re-armed after the buffer ring ran dry) |

### Validity

A point is invalid when it has any error reply, timeout or connection failure, when the busiest thread's `util` is above `--max-util` (0.70), or when `lag` p99 is above `--max-lag-p99-us` (10 us) or p99.9 above `--max-lag-p999-us` (200 us). The lag limits are the provisional rule of 2026-10-07: Azure v7 VMs stall a spinning thread for up to a few milliseconds a few times per second, which makes the original p99.9 limit of 10 us unreachable; the rule tightens again if core isolation removes the stalls. `saturated` marks points whose completed rate fell more than 1% below the issued rate. The process exits with status 2 when any point is invalid.

## Null server

`nullserver` answers every request with a canned reply of the right shape: RESP (`GET`, `EVALSHA`, `INCRBY`, `EXPIRE`, `SET`, `SETEX`, `DEL`, `EXISTS`, `SCAN`, `SCRIPT LOAD`, `CLUSTER SLOTS`, `PING`, `AUTH`), memcached meta (`mg`, `ms`, `ma`, `md`, `mn`) and the Keep protocol. Connections are handed round robin to pinned worker threads. `--cluster-nodes host:port,...` makes `CLUSTER SLOTS` split the slots over several null servers. It prints its request rate and CPU every `--report-every` seconds.

## Limits

- No reconnect: a connection error ends the worker and marks the run invalid.
- `delete_prefix` over RESP is Stalwart's `SCAN MATCH ... COUNT 100` loop plus `DEL` of each batch on one connection; it is not supported with `--cluster` or memcached meta.
- The Keep frame limits keys to 255 bytes and needs a TTL for `try_lock` and `renew` (checked at load time; trace records without one are rejected or dropped). `renew` is sent without a token (an unconditional extend, like Stalwart's `SET EX` renew); `unlock` carries no value.
