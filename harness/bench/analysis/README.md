# analysis

Merges `loadgen` results across hosts and runs, and compares two sets of runs with bootstrap confidence intervals, as the benchmark rules ask for every A/B result.

```
cargo build --release -p analysis
analysis merge RUN...
analysis compare --a RUN... --b RUN... [--paired]
```

## Inputs

A RUN is a directory holding `summary.json` and `hdr.hlog` from `loadgen --out`, or a directory whose subdirectories each hold one (one subdirectory per generator host of the same run). Hosts of one run are concurrent: their histograms are summed and their rates added. Several RUNs are repetitions: their histograms are pooled and their rates averaged. Points are matched by index, so A and B must have run the same plan.

## merge

Prints, per point and op, the pooled count and percentiles (default p50, p90, p99, p99.9, p99.99 and max, in microseconds), the mean offered and completed rates, whether every run was valid, and the busiest generator thread.

```
analysis merge harness/work/RUN/results/SWEEP1 harness/work/RUN/results/SWEEP2 --metric latency --format md
```

`--metric service` uses the time from the actual send instead of the intended send time. `--format csv` for spreadsheets.

## compare

For every point present in all runs, every op (and `all`), every requested quantile, and the completed throughput, prints A, B, the ratio B/A, its 95% CI and a verdict.

```
analysis compare --a OUT/a-r1 OUT/a-r2 ... --b OUT/b-r1 ... \
    --paired --quantiles 0.5,0.99,0.999 --floor-us 64 --noise-floor 0.03
```

- `--paired`: A and B ran as interleaved pairs on the same VMs; the bootstrap resamples pair indices, so A_i and B_i stay together. Without it, A and B runs are resampled independently.
- `--floor-us`: the RTT floor of the VM pair (bench/kv.sh rtt), subtracted from every latency before the ratio, to report latency as excess over the network floor.
- `--noise-floor`: the noise floor of repeated identical runs, as a fraction. The verdict is `B better` or `B worse` only when the whole CI lies beyond 1 plus or minus the noise floor; otherwise `no difference`. Lower is better for latency, higher for throughput.
- `--ops`: restrict the ops, e.g. `--ops all,incr`.
- `--replicates`, `--seed`: bootstrap size (default 10,000) and seed.

### Method

The CI is a two-level bootstrap with the percentile method:

1. Runs are resampled with replacement (pairs jointly with `--paired`). This captures run-to-run variance, which dominates on cloud VMs.
2. Within the pooled histogram of the resampled runs, the quantile is drawn as the k-th order statistic of N draws from that distribution: `U ~ Beta(k, N - k + 1)` with `k = ceil(q N)`, then the value whose cumulative count first reaches `U N`. This is the exact distribution of the sample quantile under multinomial resampling of the N requests, without resampling them one by one.

Each run's histogram is reduced once to cumulative counts over the union of recorded buckets, so a replicate costs one binary search over the buckets, weighted by the run multiplicities, per quantile. 10,000 replicates over 5 + 5 runs take milliseconds per (point, op, quantile).

Throughput ratios bootstrap only over runs (level 1), from each run's completed rate.

With a single run per arm, the interval reflects only the sampling noise of the requests inside that run, which is far narrower than the real run-to-run variation; use at least 5 runs per arm before reading a verdict.

## attribute

Assigns a cause to every slow request of a run, from the run's `outliers.jsonl` (`loadgen run --outlier-us N`) and the probe events collected on the client and server VMs.

```
analysis attribute OUT/run --client-probes OUT/tail-cli.jsonl --server-probes OUT/tail-srv.jsonl \
    [--min-us N] [--tolerance-us 200] [--explain-share 0.5] [--out OUT/attribution.jsonl]
```

- Outliers considered: those at or above each point's p99.9 from `summary.json`, or above `--min-us`.
- Candidate causes and the time each explains: `tcp_retransmit` (a retransmission on the outlier's own flow on either side: 200 ms for an RTO, 1 ms for a fast retransmit), `client_schedule_lag` (the request's schedule lag), `client_preempt_or_runq` (the generator thread off its CPU), `client_irq` and `client_steal` (on the thread's pinned CPU), the server stages of a matching stage sample (`server_shard_queueing`, `server_cross_shard_hop`, `server_exec` with its tag such as expiry or resize, `server_send`), and `server_preempt_or_runq`, `server_irq`, `server_steal` on any server CPU.
- The primary cause is the candidate explaining the most time, if it explains at least `--explain-share` of the latency above the point's p50. Otherwise the outlier is `global_stall` when at least a quarter of the run's connections (and at least 4) complete outliers within the same +-1 ms (a stall of the whole server, client or path that no probe saw), else `unattributed`. The table counts stall events per point.
- On Azure, multi-connection runs have RTO stalls of 0.2 to 2 s at p99.9, so outliers selected at p99.9 are retransmissions whatever else happens; attribute other mechanisms with `--min-us` in a band below the RTO range (for example 1 ms).
- Server times are shifted onto the client's clock with the monotonic/realtime pairs both hosts recorded; `--tolerance-us` widens server windows for the remaining offset.
- Prints a per-point table (outliers, attributed share, primary causes); `--out` writes every outlier with all candidate causes.
