# stalwart-workload: the stalwart-mix and stalwart-stream definitions

The workload every Keep benchmark runs: what one Stalwart node asks of the KV store and of the cluster stream, per profile, per deployment size, at Stalwart's demand and scaled far beyond it. The definitions combine measurements of a real Stalwart node with a per-request model read from Stalwart's code at 69c8b4edb.

Separate from these definitions, loadgen has a small built-in `stalwart-mix` profile, used in smoke tests.

## Use

| Consumer | How |
|---|---|
| mock-stalwart (`harness/bench/mock-stalwart`) | `mock-stalwart run --workload mixed\|mta\|imap\|http\|attack\|swap\|swap-batch\|lease --deployment lab\|small\|medium\|large\|hyper --events stalwart\|events-mta\|events-imap\|events-http\|provisional` |
| loadgen (`harness/bench/loadgen`) | `loadgen run --profile stalwart:<profile>[@<tier>]` (for example `stalwart:mixed@large`); `loadgen profile stalwart:mixed@large` prints the flattened single-op profile it runs |
| Any Rust crate | depend on `stalwart-workload = { path = "../workload" }`; `profiles::builtin(name)` gives the TOML, `mix::Mix::from_spec(toml::from_str(text)?, KeyScope::Cluster, deployment::builtin("large").unwrap())` the resolved families (prefix, key length, TTL policy, population, live keys, popularity, value sizes) and weighted actions; `events::EventMix` the event classes |
| Anything else | the TOML in `profiles/` (families shared by every profile in `families.toml`, actions and demand in `kv-*.toml`, event mixes in `events-*.toml`) |

`cargo run -p stalwart-workload --example summary` prints the per-profile, per-tier table of section 5.

## 1. Key classes (`profiles/families.toml`)

Key = class prefix byte + identity + 8-byte window index for rate-limit buckets. Values are the 8-byte counter or lock expiry unless stated. Population is the number of distinct identities a family draws from; live is the steady-state number of keys present, used for preload and footprint. Both scale with the deployment tier (section 4).

| Family | Stalwart class (prefix) | Key bytes | TTL | Population per tier | Live per tier | On by default |
|---|---|---|---|---|---|---|
| smtp-connect | rl_smtp_throttle "Sender IP" (6) | 41 | rest of 1 s window | 5% of client IPs | (1 s keys) | yes, 1 incr per inbound connection |
| smtp-rcpt | rl_smtp_throttle "sender domain + rcpt" (6) | 41 | rest of 3600 s window | 0.8 x messages per hour | 0.4 x messages per hour | yes, 1 incr per accepted RCPT |
| rcpt-fail-ip, rcpt-fail-addr | rl_rcpt (2) | 13, 41 | rest of 1 d | 2%, 6% of client IPs | same | yes, per rejected RCPT |
| scan | rl_scan (3) | 13 | rest of 1 d | 5% of client IPs | same | yes, per invalid unauthenticated command |
| auth-fail-ip, auth-fail-login | rl_auth (5) | 13, 41 | rest of 1 d | 2%, 10% of client IPs | same | yes, 2 incr per failed login |
| auth-fail-ip6, scan-ip6 | rl_auth, rl_scan from rotating IPv6 (5, 3) | 25 | rest of 1 d | 50 x client IPs (min 1M) | created by the run | attack scenario |
| http-auth | rl_http_auth (8) | 13 | rest of 60 s | active accounts | 0.5 x active accounts | yes, per authenticated HTTP request |
| http-anon | rl_http_anon (9) | 13 | rest of 60 s | 10% of client IPs | 2% of client IPs | yes, per anonymous request |
| imap-rate | rl_imap (10) | 13 | rest of 60 s | active accounts | 0.5 x active accounts | yes, per IMAP/POP3/ManageSieve command after login |
| blob-quota | quota_blob (11) | 13 | rest of 3600 s | 20% of active accounts | 10% | yes, per upload |
| availability | rl_availability (12) | 13 | rest of 60 s | 5% of active accounts | | yes, per getAvailability |
| greylist | greylist (13) | 33 | 1 d | 19.2 x messages per hour | 9.6 x | no (`greylistFor` unset); not used by any profile |
| queue-lock | lock_queue_message (14) | 17 | 600 s | 24 x messages per hour | in flight | yes, lock pair per delivery attempt |
| task-lock | lock_task (15) | 9 | 3600 s | accounts | in flight | yes, lock pair per task |
| dav-lock | lock_dav (16) | 6, value 60 B to 16 KiB | 3600 s | 10% of accounts | 1% of accounts | reads yes, writes on DAV LOCK |
| index-lock | lock_search_index (17) | 6 | 600 s | active accounts | in flight | cluster only, lock pair per partition drain |
| oauth-code | oauth (1) | 41, value 100 B to 2.2 KiB | 600 s | 10 x accounts | in flight | yes, per OAuth login |
| acme | acme (0) | 13 to 61 | 3600 s | 1000 | | no (no provider seeded) |
| swap-pointer, swap-chunk | cache swap tier (`{`) | 21, 49, values 48 B and 64 KiB to 1 MiB | 7 d | 40%, 200% of accounts | 20% of accounts | no (`Cache.swap` Disabled) |

Popularity is Zipf 0.99 for account- and IP-keyed classes (a few heavy accounts and noisy IPs), uniform for locks and per-pair keys. Code facts the profiles follow: login, recipient and greylist keys are fixed-size blake3 hashes (no long keys left except ACME domains), IMAP/POP3/ManageSieve `incr` per command, Sieve ids left the KV store, the DAV lock TTL bug is fixed (TTL at most 3600 s).

## 2. Per-request recipes (from code, cluster mode, default configuration)

| Request | KV ops | Events |
|---|---|---|
| SMTP connect | 1 incr smtp-connect | |
| SMTP MAIL FROM, EHLO, DATA | 0 | DATA: 1 queue_refresh per queued message |
| SMTP RCPT accepted / rejected | 1 incr smtp-rcpt / 2 incr rl_rcpt | |
| Local delivery, per message | 1 queue-lock pair; index-lock pair per partition drain (1 per message at low load, 0.25 at the measured saturation: drains batch) | 1 email_push and 1 index_queue_refresh per recipient account |
| Outbound attempt | 1 queue-lock pair per attempt and virtual queue | |
| IMAP command after login | 1 incr imap-rate | writes (STORE, EXPUNGE, MOVE, APPEND...): 1 state_change; APPEND adds index_queue_refresh and task_queue_refresh |
| HTTP authenticated request | 1 incr http-auth (+1 blob-quota per upload, +1 availability per getAvailability) | JMAP /set and DAV writes: 1 state_change |
| DAV write / LOCK | 1 get dav-lock per resource / get + set | |
| Anonymous HTTP | 1 incr http-anon | |
| OAuth login | anon incr + set oauth-code; token exchange anon incr + get + delete | |
| Failed login (any protocol) | 2 incr (IP key, login key) | ban: registry_insert |

## 3. KV profiles (`profiles/kv-*.toml`)

Action weights are action rates per second at the stated load; one action is one Stalwart operation pattern (rate = 1 incr, lock = try_lock, `renews` renew_lock calls, then unlock after the hold time; lease = try_lease with a random token, `renews` renew_lease calls (try_lease again when one fails), then release_lease; set-many = one key_set_many call of `batch` neighbouring keys; token = set then get and delete after the redeem time). Task locks (`task-lock`, 300 s) are leases since Stalwart's code, in imap and http too.

| Profile | Basis | Op mix (share of KV ops) | Typical / peak KV ops/s per node | Events/s per node |
|---|---|---|---|---|
| mixed | measured: the rates of a mixed client run (IMAP, JMAP, DAV and SMTP clients) plus a modelled internet background (failed logins, scans, anonymous HTTP, OAuth) | incr 62.7% (imap-rate 36.1%, smtp-rcpt 14.8%, http-auth 11.5%, background 0.2%), try_lock 18.6%, unlock 18.6% (queue-lock 29.6%, index-lock 7.5% of all ops), OAuth set/get/delete 0.1% | 316 / 3,816 (the light / saturated runs) | 104 / 1,558 |
| mta | modelled per message from code, typical calibrated on a measured low-load MTA relay run | incr 42.4% (smtp-rcpt 26.6%, smtp-connect 14.8%, rejects and failures 0.9%), try_lock 28.8%, unlock 28.8% (queue-lock 45.8%, index-lock 11.8% of all ops) | 716 / 7,160 (peak 10x, modelled: the saturation run collapsed on a Stalwart RocksDB issue) | 390 / 3,900 |
| imap | modelled from code | incr 98.6%, try_lock and unlock 0.7% each (index and task locks) | 1,015 / 10,150 (modelled) | 109 / 1,090 |
| http | modelled from code (JMAP and DAV) | incr 96.4% (http-auth 93.0%, http-anon 2.0%, quota and availability 1.1%), get 2.0% (DAV lock sets), set 0.2%, delete 0.1%, try_lock and unlock 0.7% each | 1,080 / 10,800 (modelled) | 83 / 830 |
| attack | scenario: failed logins and scans from rotating IPv6 addresses, every key new and living to the end of a 24 h window | incr 100% | 7,000 / 70,000 (scenario) | |
| swap | modelled (cache swap tier, off by default) | set 35.3%, get 35.3%, delete 29.4% (64 KiB to 1 MiB chunks 73.5%, 48 B pointers 26.5%) | 34 / 340 (modelled) | |
| swap-batch | swap with Stalwart's `key_set_many`: chunks in pipelined batches of 1 to 8, the pointer as a one-item batch | set_many 15.4%, get 46.2%, delete 38.5% of calls | 18 / 180 (modelled) | |
| lease | modelled from Stalwart's code for the lease and lock renewal paths; hold times compressed so renewals happen within a run | try_lease 35.5%, renew_lease 6.5%, release_lease 35.5%, try_lock 9.7%, renew 3.2%, unlock 9.7% | 120 / 1,200 (modelled) | |

Arrival: independent single operations from many tasks (Poisson); at 10 ms the measured load had CV 0.99 (light) to 0.5 (saturated), 1 s buckets p99/mean 1.3 to 1.8. Window boundaries: every active account's 60 s counter is recreated in the first seconds of each minute (measured at medium and saturated load: 98 to 100% of new http-auth keys in the first tenth of the minute); the 1 s and 3600 s windows roll over continuously. Hot keys come from Zipf 0.99 popularity; a hot-key test raises one key's share explicitly.

## 4. Deployment tiers (`src/deployment.rs`) and scaled variants

Populations and live keys scale with a tier; rates scale with the node count and the load point. Assumptions (not measured): 20% of accounts active in a peak minute, 2 client IPs per account per day, 4 inbound messages per account per peak hour. `lab` reproduces the lab (500 very active accounts, one client IP, 140k messages per hour at light load).

| Tier | Nodes | Accounts | Active accounts | Client IPs | Messages per hour |
|---|---|---|---|---|---|
| lab | 1 | 500 | 500 | 1 | 140,000 |
| small | 3 | 5,000 | 1,000 | 10,000 | 20,000 |
| medium | 8 | 100,000 | 20,000 | 200,000 | 400,000 |
| large | 32 | 1,000,000 | 200,000 | 2,000,000 | 4,000,000 |
| hyper | 256 | 10,000,000 | 2,000,000 | 20,000,000 | 40,000,000 |

Standard load points for a profile at a tier (Keep sees the sum over all Stalwart nodes):

| Point | Offered KV ops/s |
|---|---|
| demand | typical per node x nodes |
| peak | peak per node x nodes |
| 10x | 10 x peak (market headroom) |
| saturation | swept until the server under test saturates, same mix and cardinality |

Market comparisons run the mix at every point, including far beyond Stalwart's demand. Single-server benchmarks default to the `large` tier's cardinality (2.2M live keys for `mixed`); the `attack` profile adds more on top for 10M and 100M keys.

## 5. Footprint and demand per tier (from `cargo run -p stalwart-workload --example summary`)

| Profile | Tier | Ops per action | Cluster peak ops/s | Live keys | Live key bytes | Live value bytes |
|---|---|---|---|---|---|---|
| mixed | lab | 1.23 | 3,816 | 56,500 | 2.3 MB | 0.45 MB |
| mixed | large | 1.23 | 122,112 | 2,180,000 | 78.7 MB | 17.4 MB |
| mixed | hyper | 1.23 | 976,896 | 21,800,000 | 787 MB | 174 MB |
| mta | large | 1.40 | 229,120 | 2,100,000 | 81.1 MB | 16.8 MB |
| imap | large | 1.01 | 324,800 | 440,000 | 11.3 MB | 3.5 MB |
| http | large | 1.01 | 345,600 | 410,000 | 10.9 MB | 32.3 MB |

The live set is dominated by `smtp-rcpt` (one key per sender domain and recipient per hour); value bytes in `http` by DAV lock sets.

## 6. stalwart-stream

| Item | Definition | Source |
|---|---|---|
| Event mixes | `events-mixed` (alias `stalwart`): 37.4% queue_refresh 1 B, 31.3% index_queue_refresh 1 B, 31.3% email_push 4 to 7 B. `events-mta`: queue_refresh, email_push, index_queue_refresh, rare registry_insert (bans). `events-imap`: 92% state_change 5 to 8 B, index and task refresh, viewer_state_change. `events-http`: 85% state_change, index and task refresh, push_server_update, calendar_alert 50 to 80 B | measured (mixed, mta-relay); code (imap, http) |
| Batching | Stalwart's publisher: up to 100 events per message, refresh events deduplicated per batch, 2 B header (up to 10 B on FoundationDB), split above 512 KiB; measured: 1.2 events per message, messages 3 B p50, 17 B p99, 444 B max | measured, code |
| Per-node publish rate | mixed 104 / 1,558 events/s (typical / peak), mta 390 / 3,900, imap 109 / 1,090, http 83 / 830 | measured, model |
| Fan-out | every node subscribes and receives the other N-1 streams: received per node = (N-1) x per-node rate; deliveries in the cluster = N x (N-1) x rate. Mixed peak at 32 nodes: 48k events/s received per node, 1.5M deliveries/s; at 256 nodes: 397k events/s per node, 102M deliveries/s | code |
| Cluster sizes | 3, 8, 32 nodes real (mock-stalwart processes), 256 emulated | benchmark rules |
| Large-message tail | no measured Stalwart event exceeds 444 B; `provisional` keeps a 1% tail to 512 KiB for head-of-line tests only | measured |

## 7. What the measurements did not cover (modelled here)

| Missing run | Model |
|---|---|
| a second mixed run with a close load host | not needed: the mixed class shares and per-request ratios are stable from light to saturated load and match the code model; latency seen by Stalwart is replaced by Keep's own measurements |
| `mta` saturation | per-message recipe from code (section 2) with the measured low-load calibration (3.0 KV ops per relayed message); peak modelled as 10x the low-load rate (10% CPU there). The collapse the measurement saw (RocksDB "Resource busy" on the queue spool in cluster mode) is a Stalwart issue, not demand |
| `mta-inbound-local` | the local-delivery recipe of section 2: connect 0.67, recipient throttle 1.2, queue-lock pair 1, index-lock pair 0.4 per message; events 1 queue_refresh, 1.2 email_push, 1.2 index_queue_refresh |
| cache swap tier | `swap` profile and `swap-*` families from the code (flush thresholds 2000 changes and 10 min, chunks up to 1 MiB, 7 d retention); off by default in Stalwart |
| two-node cluster | from code: queue and task locks are tried by every node with the role (1 to N try_locks per due item, losers back off 300 to 610 s), index locks by every node woken by index_queue_refresh (losers defer 30 s), each scheduler node enqueues its own daily purge tasks (N x accounts lock pairs per day unless one node holds the scheduler role), rate-limit keys are cluster-wide (cardinality does not grow with N, per-key rate does), events fan in N-1 times. mock-stalwart emulates N nodes with `--key-scope node` (per-node lock and queue keys) or `cluster` (shared limiter keys) |
| per-IP cardinality | the client IP basis of section 4 (assumed 2 per account per day; behind a proxy without X-Forwarded-For all IP-keyed classes collapse to one key) |
