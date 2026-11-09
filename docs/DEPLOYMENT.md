# Keep deployment guide

This guide gives the conditions Keep's guarantees rest on, the server configuration, host tuning, CPU placement and Kubernetes, and the operating rules for restarts, member-list changes and the lock quarantine. "DESIGN" is [DESIGN.md](DESIGN.md) and "PROTOCOL" is [PROTOCOL.md](PROTOCOL.md); a number after either names a section.

## 1. Hard requirements

These are conditions of Keep's safety claims, not tuning. A deployment that breaks one is outside the model.

| Requirement | Why |
|---|---|
| Linux kernel 6.12 or later | minimum supported kernel; no feature probe beyond it |
| glibc 2.34 or later | the x86_64 and aarch64 builds of `keep` link no newer glibc symbol version; the build checks fail a build that does |
| A clock source whose raw rate is within 833 ppm of real time | every server time is `CLOCK_MONOTONIC_RAW`; NTP cannot reveal a worse oscillator, because chrony corrects up to 500,000 ppm (DESIGN 1.1) |
| No pause with a stopped clock longer than `max_stop` (default 30 s) in total within any span as long as the quarantine Q (section 5.4) | a VM frozen with its clock restored, or an OS suspend, is safe only up to `max_stop`; two freezes inside one Q add up; beyond it the server must be restarted (DESIGN 1.1) |
| No checkpoint and restore onto another host (CRIU) | its clocks jump in ways no check sees (DESIGN 1.1) |
| The peer port (UDP heartbeats and TCP handoffs) reachable only from cluster members (a private network or a firewall) | the heartbeat MAC cannot tell a replayed heartbeat of a removed member from a live one; about 5 datagrams a second keep a cluster down (PROTOCOL 12.1, "Peer port exposure"; DESIGN 1.1) |
| Each cluster has its own peer secret | two clusters sharing a peer secret and node ids accept each other's heartbeats (DESIGN 1.1, "Membership") |
| The peer secret is never given to clients | it keys the heartbeat MAC and the peer handshake; clients hold only the cluster secret (PROTOCOL 12, "Peer secret"; DESIGN 1.1, "Secrets") |
| Every server has the same member list, peer secret and `max_lock_ttl` | assumption of the membership claims (DESIGN 1.1, "Membership") |
| Keys used as locks are in a pinned key class | the memory cap never evicts them; the pinned first bytes are 0, 1 and 14 to 17, so lock keys start with one of them (DESIGN 1.1, "Lock keys"; PROTOCOL 3.8) |

Clock notes:
- Wall-clock steps and slews (NTP, chrony, `date -s`) do not affect Keep (DESIGN 1.1).
- On macOS Keep reads the raw clock too; other platforms without `CLOCK_MONOTONIC_RAW` are refused at startup or do not build (PROTOCOL 12.1, "Clock").
- An OS suspend is detected and handled as a clock loss (credits dropped, a lapse, re-quarantine); a suspend shorter than `max_stop` is safe without that check.
- keep-server logs its clock source at startup and on any change, and logs when the kernel's correction goes above 500 ppm and when it comes back, with the peak. A slew shows as a bounded excursion; a raw clock that is far off shows as one that never ends. Watch for that line.
- Azure x86 v7 VMs use clocksource `tsc`, which typically runs within 35 ppm, well inside the 833 ppm requirement.
- Checking a host against the 833 ppm requirement: run the member with NTP (chrony) for at least a day. If keep-server's correction log stays silent, the kernel's correction, which is the raw oscillator's error, stayed under 500 ppm. A logged correction above 500 ppm means the host fails the requirement until shown otherwise.

## 2. Server configuration

`keep` flags, with the rules that govern them. Every flag can also be set in a TOML file or an environment variable (section 2.2):

| Flag | Default | Rule |
|---|---|---|
| `--listen` | `0.0.0.0:7000` | the client address to bind |
| `--cpus` | the allowed CPUs | the CPUs for the shards, as in `0-3,8` |
| `--pin` | `auto` | `auto` pins the shards when the CPU budget is exclusive and has a CPU per shard; `never` leaves them unpinned |
| `--memory` | 75% of the memory budget | memory cap in bytes, split evenly between the shards. The budget is the smaller of the cgroup's `memory.max` and the total memory, read once at start; the cap and its source are logged. A cgroup v1 limit is not read: there, set `--memory` below it. Nor is `memory.high`, which alone throttles the server rather than bounding it. Section 2.3 describes what happens at the cap |
| `--max-value` | 4,194,304 | largest value and publish payload in bytes, at most 4 MiB; larger requests get `TooLarge` |
| `--stats-shards` | off | adds one stats line per shard; needs `--stats-interval` |
| `--stream-coalesce-us` | 100 | microseconds a subscriber's output may be held to coalesce events into fewer writes, at most 10,000; 0 for never |
| `--stream-coalesce-fanout` | 16 | subscriber connections this server needs before `--stream-coalesce-us` applies; 0 coalesces at any number |
| `--spin-us` | 0 | microseconds an idle shard keeps polling before it parks, at most 1,000; takes effect only with pinned shards, no `cpu.max` quota and no cgroup v1 |
| `--members node@client_ip:port/peer_ip:port/shards,...` | single server | every member, this one included; the peer address serves UDP heartbeats and the TCP peer port on the same port number, so a member needs one firewall rule and one address for its peers; each entry carries that member's shard count, and a conflicting `--shards` is refused |
| `--peer-listen` | the unspecified address with this member's peer port | binds the UDP and TCP peer address |
| `--peer-secret-file` | the `KEEP_PEER_SECRET` variable | 32 hex digits; one per cluster; never on clients (DESIGN 1.1) |
| `--secret-file` | the `KEEP_SECRET` variable | the cluster secret, 32 hex digits; clients hold it (DESIGN 1.1) |
| `--start` | `rejoin` | `bootstrap` only for a cluster's first bring-up, or a restart together with every client; never in restart-on-failure units or StatefulSet templates (DESIGN 1.1, "Start mode") |
| `--node` | random when absent | required in cluster mode |
| `--advertise` | the bound address | in cluster mode, this server's client address from its `--members` entry |
| `--max-lock-ttl` | 600 s | TryLock and Renew with a longer TTL get `BadRequest`; it sets the quarantine (section 5.4). Cluster members need at least 6 s at the default self-fence delay; keep-server refuses less, and warns below 16 s, where a leave's hold (section 5.2) is shorter than the 10 s leave wait |
| `--max-stop` | 30 s | the longest total pause with a stopped clock within any span as long as Q this deployment accepts; added to the quarantine; at most 600 s |
| `--shards` | the CPU budget (cpuset and cgroup `cpu.max`) | in a cluster the `--members` entry is authoritative; see section 4 |
| `--steering` | `auto` | CPU steering when every shard is pinned and each shard's CPU owns exactly one RX queue; otherwise hash placement, and the server logs why |
| `--stats-interval` | 0 (no stats lines) | the `keep: control epoch ... settled true/false` line is printed with each stats line, so rollouts need a non-zero interval (1 s works; section 5.1); so is `keep: memory footprint BYTES large BYTES evicted COUNT orphaned COUNT` (section 2.3), per shard too with `--stats-shards` |
| `--ready-file PATH` | none | the control thread creates the file while this member is settled and removes it otherwise; probe with `test -e PATH`. |
| `--incarnation` | the wall-clock second | a starting point only; peers raise it past any newer run they remember (PROTOCOL 12.1, "Incarnation") |
| `--tls rpk\|ca\|off` | `rpk` | `rpk`: mutual TLS 1.3 with raw public keys on the client and peer ports; `ca`: certificates from a cluster CA; `off`: plaintext with the `Auth` challenge. A `--tls-*` flag the mode does not use is refused at startup (PROTOCOL 10) |
| `--tls-key PATH` | none | this server's private key, PEM (PKCS#8 or SEC1) or DER; `rpk` and `ca` (PROTOCOL 10.1) |
| `--tls-clients PATH` | none | the client public keys the client port accepts (PEM `PUBLIC KEY` blocks); `rpk` (PROTOCOL 10.1) |
| `--tls-servers PATH` | none | the server public keys the peer port accepts and dials, this server's own included (startup refuses a list without it); `rpk`, cluster members only (PROTOCOL 10.1, 12.2) |
| `--tls-cert PATH` | none | this server's certificate chain, PEM; `ca` (PROTOCOL 10.1) |
| `--tls-ca PATH` | none | the cluster CA that every client chain must lead to, PEM; `ca` (PROTOCOL 10.1) |
| `--tls-peer-ca PATH` | none | the CA that every peer chain must lead to, on the peer port and when dialing members, PEM; required with `ca` on cluster members, refused elsewhere (section 2.1; PROTOCOL 10.1, 12.2) |
| `--config PATH` | the `KEEP_CONFIG` variable; none when absent | a TOML file holding any of the flags above (section 2.2) |

### 2.1 TLS

- Keys (`rpk`). One key pair per Keep server and per client node; Ed25519 is recommended: `openssl genpkey -algorithm ed25519 -out key.pem`, then `openssl pkey -in key.pem -pubout` for its public key. Concatenate the server public keys into the `--tls-servers` file (the same file on every member, and the client URL's `servers=`) and the client node public keys into the `--tls-clients` file. The two lists are separate, so a client node's key cannot pose as a Keep server (PROTOCOL 10.1).
- Certificates (`ca`). Every member certificate carries its client address's name or IP address for clients and its peer IP address as an IP SAN, since the dialing member checks the dialed member's peer IP (PROTOCOL 12.2), and both the serverAuth and the clientAuth extended key usages, since a member also dials. Clients present a chain from the cluster CA (`--tls-ca`). Members issue from a member CA under the cluster CA that issues nothing else, and pass it as `--tls-peer-ca`: clients verify a member to the cluster CA, members verify each other to the member CA, and a client node's certificate does not lead to it. One member certificate serves both ports. For example:

  ```
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 -subj /CN=keep-cluster-ca -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign,cRLSign -keyout ca.key -out ca.pem
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=keep-members -keyout members.key -out members.csr
  openssl x509 -req -in members.csr -CA ca.pem -CAkey ca.key -days 3650 -extfile <(printf 'basicConstraints=critical,CA:TRUE,pathlen:0\nkeyUsage=critical,keyCertSign,cRLSign') -out members.pem
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=keep-1 -keyout keep-1.key -out keep-1.csr
  openssl x509 -req -in keep-1.csr -CA members.pem -CAkey members.key -days 825 -extfile <(printf 'subjectAltName=DNS:keep-1.example,IP:10.0.0.1\nextendedKeyUsage=serverAuth,clientAuth') -out keep-1.pem
  cat keep-1.pem members.pem > keep-1-chain.pem
  ```

  Then `--tls ca --tls-cert keep-1-chain.pem --tls-key keep-1.key --tls-ca ca.pem --tls-peer-ca members.pem`. Passing `ca.pem` as `--tls-peer-ca` also works, but then any certificate from the cluster CA completes TLS on the peer port, member identity rests on the peer secret alone, and the peer port must be reachable only from members (its handshake costs CPU before the peer secret is checked).
- The client port should be reachable only from client nodes, like the peer port only from members (section 1). Every unauthenticated connection costs a full handshake on a shard thread, about 82 us of CPU (81.9 us measured), before the server can refuse it; at that cost 1,024 reconnects after a restart take about 84 ms of CPU spread over the shards. A connection that has not authenticated within 10 s is closed, and a handshake keeps at most about 100 KB of unfinished input (PROTOCOL 10.1, "Records").
- Under TLS the peer port also runs the peer-secret handshake (PROTOCOL 12, "Peer secret").
- Clients (keep-client URL): `tls=rpk&key=PATH&servers=PATH`, or `tls=ca&ca=PATH[&sni=NAME]&cert=PATH&key=PATH`. A plaintext client that reaches a TLS server fails with "server requires TLS".

### 2.2 Config file and environment variables

Each setting is taken from the first of these that holds it: the command line, its environment variable, the config file, the flag's default. A server started with flags only behaves as without a file.

- File: `--config PATH` (or `KEEP_CONFIG=PATH`) names a TOML file of top-level keys. Each key is a flag's name without the leading dashes: `max-lock-ttl = 60` is `--max-lock-ttl 60`. Numbers are TOML integers (or strings of digits, for a `node` id above 2^63 - 1, the largest TOML integer), names, addresses and paths are strings, `stats-shards` is a boolean, and `members` is an array of strings, one per member. Relative paths (`secret-file`, `peer-secret-file`, `ready-file`, the `tls-*` files) resolve against the server's working directory, as on the command line, not against the config file's directory; absolute paths avoid the difference.
- Variables: each flag is also `KEEP_` followed by its name in upper case with `-` as `_`: `--max-lock-ttl` is `KEEP_MAX_LOCK_TTL`, `--secret-file` is `KEEP_SECRET_FILE`. `KEEP_STATS_SHARDS` takes `true` or `false`, and `KEEP_MEMBERS` takes the comma-separated list of `--members`.
- Secrets stay out of the file: it names the secret files (`secret-file`, `peer-secret-file`), and the secrets themselves are only ever in those files or in `KEEP_SECRET` and `KEEP_PEER_SECRET`. These two keep their meaning: they hold the secret, rank as variables (above the file's `secret-file`, below the `--secret-file` flag), and setting one together with its `_FILE` variable is refused.
- Startup refuses an unknown key, a value of the wrong TOML type, a value its flag would refuse (checked in every layer, also when a higher layer overrides it), and an unreadable or malformed file; each message names the file and key or the variable, and a syntax error gives the line and column without quoting the file, so a secret written into it by mistake is not logged. These are usage errors (exit status 2, like a bad flag). The checks between settings are those of the flags, with the same refusals and warnings, and their messages name the flags.
- Variables that start with `KEEP_` but match no flag are ignored, since other software sets some (in Kubernetes, the service links of a Service named `keep`, such as `KEEP_PORT` and `KEEP_SERVICE_HOST`); a misspelled variable therefore has no effect. Set `enableServiceLinks: false` on Keep pods (section 4.1) so no such variable can ever collide with a setting.

For example, the cluster member of section 4.1 (paths and addresses are placeholders):

```toml
members = [
  "1@10.0.0.1:7000/7100/4",
  "2@10.0.0.2:7000/7100/4",
  "3@10.0.0.3:7000/7100/4",
]
secret-file = "/secrets/cluster"
peer-secret-file = "/secrets/peer"
tls-clients = "/secrets/clients.pem"
tls-servers = "/secrets/servers.pem"
max-lock-ttl = 600
stats-interval = 1
ready-file = "/run/keep/settled"
```

started with `keep --config /etc/keep/keep.toml --node 1 --tls-key /secrets/keep-0.key`, or with `KEEP_NODE=1` and `KEEP_TLS_KEY=/secrets/keep-0.key` in place of those flags.

### 2.3 Memory

What a shard does at its share of the cap:

- Large values, at least 8 KiB in an evictable class, leave first, oldest written first (PROTOCOL 3.3). A write evicts at most 256 of them, and an upkeep step every 10 ms another 64 while memory is over. A write that still leaves the shard over its share by more than 16 MiB or an eighth of the share, whichever is smaller, answers `Full`, and so does one that could not stay with every large value gone.
- Other evictable entries leave only once memory without the large values reaches the share. A new key in a full segment then evicts in that segment by the order in PROTOCOL 3.3, which takes large values there by the same order. Small keys therefore use all the memory they need and large values use what is left, with no setting between them.
- Pinned keys are never evicted and grow past the cap.

What the figures bound. With `--stats-interval`, `footprint` is what the shards count (a value of 9 bytes to 1 KiB as the allocator's chunk for it: its length plus 8, rounded up to 16), `large` the part in large values, `evicted` how many large values left at the cap, `orphaned` how many queued values eviction found without their entry. `orphaned` stays 0; any other figure is a defect to report, and each such value keeps its memory until a restart.

- For large values the footprint stays within the share plus that margin per shard. Up to a sixteenth of the live arena bytes is kept warm for reuse and counts against the share. After a shift to larger values, slots of the old sizes that the trim cannot release (under 8 KiB, or the part of a slot that does not fill a page) stay counted, so less of the share is left for large values.
- The tables keep their size when their keys leave. After small keys have filled a share, room for large values there comes back only with a restart.
- Values under 8 KiB leave with their entries. Once memory without the large values passes the share by a quarter, the memory a write adds for one of them, a new key's whole value or the growth of an overwrite, is first evicted in its segment, or the write answers `Full` (PROTOCOL 3.3). So for these values the footprint stays within the share plus a quarter, plus what one 10 ms upkeep step writes.
- Memory freed by values of 1 KiB to 8 KiB stays with the shard and serves only values of the same size. After the sizes an application writes change, that memory still counts until a restart, and writes of the new sizes past the quarter evict for their room, or answer `Full` in a segment with nothing to evict.
- Outside the footprint: connection buffers (each up to the 16 MiB output cap), partial input up to one value, the forward lanes between shards, pages that transparent huge pages keep faulted, and heap the allocator keeps after small entries leave (at the cap, RSS measured 1.29 times the footprint with 16-byte values, 1.13 with 64 bytes and 1.02 with 100 bytes). With the default of 75%, the footprint stays within about 94% of the budget, which leaves room for these, except at the cap with values of 9 to 24 bytes, where RSS can reach the budget: such a deployment sets `--memory` to at most 70% of it. An explicit `--memory` close to the host's memory leaves no room at all.

A graceful leave or restart leaves values of at least 64 KiB in evictable classes behind (DESIGN 1.3, claim K2), so it takes no longer with them than without. Pinned values move at any size. An application that stores large values it cannot lose keeps them under 64 KiB or in a pinned class; a pinned class has no bound at the cap.

## 3. Host tuning

| Setting | Rule |
|---|---|
| Minimum RTO 5 ms | on kernels before 6.15 set `net.ipv4.tcp_rto_min_us=5000` (6.11 and later) or a route `rto_min`, on Keep servers and on client nodes; on 6.15 and later each end sets `TCP_RTO_MIN_US` per socket itself. A kernel without that socket option makes keep log `rto_min false`; there the sysctl is required (PROTOCOL 1) |
| `TCP_USER_TIMEOUT` 1 s | set by both ends; nothing to configure (PROTOCOL 1) |
| Idle policy on Azure x86 v7 | shards park when idle (the default); this guide sets the 5 ms minimum RTO only and does not recommend `idle=poll` |
| `idle=poll` (optional) | only for VMs given wholly to Keep that want the lowest tail latency: boot with `idle=poll`, no fill; F-series only (one vCPU per core). It costs every vCPU at 100% busy at all times |
| Client node VMs | a halting client VM costs about 10 us of p99 at 1k requests/s, which only the client side can remove |
| No fill | Keep ships no built-in fill (threads that spin to keep CPUs out of idle states); spinners inside a pod would consume its CPU quota |
| Spinning (`--spin-us`, optional) | an idle shard keeps polling for up to this many microseconds before it parks; it takes effect only with pinned shards, no `cpu.max` quota and no cgroup v1 |
| NIC IRQ affinity (Azure MANA; recommended where available, not a requirement) | keep the driver's IRQ affinity; concentrating the queue IRQs on a few CPUs gives 41 to 125 RTOs per 15 s and p99 of 190 to 590 ms |
| NIC RX ring (Azure MANA; recommended where available, not a requirement) | Debian 6.12 defaults the VF RX ring to 512 entries, which overflows under bursts; raise it to 8192 |
| Other NICs and clouds | unmeasured |
| CPU steering | needs one RX queue per shard CPU; the driver's map is sometimes uneven on F sizes and permanently so on D16as_v7; the server detects it and falls back to hash placement (clients reach the shard through forwarding) |
| Huge pages | the engine advises its large regions `MADV_HUGEPAGE`; at startup `keep` re-executes itself once with `glibc.malloc.hugetlb=1` added to `GLIBC_TUNABLES` unless the environment already sets that tunable (an explicit value, 0 included, is respected). The tunable exists from glibc 2.35: on the 2.34 floor (RHEL 9, Amazon Linux 2023) the restart has no effect and malloc's heap gets huge pages only under THP `enabled=always`; the engine's own regions are advised on every glibc |
| THP | `madvise` on Keep hosts (recommended): the engine's allocator was chosen under it, and large values then hold RSS within 0.3% of their footprint at the cap. Under `enabled=always`, which fresh Debian 13 images boot with, khugepaged collapses regions holding released pages back into huge pages, so RSS passes the footprint: on a server with `--memory` of 256 MiB under swap traffic, by 34 to 42 MiB at the default `khugepaged/max_ptes_none` of 511 and by 24 to 30 MiB at 0. A host that must keep `always` sets `max_ptes_none` low. `never` is supported, its cost unmeasured |
| SMT | F-series v7 has one vCPU per core, D-series two; `idle=poll` is for F-series only |

Limits and logging (checked behaviour, not a rule; see section 6, item 2): keep started under `systemd-run -p LimitMEMLOCK=64K -p SystemCallFilter=@system-service -p NoNewPrivileges=yes` serves with 0 errors, and keeps serving with stderr on a full 1 MiB tmpfs (each checked over 60 s).

## 4. CPU placement and Kubernetes

| Rule |
|---|
| In a cluster, each member's shard count in `--members` is authoritative. Size it from that member's CPU limit by the next rule when the list is written. A CPU limit changed so that it no longer matches the member's `--members` count needs a member-list change (section 5.3), never only the limit |
| The shard count comes from the CPU budget: the minimum of the cpuset and `cpu.max`, never `nproc`. 16 pinned shards under a 4-CPU quota throttled every period (p99 above 70 ms); a 4-shard server under the same quota stayed within the floor |
| Burstable pods with a CPU limit are supported when the shard count comes from `cpu.max`: p99.9 within 1.3% of dedicated at 10k and 50k |
| Pinning is not required; unpinned quota-sized servers cost 3 to 4% closed-loop throughput at saturation |
| Exclusive cores (Guaranteed QoS), or CPU weight at least 100:1 over neighbours: an equal-weight spinning neighbour gave 44x p99 at 50k |
| A scale by k members is k sequential changes: a StatefulSet rolling update with one unavailable at a time |
| `--start bootstrap` never in StatefulSet templates or restart-on-failure units |
| During a member-list change, readiness is the client port accepting connections (a TCP probe), not serving: the first members restarted with the new list stay fenced until a majority of the new list runs it (PROTOCOL 12.1, "Member list") |
| Stop is SIGTERM: the server freezes its shards (keyed requests get `Busy`, at most 1 s), then leaves and hands its slots over, up to the leave limit of 11 s from the leave; a fenced leaver stops at once |
| Termination grace at least 16 s (the 1 s freeze, the 11 s leave limit, 1 s grace, 1 s barrier and a 2 s margin) |
| Rolling restart (same member list): a per-pod readiness probe that is ready only while this member reports `settled`. A StatefulSet rolling update waits for the updated pod to be Ready and does not proceed while any pod is unready, so every pod Ready is a settled cluster (section 5.1). `minReadySeconds: 1` covers the heartbeat-timeout window in which peers still report `settled` after a death |
| Member-list change: the TCP probe instead, one pod at a time (PROTOCOL 12.1, "Member list") |
| Member addresses are stable IP literals (`--members` takes no names). In Kubernetes: `hostNetwork: true` with each pod pinned to its node, so `--members` holds the node IPs, or a CNI with static pod IPs. Plain pod IPs are not supported: a restarted pod gets a new IP. Resolving names only at startup would not help, since peers that resolved the old IP would never reach the restarted pod |
| `--node` from the pod ordinal: the container command derives it from the pod name (`--node $(( ${POD_NAME##*-} + 1 ))`, with `POD_NAME` from the downward API, since a `hostNetwork` pod's hostname is the node's), and the `--members` entry of ordinal i carries node i+1, so one template serves every pod |

### 4.1 Example StatefulSet

An example derived from the rules above, not a tested manifest. The settings every member shares are in a ConfigMap read as the config file (section 2.2), and only the per-pod `--node` and `--tls-key` are flags: `hostNetwork: true` with each pod pinned to its node, so `--members` holds the node IPs; `Parallel` pod management (a member cannot settle until a majority runs, so `OrderedReady` would never start the second pod; the policy applies only to scaling, and rolling updates still go one pod at a time), `RollingUpdate`, one pod per node by anti-affinity (failure independence), the grace and `minReadySeconds` of section 4, and Guaranteed QoS (requests equal to limits). The pin is a small local volume per ordinal: a local PersistentVolume carries node affinity, so pod `keep-i` always returns to the node whose IP is entry i+1 of `--members` (node affinity alone cannot pin each ordinal from one template). The `settled` readiness probe uses `--ready-file` (section 2). Node IPs, names, image, storage class, shard counts and paths are placeholders.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: keep-config-1
immutable: true
data:
  keep.toml: |
    members = [
      "1@NODE_IP_0:7000/7100/4",
      "2@NODE_IP_1:7000/7100/4",
      "3@NODE_IP_2:7000/7100/4",
    ]
    secret-file = "/secrets/cluster"
    peer-secret-file = "/secrets/peer"
    tls-clients = "/secrets/clients.pem"
    tls-servers = "/secrets/servers.pem"
    stats-interval = 1
    ready-file = "/run/keep/settled"
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: keep
spec:
  serviceName: keep
  replicas: 3
  podManagementPolicy: Parallel
  updateStrategy:
    type: RollingUpdate
  minReadySeconds: 1
  selector:
    matchLabels:
      app: keep
  template:
    metadata:
      labels:
        app: keep
    spec:
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      enableServiceLinks: false
      terminationGracePeriodSeconds: 16
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app: keep
              topologyKey: kubernetes.io/hostname
      containers:
        - name: keep
          image: KEEP_IMAGE
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef: {fieldPath: metadata.name}
            - name: KEEP_CONFIG
              value: /etc/keep/keep.toml
          command: ["/bin/sh", "-c"]
          args:
            - >-
              exec keep
              --node $(( ${POD_NAME##*-} + 1 ))
              --tls-key /secrets/keep-${POD_NAME##*-}.key
          resources:
            requests: {cpu: "4", memory: MEMORY}
            limits: {cpu: "4", memory: MEMORY}
          readinessProbe:
            exec:
              command: ["test", "-e", "/run/keep/settled"]
            periodSeconds: 1
          volumeMounts:
            - {name: pin, mountPath: /var/lib/keep-pin}
            - {name: run, mountPath: /run/keep}
            - {name: secrets, mountPath: /secrets, readOnly: true}
            - {name: config, mountPath: /etc/keep, readOnly: true}
      volumes:
        - name: config
          configMap: {name: keep-config-1}
        - name: run
          emptyDir: {medium: Memory}
        - name: secrets
          secret: {secretName: keep-secrets}
  volumeClaimTemplates:
    - metadata:
        name: pin
      spec:
        storageClassName: LOCAL_STORAGE_CLASS
        accessModes: [ReadWriteOnce]
        resources:
          requests: {storage: 1Mi}
```

Notes on the example:
- `--start` is left at its default `rejoin`, so the first bring-up refuses new locks on absent keys for the quarantine (section 5.4), a cost version 1 accepts.
- `--node` comes from the pod name, not `HOSTNAME`: a `hostNetwork` pod takes the node's hostname.
- The server reads the config file only at start, so an edited ConfigMap would reach each pod at its next restart, planned or not. The ConfigMap is therefore immutable and versioned: a change creates `keep-config-2` and points the template at it, which makes it a StatefulSet rolling update under the same rules as a flag change (a new `members` list is a member-list change, section 5.3). `enableServiceLinks: false` keeps Kubernetes from setting `KEEP_*` service variables in the pod (section 2.2).
- Create the three local PersistentVolumes, one per node, before the StatefulSet, each with a `claimRef` to `pin-keep-i`, so ordinal i binds to the node whose IP is entry i+1 of `--members`; an unbound local volume is chosen at first scheduling and could swap two ordinals.
- The shard count in each `--members` entry matches the pod's CPU limit (section 4); a member-list change swaps the readiness probe for a `tcpSocket` probe on port 7000 for that change (section 4).
- TLS is `rpk`, the default (section 2.1). The Secret `keep-secrets` holds each member's private key as `keep-0.key` to `keep-2.key` (pod ordinal i reads `keep-i.key`), the member public keys concatenated in `servers.pem` (also the clients' `servers=` file), and the client node public keys in `clients.pem`; section 2.1 gives the openssl commands. One Secret mounted into every pod puts every member's private key on every member node; keeping each key only on its own node needs a per-pod key source, such as an init container that fetches `keep-i.key` from a secret store.
- The peer secret is mounted only into the Keep pods, never into clients. The peer port must be reachable only from cluster members (section 1); Kubernetes leaves NetworkPolicy for `hostNetwork` pods undefined and most CNIs ignore it, so this needs a host or cloud firewall that admits port 7100 (UDP and TCP) only from the member node IPs.

## 5. Operations

### 5.1 Readiness: `settled`

- keep-cluster exposes `settled`, and keep-server prints it in its control line next to the stats line: `keep: control epoch N fenced F lapses N awaiting N refused N unsent N ... settled true|false` (printed only when `--stats-interval` is above 0).
- `settled` means every live member places every other live member and no slot is gated for an import or held for another member. It also requires that this life ran for the lock window and that this server is not fenced, so a restart waits at least the lock window (1.25 s).
- With `--ready-file` (section 2) the same state is a file that exists only while this member is settled.
- After a crash or power-off of a member, every server still reports `settled true` for about the heartbeat timeout (225 ms) before the death is counted. Trust only `settled` lines logged at least that long after the event (2 s leaves a wide margin). In Kubernetes, `minReadySeconds: 1` covers it (section 4).

### 5.2 Restarts and rolling upgrades

- One membership change at a time: a leave, a start or restart, or a rejoin after a heal is in flight from its first action until the cluster is settled. The next change starts only when every running server reports `settled true`.
- Rolling restarts wait for `settled`, not for Active.
- A graceful leave (SIGTERM) hands slots over; a restart without one quarantines the server's lock slots.
- Each restart refuses new locks on the restarted server's free keys for about Q (634 s by default, section 5.4). A leave within Q of the same server's previous restart extends that to the leave's answer time plus Q on its moved slots. Held locks keep working. Expect it during rolling upgrades, or space passes over the same server by Q (DESIGN 1.2). A join cannot skip it in version 1: a restarted server starts fenced, and its first unfenced view regains its slots with that quarantine. Q follows `--max-lock-ttl` (section 5.4), so setting it to the longest lock TTL the clients use shortens the window. Version 1 accepts this cost.
- A leave's hold lasts min(30 s, (Q - `max_stop`) / 2); 30 s at the defaults.
- After a pause or a partition heals, the returning member pulls the slots its takers hold, so the cluster settles without waiting out the 30 s hold (DESIGN 1.3, claim K3).

### 5.3 Member-list changes

- Change the configured list one member at a time, restarting every server with the new list before the next change, with a Leaving handoff first when the leaving member's slots should not be quarantined (PROTOCOL 12.1, "Member list").
- Within one list change, the restarts follow each other by TCP readiness (a server on the new list cannot settle until a majority of the new list runs). Between two list changes, and around every ordinary restart, wait for `settled true` on every server (PROTOCOL 12.1, "Member list").
- Never change two members at once. Servers fence themselves when they hear a list more than one member away, and the cluster is unavailable until the old-list servers are restarted or stopped. Servers cannot see a change that leaves no member on both lists running (disjoint lists, shared members down or cut off): that makes two clusters that can split (DESIGN 1.5).
- A member-list change loses every KV entry and quarantines the moved lock slots, like a full-cluster restart, and a removal has a window without a majority between two of its restarts (DESIGN 1.5).

### 5.4 Lock quarantine

- After a crash, a restart, or a slot changing owner without a handoff, TryLock on an absent key of those slots answers `Conflict` for the quarantine Q; Renew and Unlock with a token on an absent key answer `NotFound` (DESIGN 1.2).
- Q = `max_lock_ttl` + 3 s + `max_stop` + max(1 s, (2 x `max_lock_ttl` + `max_stop` + 5 s) / 1,199), with the drift term rounded up to the millisecond and the self-fence delay at its 1 s (PROTOCOL 3.4): 634.031 s at the defaults, 604.006 s on a single server (no `max_stop`), 3,639.035 s at a 3,600 s `max_lock_ttl`. The drift term is 1 s while `max_lock_ttl` is at most 582 s at the default `max_stop`.
- A whole-cluster restart refuses new locks on absent keys for Q, the same cost version 1 accepts after a crash.

### 5.5 Pauses, suspends and VM maintenance

- A pause with a stopped clock (VM freeze, OS suspend) is safe up to `max_stop`. An operator who stops or suspends a Keep VM for longer must restart Keep (DESIGN 1.1).
- Process pauses of any length (SIGSTOP, scheduler stalls) are covered, because the raw clock keeps counting through them (DESIGN 1.1).

### 5.6 Clients

- Set the 5 ms minimum RTO on client nodes as on servers (PROTOCOL 1).
- A client that times its own lock holds needs a clock in the same model: std's `Instant` follows NTP slews, so a host whose wall clock was stepped far can misjudge a hold by up to 8.3% of its TTL. Renew long before expiry (DESIGN 1.1).
- Non-idempotent operations (Incr, Set with IF_ABSENT, Publish) are not resent after an unknown outcome (DESIGN 1.5).

## 6. Known gaps

1. THP `never`: supported, but its cost is unmeasured.
2. memlock and seccomp: no rule is given. Section 3's "Limits and logging" paragraph states checked behaviour, not a supported-configuration rule.
3. Member addresses by DNS name, re-resolved after a restart, so Kubernetes deployments can use plain pod IPs behind a headless service, are not in version 1, which uses `hostNetwork` with each pod pinned to its node, or a CNI with static pod IPs (section 4).
4. Peer-secret rotation with every member removal is not in version 1: there is one peer secret per cluster, and the peer-port exposure rule applies (section 1; PROTOCOL 13).
