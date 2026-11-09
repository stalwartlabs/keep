# Keep wire protocol

This document specifies version 1 of the Keep protocol, between clients and servers and between servers. It is implemented once, in `keep-proto` (`crates/keep-proto`), and spoken by `keep-server` and `keep-client`. The consistency claims it serves are in [DESIGN.md](DESIGN.md) and the deployment rules in [DEPLOYMENT.md](DEPLOYMENT.md); a reference such as "DESIGN 1.3" or "DEPLOYMENT 2" names a section of those documents.

## 1. Transport

- One TCP connection carries one ordered byte stream of frames in each direction.
- Both ends set `TCP_NODELAY` and batch in user space instead (section 5.2).
- Minimum retransmission timeout of 5 ms on both ends.
  - Where the kernel has it (6.15 and later), each end sets `TCP_RTO_MIN_US` = 5000 on every connection. On older kernels the deployment guide sets `net.ipv4.tcp_rto_min_us=5000` (6.11 and later) or a route `rto_min`, on Keep servers and on client nodes.
  - Reason: with one batch in flight, Linux sends its tail loss probe no earlier than the minimum RTO. At the 200 ms default, one lost segment costs every request queued on the connection 200 ms or more. At 0.01% loss under a mail-server cluster load modelled on Stalwart, p99.9 at 2,000 actions/s is 97.5 us with 5 ms against 184 ms with the default, and p99 at 20,000 is 122 us against 189 ms.
- `TCP_USER_TIMEOUT` of 1 s on both ends. A connection with unacknowledged data for 1 s is aborted, so a black-holed peer is detected within about a second. Without it, a black-holed connection reused after a partition heals can stall a server's keys for 3.1 to 3.4 s.
- Encryption is TLS 1.3 only (RFC 9846, which obsoletes RFC 8446; cited section numbers are the same in both except NewSessionTicket, 4.7.1, and KeyUpdate, 4.7.3).
- TLS runs in user space (rustls) on both ends, never kTLS. Server CPU against plaintext at 100,000 requests/s was 1.50x with kTLS and 1.17x with rustls. At 64 KiB values it was 2.95x and 1.66x (x86_64 and aarch64 alike).
- Cipher suite: TLS_AES_128_GCM_SHA256 first. ChaCha20-Poly1305 was never cheaper, and AES-256-GCM was within about 2% of AES-128.
- Key updates: each end lets rustls send a KeyUpdate before the AES-GCM limit of 2^24 records per key, and answers the peer's. A key update every 10,000 records costs +1.9% server CPU, inside the noise floor.
- Records: a server writes one TLS record per receive batch. One record per reply would cost 5.5x the server CPU at saturation.
- Plaintext mode is kept, behind an explicit setting. It saves server CPU: TLS costs +17% per op at 4,000 and +14.5% at 20,000 requests/s; +30 to 34% with 4 KiB values; +60 to 64% with 64 KiB values. Client CPU is +9 to 12% at 2,000 to 20,000 actions/s. Latency at 4,000 requests/s, the measured demand of one Stalwart node, is inside the noise floor (p50 +4 us, p99 +3.4 us).
- Frames are the same in every mode. TLS is below the frame layer, and no frame field depends on it.

## 2. Frame

Every frame, in both directions, is a 16-byte header, then `ext_words * 8` bytes of header extension, then `key_len` key bytes, then `value_len` value bytes. Integers are little endian everywhere in this protocol, inside values too.

| Offset | Size | Field | Meaning |
|---|---|---|---|
| 0 | 4 | `id` | Request id, echoed by the response. Sequence number in stream events. |
| 4 | 1 | `code` | Operation in requests, status in responses, push code in server pushes |
| 5 | 1 | `flags` | Request flags (section 3.6); 0 in server frames |
| 6 | 1 | `key_len` | Key length, 0 to 255 |
| 7 | 1 | `ext_words` | Header extension length in 8-byte words |
| 8 | 4 | `value_len` | Value length, at most 4 MiB (4,194,304) |
| 12 | 4 | `arg` | Per-code argument: TTL in seconds, topic, protocol version, status, count |

Rules:

- The total length is known from the header alone, so a receiver splits a whole receive buffer into frames before executing any of them. Client plus server cost 42.2 ns per op at batch 1 on x86_64, 57 to 67% below RESP3 and memcached meta.
- A header whose `value_len` exceeds 4 MiB is a framing error (section 4.1).
- Keys are opaque bytes, 1 to 255 long, never transcoded. The first byte is the key class (section 3.8).
- Header extension: version 1 defines no extension word, so senders write `ext_words = 0`. A receiver skips extension words it does not understand. A sender writes an extension word only after both ends announced it in the Hello (types 3 and 5, section 7.2), so a peer never receives one it should have acted on. Reserved uses: a fencing epoch, an idempotency token for retried non-idempotent ops, a trace id, a publisher sequence for end-to-end stream loss detection. None of them is planned for version 1.

## 3. Codes

### 3.1 Code ranges

| Range | Use |
|---|---|
| 0x01 to 0x1F | KV and stream requests |
| 0x20 to 0x2F | Connection, cluster and admin requests |
| 0x30 to 0x3F | Server-to-server frames, valid only on the peer port, defined by `keep-cluster`. On the client port they are unknown ops. |
| 0x40 to 0x7F | Reserved for requests |
| 0x00 to 0x7F | Response statuses (the direction tells requests and responses apart) |
| 0x80 to 0xFF | Server pushes |

### 3.2 Requests (client to server)

| Code | Op | key | value | arg | Success response |
|---|---|---|---|---|---|
| 0x01 | Get | key | empty | 0 | `Ok`, value; with WITH_TTL, arg = whole seconds certainly left, at least 1 (0: no TTL). `NotFound` |
| 0x02 | Exists | key | empty | 0 | `Ok` or `NotFound` |
| 0x03 | Set | key | value | TTL or 0 | `Ok`; with IF_ABSENT, `Conflict` when the key exists |
| 0x04 | Delete | key | empty | 0 | `Ok` or `NotFound` |
| 0x05 | Incr | key | delta, i64 | TTL or 0, applied on creation only | `Ok`, value = new value, i64 |
| 0x06 | TryLock | key | holder token, 8 B, required | TTL, required | `Ok` acquired (value empty); `Conflict` held by another token |
| 0x07 | Unlock | key | holder token, 8 B, required | 0 | `Ok` deleted; `NotFound` absent; `Conflict` held by another token |
| 0x08 | Renew | key | holder token 8 B, or empty | TTL, required | `Ok` extended; `NotFound` absent; `Conflict` held by another token |
| 0x09 | DeletePrefix | prefix | empty | 0 | `Ok` at once; the deletion completes in bounded steps |
| 0x10 | Publish | empty | payload | topic | `Ok` |
| 0x11 | Subscribe | empty | empty | topic | `Ok`, then Event frames; `Conflict` when the connection already subscribes the topic (nothing changes) |
| 0x12 | Unsubscribe | empty | empty | topic | `Ok`, then `End`; `NotFound` when the connection does not subscribe the topic |
| 0x20 | Hello | empty | client info (section 7.2) | highest version the client speaks | `Ok`, arg = version in use, value = server info |
| 0x21 | Ping | empty | empty | 0 | `Ok` |
| 0x22 | Topology | empty | empty | 0 | `Ok`, value = topology (section 9.4) |
| 0x24 | Auth | empty | client proof, 32 B | 0 | `Ok`; a wrong proof gets `GoAway` with `Unauthorized` (section 10.2) |
| 0x26 | Leave | empty | empty | 0 | `Ok`; the server hands its slots over and leaves the cluster. Admin listener only; `Unauthorized` on the client port. |

0x23 is not assigned; 0x25 is reserved.

### 3.3 KV semantics

- TTLs are whole seconds. 0 means no TTL where the TTL is optional. An expired key is absent for every op, including Incr and TryLock.
- The server keeps time in whole seconds and rounds every deadline up to its next whole second: a key given a TTL by Set, by Incr on creation, by TryLock or by Renew lives at least TTL and at most TTL + 1 s after the server executes the request. "Deadline now + TTL" below means this rounded deadline. A handoff carries the deadline as an upper bound and the import rounds up again, so a handoff never shortens a life; it can lengthen it by the transfer time plus up to 2 s of rounding (the export computes the seconds left, then network and queue time pass before the import).
- Set replaces the value and the TTL. With IF_ABSENT it stores only when the key is absent or expired.
- Incr creates the key at 0 and sets the TTL only on creation. The delta and the result are i64; a result outside i64 is `Overflow`, and Incr on a key whose value is not a counter is `WrongType`. A key created by Set or TryLock is not a counter.
- No window mode for Incr and no fused rate-limit op. A client keeps the window index in its rate-limit keys and sends the rest of the window as the TTL, so every key of a window expires at the window's end. One Incr round trip already is the rate check.
- Get with WITH_TTL reports in `arg` the whole seconds the key certainly has left, 1 during its last second; 0 means the key has no TTL.
- DeletePrefix returns at once. The shard that received it fans the purge out to every shard of its server; the client sends it to every server.
  - Each shard purges 1,024 entries per event-loop turn, so no operation waits behind it (step maximum 43.3 us; 277 ms for 1M keys).
  - Every key that existed when the delete was ordered and was not written again is gone when the purge ends. A key written while the purge runs may survive (about 7% of re-created keys). Callers that need a clean slate stop writing under the prefix first.
- Expiry: lazy on access, plus a cursor sweep that covers the whole table every 120 s in budgeted steps.
- Memory cap:
  - Each shard enforces its share of the server's cap: `--memory`, or by default 75% of the server's memory budget (DEPLOYMENT section 2).
  - Large values are values of at least 8 KiB in an evictable class. They leave first. A write of one, and an upkeep step every 10 ms, evict the oldest written across the shard's tables while the shard's memory would pass its share. A write makes its key the youngest; a read does not.
  - Other entries of evictable classes leave only once the shard's memory without its large values reaches its share. A new key in a full segment then evicts in that segment, down to three quarters of it, large values included: non-counters and counters at or below zero first, soonest to expire first, then counters, lowest value first. A counter above its throttle limit is therefore the last to go, so a flood of new keys cannot reset a throttle.
  - Once the shard's memory without its large values passes its share by a quarter, the memory a write adds for a value of 9 bytes to 8 KiB is first evicted in its key's segment, in the same order but only among entries other than its own that hold such values or large values. Counters, tokens and other values of up to 8 bytes are therefore never evicted for bytes. Such a write is a new key with a value under 1 KiB, an overwrite that grows a value to under 1 KiB (it pays the growth), or a value of 1 KiB to 8 KiB for which no freed slot of its size is waiting; an overwrite of the latter kind first deletes its own key. One such eviction frees up to eight times what its write needs, and the writes after it use the rest before evicting again.
  - Pinned key classes (section 3.8) are never evicted, and a write to one is never refused: it grows past the cap.
  - `Full` is answered in three cases: a new key in an evictable class whose full segment holds only pinned entries; a write that adds memory once memory passes its share by a quarter, when its segment holds too little to evict for it; and a large value when evicting cannot keep the shard within 16 MiB or an eighth of its share, whichever is smaller, or when it could not stay with every large value gone. The write was not executed.

### 3.4 Lock semantics

Locks are entries whose value is the 8-byte holder token. Section 11 shows how one application's locks and leases map onto them without a new op.

- Tokens are exactly 8 bytes, chosen by the client, unique per acquisition attempt across all holders: 8 random bytes, or a node id plus a counter. Two holders sharing a token would both own the lock. `keep-client` sends a fresh random token with every TryLock and the same token again when it retries that TryLock.
- TryLock:
  - key absent or expired: the server stores the token with deadline now + TTL (rounded up, section 3.3) and answers `Ok`;
  - key held with the same token: the deadline becomes now + TTL and the answer is `Ok`. The request's TTL wins, so a reacquire can shorten the lock as well as extend it, as Renew does;
  - key held with another token: `Conflict`, and nothing changes.
  - So a retried TryLock never reports a lock its caller holds as held, and a holder can extend a lock by renewing, then trying again with the same token.
- The `Ok` value of TryLock is empty. There are no fencing tokens in version 1: fencing tokens do not prevent double grants under Keep's failure model. An application whose protected resource must reject a stale holder needs its own check there.
- Clocks: the server measures a lock's TTL on its raw monotonic clock (section 12.1, Clock), which NTP does not slew. A holder that times its own hold must use a clock in the same model: on a host whose clock is being slewed after a large offset (for example after a VM pause), std's `Instant` can run up to 8.3% slow or fast (chrony's 83,333 ppm, while the offset is at least that share of the TTL); running slow, it underestimates the time held. Renew well before expiry (for example every TTL / 3). `keep-client` keeps no local hold state: every lock outcome is the server's.
- Unlock compares the token, then deletes: `Ok` when it matched, `Conflict` when the key holds another token, `NotFound` when it is absent or expired. Unconditional removal is Delete.
- Renew with a token compares, then sets the deadline to now + TTL (rounded up, section 3.3); same answers as Unlock. Renew without a token sets the deadline of any live key unconditionally (`Ok` or `NotFound`); it serves `keep-client`'s `renew_lock`.
- Maximum lock TTL: the cluster configures `max_lock_ttl`, 600 s by default. TryLock and Renew with a longer TTL get `BadRequest` and are not executed. The value must be at least the longest lock TTL the application uses.
- Quarantine after a failover. A slot whose owner changed without a handoff is in quarantine on its new owner for at least `max_lock_ttl` + 1 s (the deadline round-up) + 2 x the self-fence delay + `max_stop` + a drift term, which with the 1 s self-fence delay is Q = `max_lock_ttl` + 3 s + `max_stop` + max(1 s, (2 x `max_lock_ttl` + `max_stop` + 5 s) / 1,199), the drift term rounded up to the millisecond (it covers two clocks each 833 ppm off real time; a lock death keeps its start across a break in its certificate of up to one self-fence delay; `--max-stop` is the longest stopped-clock pause a cluster member must survive, 30 s by default; 634.031 s by default in a cluster, 3,639.035 s at a 3,600 s `max_lock_ttl`; a single server adds no `max_stop`: 604.006 s by default):
  - TryLock on an absent key in a quarantined slot answers `Conflict` (not executed): the pre-failover holder may still act, which is exactly what `Conflict` tells the caller;
  - Renew and Unlock with a token on an absent key answer `NotFound` (as anywhere else): the holder learns it lost the lock and stops;
  - every other op, and TryLock on a key that is present, behaves as usual.
- A server that lost its majority fences itself and answers `Busy` with scope 1 (section 5.3) to every KV request until its view places a majority again. A slot gated during a handoff answers `Busy` with scope 0. Stream ops are not gated by the lease or placement: Publish, Subscribe and Unsubscribe keep working on a fenced or non-placing server. A turn whose KV output is discarded at flush also drains its subscribers, which the client sees as `Lost` (section 8.3).

### 3.5 Responses (server to client, codes 0x00 to 0x7F)

| Code | Status | Class | Executed? |
|---|---|---|---|
| 0x00 | Ok | outcome | yes |
| 0x01 | NotFound | outcome | yes |
| 0x02 | Conflict | outcome | yes |
| 0x10 | BadRequest | error: well framed, invalid content | no |
| 0x11 | WrongType | error: Incr on a value that is not a counter | no |
| 0x12 | Overflow | error: Incr past the i64 range | no |
| 0x13 | TooLarge | error: value over the server's limit | no |
| 0x15 | Unauthorized | error: before authentication, or not allowed on this listener | no |
| 0x16 | Moved | error: key owned by another server (section 9.4) | no |
| 0x17 | Busy | error: in-flight limit, handoff gate, self-fence, drain (sections 3.4, 5.3, 7.5); `arg` is its scope: 0 this request, 1 the whole server (section 5.3) | no |
| 0x18 | Internal | error: server fault | unknown |
| 0x19 | Full | error: refused at the memory cap (section 3.3), or a Subscribe beyond 4,096 subscriptions on one connection (section 8) | no |

- 0x14 is unassigned.
- Codes 0x10 and above are errors and may carry a UTF-8 message as the value.
- A response has `key_len = 0`, `flags = 0` and `ext_words = 0`.
- Only `Internal` and a connection lost before the response leave the outcome unknown (section 5.4).

### 3.6 Request flags

| Bit | Name | Ops | Meaning |
|---|---|---|---|
| 0x01 | QUIET | all but Hello and Auth | Reply only on error. Fire-and-forget publish or unlock. |
| 0x02 | IF_ABSENT | Set | Set only when the key is absent or expired |
| 0x04 | WITH_TTL | Get | Report the remaining TTL in `arg` |

Any other bit, or a bit on an op that does not take it, gets `BadRequest`. Bit 0x08 is unassigned.

### 3.7 Server pushes (codes 0x80 to 0xFF)

| Code | Push | id | arg | value |
|---|---|---|---|---|
| 0x80 | Event | sequence number | topic | payload |
| 0x81 | GoAway | 0 | status | UTF-8 reason |
| 0x82 | Heartbeat | last sequence number sent | topic | empty |
| 0x83 | End | last sequence number sent | topic | empty |
| 0x84 | TopologyChanged | 0 | low 32 bits of the server's topology epoch | empty |

A push has `key_len = 0`, `flags = 0` and `ext_words = 0`. Heartbeat and End are section 8; TopologyChanged is section 9.4.

### 3.8 Key classes

- A key's class is its first byte. Nothing on the wire names a class.
- The server holds the pinned first bytes as a 256-bit set; every other class is evictable at the memory cap (section 3.3). A handoff also leaves values of at least 64 KiB behind in those classes (section 12.4), and moves pinned values of any size.
- The set is **{0, 1, 14, 15, 16, 17}**, with no setting to change it. An application chooses its key prefixes around it: keys whose loss is unsafe, locks above all, start with a pinned byte. The table lists Stalwart's prefixes (`KV_*`, `crates/common/src/lib.rs` in Stalwart) as an example (section 11).

| First byte | Class in Stalwart | Eviction |
|---|---|---|
| 0, 1 | ACME challenges, OAuth codes | pinned |
| 14, 15, 16, 17 | locks: queue message, task, WebDAV, search index | pinned |
| 2 to 10, 12, 19 | rate-limit counters (RCPT, scan, loiter, auth, SMTP, contact, HTTP authenticated and anonymous, IMAP, availability, OAuth client metadata) | evictable, after non-counters and counters at or below zero, lowest count first |
| 11 | blob upload quota | evictable |
| 13 | greylist | evictable |
| 18 | OAuth client metadata document cache | evictable |
| 20 | key lookup: keyserver rate counter and block key | evictable |
| any other | no Stalwart prefix | evictable |

## 4. Errors

### 4.1 Fatal: framing and protocol violations

- A header that cannot be framed (value over 4 MiB), a frame from the client with a code at or above 0x80, and a malformed Hello are fatal for the connection.
- The server sends `GoAway` with `BadRequest` and a reason, then closes. A client that receives a frame it cannot frame or parse closes the connection.
- The client treats every request still in flight on a closed connection as failed with an unknown outcome.

### 4.2 Per request

- A well-framed request with invalid content (unknown op, unsupported flag, missing or unexpected key, value or arg, a token or delta that is not 8 bytes, a TTL above `max_lock_ttl`) gets an error status carrying its id. The connection stays usable.
- Outcomes (`Ok`, `NotFound`, `Conflict`) are not errors.

## 5. Multiplexing, batching and flow control

### 5.1 Request ids

- The client chooses the id, unique among its in-flight requests on that connection, and may reuse it after the response.
- `keep-client` uses a 20-bit slot index and a 12-bit generation: 1,048,576 requests in flight per connection, so an RTO stall at 300,000 actions/s cannot exhaust the id space (65,536 slots do not suffice), and a late response to an abandoned request cannot complete a newer one. This is internal to the client; the server only echoes the id.
- Responses may arrive in any order, for example when the server forwards a request to another shard. The client matches by id, never by position.
- Id 0 is allowed. Pushes are told apart by their code.

### 5.2 Pipelining and batching

- Any number of requests may be written back to back.
- The server splits each receive into frames, executes them, and writes the replies of one receive batch with one write per connection (one TLS record under TLS), in any order.
- Neither side may delay a frame waiting for more data on an otherwise idle connection.
- Client policy (`keep-client`): callers write directly, a reader task on the caller's tokio runtime completes replies, and flushing is response-clocked with window 1: a caller flushes only while no written batch is unanswered; otherwise its frame waits in the buffer and the reader flushes everything queued when a reply arrives. Window 1 is the setting that sustains 300,000 actions/s; its cost is p50 +13.9% at 100,000 actions/s. There is no dedicated client I/O thread.

### 5.3 Connections, selection and limits

- Connections are multiplexed and long-lived; the client never connects on the request path.
- Unrouted (no secret, or `route=none`): 16 connections per server, a request goes to the less loaded of two connections (`least`). `least` steers new requests away from a connection whose batch is delayed, which keeps the tail low under loss; 16 is the only count within the noise floor of the best p50, p99 and p99.9 under 0.01% loss, at 1.12x the client CPU per op of 4 connections at 20k ops/s. With per-shard routing the client holds `shard_conns` connections per shard (section 9.3). One connection per shard is not the default: with a 500 us escape to another connection its p99 is 1.36 to 1.39x that of `least` at 100k and 300k ops/s, since a request waits for its shard connection's batch in flight (window 1). Two connections per shard give p50 0.87 to 0.89 and server CPU per op 0.63 to 0.72 of `least`, with p99 within the noise floor at every rate, also under loss (32 connections against 16 per server on a 16-shard server). So a client URL with `secret=` routes by shard with 2 connections per shard unless it names `route=`, `conns=` or `select=` (unrouted parameters); `shard_conns=` without them also implies routing, and naming `shard_conns=` with `conns=` or `select=` needs `route=`; `route=none` keeps `least` with 16 connections, and a URL without a secret is unrouted. `ClientConfig::default()` stays unrouted.
- The server announces its in-flight limit per connection in the Hello reply (type 3). Requests beyond it get `Busy`, not executed.
- A client that receives `Busy` retries on another connection or later, with backoff (2 to 50 ms, doubling) until the request's deadline. It never retries in a tight loop.
- `Busy`'s `arg` is its scope. 0 refuses this request only: the in-flight limit, a full output or reply queue, a gated or unplaced slot, a leave freeze, a drain. 1 refuses every keyed request of the server, whose lease is invalid: it fenced itself, its lease lapsed before the fence, or a shard's view is still below the lease floor; it lasts until a view places a majority again. A client reads any other value as 0. Keyless requests (DeletePrefix, Ping, the stream ops) are not lease-gated (section 3.4) and never get scope 1.
- `keep-client` keeps one gate per server for scope 1, shared by the server's connections. A reply with scope 1 closes it. A keyed call routed to a server whose gate is closed does not send: it waits until the gate opens, the topology changes (it routes again, since its key may have moved), or its deadline passes (it then ends as a call refused `Busy` until its deadline does, section 5.4, and was never sent). One waiting call at a time probes the server: 2 ms after the gate closed, then at a backoff that doubles up to 50 ms while the probes draw scope 1; any other answer to a probe opens the gate and wakes every waiting call, and a probe lost with its connection lets the next waiting call probe. Scope 0 keeps the per-call backoff above. So a fenced server draws at most one probe per backoff from each client instead of a retry per waiting call (without the gate, about 8k waiting calls would make about 167k retries/s against a fenced server); `gate_waits` and `gate_probes` in the client stats count waits at a gate and probes sent.

### 5.4 Retries

No hedging and no idempotency token in version 1: `least` already gives p99.9 at the no-loss level under 0.01% loss with 5 ms RTOs, while hedging costs 11 to 16% client CPU, makes p99.9 1.15x to 16.7x worse and duplicates increments.

A client resends a request only when it is safe to repeat, or when it is known not to have executed (`Busy`, `Moved`, `Unauthorized`, `TooLarge`, `Full`, `BadRequest`; `Busy` after a `GoAway`).

A call's outcome is unknown once any of its attempts had an unknown outcome: when a resend after such an attempt ends without executing (`Busy`, `Moved`, no healthy connection) or runs out of time, `keep-client` returns the earlier attempt's unknown outcome, not the resend's error, since the earlier attempt may have executed. DeletePrefix runs on every server in turn; when it ran on some and a later one refuses it, the call's outcome is unknown too (`Cause::Partial`).

A connection that fails (an attempt timeout, silent heartbeats, a protocol error, an I/O error, the end of the stream, a drain past its limit) is reset, not closed: `keep-client` sets `SO_LINGER` 0 and on Linux drops the association at once with `connect` to `AF_UNSPEC`, which sends RST and purges the kernel's send queue (elsewhere the RST goes out when the socket closes). So the kernel never retransmits an attempt the client gave up on, and a resend is not overtaken by its own first attempt. A graceful close lets the kernel deliver it later: a Set aborted across a partition and resent with success on another connection can have its first copy retransmitted when the partition heals and executed after other clients' acknowledged Sets of the key. The residual: bytes that left the client before the reset (on the path, or in the server's receive queue, which an RST does not purge) can still execute after the resend, when the path delays or reorders them past it or the server executes them more than an attempt timeout after they were sent (a stalled shard or process, including a request read in time and forwarded to a stalled shard); a stalled owner that lost its slots no longer serves them once its lease lapses (section 12.1). The client's own close (the client dropped) stays graceful, so QUIET publishes it already wrote still go out. A server that leaves the client's view (section 9.4) is closed by how the new topology lists it, after the client switched to that topology. Listed alive but no longer placing (leaving or joining), its connections drain as after a GoAway: nothing new is written, requests not yet written fail as not executed, and requests already sent take its replies (`Moved` or `Busy` from a server that no longer serves their slots, so a non-repeatable request is resent instead of ending unknown). Each connection closes gracefully once nothing awaits a reply; one whose oldest unanswered request waited one attempt timeout, as on a healthy connection, or that is still open one attempt timeout after the view dropped the server, is reset, and its requests end `Unknown(TimedOut)`. Listed dead, no longer listed, or listed with another layout (a restart), its connections are reset at once, and so are those of a draining server that a later view lists dead or no longer lists; requests in flight end `Unknown(Closed)`, and those safe to repeat (below) are resent.

| Op | Resend after an unknown outcome | Note |
|---|---|---|
| Get, Exists, Ping, Topology | safe | |
| TryLock | safe with the same token | reentrant (section 3.4) |
| Renew with a token | safe with the same token | a resend extends only the token's own lock |
| Unlock | safe with the same token | when the first one executed, a resend answers `NotFound`, or `Conflict` if another holder took the lock in between; the caller's lock is released either way, so `keep-client` returns the unknown outcome instead of "not held" |
| DeletePrefix | safe for the caller's own write | it is not ordered with concurrent writes anyway (DESIGN 1.3) |
| Set without IF_ABSENT, Delete, Renew without a token | not safe | a resend lands after the first execution, so it would undo a write another client made to the key in between, and the operation would take effect twice; returned to the caller as an error with an unknown outcome |
| Incr, Set with IF_ABSENT, Publish, Subscribe, Unsubscribe | not safe | returned to the caller as an error with an unknown outcome |
| Delete as lock removal | not safe | the first Delete may have freed the lock and another holder taken it; a resend would delete that holder's lock and let a third in. The lock ends by its TTL instead |

## 6. Values and limits

| Item | Limit |
|---|---|
| Key | 1 to 255 bytes |
| Value or payload | 0 to 4 MiB on the wire; a server may enforce less and answer `TooLarge` |
| Incr delta and result | i64, exactly 8 bytes |
| Lock token | exactly 8 bytes |
| Hello value | at most 1,024 bytes; longer is a malformed Hello |
| Topology members | at most 4,096 |
| Topic | u32; 0x0000_0000 to 0x0000_FFFF are topics, the rest is reserved (section 8.4) |

## 7. Connection lifecycle

### 7.1 Opening

1. TCP connect, then the TLS 1.3 handshake when encrypted (section 10).
2. The client sends `Hello` first.
3. Under TLS, the connection is ready after the Hello reply. In plaintext mode the client then sends `Auth` (section 10.2), and the connection is ready once Auth is accepted. The client may write its first requests right after Auth in the same write; the server executes them in order.
4. Until the connection is ready, the server answers every request other than Hello and Auth with `Unauthorized`, not executed. A second Hello, or Auth under TLS or after it was accepted, gets `BadRequest`.

New connections cost 0.8 to 3.4 ms at p50 and 18 ms at p99 on Azure, against a 65 us round trip, so clients keep their connections open.

### 7.2 Hello

- Request: `arg` = the highest protocol version the client speaks (1). `value` = client info.
- Reply: `Ok`, `arg` = the version the server will speak on this connection, at most the client's. A server that speaks no version up to the client's answers `GoAway` with `BadRequest`. `value` = server info.
- Client and server info are TLV lists: a type byte, a length byte, then that many bytes. Integers are little endian. Each type appears at most once, and a known type must have exactly its length; a repeated type or a wrong length makes the Hello malformed (section 4.1). Unknown types are skipped, and no meaning depends on the order.

Client info (Hello request):

| Type | Length | Content | When |
|---|---|---|---|
| 1 | 1 to 255 | client software name and version, UTF-8 | optional |
| 2 | 8 | client node id, u64 (the application's node id) | optional, for logs and metrics |
| 3 | 8 | extension words the client understands, u64 bitmap | optional; version 1 defines none |
| 4 | 16 | client nonce for plaintext authentication | required in plaintext mode, ignored under TLS |
| 5 | 2 | wanted shard, u16 | reserved and ignored (section 9.3) |

Server info (Hello reply):

| Type | Length | Content | When |
|---|---|---|---|
| 1 | 8 | server node id, u64 | always |
| 2 | 4 | shard serving this connection, u16, then the server's shard count, u16; shard < count | always |
| 3 | 4 | in-flight limit per connection, u32 | always |
| 4 | 4 | stream heartbeat interval in ms, u32 (section 8.1) | always |
| 5 | 8 | extension words the server understands, u64 bitmap | optional; version 1 defines none |
| 6 | 9 | placement hash id, u8 (1 = SipHash-1-3), then the cluster secret fingerprint, u64 (section 9.1) | always |
| 7 | 48 | server nonce (16 bytes), then the server proof (32 bytes) | plaintext mode only (section 10.2) |

Example (shard 2 of 3, cluster secret `00112233445566778899aabbccddeeff`, types 2 and 6 only): `02 04 02 00 03 00 06 09 01 fc e2 cf a7 2a c9 e1 ad`.

### 7.3 Keepalive and timeouts

- A client sends `Ping` on every connection that sent nothing for 15 s.
- The server closes a connection that sent nothing for 60 s.
- Dead peers with traffic in flight are detected by `TCP_USER_TIMEOUT` (1 s, section 1); idle connections by the ping.
- Connect plus handshake has a 5 s timeout in `keep-client`, of which the TCP connect step gets at most 1 s: Linux retransmits an unanswered SYN after 1 s and 3 s, so a longer step leaves up to 4 s after a partition heals with no SYN on the wire. A connection whose connect or handshake timed out is dropped, never reused.
- `keep-client` sends the ping from its per-server maintenance tick (every 100 ms), on connections with nothing in flight and no write for 15 s (`keepalive`, configurable).

### 7.4 Draining and GoAway

A server that wants a client to leave sends `GoAway`: for shutdown or a rolling upgrade, with `Busy`, and on a protocol or authentication error, which ends only that connection. Ownership moves never send GoAway: a moved slot answers `Moved` per request (section 9.4). GoAway carries no routing data; the client refreshes the topology (section 9.4). Then:

- Every request the server read before sending GoAway is answered normally.
- For a grace period (1 s), the server keeps reading. It executes and fans out every `Publish` that arrives after the GoAway (QUIET or not; a publish has no owner to move) and answers every other request with `Busy`, not executed, Subscribe and Unsubscribe included. Live subscriptions keep receiving events, and Heartbeats when idle; section 8 gives the order of the final End frames. The grace ends after 1 s, or earlier on a shard with no connections left. From then on a shard reads and discards the input of its draining connections, so nothing more executes and unread bytes cause no RST; a QUIET publish that arrives after the grace is lost without a signal. A connection without subscriptions closes once its output is sent; one with subscriptions closes after their End frames (section 8) and the replies to its forwarded requests still outstanding, and at the latest about 1 s after the grace (the deadline counts from the turn that ended the grace), without End if the drain barrier did not complete by then (the client sees Lost).
- A client that receives GoAway sends nothing more on that connection except the QUIET publishes it had already queued, which arrive inside the grace. It resends every request answered with `Busy` elsewhere, except Unsubscribe (the drain's End ends that subscription), and opens a replacement connection with backoff.
- `keep-client` on GoAway: the connection stops taking requests; requests still in its write buffer fail as not executed (retried elsewhere), while QUIET publishes in it are still written; the rest of a write the socket accepted only in part goes out unchanged, so a request in that rest reaches the server after the GoAway and gets `Busy`; replies keep completing until the server closes, and a connection still open 3 s after its GoAway is closed (the server writes its last End about 2 s after GoAway at the latest: the 1 s grace plus the 1 s barrier deadline plus event-loop scheduling delay, normally milliseconds, and later only when the socket is backed up); the replacement is opened once the old socket is gone; in cluster mode the topology is refreshed. A call whose key's owner has no healthy connection (it drains, died or is cut off) while another member is reachable waits for refresh rounds and goes to the new owner once a round moves the key, for at most one attempt timeout from its first wait and within its deadline; a batch shares one deadline, counted from its start, and one such wait, counted from its first failed request. A graceful leave or a rolling restart then shows callers only the wait, and so does a failover that re-places the slots within the attempt timeout. A call that waited without a new owner fails with `Unavailable` (counted in `owner_timeouts`); one for a single server, or with no member reachable, fails at once (`fail_fast`).

Every request on a drained connection therefore ends with a definite outcome. Only an abrupt close (crash, network failure, `TCP_USER_TIMEOUT`, the attempt-timeout watchdog) leaves outcomes unknown, and only for requests already handed to the socket, the rest of a write it accepted only in part included: `keep-client` fails the ones its window still held in the write buffer behind that rest as not executed, so they are resent. A server that loses its lease between executing a batch and sending its replies is such a close: it closes the connections that batch answered (replies to forwarded requests included), and their outcomes are unknown. So is a lock turn whose replies a shard drops after an OS suspend (section 12.1, Clock). Requests that arrive while the lease is already invalid are answered `Busy`, not executed.

### 7.5 Reconnecting

- Exponential backoff from 50 ms, doubling to 2 s, with equal jitter: each wait is half the current step plus a uniform draw over the other half. Every client is healthy again 114 to 140 ms after a server restart, against 674 to 704 ms with fixed steps; the last connections return about 1.55 s after the restart, which costs capacity, not availability.
- Reconnects to one server run in rounds: one connect per missing connection, all of a round in parallel, the next round only after the previous one ended, and the backoff above after a round with a failed connect. Connects are never made on the request path, and a server never has more connects in flight than missing connections. A round whose connects all timed out already waited 1 s per attempt, so the next one starts after the first step (50 ms) instead of a doubled one.
- No TLS session resumption: a resumed TLS 1.3 handshake in rustls still runs the key exchange and cost the server more CPU than a full one (87.7 against 81.9 us). One server brought back 1,024 simultaneous connections in well under 1 s.
- While a connection reconnects, its requests go to the other healthy connections of that server. A routed request for a shard without a connection goes to any connection of the server, which forwards it (section 9.3).

## 8. Streams

- `Subscribe(topic)` starts a subscription on the connection. Each payload published to the topic on that server then arrives as an `Event` frame: `id` = subscription sequence number, starting at 1 and increasing by one per event, wrapping modulo 2^32; `arg` = topic; `value` = payload.
- Ordering: events of one subscription arrive in order. Payloads from one publisher connection keep their publish order. There is no order across publishers.
- Publishing: `Publish(topic, payload)`, usually with QUIET. A payload over the server's value limit gets `TooLarge`, QUIET or not, and reaches nobody.
- Delivery points: the events of one receive batch reach subscribers at the end of that batch. A Publish followed by an Unsubscribe of the same topic in one batch, even on one connection, may therefore miss that subscription: its End comes first. Unsubscribe answers `Ok`, then End with the last sequence at once. That End is not a delivery barrier: it counts only the events the subscriber's shard assigned before it executed the Unsubscribe, so events in flight from other shards of the server, even ones whose publisher already got `Ok`, may be missing without a gap. Only a drain End is a server-wide barrier (section 7.4). `keep-client` unsubscribes from a server only after its 1 s linger (below), which bounds the same race with publishers on older views.
- Scope: subscribe and publish work on every server and shard. A publish reaches every subscription of its topic on the same server, across shards, with one shared payload. There is no server-to-server forwarding, so cluster-wide delivery is a client policy: `owner` publishes and subscribes on the topic's home server: the placing member with the highest rendezvous score of section 9.2 step 4 for the topic id as a u64 point in place of the slot, `everywhere` subscribes on every server and publishes on one.
- Subscriptions belong to their connection; one connection may hold subscriptions to many topics, one per topic, each with its own sequence. It holds at most 4,096 (`MAX_SUBSCRIPTIONS`); a Subscribe beyond answers `Full`, not executed, and an Unsubscribe makes room at once. There is no resume: every Subscribe starts a new subscription at sequence 1, so a client that resubscribes (after a lost connection, a drain or a moved home) treats the gap as possible loss and runs its catch-up.
- Subscriptions on different servers are independent: a connection closing ends only its own subscriptions.
- Drain (section 7.4): the server sends GoAway, then keeps executing publishes and delivering events to its live subscriptions for the grace period. At its end each shard stops executing the input of its draining connections (it reads and discards it) and puts a drain marker on every outbound mesh ring after its last fan-out (the rings are FIFO per pair). A shard sends End for a subscription of a draining connection only once it holds the marker of every shard, its own included, any lost count it owed that subscription is applied (section 8.2), and the subscription's queued events are written or skipped with the usual Heartbeat mark; then it closes the connection, once the replies to requests it forwarded to other shards are written. End's sequence therefore counts every event the server assigned to the subscription, and a subscriber whose last sequence equals it lost nothing published on that server. The barrier runs once per server life, at the shutdown drain that sends GoAway on every connection. A GoAway that ends a single connection (after an error) is not a drain: its subscriptions end without End, which clients see as Lost. If the barrier is not complete 1 s after the grace, the remaining connections close without End, which clients see as Lost.
- A drain moves a publisher to another server: its publishes still in flight reach subscribers through the draining server, its next ones through another, so a subscriber can see them out of order. End is not an ordering point.
- Publishers and subscribers learn a new topology independently, so a subscription that becomes live on a server after the subscription started (a resubscribe after a lost connection or a drain, a new placing member, a moved home) may have missed events published there before: the client reports possible loss and the caller runs its catch-up once the subscription is live there. When a server stops being selected for a subscription (it left the placing set, or the home moved), the client keeps the subscription there for 1 s, so events from publishers with an older view still arrive, and then sends Unsubscribe. The linger is client-side: a server has no timer and keeps every subscription until it is unsubscribed, its connection closes or the server drains.

### 8.1 Loss detection

- A gap in the sequence numbers is loss; the application runs its catch-up, reloading what it may have missed from its own source of truth.
- A gap cannot reveal the loss of the last events before a quiet period, so the server sends `Heartbeat` with the last sequence it sent:
  - on every subscription idle for one heartbeat interval (Hello type 4; 100 ms). keep-server's timer pass serves a 25 ms window, so an idle subscription gets its Heartbeat after 75 to 100 ms;
  - and once at the start of a run of skipped events (section 8.2), so the gap shows without waiting for the next event.
- When the subscription ends cleanly (Unsubscribe or drain), the server sends `End` with the last sequence. After a drain it covers every event the server assigned; after Unsubscribe only those of the subscriber's own shard (section 8).
- A subscriber whose last received sequence is behind a Heartbeat or End has lost events. A subscription that ends without `End` (abrupt close) is treated as loss.
- The interval bounds how long tail loss stays invisible, at 16 bytes per idle subscription per interval.

### 8.2 Slow subscribers

The server never blocks publishers on a slow subscriber.

- Bound: 16 MiB of unsent output on the subscriber's connection. While the output is over it, the server skips that connection's events and advances their subscriptions' sequences.
- The first skipped event of a run writes one Heartbeat carrying the advanced sequence (the immediate mark). Events skipped later in the same run show as the next Event's sequence jump, or in the idle Heartbeat once the output is back under the bound. No Heartbeat is written while the output stays over it.
- Loss inside a server: a shard whose subscribers' events lag more than 16 MiB behind a publishing shard misses that shard's events. Its subscriptions advance their sequence and get a Heartbeat before any newer event, so a gap can appear without the subscriber itself being slow. The signal is the same and so is the remedy: the catch-up. The lost count is applied when it arrives to every subscription of the topic on the receiving shard, including one that started after the events were lost, so a new subscription can open with a spurious gap (a Heartbeat ahead of its first event). That costs one needless catch-up; a new subscription may have missed earlier events anyway.

### 8.3 Client behaviour (`keep-client`)

- One stream connection per server, beside the KV connections, opened on first use. It carries the client's Subscribe and Unsubscribe and its publishes, so one client's publishes to a server stay in order. `publish` uses QUIET: it returns once the frame is queued, errors the server reports later are only counted, and a frame lost with its connection before the server read it is lost without a signal to anyone. `publish_confirmed` waits for the server's `Ok` (the server accepted the event for fan-out to every subscription of the topic it holds; the fan-out to other shards happens when that turn flushes, and a turn whose output is discarded shows its subscribers Lost) and returns the error otherwise, `Unknown` included, for callers that must not lose an event silently; it is one attempt, so the caller decides whether to repeat (a repeat can deliver twice). Both share the stream connection and its order.
- Placement of the stream connection (`feeds=`, client-side, no wire change): `random` (the default) keeps the connection on whichever shard the server put it. `spread` places it on shard `node mod S` and `feeds=k` (an integer 1 to 65535) on shard `node mod min(k, S)`, where S is the shard count of the Hello reply and `node` comes from `node=<u64>` (the application's node id; without it the client draws one at random and reports `feed_node_drawn`). The client reconnects until a Hello names the target shard, for at most 7 x S connects per placement (99.9% placed) and at most half the attempt timeout since the placement began, so placement never makes `subscribe` miss its timeout; when either runs out or a connect fails, it keeps the last connection it opened and counts `feed_misplaced`, as it does for a Hello without shard information. A connect still in flight at the time limit is dropped; the first connect, which learns S, is never cut. `feed_misplaced` counts the placements that ended off target since the client started; it does not say where a feed is now. Placement happens only when the stream connection (re)connects: nothing moves a live connection, and the publish and delivery paths are unchanged.
- `stream=everywhere` (default) subscribes on every placing member and publishes on the placing member with the highest rendezvous score (section 9.2 step 4) for a random point chosen per client, which spreads publishers; a publisher sends to one member at a time, so a subscriber sees one publisher's events in order while that publisher stays on one member, and no order holds across members (a topology change can move the publisher). `stream=owner` subscribes and publishes on the topic's home. A static client uses its one server.
- A stream connection holds at most 4,096 subscriptions (section 8). A Subscribe beyond waits in the client until an Unsubscribe or End on that connection makes room, so the client never draws the per-connection `Full`; `subscribe` fails with `Unavailable` when no server took the subscription within one attempt timeout. A Subscribe the server refuses (`Full` at the memory cap, `Busy`) is sent again after 250 ms.
- One subscription per topic per client. A subscription delivers, in order per server: Event (server, sequence, payload); Gap (events missed: a sequence jump, or a Heartbeat or End ahead of the last event); Lost (the connection closed without End); Resubscribed (live on a server again, or newly after `subscribe` returned); Ended (End: after a drain nothing was lost; after the client's Unsubscribe, events in flight from other shards may be missing). Gap, Lost and Resubscribed mean catch-up. `subscribe` returns when every selected server took the subscription or after one attempt timeout, and fails only when none did.
- Each subscription buffers at most 16 MiB of undelivered events; beyond that the client drops events, and the next event or Heartbeat of that server shows the gap. Heartbeats are dropped first; lane changes (End, Lost, Resubscribed) are always kept.
- A connection with a live subscription hears an event or a Heartbeat at least once per heartbeat interval (section 8.1), so the client drops it as lost when it read nothing for 3 intervals and at least one attempt timeout (1 s by default): its subscriptions report Lost, and resubscribe on the replacement. A subscribe-only client therefore notices a silent partition in about a second, not after the 15 s keepalive.
- A subscription that ends on a connection without End (the connection closed, or the client dropped it while its Unsubscribe was in flight) reports Lost.
- `publish` fails at once, not executed, while the stream connection holds 16 MiB of unsent data (a server that stopped reading). In `everywhere` mode a publisher whose server has no healthy connection (it drains, died or is cut off) publishes through the next placing member by rendezvous score that has one, since every placing member reaches every subscriber; when none has one yet, it opens the next members in that order that the client reaches (a healthy KV connection) and waits for one within the call's connect timeout (at most the call deadline), failing at once when none is reachable, and a publish its connection refused unsent (a GoAway or a lost socket between choice and send) is sent on another, so a graceful leave or rolling restart costs publishers no errors. `owner` mode fails until the home is back or moves.
- The reader hands each run of consecutive events of one topic in a receive batch to the subscription under one lock, and the subscription takes everything queued in one swap: no allocation per event.

### 8.4 Topics

- `arg` carries a u32 topic. The application chooses its topic numbers; the server gives none a special meaning.
- Topics 0x0000_0000 to 0x0000_FFFF are topics; the rest of the range is reserved for interest filters and gets `BadRequest` in version 1.
- One connection subscribes at most 4,096 topics (section 8).

## 9. Cluster routing

### 9.1 The cluster secret

- Every Keep server and every client node has the same cluster secret from its configuration; it never crosses the wire.
- The secret is 16 bytes, written in the configuration as 32 hex digits. The first 16 digits are `k0` and the last 16 are `k1`, each a u64 written most significant digit first.
- It keys placement (SipHash-1-3 with `k0`, `k1`, section 9.2) and plaintext authentication (HMAC-SHA-256 with the 16 bytes the hex spells, section 10.2).
- Fingerprint (Hello type 6): SipHash-1-3 with `k0`, `k1` over the 29 ASCII bytes `keep placement fingerprint v1`. A client routes by key only when the hash id and the fingerprint equal its own. A client whose secret differs stops routing and stays correct, because every connection accepts every key its server owns (section 9.3). Example: secret `00112233445566778899aabbccddeeff` has fingerprint `0xade1c92aa7cfe2fc`.

### 9.2 Placement

One function, implemented once in `keep-proto` and used by the server, the client, the load generator and the simulator. Hash id 1 is the only one defined; unkeyed hashes are never used for anything computed over client-chosen keys.

1. `h` = SipHash-1-3 with keys `k0`, `k1` over the key bytes, as u64.
2. `slot` = `h >> 50` (the top 14 bits; 16,384 slots).
3. Placing members: the topology members with status active and liveness not dead, sorted by node id ascending.
4. Owner of a slot: the placing member with the highest `fmix64(slot ^ fmix64(node_id ^ 0x6b65_6570_6872_7731))`; on a tie, the one with the lowest node id. `fmix64` is the MurmurHash3 64-bit finalizer: `x ^= x >> 33; x *= 0xff51_afd7_ed55_8ccd; x ^= x >> 33; x *= 0xc4ce_b9fe_1a85_ec53; x ^= x >> 33` (wrapping).
5. Shard within the owner: `((h mod 2^32) * shard_count) >> 32`.

Rendezvous over slots moves only the slots of a member that joins or leaves (measured: amplification 1.000, 10.5 ns per lookup at 41 B and 32 servers, table rebuild 0.53 ms). Taking the shard from the key bits instead of the slot balances shards 1.02 to 1.15 against 1.13 to 1.51.

### 9.3 Shards and per-shard connections

- One listening port per server, not one per shard. Shard counts follow each server's CPU budget, and one port works with firewalls, Kubernetes Services and RX-queue steering. With steering, the shard pinned to the CPU that took the SYN serves the connection.
- The Hello reply (type 2) names the shard serving the connection. A routing client opens connections, in parallel and bounded, until it holds `shard_conns` per shard (2 by default; for one per shard, on average N times the N-th harmonic number connects: about 22 for 8 shards, 54 for 16), and closes the extra ones. `keep-client` stops after 8 connects per missing connection slot (shards times `shard_conns` slots) and leaves the missing shards' keys to forwarding; it tries again after one of the server's connections is lost, or after about 1 s (with the reconnect jitter), doubling up to 30 s while rounds keep running out (with one connection per shard and 8 connects per missing shard, about 0.4% of starts on a 16-shard server miss one shard), and `Client::connect` does not wait for the missing shards. With `shard_conns=n` (2 by default under shard routing, at most 8, only with shard routing) it holds n connections per shard, and a request takes the less loaded healthy one of its shard, so a batch in flight or a stalled segment on one does not hold the shard's next requests.
- Every connection accepts every key its server owns. A key owned by another shard is forwarded inside the server and answered on the arriving connection, possibly out of order. A client that does not route, routes with a stale view or a different secret, or reconnects is therefore always correct; routing saves the hop (about 15 us) and server CPU (forwarding costs +20 to 36% p50 and 1.25 to 1.74x CPU at the demand points of the mail-server workload).
- The Hello request's wanted shard (type 5) is reserved and ignored: a handoff to another shard would discard the RX locality steering provides.

### 9.4 Topology and Moved

- `Topology` (0x22) returns the server's current view:
  - epoch, u64; member count, u32 (at most 4,096);
  - then 30 bytes per member: node id (u64), address (16 bytes, IPv6 or IPv4-mapped), client port (u16), shard count (u16, at least 1), status (u8: 0 active, 1 joining, 2 leaving), liveness (u8: 0 alive, 1 suspect, 2 dead). Any other status, liveness or a shard count of 0 makes the topology malformed.
- Epochs count per server life: a server numbers its views from 1 each time it starts. Epochs of two servers, or of two lives of one server, are not comparable, so a client compares an epoch only with the epoch it last fetched from the same server, and forgets that one whenever it opens a connection to the server (a new life may reuse the number).
- Clients compute placement (section 9.2) over the placing members of the topology the servers agree on. A refresh fetches the topology from every placing member of the client's view (from the seeds before it has one) and groups the answers by placing members (node id, address, shard count). Answers with the same placing members and the same member list form one group; each server votes once per refresh, with its latest answer: a server that announces another epoch (`TopologyChanged`) during the refresh is asked again and votes with that answer. A topology is agreed when more than half of its own member list reports it, a reporter counting when its address is in that list, whether it places or not (Leaving members report the set they hand over to). A serving server is confirmed by a majority of its list (heartbeats of another member list are refused, 12.1), and members that hear each other report one placing set, so the view of a fenced or cut-off minority, whatever its epoch, never wins. While every member-list change follows 12.1's rule, two lists never both have a majority; a change of two members at once can break that, and the client then follows the group with more reporters. The order: agreed first, then more reporters, then the placing set already installed, then the larger placing set, then a fixed order. A refresh acts once the answers in hand decide it, checked after every answer or failure: a topology is agreed and its reporters outnumber those of any other group even if every server still being asked joined that group. It does not wait for the rest (a frozen or powered-off member, whose socket neither answers nor resets, would hold it for the attempt timeout), and their answers still record their epochs when they come. Survivors adopt the view without a silent member a few heartbeats apart; the re-asking above lets the refresh started by the first one's push complete on the second one's. When none is agreed, the client also asks its seeds and the members the answers name that it did not ask (at most 64); still without agreement it keeps its view, and a client without one installs the topology with the most reporters. A refresh without agreement in which a server did not answer (a restarted member not reconnected yet, or an unreachable seed or member asked, each given 250 ms to connect, which assumes clients in the cluster's region) is repeated, 8 rounds 50 ms apart and then once a second while it lasts, and a server that missed a refresh asks for another when the client reconnects to it.
- `Moved` is only for a key owned by another server. `arg` = the owner's index in the member list of the topology at that epoch; `value` = that epoch (u64). When the epoch is not the one last fetched from that server, the client refreshes, waits for the refresh at most one attempt timeout, and then resends; when it is the same, the client has already weighed that server's view and resends with the `Busy` backoff, or after the refresh under way if another signal started one. The owner's address always comes from the topology, never from `Moved`.
- `TopologyChanged` (0x84) announces a new epoch without a request; a server sends it on every client connection when it adopts a view, and the client refreshes when its low 32 bits differ from those of the epoch last fetched from that server. When a member stops answering without closing its connections, its survivors' push is what moves its keys: the refresh installs the view without it, which closes the client's connections to it, so requests still unwritten there are resent to the new owners (section 7.4).
- Refreshes are rate limited in the client (without a limit, one detection window can produce about 317 refreshes in 0.4 s), and every refresh closes the connections to servers that left the placing set as section 5.4 describes.
- Failover: membership by fixed heartbeats every 25 ms, dead after 225 ms of silence; every server and client computes slot owners locally; no consensus. A slot that changed owner without a handoff is quarantined (section 3.4).

## 10. Authentication

Every connection is authenticated, in every mode.

### 10.1 TLS mode (the default)

- Mutual TLS 1.3 with raw public keys (RFC 7250, rustls 0.23.16 and later): each Keep server and each client node has a key pair, and the cluster configuration lists the allowed public keys. No certificate authority to run. Both ends offer only the RawPublicKey certificate type (`client_certificate_type` and `server_certificate_type`); a peer that presents X.509 is refused.
- Keys: any key the rustls aws-lc provider loads; Ed25519 is recommended (`openssl genpkey -algorithm ed25519`), ECDSA P-256 and P-384 work. A private key is a PEM file (PKCS#8 or SEC1) or DER; the own public key is derived from it, never configured separately.
- Allowed keys: a PEM file of `PUBLIC KEY` blocks (SubjectPublicKeyInfo DER, `openssl pkey -pubout`), one per peer. There are two lists: server keys, which clients accept and servers accept on the peer port, and client keys, which servers accept on the client port, so a client node's key cannot pose as a Keep server. A peer is accepted when its SubjectPublicKeyInfo equals a listed one byte for byte and its TLS 1.3 CertificateVerify verifies with that key. Keys are not pinned per address: any listed key is accepted on any connection.
- Certificates from a cluster CA are the configuration alternative, also mutual: the server requires a client chain that leads to the cluster CA (`--tls-ca`), and the client checks the server's chain and name. The peer port has its own anchor (`--tls-peer-ca`, required on cluster members): both ends require the other's chain to lead to it, and the dialing member checks the dialed member's peer IP address as the name, so a member certificate needs its peer IP address as an IP SAN. The peer anchor is a CA that issues only member certificates, such as an intermediate under the cluster CA, so a client node's certificate cannot pose as a Keep server, as with the two key lists of `rpk`. When `--tls-peer-ca` names the `--tls-ca` file, any certificate from the cluster CA completes TLS on the peer port; member identity then rests on the peer secret (section 12), and the peer port must be reachable only from members.
- In both TLS modes a client that offers no key or certificate is refused.
- TLS modes send no client nonce (Hello type 4) and no `Auth`; the Hello reply carries no type 7, and a client refuses a reply that does. TLS authenticates both ends.
- Configuration. Client URL: `tls=rpk&key=PATH&servers=PATH`, or `tls=ca&ca=PATH[&sni=NAME][&cert=PATH&key=PATH]`; without `tls` the client uses plaintext mode, and reports "server requires TLS" when a server answers its Hello with a TLS record. Server flags (DEPLOYMENT 2): `--tls rpk|ca|off`; `rpk` takes `--tls-key`, `--tls-clients` and, on cluster members, `--tls-servers`, which must list this server's own key; `ca` takes `--tls-cert`, `--tls-key`, `--tls-ca` and, on cluster members, `--tls-peer-ca`. A `--tls-*` flag the mode does not use is refused at startup. `--tls` defaults to `rpk`, so a server started without its key files refuses to start; `--tls off` selects plaintext mode with `Auth` (section 10.2).
- Records. The server decrypts everything one receive holds before it parses frames, and seals a connection's output of one loop turn in one pass, in records of up to 2^14 bytes of plaintext (RFC 9846 5.1); output a turn discards is never sealed. A record header announcing more than 2^14 + 256 bytes ends the connection with a `record_overflow` alert; a handshake message declaring more than 0xffff bytes ends it too (rustls 0.23 sends no alert for that one). While the handshake runs, a server keeps at most 6 records of unfinished input per connection (a 4 + 0xffff-byte handshake message can span 6 records) and one record afterwards; more closes the connection.
- Key updates. Each end refreshes its sending keys before the cipher's confidentiality limit and asks the peer to refresh too; an end that receives a KeyUpdate with `update_requested` sends its own before its next application data record (RFC 9846 4.7.3). rustls closes a connection that sends more than 32 KeyUpdates without application data in between.
- Closing. A server that drains a connection sends close_notify after its last frame (the GoAway and any End frames, section 7.4), then closes its write side (RFC 9846 6.1). A connection closed for an error, a timeout or a reset gets no close_notify.
- rustls has no TLS 1.3 external PSK, so there is no shared-secret PSK mode.
- No 0-RTT early data: every request would need replay protection (RFC 9846 section 8), and rustls accepts 0-RTT only with stateful tickets. No session tickets (section 7.5).

### 10.2 Plaintext mode

A challenge and response over the cluster secret, mutual, with one Auth frame after the Hello:

1. Client: `Hello` with a fresh random 16-byte client nonce `cn` (type 4).
2. Server: a fresh random 16-byte server nonce `sn` and the server proof `HMAC-SHA-256(secret, "keep auth server v1" || cn || sn)` in the Hello reply (type 7).
3. Client: checks the server proof in constant time and closes the connection if it differs or the reply has no type 7 (wrong cluster or an impostor). Then sends `Auth` with the client proof `HMAC-SHA-256(secret, "keep auth client v1" || cn || sn)` as its value, followed by its first requests if it has any.
4. Server: checks the client proof in constant time. On a match the connection is ready and Auth gets `Ok`; otherwise it sends `GoAway` with `Unauthorized` and closes.

- `secret` is the 16-byte cluster secret (section 9.1); labels are ASCII without a terminator; `||` is concatenation.
- The labels keep a server proof from ever passing as a client proof, and the server nonce keeps a recorded client proof from being replayed on another connection.
- Without TLS, traffic after authentication can be read and hijacked on path. Plaintext mode is for networks where that is acceptable.

## 11. Example integration: Stalwart

This section is an example of how an application such as Stalwart maps its stores onto Keep's operations. Stalwart mail server clusters need locks and leases, rate-limit counters, one key class per kind of short-lived state (section 3.8) and one broadcast topic.

Stalwart's in-memory store API (`crates/store/src/dispatch/lookup.rs` in Stalwart, Redis backend `crates/store/src/backend/redis/lookup.rs`) maps onto existing ops; no op is added. Keys are `KeyValue::build_key(prefix, key)`, so the first byte is the class (section 3.8). Lease and lock tokens are Stalwart's `u64`, sent as its 8 little-endian bytes.

| Stalwart call | Keep requests | Result |
|---|---|---|
| `try_lease(key, token, ttl)` | TryLock with the token | `Ok` true, `Conflict` false |
| `renew_lease(key, token, ttl)` | Renew with the token | `Ok` true; `NotFound`, `Conflict` false |
| `release_lease(key, token)` | Unlock with the token | `Ok` true; `NotFound`, `Conflict` false |
| `try_lock(key, ttl)` | TryLock with a fresh random token per call; a retry of the same call resends the same token | `Ok` true, `Conflict` false |
| `renew_lock(key, ttl)` | Renew without a token; on `NotFound`, TryLock with a fresh token | `Ok` done; `Conflict` from the TryLock is an error |
| `remove_lock(key)` | Delete, sent once (section 5.4) | done either way; an unknown outcome is an error and the lock ends by its TTL, as when a Redis `DEL` fails |
| `key_set(kv)` | Set, TTL = `expires`, none without | |
| `key_incr(kv)` | Incr, TTL = `expires` (on creation only), none without | new value |
| `key_get`, `key_exists` | Get, Exists | value or none; bool |
| `key_delete(key)`, `counter_delete(key)` | Delete | `Ok` true, `NotFound` false |
| `is_rate_allowed(prefix, key, rate)` | Incr by 1 on the window bucket key, TTL = rest of the window | compared with the rate by Stalwart |
| `rate_reset(prefix, key, rate)` | Delete of the window bucket key | |
| `key_delete_prefix(prefix)` | DeletePrefix to every server | returns when every server accepted it; the purge completes in bounded steps after that (section 3.3), while Redis's `SCAN` and `DEL` loop has removed the keys that existed by the time it returns |
| `key_set_many`, `key_get_many`, `key_unlink_many` | one pipelined batch of Set, Get or Delete frames per server, in one write | nothing is atomic, as with Redis pipelines; keys are sent as given, so a key over 255 bytes fails the batch (swap keys are under 64 bytes) |

Notes:

- Stalwart's Redis Lua script refreshes a counter's TTL on every `key_incr`; Keep's Incr sets it only on creation (section 3.3), so a mapping has to allow for the difference.
- `try_lease` in Redis is `SET NX EX` and not reentrant; Keep's TryLock is reentrant for the same token. That is what `TaskLock::extend` relies on (renew, then `try_lease` with the same token after the lease expired), and it makes a retried TryLock safe.
- `renew_lock` in Redis is an unconditional `SET EX`, which re-creates an expired lock and overwrites another holder's. The Keep mapping behaves the same where it matters: Renew without a token extends whatever lock is live, another holder's included, so both callers believe they hold it, as on Redis; an expired lock is re-created with a fresh token; `HeldByOther` comes only when another holder took the key between the Renew and the TryLock.
- Keep has no fencing tokens (section 3.4); Stalwart's critical sections read before they write.
- Stalwart's TTLs need no change: task leases of 300 s renewed every 100 s, queue message locks of 600 s never renewed, search index locks of 600 s renewed when under 60 s remain. After a server crash, about 1/N of the queued messages and index partitions wait up to about 10 minutes longer for their lock (the quarantine).
- The multi-key calls are the ones Stalwart's cache swap tier makes. Its chunks are large values: they leave first at the memory cap (section 3.3), and from 64 KiB they stay behind in a handoff (section 12.4), so a member's restart breaks most snapshots with several chunks and the next read rebuilds them from the data store. Such traffic belongs on a client of its own, because a connection answers in order: a small operation sharing it waits for the batch in flight. It also belongs in calls of at most 1 MiB and chunks of at most 1 MiB, because a shard runs one operation at a time and a small operation waits for the copy in progress.
- Lease renewal in Redis is a Lua script (`GET` equals the token, then `EXPIRE`) and release another (`GET` equals the token, then `DEL`); Renew and Unlock with the token do the same in one op each.
- TTLs: Stalwart passes whole seconds as `u64`. An `expires` of 0 is refused before anything is sent, as Redis refuses `EX 0`; values above 2^32 - 1 saturate.
- Keys longer than 255 bytes (only admin-defined expression and Sieve keys and ACME tokens can be; every other Stalwart key is at most 41 bytes, login, recipient and greylist keys being blake3 hashes) are sent as their first 223 bytes followed by the 32-byte blake3 hash of the whole key. Prefix deletes of up to 223 bytes still cover them.
- A counter read with `key_get` returns Keep's i64 encoding, not Redis's decimal text; only an expression `key_get` of a key that an expression `counter_incr` wrote can see the difference.

Admin-defined keys without a prefix (expressions, Sieve) take whatever class their first byte names.

Cluster stream (Stalwart's coordinator, `crates/coordinator`):

| Stalwart call | Keep | Notes |
|---|---|---|
| default coordinator, with Keep as its in-memory store | the same client carries KV and stream | |
| `publisher("stwt.agora")`, `Publisher::publish` | Publish to topic 0, confirmed (`publish_confirmed`, section 8.3) | `Ok` once the server accepted the event for fan-out; any error, `Unknown` and `TooLarge` included, keeps the batch in Stalwart's backlog and the publisher retries it with backoff, as after a failed Redis `PUBLISH`. A repeat after `Unknown` can deliver a batch twice, which cache invalidations tolerate. QUIET publishes are not used here: a frame lost with its connection showed no gap anywhere, so no peer caught up |
| `subscribe("stwt.agora")` | Subscribe on every server the stream mode selects | returns when every server took it or after one attempt timeout |
| `PubSubStream::next` | Event gives the payload; Gap, Lost and Resubscribed ask for catch-up; Ended is skipped | the subscriber runs its existing `catch_up` (invalidate local caches, refresh queues and tasks) without dropping the subscription; Redis triggers the same catch-up only by resubscribing |

The mapping gives Stalwart's cluster broadcast topic 0. Other topic names have no number and are refused. Stalwart batches up to 100 events and 512 KiB per payload; its measured payloads are 3 to 17 B. Stalwart keeps its configuration, the cluster secret included, in its shared data store, so every node has the secret at no operational cost.

Over this mapping, Stalwart's in-memory store differs from its Redis backend in these ways:

- Opening the store connects at once and retries for up to 30 s while Keep refuses connections; Redis's pool connects lazily. Start Keep first; a store that fails to open leaves Stalwart without an in-memory store until it restarts.
- Repeatable ops are resent within the client's deadline (5 s) and fail with `Unavailable` while the key's server has no healthy connection: at once for a single server; in a cluster after waiting up to one attempt timeout (1 s) for a refresh to give the key another owner (section 7.4); the Redis backend never resends. Writes without a lock token (`key_set`, `key_incr`, `key_delete`, `counter_delete`, `rate_reset`, `renew_lock`'s Renew, `key_set_many`, `key_unlink_many`) end with an unknown outcome instead, as with Redis.
- A server blip costs each node two catch-ups (Lost, then Resubscribed), against one resubscribe with Redis.
- Keep-side requirements: `max_lock_ttl` of at least 600 s, Stalwart's longest lock TTL (queue message and search index locks; task locks are 300 s leases, and the 3,600 s `DEFAULT_LOCK_EXPIRY` is a local deadline that never reaches the store), or lock and lease calls answer `BadRequest` (a fresh cluster started with `rejoin` also refuses new locks for the quarantine, DESIGN 1.1, Start mode); keep the value limit at least 512 KiB plus header for Stalwart's broadcast payloads (default 4 MiB).

## 12. Server-to-server frames

Codes 0x30 to 0x3F are valid only on the peer port and are defined by `keep-cluster`. Version 1 assigns 0x30 to 0x36 (section 12.3); 0x37 to 0x3F are reserved, and on the peer port end the connection. On the client port 0x30 to 0x3F are unknown ops (`BadRequest`, section 3.1). Version 1 has no stream forwarding between servers.

Peer secret: every Keep server has a 16-byte `peer_secret` from its configuration (32 hex digits), separate from the cluster secret, never given to clients and never sent on the wire. Clients hold the cluster secret, so nothing keyed by it can authenticate a server. The peer secret keys the membership heartbeat MAC and the peer port handshake, in every transport mode; under TLS the peer port also requires a key from the server key list (section 10.1).

- Peer handshake proofs: `HMAC-SHA-256(peer_secret, label || initiator hello || acceptor hello)`, where each hello is the 44-byte PeerHello value exactly as sent (section 12.3: sender, receiver, incarnation, member list digest, nonce). The initiator's hello comes first on both sides and for both proofs; the acceptor's hello is the one it sends. The nonces are covered as the last 16 bytes of each hello and are not added again. Labels `keep peer initiator v2` and `keep peer acceptor v2` (ASCII, no terminator), distinct from the client labels of section 10.2 so no proof passes for another role (`keep_proto::PeerSecret::transcript_proof`, `verify_transcript`, `PEER_HELLO_LEN`). Binding both hellos stops an on-path party without the secret from splicing two members' handshakes (relaying one member's acceptor proof under a forged sender), which proofs over the nonces alone allowed. Example: peer secret `00112233445566778899aabbccddeeff`, initiator hello `0100000000000000020000000000000007000000080706050403020101010101010101010101010101010101` (sender 1, receiver 2, incarnation 7, digest 0x0102030405060708, nonce sixteen 0x01), acceptor hello `0200000000000000010000000000000009000000080706050403020102020202020202020202020202020202` (sender 2, receiver 1, incarnation 9, same digest, nonce sixteen 0x02): initiator proof `fbab3dda1be7234dcf933cd14b8cb4c9c9c99cbb01a331c865b37faae7f1d113`, acceptor proof `70244acd37503ddc3172891454797cddfaf92e45c75ce5485959bc964d5922a3`.
- Each side of a peer connection uses a fresh random 16-byte nonce per connection; the handshake is section 12.2.

### 12.1 Membership heartbeat (UDP, server to server)

Implemented in `crates/keep-cluster/src/membership/` (`datagram.rs`, `mod.rs`). Every 25 ms each server sends every other configured member one datagram addressed to it. Integers are little endian.

| Offset | Size | Field |
|---|---|---|
| 0 | 2 | `KH` |
| 2 | 1 | version, 1 |
| 3 | 1 | kind, 1 (heartbeat) |
| 4 | 8 | sender node id |
| 12 | 8 | receiver node id |
| 20 | 4 | incarnation |
| 24 | 4 | sequence |
| 28 | 4 | echo incarnation |
| 32 | 4 | echo sequence: with the echo incarnation, the receiver's (incarnation, sequence) as last accepted by the sender; zero without the echo flag |
| 36 | 4 | uptime in ms, saturating |
| 40 | 4 | cluster age in ms, saturating: the oldest uptime the sender knows of, aged on its clock |
| 44 | 1 | status (MemberStatus wire value) |
| 45 | 1 | flags: bit 0 the echo is valid; bit 1 the sender confirms the receiver, only with bit 0; bit 2 the sender hears a far list (rule Far lists below), from its own reception within F, never from another server's bit 2; other bits zero. Senders set it only while they hear a far list |
| 46 | 2 | member count n |
| 48 | 8 | member list digest |
| 56 | ceil(n/8) | routing bitmap: members the sender heard within the heartbeat timeout |
| then | ceil(n/8) | lock bitmap: members heard within the lock window |
| then | 8 | tag |

Bit i % 8 of byte i / 8 is the member with the i-th smallest node id; unused bits are zero. Length: 56 + 2 x ceil(n/8) + 8 bytes (66 B at 5 members, 192 B at 512).

Keys: base = SipHash-2-4 with k0 = `peer_secret` bytes 0 to 7 and k1 = bytes 8 to 15, each read little endian; k0' = base(`keep membership key v1 k0`), k1' = base(`keep membership key v1 k1`) (ASCII). The tag is SipHash-2-4(k0', k1') over every byte before it. The member list digest is the wrapping sum modulo 2^64, over the members, of SipHash-2-4(k0', k1') over `keep member list v2` followed by the member's node id as u64: it does not depend on order, and two lists one member apart differ by that member's term. PeerHello carries the same digest.

A receiver refuses a datagram with a wrong length, magic, version, kind, tag, status, flag combination or unused bit; whose member count or digest differs from its own; whose receiver is not itself or whose sender is not another member; or whose (incarnation, sequence) is not above the last one accepted from that sender. A heartbeat refused only for its member count or digest, once authenticated and addressed to this server (a misaddressed one is ignored before its list or flags are read), is still read for both and for its bit 2, unless it is a replay: its (incarnation, sequence) is at or below the last one recorded for that sender, or the sender is a member of this server's list already heard at that incarnation or a newer one. The floor of a sender on the receiver's own list (its last accepted heartbeat) never expires while the receiver runs; the floors of foreign senders do not expire either, except that the receiver keeps at most 4,096 (`MAX_MEMBERS`) and a new sender replaces the least recently heard one, whose floor is dropped. So each captured heartbeat is read at most once per run of the receiver, unless more than 4,096 foreign senders were heard since.

Rules:

- Windows: a member hears a peer while it accepted a datagram from it within the heartbeat timeout F = 225 ms (routing); the lock window W is the self-fence delay + 250 ms (1.25 s by default).
- Listen: a server with peers sends no heartbeat at start until it has received one from every other member, or it has heard at least one member and every member it has not heard is reported unheard by every member it heard (a down member costs no wait), or F has passed. A restart into a live cluster ends this within about one interval; when every member starts cold at once, each waits the full F. A renewal during the listen window (below) sends nothing early: the first heartbeat still waits for the listen to end and carries sequence 1 of the new incarnation.
- Fresh reports: a peer's bitmaps vote only while its last datagram echoes the receiver's current incarnation and a sequence the receiver sent within F, on the receiver's own clock. A reporter whose uptime is below the window does not vote; a server counts its own observation only after running for the window.
- Verdicts: routing death when a majority of the members report not hearing X in the routing bitmap; lock death by the same majority rule on the lock bitmap.
- Confirmations: a server sets the confirm flag for X only if it hears X and reported X unheard to nobody within F; a new process confirms nobody during its first F. X credits a confirmation at its own send time of the echoed heartbeat, and only if the echo carries X's current incarnation and one of its last 64 sequences.
- Lease: a server serves until the majority-th most recent credit, itself included, plus the self-fence delay (1 s). Each lapse re-quarantines its own slots, and so does a first contact with a cluster older than the quarantine, read from the cluster age.
- Status: Leaving is final for a life. A server places itself as Active only while every member that is not lock-dead has echoed a heartbeat at or after the one that announced Active; a member whose life never accepted this server's heartbeats counts as acknowledging. The rule is standing, not one-time. An echo acknowledges only a heartbeat this run sent at or after the announcement: announced <= echo <= (incarnation, last sequence sent); an echo of a newer earlier run never acknowledges when that run had a higher incarnation, or the same incarnation while this run's sequence is still below the echoed one. An earlier run with the same incarnation whose peer is first heard after this run's sequence passed the echoed one is taken for this life continuing: no restart is seen and its echo acknowledges. That needs two starts within one wall-clock second (or a clock stepped back onto it), and it stays safe through the handoff gate and the rejoin quarantine.
- Incarnation: a run starts at `--incarnation`, by default the wall-clock second, which is only a starting point. Peers refuse heartbeats not above the last pair they accepted from that member, but keep echoing that pair. So while a server does not place itself (it is not acknowledged as Active) and is not Leaving, a peer's echo above the last (incarnation, sequence) it sent comes from an earlier run of this server whose incarnation was not lower (a clock stepped back, or two starts within one second): it continues as incarnation = the highest such echoed incarnation + 1, sequence 1. When it is Active, that first heartbeat counts as its activation and its uptime restarts; peers see a restart. A sequence overflow continues the same process with incarnation + 1 and sequence 1.
- Clock: every server time is CLOCK_MONOTONIC_RAW, on Linux the oscillator's rate, which no NTP daemon and no `adjtime` changes (chrony slews CLOCK_BOOTTIME and CLOCK_MONOTONIC at up to 83,333 ppm); a platform without it does not build or is refused at startup. A raw clock within 833 ppm of true time is a deployment requirement: chrony corrects up to 500,000 ppm, so NTP does not reveal a worse clock; keep-server logs its clock source, and logs when the kernel's correction goes above 500 ppm and when it comes back, with the peak (the TSC clocks of the Azure VMs Keep was tested on run within 35 ppm). The raw clock stands still during an OS suspend: the control thread reads CLOCK_BOOTTIME minus CLOCK_MONOTONIC on every turn, BOOTTIME first, and a rise of 1 ms or more is a suspend, declared a clock loss (below). Until then shards check their own lock turns: a shard turn that executed a lock operation (TryLock, Renew, Unlock) reads the boot clock before flushing its replies and, when it ran ahead of the raw clock since the control thread's last mark by more than 100 ms plus one eighth of the raw time since that mark, drops them and closes their connections, as for an ended lease (their outcomes are unknown, section 7.4). A suspend longer than about 103 ms (at 25 ms control turns) is caught that way before the next control turn, and a shorter one is far within `max_stop`. Turns without a lock operation flush without the check: a stale lease breaks only lock claim L, with one exception within DESIGN claim K3 (KV across a failover): an old owner suspended longer than the leave wait (10 s) can serve, for up to one control turn (about 25 ms), a slot that a run-out handoff opened on the new owner (DESIGN claim K2 and 1.1). A checkpoint restored on another host (CRIU) is not supported. A clock that stood still is also detected from any one authenticated peer: since the previous check the peer ran more than Q/2 longer than the local clock, measured as sequence advance x 25 ms within one life; for incarnation + 1, the smaller of the uptime and the sequence steps across the overflow; for any later incarnation, its full uptime. On a detected stop the server drops its credits and reports and counts a lapse. A stop with a stopped guest clock is safe up to about `max_stop` + 2.25 s whatever the network does at the thaw (the quarantine includes `max_stop`); longer stops are detected only with peer contact, as above, so an operator who suspends a Keep VM for longer must restart Keep.
- Far lists: a sender's list is far from the receiver's when the member counts differ by two or more; or the counts are equal (the digests differ, so at least two members do); or the sender's count is one lower and its digest is not the receiver's digest minus the term of one of the receiver's members; or the sender's count is one higher, the sender is not on the receiver's list, and its digest is not the receiver's digest plus the sender's own term (the sender-id rule: the one member the sender's list can add is the sender). Only a sender on both lists with one member more leaves the receiver unable to tell; it treats the sender as near, and the sender's own check (the larger side) decides. A server hears a far list while a far heartbeat it recorded is within F, and then sets bit 2 on every heartbeat it sends. A receiver of a heartbeat with bit 2, from a list-mate or read as a foreign heartbeat, is fenced for F as if it heard the far list itself, and does not set bit 2 for it: the fence travels one hop. Recovery: the detector sets bit 2 for F after its last far heartbeat and a list-mate stays fenced for F after the last flagged heartbeat it accepted, so the detector's list serves again about 2F (450 ms) plus a lease after the far list goes quiet. While a server hears a far list or a bit 2 within F, it sets no confirm flag, credits no confirmation, drops the credits it held and ends its lease at once. That is a lapse: its next unfenced view re-quarantines its slots. It reports `settled false`, as it also does while any foreign heartbeat, near or far, was heard within F. Guarantee: two lists in use never both serve while some member on both lists runs, can decide (its list is not the smaller by exactly one member, or it hears a member of the other list that is not on its own) and is heard by its own list-mates. Residuals: (1) a change that leaves no such member running (disjoint lists, the shared members down or cut off, or the only shared members on the list smaller by one hearing only shared members of the other, as with {0,1,2} to {0,1,3,4} under one-way cuts 3 to 0, 4 to 0, 0 to 1 and 2 to 1) makes two clusters that no heartbeat can tell apart, so one member per change stays the operator's rule (Member list below); (2) a new list whose majority formed before a shared member started can serve KV for about one heartbeat interval plus a check (about 31 ms) until bit 2 reaches it; new-list servers confirm nobody for their first 225 ms, so in the usual start order there is no window, and locks are covered by the Rejoin quarantine; (3) injected heartbeats: a captured stream of a removed member's heartbeats, sent to the peer port by anyone who can reach it (the port checks no source address, so no on-path position is needed), is accepted beat by beat, since each beat advances its sender's floor, and a receiver that restarts starts with no floor. Through bit 2 one accepted far beat fences the receiver's whole list, and every lapse re-quarantines its lock slots for Q (634 s by default), so about 5 datagrams a second keep a cluster down: fail-closed, but a denial of service. Peer port exposure (below) removes the injection.
- Member list: change the configured list one member at a time, restarting every server with the new list before the next change, with a Leaving handoff first when the leaving member's slots should not be quarantined. Servers enforce the dangerous part: a change of two or more members at once, or a second change before every server runs the first, fences every server that hears a server on the other list, and through bit 2 its list-mates; the cluster is then unavailable until the servers on the old list are restarted or stopped, and does not split while a member on both lists runs and hears the other list (Far lists above). During a change the first members restarted with the new list stay fenced until a majority of the new list runs it (members still on the old list refuse their digest), so a rollout must not wait for a restarted member to serve before restarting the next one: readiness is the client port accepting connections (a TCP probe), not serving. Between two list changes, and around every ordinary restart, the next step waits until every server reports `settled true` (DEPLOYMENT 5.1). Epochs restart at 1 with each life, and clients follow the view a majority agrees on (section 9.4).
- Peer port exposure: the peer port, UDP heartbeats and TCP handoffs alike, must be reachable only from cluster members (a private network or a firewall). This is a deployment requirement: the MAC authenticates a heartbeat but cannot tell a replayed one of a removed member from a live one (residual (3) under Far lists).

### 12.2 Peer port handshake

Implemented in `crates/keep-cluster/src/handoff/` (`frames.rs`, `peer.rs`). Peer connections are TCP on the peer port and carry the frame header of section 2; in TLS mode the TLS handshake comes first: in `rpk` mode both ends require a key from the server key list, in `ca` mode a chain to the peer anchor (`--tls-peer-ca`), with the dialed member's peer IP address as an IP SAN (section 10.1). The peer-secret handshake below runs in every mode. Every frame field not listed in section 12.3 is zero and `ext_words` is 0; a frame with a nonzero reserved field, a wrong length or a code outside 0x30 to 0x36 ends the connection.

1. The initiator sends PeerHello addressed to the member it wants.
2. The acceptor checks it, then answers with its own PeerHello (receiver: the initiator) and its PeerProof.
3. The initiator checks the acceptor's hello and proof, then sends its own PeerProof, which the acceptor checks. Only then do handoff frames flow.

Proofs are those of section 12: the peer secret over the label, the initiator's hello and the acceptor's hello, so each side proves the exact pair of hellos it saw. A reader closes the connection when a hello's receiver is not itself; its sender is the reader itself or not in the configured member list; on the initiator, its sender is not the member it dialed; its member list digest differs from the reader's (section 12.1); or its version (`arg`) is not 1. A proof that does not verify ends the connection.

### 12.3 Peer frames

Integers are little endian.

| Code | Frame | id | flags | arg | key | value |
|---|---|---|---|---|---|---|
| 0x30 | PeerHello | 0 | 0 | version, 1 | | sender node id u64, receiver node id u64, incarnation u32, member list digest u64, nonce 16 B (44 B) |
| 0x31 | PeerProof | 0 | 0 | 0 | | HMAC-SHA-256, 32 B |
| 0x32 | Pull | batch | 0 | 0 | | slot bitmap 2,048 B, then `since`: incarnation u32 and heartbeat sequence u32 (2,056 B) |
| 0x33 | Ready | batch | 0 | quarantine in ms | | slots with entries, then slots covered empty, 2,048 B each |
| 0x34 | Entry | batch | value kind: 0 bytes, 1 counter (i64), 2 lock token (8 B) | TTL in s, 0 for none | key | value |
| 0x35 | End | batch | 0 | entries sent | | |
| 0x36 | Ack | batch | 0 | 0 | | |

Slot bitmap: 16,384 bits as 256 u64 words; slot s is bit s % 64 of word s / 64.

A PeerHello with a nonzero id, and a Ready with a slot in both sets, end the connection. A Pull's `since` is the oldest heartbeat of the puller that a hold may have been recorded with and still carry entries: the newer of the heartbeat that announced the puller's current activation and the first heartbeat the puller sent after it last lost one of the pulled slots. It travels in the value rather than coming from the connection's PeerHello, because a sequence overflow moves a live process to incarnation + 1 on the same connection (section 12.1).

### 12.4 Handoffs (pulled)

- The server that gains a slot pulls it from the slot's previous owner, the source it recorded when the slot moved; only the receiver knows what it waits for. There are no pushes: a new life records the other members as previous owners, who never lost the slot, so the receiver pulls.
- Pull(batch, slots): the receiver asks the source for the slots it awaits from it; batch numbers are per receiver.
- Ready(batch): the source covers every asked slot it no longer serves, "with entries" for slots whose entries it holds for a pull and "empty" for the others. A slot it still serves is in neither set and is pulled again after the retry wait (200 ms). `arg` is the remaining quarantine of the slots with entries, in ms from now, rounded up: the receiver serves them under at least that quarantine, and never under less than its own regain or rejoin quarantine. Within Q after the receiver starts placing (every switch from not placing to placing, not only the first), slots it receives with entries keep at least placement + Q: a hold made while the receiver was announced but not yet placing misses the grants of members that had not acknowledged it yet. Leave handoffs to members that have been placing for longer keep the source's quarantine.
- Empty slots open on the receiver at once. The source then sends one Entry per live key of the data slots, from every shard, then End with the count of entries sent (0 included). It sends no Entry for a value of at least 64 KiB in an evictable class (section 3.8): a move acts on those keys as an eviction. Pinned values move at any size.
- Entry: the TTL is the whole seconds left on the source, rounded up, as an upper bound; the import rounds up again (section 3.3), so a handoff never shortens a key's life; it can lengthen it by the transfer time plus up to 2 s of rounding. The receiver imports an entry only if the key is absent (a key written on the receiver meanwhile wins) and ignores entries for slots it does not, or no longer, await from that source.
- End: when its count equals the entries received for the batch, the data slots open on the receiver, which then sends Ack; otherwise the batch is dropped and its slots are pulled again.
- Ack: the source purges the entries of the acknowledged slots when the batch is its latest answer to that incarnation of the receiver, on any connection: an answer outlives the connection it went out on. Nothing confirms an Ack, so whenever the receiver's dial to a source closes it sends the Ack of its last import from that source again 200 ms later, over a new dial and ahead of any Pull on it, until the hold time (30 s) after the import passed or the source restarted. A lost Ack therefore ends the hold at the next dial instead of letting it run out.
- Holds are bound to the receiver. When a view moves held slots to a member, the source records that member and the (incarnation, sequence) it last accepted from it. It answers a held slot "with entries" only to that member, only when the recorded pair is at least the Pull's `since` (compared lexicographically) and only while it is not fenced; otherwise it covers the slot empty, and the puller keeps its gain quarantine. The lost-slot part of `since` exists because a hold made before the puller lost and regained the slot lacks the puller's own later grants.
- A fenced source covers no held slot: they are in neither set and the puller retries. A hold's quarantine is the slot's quarantine end at the source, including the lock deaths it waits for, raised to at least now + Q at each regain of the source's lease. A lost slot whose end is still unbounded (a dead member not yet lock-dead) is purged instead of held, so its puller gets it empty.
- A puller never pulls a slot it still has to purge (stale entries from an earlier ownership); it pulls once the purge walk ended.
- Gate: a slot gained by activation opens only on the recorded source's coverage (Ready empty, or the End of its data), on the source's lock death (then under a quarantine to lock death + Q) or on its restart (a new incarnation holds none of the entries); never on a timeout. A slot gained from a Leaving member also opens after `leave_wait` (10 s). An orphan slot (its source lock-dead, then recovered in the same life) is quarantined to the lock death + Q, not forever.
- Timers: a source holds a lost slot's valid entries for a pull for 30 s, then purges them. A receiver pulls again 200 ms after a Ready that left slots uncovered, an End that did not match or a closed connection, and resends a Pull that got no Ready after 5 s. The receiver checks every 5 s whether new Entry frames arrived: an import without one is dropped 5 to 10 s after its last Entry (End does not count as progress) and pulled again 200 ms later.
- A lost connection: the receiver pulls again after the retry wait and resends the Ack of its last import from that source; the source keeps the held entries while the hold runs, and its last answer to the receiver for that Ack.
- A slot that comes back to a server still holding stale entries for it stays gated (`Busy`, section 3.4) until its purge walk ends.

## 13. Not in version 1

- Extension words (fencing epoch, idempotency token, trace id, publisher sequence): none are defined (section 2).
- Peer secret rotation with every member removal, with a previous secret accepted beside the current one: version 1 has one peer secret per cluster, and the peer port must be reachable only from cluster members (section 12.1).
