# keep-client

[![crates.io](https://img.shields.io/crates/v/keep-client)](https://crates.io/crates/keep-client)
[![docs.rs](https://img.shields.io/docsrs/keep-client)](https://docs.rs/keep-client)
[![crates.io](https://img.shields.io/crates/l/keep-client)](http://www.apache.org/licenses/LICENSE-2.0)

_keep-client_ is the Rust client for **Stalwart Keep**, an ephemeral key-value store and event stream server for clustered applications. Keep holds the short-lived state a cluster shares (locks, leases, rate-limit counters, tokens) and carries its cluster events. [Stalwart](https://github.com/stalwartlabs/stalwart) mail server clusters are one use case. Keep is neither a general Redis replacement nor a durable message queue: nothing is persisted and events are not replayable.

The client runs on the caller's tokio runtime and speaks Keep's binary protocol ([keep-proto](https://crates.io/crates/keep-proto)) to a single server or to a whole cluster.

## Features

- **Multiplexed connections**: many requests in flight per connection; requests queued while a write is in flight go out together in the next write, so a busy caller sends few large writes instead of many small ones.
- **Cluster placement**: in a cluster the client learns the members from its seeds and sends each key to the member and, optionally, the shard that owns it; it follows topology changes and resends requests a server refused.
- **Safe retries**: each operation knows whether it may be repeated after an unknown outcome. Idempotent requests are resent within the deadline; the others return `Error::Unknown` so the caller decides.
- **Locks and leases**: a lock and lease API (`try_lock`, `renew_lock`, `remove_lock`, `try_lease`, `renew_lease`, `release_lease`), plus token-based locks (`try_lock_with`, `renew`, `unlock`).
- **Event streams**: publish and subscribe on 65536 topics, with per-server sequence numbers that tell the subscriber when an event may have been missed.
- **TLS 1.3**: mutual TLS with raw public keys (RFC 7250) or with certificates from a cluster CA, through rustls.

## Usage

### Connect

A client is configured from a URL or a `ClientConfig`:

```rust
use keep_client::Client;

let client = Client::connect_url(
    "keep://10.0.0.1:7000,10.0.0.2:7000?cluster=on&secret=00112233445566778899aabbccddeeff&tls=rpk&key=/etc/keep/client.key&servers=/etc/keep/servers.pub",
)
.await?;
```

The endpoints are the servers (or, with `cluster=on`, the seeds of a cluster). Common parameters:

| Parameter | Meaning |
|---|---|
| `cluster=on\|off` | discover the cluster members from the endpoints, or use the first endpoint only |
| `tls=rpk\|ca\|off` | raw public keys (`key=`, `servers=`), a CA (`ca=`, `sni=`, `cert=`), or plain TCP |
| `secret=` | the cluster secret (32 hex digits), required with `cluster=on` and to route requests by shard |
| `route=none\|shard` | one pool of connections per server, or connections per shard |
| `conns=`, `shard_conns=` | connections per server, or per shard |
| `stream=everywhere\|owner` | subscribe on every member, or on the topic's home member only |
| `timeout=ms:N`, `deadline=ms:N`, `connect_timeout=ms:N` | per-attempt timeout, per-call deadline, connect timeout |

`ClientConfig::from_url` parses the same URL into a config that can be adjusted before `Client::connect`.

### Keys and counters

```rust
use keep_client::Ttl;

let ttl = Ttl::from_secs(60).expect("non-zero");
client.set(b"session:42", b"token", Some(ttl)).await?;
assert_eq!(client.get(b"session:42").await?.as_deref(), Some(&b"token"[..]));

let hits = client.incr(b"rate:10.0.0.7", 1, Some(ttl)).await?;
client.delete(b"session:42").await?;
```

Batch calls (`set_many`, `get_many`, `delete_many`) take many keys in one call, and `delete_prefix` purges every key under a prefix on every server.

### Locks and leases

```rust
use keep_client::{LockToken, Ttl};

let ttl = Ttl::from_secs(30).expect("non-zero");
let token = LockToken::from_u64(7);
if client.try_lock_with(b"\x0elock:queue", ttl, token).await? {
    client.renew(b"\x0elock:queue", ttl, Some(token)).await?;
    client.unlock(b"\x0elock:queue", token).await?;
}
```

The `bool` results tell whether the call took effect or another holder has the key; `renew_lock` returns `Error::HeldByOther` in that case. Start lock keys with a first byte of 0, 1 or 14 to 17 (here 14): the server's memory cap never evicts those.

### Event streams

```rust
use keep_client::{Delivery, Topic};

let mut subscription = client.subscribe(Topic::from(3)).await?;
client.publish(Topic::from(3), b"mailbox changed").await?;

while let Some(delivery) = subscription.next().await {
    match delivery {
        Delivery::Event { payload, .. } => println!("{payload:?}"),
        other if other.needs_catch_up() => { /* reload state from the source of truth */ }
        _ => {}
    }
}
```

`publish` queues the event without waiting for a reply; `publish_confirmed` waits for the server's acknowledgement. A subscription reports `Gap`, `Lost` and `Resubscribed` when events may have been missed, so the caller knows when to catch up.

## Testing

The client is tested with unit and integration tests against real Keep servers, with deterministic simulation of the cluster, with fault injection (partitions, crashes, clock jumps) and with [Jepsen](https://jepsen.io). See the Keep repository for details.

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSES/Apache-2.0.txt](LICENSES/Apache-2.0.txt) or <https://www.apache.org/licenses/LICENSE-2.0>)
 * MIT license ([LICENSES/MIT.txt](LICENSES/MIT.txt) or <https://opensource.org/licenses/MIT>)

at your option.

## Copyright

Copyright (C) 2020, Stalwart Labs LLC
