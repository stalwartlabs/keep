# keep-proto

[![crates.io](https://img.shields.io/crates/v/keep-proto)](https://crates.io/crates/keep-proto)
[![docs.rs](https://img.shields.io/docsrs/keep-proto)](https://docs.rs/keep-proto)
[![crates.io](https://img.shields.io/crates/l/keep-proto)](http://www.apache.org/licenses/LICENSE-2.0)

_keep-proto_ is the wire protocol of **Stalwart Keep**, an ephemeral key-value store and event stream server for clustered applications such as [Stalwart](https://github.com/stalwartlabs/stalwart) mail server clusters. It is shared by the Keep server and its clients, and is sans-I/O: it encodes and decodes bytes and leaves the sockets to the caller.

## Features

- **Frames**: the request and reply frames, and a decoder that walks the frames of a read buffer.
- **Commands and replies**: key-value, counter, lock, lease and stream commands, their statuses and payloads.
- **Hello handshake**: the client and server info exchanged when a connection opens.
- **Authentication**: the HMAC proofs of the peer handshake and the cluster secret.
- **Topology and placement**: cluster members and the slot and shard a key belongs to, by rendezvous hashing over the members.

Most users want [keep-client](https://crates.io/crates/keep-client), which is built on this crate.

## Feature flags

- `tls`: loading private keys and raw public keys from PEM or DER files for TLS 1.3.

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSES/Apache-2.0.txt](LICENSES/Apache-2.0.txt) or <https://www.apache.org/licenses/LICENSE-2.0>)
 * MIT license ([LICENSES/MIT.txt](LICENSES/MIT.txt) or <https://opensource.org/licenses/MIT>)

at your option.

## Copyright

Copyright (C) 2020, Stalwart Labs LLC
