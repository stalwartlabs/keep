/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::fmt;
use std::num::NonZeroU16;
use std::str::from_utf8;

use crate::auth::{Nonce, Proof};
use crate::limits::{MAX_HELLO_LEN, MAX_KEY_LEN};
use crate::topology::NodeId;

const CLIENT_NAME: u8 = 1;
const CLIENT_NODE: u8 = 2;
const CLIENT_EXTENSIONS: u8 = 3;
const CLIENT_NONCE: u8 = 4;
const CLIENT_WANTED_SHARD: u8 = 5;

const SERVER_NODE: u8 = 1;
const SERVER_SHARD: u8 = 2;
const SERVER_IN_FLIGHT: u8 = 3;
const SERVER_HEARTBEAT: u8 = 4;
const SERVER_EXTENSIONS: u8 = 5;
const SERVER_PLACEMENT: u8 = 6;
const SERVER_CHALLENGE: u8 = 7;

/// One type-length-value item of a Hello value.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Tlv<'a> {
    /// The item type.
    pub kind: u8,
    /// The item value.
    pub value: &'a [u8],
}

impl<'a> Tlv<'a> {
    /// Iterates over the items of a Hello value.
    pub fn iter(bytes: &'a [u8]) -> Tlvs<'a> {
        Tlvs(bytes)
    }

    fn fixed<const N: usize>(self) -> Result<[u8; N], HelloError> {
        <[u8; N]>::try_from(self.value).map_err(|_| self.bad_length())
    }

    fn bad_length(self) -> HelloError {
        HelloError::BadLength {
            kind: self.kind,
            len: self.value.len(),
        }
    }
}

/// Iterator over the TLV items of a Hello value; a truncated item ends it
/// with an error.
pub struct Tlvs<'a>(&'a [u8]);

impl<'a> Iterator for Tlvs<'a> {
    type Item = Result<Tlv<'a>, HelloError>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = match self.0 {
            [] => return None,
            [kind, len, rest @ ..] => rest
                .split_at_checked(usize::from(*len))
                .map(|(value, rest)| (Tlv { kind: *kind, value }, rest)),
            [_] => None,
        };
        Some(match item {
            Some((tlv, rest)) => {
                self.0 = rest;
                Ok(tlv)
            }
            None => {
                self.0 = &[];
                Err(HelloError::Truncated)
            }
        })
    }
}

/// The shard serving a connection and the server's shard count (server
/// type 2).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ShardInfo {
    shard: u16,
    count: NonZeroU16,
}

impl ShardInfo {
    /// Shard `shard` of `count`, or `None` unless `shard < count`.
    pub fn new(shard: u16, count: u16) -> Option<Self> {
        NonZeroU16::new(count)
            .filter(|count| shard < count.get())
            .map(|count| ShardInfo { shard, count })
    }

    /// This connection's shard.
    pub fn shard(self) -> u16 {
        self.shard
    }

    /// The server's shard count.
    pub fn count(self) -> NonZeroU16 {
        self.count
    }
}

/// Placement hash function id (server type 6).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct HashId(pub u8);

impl HashId {
    /// SipHash-1-3 keyed by the cluster secret.
    pub const SIP13: HashId = HashId(1);
}

/// The placement hash id and the cluster secret fingerprint (server type 6).
/// A client routes by key only when both equal its own.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Fingerprint {
    /// The placement hash function.
    pub hash: HashId,
    /// The secret fingerprint.
    pub value: u64,
}

/// The server's half of plaintext authentication (server type 7).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Challenge {
    /// The server's nonce.
    pub nonce: Nonce,
    /// The server's proof over both nonces.
    pub proof: Proof,
}

/// A client software name: 1 to 255 bytes of UTF-8.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SoftwareName<'a>(&'a str);

impl<'a> SoftwareName<'a> {
    /// Wraps `name`, or `None` unless it is 1 to 255 bytes.
    pub fn new(name: &'a str) -> Option<Self> {
        (1..=MAX_KEY_LEN)
            .contains(&name.len())
            .then_some(Self(name))
    }

    /// The name.
    pub fn as_str(&self) -> &'a str {
        self.0
    }
}

/// The client info of a Hello request.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct ClientInfo<'a> {
    /// The client software name.
    pub name: Option<SoftwareName<'a>>,
    /// The client's node index.
    pub node: Option<u64>,
    /// Extension bits the client supports.
    pub extensions: u64,
    /// The client's nonce for plaintext authentication.
    pub nonce: Option<Nonce>,
}

/// The server info of a Hello reply. Every type is optional on the wire; which ones a client
/// requires is its policy.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct ServerInfo {
    /// The server's node id.
    pub node: Option<NodeId>,
    /// The connection's shard and the server's shard count.
    pub shard: Option<ShardInfo>,
    /// Requests the server takes in flight per connection.
    pub in_flight: Option<u32>,
    /// The stream heartbeat interval in milliseconds.
    pub heartbeat_ms: Option<u32>,
    /// Extension bits the server supports.
    pub extensions: u64,
    /// The placement hash and secret fingerprint.
    pub placement: Option<Fingerprint>,
    /// The plaintext authentication challenge.
    pub challenge: Option<Challenge>,
}

impl<'a> ClientInfo<'a> {
    /// Parses a client info value.
    pub fn parse(bytes: &'a [u8]) -> Result<ClientInfo<'a>, HelloError> {
        let mut info = ClientInfo::default();
        for tlv in checked(bytes)? {
            let tlv = tlv?;
            match tlv.kind {
                CLIENT_NAME => {
                    let name = from_utf8(tlv.value).map_err(|_| HelloError::BadName)?;
                    info.name = Some(SoftwareName::new(name).ok_or(tlv.bad_length())?);
                }
                CLIENT_NODE => info.node = Some(u64::from_le_bytes(tlv.fixed()?)),
                CLIENT_EXTENSIONS => info.extensions = u64::from_le_bytes(tlv.fixed()?),
                CLIENT_NONCE => info.nonce = Some(Nonce(tlv.fixed()?)),
                CLIENT_WANTED_SHARD => {
                    tlv.fixed::<2>()?;
                }
                _ => {}
            }
        }
        Ok(info)
    }

    /// Appends the encoded client info to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        if let Some(name) = self.name {
            out.extend_from_slice(&[CLIENT_NAME, name.0.len() as u8]);
            out.extend_from_slice(name.0.as_bytes());
        }
        if let Some(node) = self.node {
            put(out, CLIENT_NODE, node.to_le_bytes());
        }
        if self.extensions != 0 {
            put(out, CLIENT_EXTENSIONS, self.extensions.to_le_bytes());
        }
        if let Some(nonce) = self.nonce {
            put(out, CLIENT_NONCE, nonce.0);
        }
    }
}

impl ServerInfo {
    /// Parses a server info value.
    pub fn parse(bytes: &[u8]) -> Result<ServerInfo, HelloError> {
        let mut info = ServerInfo::default();
        for tlv in checked(bytes)? {
            let tlv = tlv?;
            match tlv.kind {
                SERVER_NODE => info.node = Some(NodeId(u64::from_le_bytes(tlv.fixed()?))),
                SERVER_SHARD => {
                    let [s0, s1, c0, c1] = tlv.fixed()?;
                    let (shard, count) =
                        (u16::from_le_bytes([s0, s1]), u16::from_le_bytes([c0, c1]));
                    info.shard = Some(
                        ShardInfo::new(shard, count)
                            .ok_or(HelloError::BadShard { shard, count })?,
                    );
                }
                SERVER_IN_FLIGHT => info.in_flight = Some(u32::from_le_bytes(tlv.fixed()?)),
                SERVER_HEARTBEAT => info.heartbeat_ms = Some(u32::from_le_bytes(tlv.fixed()?)),
                SERVER_EXTENSIONS => info.extensions = u64::from_le_bytes(tlv.fixed()?),
                SERVER_PLACEMENT => {
                    let [hash, value @ ..] = tlv.fixed::<9>()?;
                    info.placement = Some(Fingerprint {
                        hash: HashId(hash),
                        value: u64::from_le_bytes(value),
                    });
                }
                SERVER_CHALLENGE => {
                    let (nonce, proof) = tlv
                        .value
                        .split_first_chunk::<{ Nonce::LEN }>()
                        .and_then(|(nonce, proof)| Some((*nonce, proof.try_into().ok()?)))
                        .ok_or(tlv.bad_length())?;
                    info.challenge = Some(Challenge {
                        nonce: Nonce(nonce),
                        proof: Proof(proof),
                    });
                }
                _ => {}
            }
        }
        Ok(info)
    }

    /// Appends the encoded server info to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        if let Some(node) = self.node {
            put(out, SERVER_NODE, node.0.to_le_bytes());
        }
        if let Some(shard) = self.shard {
            let [s0, s1] = shard.shard.to_le_bytes();
            let [c0, c1] = shard.count.get().to_le_bytes();
            put(out, SERVER_SHARD, [s0, s1, c0, c1]);
        }
        if let Some(in_flight) = self.in_flight {
            put(out, SERVER_IN_FLIGHT, in_flight.to_le_bytes());
        }
        if let Some(heartbeat) = self.heartbeat_ms {
            put(out, SERVER_HEARTBEAT, heartbeat.to_le_bytes());
        }
        if self.extensions != 0 {
            put(out, SERVER_EXTENSIONS, self.extensions.to_le_bytes());
        }
        if let Some(placement) = self.placement {
            let [f0, f1, f2, f3, f4, f5, f6, f7] = placement.value.to_le_bytes();
            put(
                out,
                SERVER_PLACEMENT,
                [placement.hash.0, f0, f1, f2, f3, f4, f5, f6, f7],
            );
        }
        if let Some(challenge) = self.challenge {
            out.extend_from_slice(&[SERVER_CHALLENGE, 48]);
            out.extend_from_slice(&challenge.nonce.0);
            out.extend_from_slice(&challenge.proof.0);
        }
    }
}

fn checked(bytes: &[u8]) -> Result<impl Iterator<Item = Result<Tlv<'_>, HelloError>>, HelloError> {
    if bytes.len() > MAX_HELLO_LEN {
        return Err(HelloError::TooLong(bytes.len()));
    }
    let mut seen = Seen::default();
    Ok(Tlv::iter(bytes).map(move |tlv| {
        let tlv = tlv?;
        seen.insert(tlv.kind)?;
        Ok(tlv)
    }))
}

#[derive(Default)]
struct Seen {
    low: u128,
    high: u128,
}

impl Seen {
    fn insert(&mut self, kind: u8) -> Result<(), HelloError> {
        let (set, bit) = match kind.checked_sub(128) {
            Some(high) => (&mut self.high, 1u128 << high),
            None => (&mut self.low, 1u128 << kind),
        };
        if *set & bit != 0 {
            return Err(HelloError::Duplicate(kind));
        }
        *set |= bit;
        Ok(())
    }
}

fn put<const N: usize>(out: &mut Vec<u8>, kind: u8, value: [u8; N]) {
    const { assert!(N <= u8::MAX as usize) };
    out.reserve(2 + N);
    out.extend_from_slice(&[kind, N as u8]);
    out.extend_from_slice(&value);
}

/// A Hello value that cannot be read: a malformed Hello, fatal for the
/// connection.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum HelloError {
    /// The value is this many bytes, over the limit.
    TooLong(usize),
    /// An item runs past the end of the value.
    Truncated,
    /// An item of type `kind` has the wrong length `len`.
    BadLength {
        /// The item type.
        kind: u8,
        /// Its length.
        len: usize,
    },
    /// An item of this type appears twice.
    Duplicate(u8),
    /// The shard index is not below the shard count.
    BadShard {
        /// The shard index.
        shard: u16,
        /// The shard count.
        count: u16,
    },
    /// The software name is not valid UTF-8 of 1 to 255 bytes.
    BadName,
}

impl fmt::Display for HelloError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HelloError::TooLong(len) => write!(f, "hello value of {len} bytes too long"),
            HelloError::Truncated => f.write_str("truncated hello TLV"),
            HelloError::BadLength { kind, len } => {
                write!(f, "hello TLV {kind} has bad length {len}")
            }
            HelloError::Duplicate(kind) => write!(f, "hello TLV {kind} repeated"),
            HelloError::BadShard { shard, count } => {
                write!(f, "hello shard {shard} outside count {count}")
            }
            HelloError::BadName => f.write_str("client name is not UTF-8"),
        }
    }
}

impl std::error::Error for HelloError {}
