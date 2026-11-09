/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! The Keep wire protocol: frames, commands, replies, the Hello handshake,
//! authentication and cluster placement, shared by the Keep server and clients.

#![warn(missing_docs)]

mod auth;
mod command;
mod frame;
mod header;
mod hello;
/// Loading TLS keys and certificates from PEM or DER files.
#[cfg(feature = "tls")]
pub mod keys;
mod limits;
mod op;
mod placement;
mod reply;
mod topology;
mod types;

pub use auth::{Nonce, Nonces, PEER_HELLO_LEN, PeerSecret, PeerSide, Proof, Side};
pub use command::{Command, CommandError, PROOF_LEN, ReplyMode, Request, SetMode, TtlReport};
pub use frame::{Decoded, Frame, FrameError, Frames};
pub use header::Header;
pub use hello::{
    Challenge, ClientInfo, Fingerprint, HashId, HelloError, ServerInfo, ShardInfo, SoftwareName,
    Tlv, Tlvs,
};
pub use limits::{
    EXT_WORD_LEN, HEADER_LEN, MAX_HELLO_LEN, MAX_KEY_LEN, MAX_MEMBERS, MAX_SUBSCRIPTIONS,
    MAX_TOPIC, MAX_VALUE_LEN, PROTOCOL_VERSION,
};
pub use op::Op;
pub use placement::{
    ClusterSecret, FINGERPRINT_LABEL, KeyHash, KeyHasher, NodeSalt, Owner, Placement,
    PlacementError, SLOT_BITS, SLOT_COUNT, Slot, fmix64, hrw_pick,
};
pub use reply::{BusyScope, Event, Mark, Push, Response, ServerFrame, ServerFrameError, Status};
pub use topology::{Liveness, Member, MemberStatus, NodeId, Topology, TopologyError};
pub use types::{Flags, Key, LockToken, Topic, Ttl, Value};
