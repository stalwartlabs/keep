/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::fmt;

use hmac::digest::KeyInit;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::command::PROOF_LEN;
use crate::placement::ClusterSecret;

type HmacSha256 = Hmac<Sha256>;

/// A 16-byte authentication nonce, fresh and random per connection.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Nonce(pub [u8; Nonce::LEN]);

impl Nonce {
    /// Length of a nonce in bytes.
    pub const LEN: usize = 16;
}

/// An HMAC-SHA-256 proof of the cluster secret. Equality is constant time.
#[derive(Clone, Copy, Debug)]
pub struct Proof(pub [u8; PROOF_LEN]);

impl PartialEq for Proof {
    fn eq(&self, other: &Proof) -> bool {
        self.0.ct_eq(&other.0).into()
    }
}

impl Eq for Proof {}

/// The two nonces of one connection's challenge.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Nonces {
    /// The client's nonce.
    pub client: Nonce,
    /// The server's nonce.
    pub server: Nonce,
}

/// Which end a proof comes from; the label keeps a server proof from ever
/// passing as a client proof.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Side {
    /// A proof sent by the server.
    Server,
    /// A proof sent by the client.
    Client,
}

impl Side {
    fn label(self) -> &'static [u8] {
        match self {
            Side::Server => b"keep auth server v1",
            Side::Client => b"keep auth client v1",
        }
    }
}

/// Which end of a peer port handshake a proof comes from; the labels differ from the client labels
/// of `Side`, so no proof passes for another role.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PeerSide {
    /// The end that opened the peer connection.
    Initiator,
    /// The end that accepted the peer connection.
    Acceptor,
}

impl PeerSide {
    fn label(self) -> &'static [u8] {
        match self {
            PeerSide::Initiator => b"keep peer initiator v2",
            PeerSide::Acceptor => b"keep peer acceptor v2",
        }
    }
}

/// Length of a PeerHello value, the unit of the peer handshake transcript.
pub const PEER_HELLO_LEN: usize = 44;

/// The server-only 16-byte secret of the peer port, never sent to clients or on the wire.
/// `Debug` hides it and it has no `==`; compare `to_bytes()` instead.
#[derive(Clone, Copy)]
pub struct PeerSecret([u8; 16]);

impl PeerSecret {
    /// Length of the secret in hex digits.
    pub const HEX_LEN: usize = 32;

    /// Wraps 16 secret bytes.
    pub fn new(bytes: [u8; 16]) -> PeerSecret {
        PeerSecret(bytes)
    }

    /// Parses exactly 32 hex digits, most significant first.
    pub fn from_hex(hex: &str) -> Option<PeerSecret> {
        if hex.len() != Self::HEX_LEN || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        u128::from_str_radix(hex, 16)
            .ok()
            .map(|value| PeerSecret(value.to_be_bytes()))
    }

    /// The secret's bytes.
    pub fn to_bytes(&self) -> [u8; 16] {
        self.0
    }

    /// `HMAC-SHA-256(peer secret, label || initiator hello || acceptor hello)` over both
    /// PeerHello values as sent, binding both identities, incarnations, members and nonces.
    pub fn transcript_proof(
        &self,
        side: PeerSide,
        initiator: &[u8; PEER_HELLO_LEN],
        acceptor: &[u8; PEER_HELLO_LEN],
    ) -> Proof {
        Proof(
            keyed(&self.0, &[side.label(), initiator, acceptor])
                .finalize()
                .into_bytes()
                .into(),
        )
    }

    /// Checks a transcript proof in constant time.
    pub fn verify_transcript(
        &self,
        side: PeerSide,
        initiator: &[u8; PEER_HELLO_LEN],
        acceptor: &[u8; PEER_HELLO_LEN],
        proof: &[u8; PROOF_LEN],
    ) -> bool {
        keyed(&self.0, &[side.label(), initiator, acceptor])
            .verify_slice(proof)
            .is_ok()
    }
}

impl fmt::Debug for PeerSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PeerSecret(..)")
    }
}

impl ClusterSecret {
    /// `HMAC-SHA-256(secret, label || client nonce || server nonce)`.
    pub fn proof(&self, side: Side, nonces: &Nonces) -> Proof {
        Proof(
            labelled(&self.to_bytes(), side.label(), nonces)
                .finalize()
                .into_bytes()
                .into(),
        )
    }

    /// Checks a proof in constant time.
    pub fn verify(&self, side: Side, nonces: &Nonces, proof: &[u8; PROOF_LEN]) -> bool {
        labelled(&self.to_bytes(), side.label(), nonces)
            .verify_slice(proof)
            .is_ok()
    }
}

fn labelled(key: &[u8; 16], label: &[u8], nonces: &Nonces) -> HmacSha256 {
    keyed(key, &[label, &nonces.client.0, &nonces.server.0])
}

fn keyed(key: &[u8; 16], parts: &[&[u8]]) -> HmacSha256 {
    let mut block = [0u8; 64];
    for (byte, key) in block.iter_mut().zip(key) {
        *byte = *key;
    }
    let mut mac = <HmacSha256 as KeyInit>::new(&block.into());
    for part in parts {
        mac.update(part);
    }
    mac
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-proto/auth.rs"]
mod tests;
