/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::fmt;
use std::num::NonZeroU16;

use siphasher::sip::SipHasher13;

use super::slots::{SLOT_BITS, Slot};
use crate::hello::{Fingerprint, HashId};

/// The 29 bytes whose keyed hash is the secret fingerprint (Hello type 6).
pub const FINGERPRINT_LABEL: &[u8] = b"keep placement fingerprint v1";

/// The cluster-wide secret from configuration: it keys placement, so clients
/// cannot aim keys at one server or shard, and plaintext authentication.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ClusterSecret {
    k0: u64,
    k1: u64,
}

impl ClusterSecret {
    /// Length of the secret in hex digits.
    pub const HEX_LEN: usize = 32;

    /// The secret from its two 64-bit halves.
    pub fn new(k0: u64, k1: u64) -> ClusterSecret {
        ClusterSecret { k0, k1 }
    }

    /// Parses exactly 32 hex digits: `k0`, then `k1`, each most significant
    /// digit first.
    pub fn from_hex(hex: &str) -> Option<ClusterSecret> {
        if hex.len() != Self::HEX_LEN || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        let (k0, k1) = hex.split_at_checked(Self::HEX_LEN / 2)?;
        Some(ClusterSecret {
            k0: u64::from_str_radix(k0, 16).ok()?,
            k1: u64::from_str_radix(k1, 16).ok()?,
        })
    }

    /// The 16 bytes the hex form spells, the HMAC key of plaintext
    /// authentication.
    pub fn to_bytes(&self) -> [u8; 16] {
        (u128::from(self.k0) << 64 | u128::from(self.k1)).to_be_bytes()
    }

    /// The keyed hasher for placement.
    pub fn hasher(&self) -> KeyHasher {
        KeyHasher(SipHasher13::new_with_keys(self.k0, self.k1))
    }

    /// Hash id and fingerprint a server announces in Hello type 6.
    pub fn fingerprint(&self) -> Fingerprint {
        Fingerprint {
            hash: HashId::SIP13,
            value: self.hasher().hash(FINGERPRINT_LABEL).0,
        }
    }
}

impl fmt::Debug for ClusterSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClusterSecret(..)")
    }
}

/// SipHash-1-3 keyed by the cluster secret: hash id 1, the only placement
/// hash of version 1.
#[derive(Clone)]
pub struct KeyHasher(SipHasher13);

impl fmt::Debug for KeyHasher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("KeyHasher(..)")
    }
}

impl KeyHasher {
    /// Hashes a key for placement.
    #[inline]
    pub fn hash(&self, key: &[u8]) -> KeyHash {
        KeyHash(self.0.hash(key))
    }
}

/// The placement hash of a key.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct KeyHash(pub u64);

impl KeyHash {
    /// The top 14 bits.
    #[inline]
    pub fn slot(self) -> Slot {
        Slot::from_top_bits(self.0 >> (u64::BITS - SLOT_BITS))
    }

    /// The shard of a server with `shards` shards: the low 32 bits scaled
    /// onto `0..shards`.
    #[inline]
    pub fn shard(self, shards: NonZeroU16) -> u16 {
        ((u64::from(self.0 as u32) * u64::from(shards.get())) >> 32) as u16
    }
}
