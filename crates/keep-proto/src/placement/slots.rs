/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use crate::topology::NodeId;

/// Bits of a key hash that pick its slot.
pub const SLOT_BITS: u32 = 14;

/// Number of slots.
pub const SLOT_COUNT: usize = 1 << SLOT_BITS;

const SALT_DOMAIN: u64 = 0x6b65_6570_6872_7731;

/// One of the 16,384 slots keys hash into.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Slot(u16);

impl Slot {
    /// The highest slot index.
    pub const MAX: u16 = (SLOT_COUNT - 1) as u16;

    /// The slot `index`, or `None` above `MAX`.
    pub fn new(index: u16) -> Option<Slot> {
        (index <= Slot::MAX).then_some(Slot(index))
    }

    #[inline]
    pub(crate) fn from_top_bits(bits: u64) -> Slot {
        Slot(bits as u16 & Slot::MAX)
    }

    /// The slot index.
    #[inline]
    pub fn get(self) -> u16 {
        self.0
    }

    /// The slot index as a `usize`.
    #[inline]
    pub fn index(self) -> usize {
        usize::from(self.0)
    }

    /// Every slot in order.
    pub fn all() -> impl Iterator<Item = Slot> {
        (0..=Slot::MAX).map(Slot)
    }
}

/// The MurmurHash3 64-bit finalizer.
#[inline]
pub const fn fmix64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^= x >> 33;
    x
}

/// A member's precomputed rendezvous salt.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct NodeSalt(u64);

impl NodeSalt {
    /// The salt of node `node`.
    #[inline]
    pub fn of(node: NodeId) -> NodeSalt {
        NodeSalt(fmix64(node.0 ^ SALT_DOMAIN))
    }

    /// Rendezvous score of this member for `point`.
    #[inline]
    pub fn score(self, point: u64) -> u64 {
        fmix64(point ^ self.0)
    }
}

/// Index of the salt with the highest score for `point`; the lowest index
/// wins a tie.
#[inline]
pub fn hrw_pick(point: u64, salts: &[NodeSalt]) -> Option<usize> {
    let (first, rest) = salts.split_first()?;
    let mut best = (first.score(point), 0);
    for (index, salt) in rest.iter().enumerate() {
        let score = salt.score(point);
        if score > best.0 {
            best = (score, index + 1);
        }
    }
    Some(best.1)
}
