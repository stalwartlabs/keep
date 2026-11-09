/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::num::NonZeroU32;
use std::ops::Deref;

use crate::limits::{MAX_KEY_LEN, MAX_TOPIC, MAX_VALUE_LEN};

/// A key of 1 to 255 bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Key<'a>(&'a [u8]);

impl<'a> Key<'a> {
    /// Wraps `bytes`, or `None` unless it is 1 to 255 bytes.
    #[inline]
    pub fn new(bytes: &'a [u8]) -> Option<Self> {
        (1..=MAX_KEY_LEN)
            .contains(&bytes.len())
            .then_some(Self(bytes))
    }

    /// The key bytes.
    #[inline]
    pub fn as_bytes(&self) -> &'a [u8] {
        self.0
    }

    /// The key's class: its first byte.
    #[inline]
    pub fn class(&self) -> u8 {
        self.0.first().copied().unwrap_or_default()
    }
}

impl Deref for Key<'_> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.0
    }
}

/// A value, payload or message of at most 4 MiB.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct Value<'a>(&'a [u8]);

impl<'a> Value<'a> {
    /// The empty value.
    pub const EMPTY: Value<'static> = Value(&[]);

    /// Wraps `bytes`, or `None` above 4 MiB.
    #[inline]
    pub fn new(bytes: &'a [u8]) -> Option<Self> {
        (bytes.len() <= MAX_VALUE_LEN as usize).then_some(Self(bytes))
    }

    /// The value bytes.
    #[inline]
    pub fn as_bytes(&self) -> &'a [u8] {
        self.0
    }

    #[inline]
    pub(crate) fn framed(bytes: &'a [u8]) -> Self {
        debug_assert!(bytes.len() <= MAX_VALUE_LEN as usize);
        Self(bytes)
    }
}

impl Deref for Value<'_> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.0
    }
}

/// Time to live in whole seconds, never zero.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct Ttl(NonZeroU32);

impl Ttl {
    /// A TTL of `secs` seconds, or `None` for 0.
    #[inline]
    pub fn from_secs(secs: u32) -> Option<Self> {
        NonZeroU32::new(secs).map(Self)
    }

    /// The TTL in seconds.
    #[inline]
    pub fn secs(self) -> u32 {
        self.0.get()
    }

    #[inline]
    pub(crate) fn wire(ttl: Option<Ttl>) -> u32 {
        ttl.map_or(0, Ttl::secs)
    }
}

/// A lock holder's token: exactly 8 bytes, chosen by the client.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct LockToken([u8; 8]);

impl LockToken {
    /// Length of a token in bytes.
    pub const LEN: usize = 8;

    /// Wraps `bytes`, or `None` unless it is 8 bytes.
    #[inline]
    pub fn new(bytes: &[u8]) -> Option<Self> {
        <[u8; 8]>::try_from(bytes).ok().map(Self)
    }

    /// A `u64` lease token, as its little-endian bytes.
    #[inline]
    pub fn from_u64(value: u64) -> Self {
        Self(value.to_le_bytes())
    }

    /// The token bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }
}

const _: () = assert!(MAX_TOPIC == u16::MAX as u32);

/// A stream topic, 0 to 0xFFFF.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct Topic(u16);

impl Topic {
    /// Topic 0, for a cluster-wide broadcast; the server gives it no special meaning.
    pub const BROADCAST: Topic = Topic(0);

    /// The topic of wire id `wire`, or `None` above `0xFFFF`.
    #[inline]
    pub fn new(wire: u32) -> Option<Self> {
        u16::try_from(wire).ok().map(Self)
    }

    /// The topic id.
    #[inline]
    pub fn get(self) -> u16 {
        self.0
    }

    /// The topic id as sent on the wire.
    #[inline]
    pub fn wire(self) -> u32 {
        u32::from(self.0)
    }
}

impl From<u16> for Topic {
    fn from(topic: u16) -> Self {
        Self(topic)
    }
}

/// Request flag bits.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct Flags(pub u8);

impl Flags {
    /// No flags.
    pub const NONE: Flags = Flags(0);
    /// Respond only on error.
    pub const QUIET: Flags = Flags(0x01);
    /// Store only when absent.
    pub const IF_ABSENT: Flags = Flags(0x02);
    /// Return the remaining TTL.
    pub const WITH_TTL: Flags = Flags(0x04);

    /// Whether every bit of `other` is set.
    #[inline]
    pub const fn contains(self, other: Flags) -> bool {
        self.0 & other.0 == other.0
    }

    /// The bits of both.
    #[inline]
    pub const fn union(self, other: Flags) -> Flags {
        Flags(self.0 | other.0)
    }

    /// These bits without those of `other`.
    #[inline]
    pub const fn without(self, other: Flags) -> Flags {
        Flags(self.0 & !other.0)
    }

    /// These bits when `condition` holds, else none.
    #[inline]
    pub const fn when(self, condition: bool) -> Flags {
        if condition { self } else { Flags::NONE }
    }
}
