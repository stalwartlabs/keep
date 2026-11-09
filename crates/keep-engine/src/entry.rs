/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::key::{Key, same_key};
use crate::time::Deadline;
use crate::value::InlineValue;

pub trait Stored {
    type Probe<'k>: Copy;

    fn probe(key: Key<'_>) -> Self::Probe<'_>;

    fn matches(&self, probe: &Self::Probe<'_>) -> bool;

    fn create(key: Key<'_>, value: InlineValue, deadline: Deadline) -> Self;

    fn key(&self) -> &[u8];

    fn deadline(&self) -> Deadline;

    fn set_deadline(&mut self, deadline: Deadline);

    fn value(&self) -> &InlineValue;

    fn value_mut(&mut self) -> &mut InlineValue;

    fn heap_bytes(&self) -> usize;

    fn vacant() -> Self;
}

#[derive(Clone, Copy, Debug)]
pub struct PaddedKey<const N: usize> {
    bytes: [u8; N],
    len: u8,
}

impl<const N: usize> PaddedKey<N> {
    #[inline]
    fn new(key: Key<'_>) -> PaddedKey<N> {
        PaddedKey {
            bytes: padded(key.as_bytes()),
            len: key.len_u8(),
        }
    }
}

#[inline(always)]
fn padded<const N: usize>(bytes: &[u8]) -> [u8; N] {
    let mut out = [0u8; N];
    let len = bytes.len();
    if N >= 16 && len + 8 > N && len <= N {
        let (head, _) = bytes.split_at(N - 8);
        let (out_head, out_tail) = out.split_at_mut(N - 8);
        out_head.copy_from_slice(head);
        let tail = bytes
            .last_chunk::<8>()
            .map_or(0, |tail| u64::from_le_bytes(*tail));
        out_tail.copy_from_slice(&(tail >> ((N - len) * 8)).to_le_bytes());
    } else if N == 8 && (4..=8).contains(&len) {
        let low = bytes
            .first_chunk::<4>()
            .map_or(0, |low| u32::from_le_bytes(*low));
        let high = bytes
            .last_chunk::<4>()
            .map_or(0, |high| u32::from_le_bytes(*high));
        let word = u64::from(low) | (u64::from(high) << ((len - 4) * 8));
        out.iter_mut()
            .zip(word.to_le_bytes())
            .for_each(|(slot, byte)| *slot = byte);
    } else {
        out.iter_mut()
            .zip(bytes)
            .for_each(|(slot, &byte)| *slot = byte);
    }
    out
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C)]
pub struct Compact<const N: usize> {
    key: [u8; N],
    key_len: u8,
    value: InlineValue,
    deadline: [u8; 4],
}

const _: () = assert!(size_of::<Compact<16>>() == 30 && align_of::<Compact<16>>() == 1);
const _: () = assert!(size_of::<Compact<48>>() == 62);

impl<const N: usize> Stored for Compact<N> {
    type Probe<'k> = PaddedKey<N>;

    #[inline]
    fn probe(key: Key<'_>) -> PaddedKey<N> {
        PaddedKey::new(key)
    }

    #[inline]
    fn matches(&self, probe: &PaddedKey<N>) -> bool {
        self.key_len == probe.len && self.key == probe.bytes
    }

    #[inline]
    fn create(key: Key<'_>, value: InlineValue, deadline: Deadline) -> Compact<N> {
        let probe = PaddedKey::<N>::new(key);
        Compact {
            key: probe.bytes,
            key_len: probe.len,
            value,
            deadline: deadline.raw().to_le_bytes(),
        }
    }

    #[inline]
    fn key(&self) -> &[u8] {
        self.key
            .get(..usize::from(self.key_len))
            .unwrap_or_default()
    }

    #[inline]
    fn deadline(&self) -> Deadline {
        Deadline::from_raw(u32::from_le_bytes(self.deadline))
    }

    #[inline]
    fn set_deadline(&mut self, deadline: Deadline) {
        self.deadline = deadline.raw().to_le_bytes();
    }

    #[inline]
    fn value(&self) -> &InlineValue {
        &self.value
    }

    #[inline]
    fn value_mut(&mut self) -> &mut InlineValue {
        &mut self.value
    }

    fn heap_bytes(&self) -> usize {
        0
    }

    #[inline]
    fn vacant() -> Compact<N> {
        Compact {
            key: [0; N],
            key_len: 0,
            value: InlineValue::default(),
            deadline: [0; 4],
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct LongSlot {
    key: Box<[u8]>,
    value: InlineValue,
    deadline: u32,
}

const _: () = assert!(size_of::<LongSlot>() == 32);

impl Stored for LongSlot {
    type Probe<'k> = &'k [u8];

    #[inline]
    fn probe(key: Key<'_>) -> &[u8] {
        key.as_bytes()
    }

    #[inline]
    fn matches(&self, probe: &&[u8]) -> bool {
        same_key(&self.key, probe)
    }

    fn create(key: Key<'_>, value: InlineValue, deadline: Deadline) -> LongSlot {
        LongSlot {
            key: key.as_bytes().into(),
            value,
            deadline: deadline.raw(),
        }
    }

    #[inline]
    fn key(&self) -> &[u8] {
        &self.key
    }

    #[inline]
    fn deadline(&self) -> Deadline {
        Deadline::from_raw(self.deadline)
    }

    #[inline]
    fn set_deadline(&mut self, deadline: Deadline) {
        self.deadline = deadline.raw();
    }

    #[inline]
    fn value(&self) -> &InlineValue {
        &self.value
    }

    #[inline]
    fn value_mut(&mut self) -> &mut InlineValue {
        &mut self.value
    }

    fn heap_bytes(&self) -> usize {
        self.key.len()
    }

    fn vacant() -> LongSlot {
        LongSlot::default()
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-engine/entry.rs"]
mod tests;
