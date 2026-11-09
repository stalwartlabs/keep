/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use keep_proto::{SLOT_COUNT, Slot};

const WORDS: usize = SLOT_COUNT / 64;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SlotSet(Box<[u64; WORDS]>);

impl Default for SlotSet {
    fn default() -> SlotSet {
        SlotSet(Box::new([0; WORDS]))
    }
}

impl SlotSet {
    pub const BYTES: usize = SLOT_COUNT / 8;

    #[inline]
    pub fn contains(&self, slot: Slot) -> bool {
        self.0
            .get(slot.index() / 64)
            .is_some_and(|word| word & (1 << (slot.index() % 64)) != 0)
    }

    pub fn insert(&mut self, slot: Slot) {
        if let Some(word) = self.0.get_mut(slot.index() / 64) {
            *word |= 1 << (slot.index() % 64);
        }
    }

    pub fn remove(&mut self, slot: Slot) {
        if let Some(word) = self.0.get_mut(slot.index() / 64) {
            *word &= !(1 << (slot.index() % 64));
        }
    }

    pub fn len(&self) -> usize {
        self.0.iter().map(|word| word.count_ones() as usize).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|word| *word == 0)
    }

    pub fn clear(&mut self) {
        self.0.fill(0);
    }

    pub fn iter(&self) -> impl Iterator<Item = Slot> + '_ {
        self.0
            .iter()
            .zip((0u16..).step_by(64))
            .flat_map(|(word, base)| Bits(*word).map(move |bit| base + bit))
            .filter_map(Slot::new)
    }

    pub fn is_subset(&self, other: &SlotSet) -> bool {
        self.0
            .iter()
            .zip(other.0.iter())
            .all(|(word, more)| word & !more == 0)
    }

    pub fn union_with(&mut self, other: &SlotSet) {
        for (word, more) in self.0.iter_mut().zip(other.0.iter()) {
            *word |= more;
        }
    }

    pub fn intersects(&self, other: &SlotSet) -> bool {
        self.0
            .iter()
            .zip(other.0.iter())
            .any(|(word, more)| word & more != 0)
    }

    pub fn intersect_with(&mut self, other: &SlotSet) {
        for (word, keep) in self.0.iter_mut().zip(other.0.iter()) {
            *word &= keep;
        }
    }

    pub fn subtract(&mut self, other: &SlotSet) {
        for (word, less) in self.0.iter_mut().zip(other.0.iter()) {
            *word &= !less;
        }
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.reserve(SlotSet::BYTES);
        for word in self.0.iter() {
            out.extend_from_slice(&word.to_le_bytes());
        }
    }

    pub fn decode(bytes: &[u8]) -> Option<SlotSet> {
        let (words, rest) = bytes.as_chunks::<8>();
        if words.len() != WORDS || !rest.is_empty() {
            return None;
        }
        let mut set = SlotSet::default();
        for (word, raw) in set.0.iter_mut().zip(words) {
            *word = u64::from_le_bytes(*raw);
        }
        Some(set)
    }
}

impl FromIterator<Slot> for SlotSet {
    fn from_iter<I: IntoIterator<Item = Slot>>(slots: I) -> SlotSet {
        let mut set = SlotSet::default();
        for slot in slots {
            set.insert(slot);
        }
        set
    }
}

struct Bits(u64);

impl Iterator for Bits {
    type Item = u16;

    #[inline]
    fn next(&mut self) -> Option<u16> {
        if self.0 == 0 {
            return None;
        }
        let bit = self.0.trailing_zeros() as u16;
        self.0 &= self.0 - 1;
        Some(bit)
    }
}
