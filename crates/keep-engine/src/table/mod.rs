/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

mod bucket;
mod buckets;
mod segment;
mod segments;

use std::cmp::Reverse;
use std::mem;

use crate::cap::{Eviction, Pinned};
use crate::entry::Stored;
use crate::hash::{Hash, KeyHasher};
use crate::key::Key;
use crate::time::Time;
use crate::value::Values;

use bucket::{Bucket, lanes};
use buckets::Buckets;
use segment::{Placed, Purged, Segment, locate, touch_home};
use segments::{PREFIX_BITS, Segments};

pub use segment::Sweep;

const _: () = {
    use crate::entry::{Compact, LongSlot};
    assert!(size_of::<Segment<Compact<16>, 4, 8>>() == 32);
    assert!(size_of::<Segment<LongSlot, 3, 8>>() == 32);
    assert!(size_of::<Bucket<Compact<8>, 5, 8>>() == 128);
    assert!(size_of::<Bucket<Compact<16>, 4, 8>>() == 128);
    assert!(size_of::<Bucket<Compact<24>, 3, 8>>() == 128);
    assert!(size_of::<Bucket<Compact<32>, 4, 8>>() == 192);
    assert!(size_of::<Bucket<LongSlot, 3, 8>>() == 128);
    assert!(size_of::<Bucket<Compact<40>, 4, 8>>() == 256);
    assert!(size_of::<Bucket<Compact<48>, 4, 8>>() == 256);
};

const MIN_BUCKETS: usize = 4;
const MAX_BUCKETS: usize = 256;
const GROWTH_PERCENT: usize = 125;
const SPLIT_BUCKETS: usize = MAX_BUCKETS * GROWTH_PERCENT / 200;
const EVICT_BATCH: usize = 8;

#[derive(Clone, Copy)]
pub enum Room {
    Free,
    Capped(Eviction, Pinned),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Walked {
    pub visited: usize,
    pub removed: usize,
    pub done: bool,
}

enum Growth {
    Same,
    Grow,
    Split,
}

type Score = (u64, u32, u32);

fn score<E: Stored>(entry: &E) -> (u64, u32) {
    let deadline = entry.deadline();
    let until = if deadline.is_never() {
        u32::MAX
    } else {
        deadline.raw()
    };
    let count = entry
        .value()
        .as_counter()
        .map_or(0, |count| count.max(0) as u64);
    (count, until)
}

pub struct LineTable<E, const L: usize, const H: usize> {
    segments: Segments<E, L, H>,
    hasher: KeyHasher,
    len: usize,
    heap: usize,
    buckets: usize,
    cursor: (usize, usize),
    scratch: Vec<E>,
    scores: Vec<Score>,
}

#[inline]
pub fn probe<'k, E: Stored, const L: usize, const H: usize>(
    _table: &LineTable<E, L, H>,
    key: Key<'k>,
) -> E::Probe<'k> {
    E::probe(key)
}

impl<E: Stored, const L: usize, const H: usize> LineTable<E, L, H> {
    fn grown(size: usize) -> usize {
        (size * GROWTH_PERCENT / 100).max(size + 1)
    }

    fn drain(
        &mut self,
        hash: Hash,
        id: usize,
        now: Time,
        values: &mut Values,
        scratch: &mut Vec<E>,
    ) -> Option<(usize, u32)> {
        let depth = self.segments.get(id)?.depth();
        let old = self.segments.replace(hash, Buckets::none())?;
        let size = old.len();
        for bucket in old.into_vec() {
            for mut entry in bucket.into_entries() {
                if entry.deadline().is_expired(now) {
                    entry.value_mut().release(values);
                    self.heap -= entry.heap_bytes();
                    self.len -= 1;
                } else {
                    scratch.push(entry);
                }
            }
        }
        Some((size, depth))
    }

    fn replace_all(&mut self, scratch: &mut Vec<E>, now: Time, values: &mut Values) {
        for entry in scratch.drain(..) {
            let hash = self.hasher.hash(entry.key());
            let id = self.segments.route(hash);
            let Some(segment) = self.segments.get_mut(id) else {
                continue;
            };
            match segment.place(hash, entry, now, values, &self.hasher) {
                Ok(Placed::Free) => {}
                Ok(Placed::OverExpired { freed }) => {
                    self.heap -= freed;
                    self.len -= 1;
                }
                Err(entry) => {
                    self.heap -= entry.heap_bytes();
                    self.len -= 1;
                    self.insert_free(hash, entry, now, values);
                }
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn rebuild(&mut self, hash: Hash, id: usize, now: Time, values: &mut Values) {
        let mut scratch = mem::take(&mut self.scratch);
        let Some((size, depth)) = self.drain(hash, id, now, values, &mut scratch) else {
            self.scratch = scratch;
            return;
        };
        let growth = if scratch.len() * 4 < Segment::<E, L, H>::limit(size) * 3 {
            Growth::Same
        } else if size < MAX_BUCKETS || depth == PREFIX_BITS {
            Growth::Grow
        } else {
            Growth::Split
        };
        let size = match growth {
            Growth::Same => size,
            Growth::Grow => {
                let grown = if size < MAX_BUCKETS {
                    Self::grown(size).min(MAX_BUCKETS)
                } else {
                    Self::grown(size)
                };
                self.buckets += grown - size;
                grown
            }
            Growth::Split => {
                self.buckets -= size - SPLIT_BUCKETS;
                SPLIT_BUCKETS
            }
        };
        self.segments
            .replace(hash, Buckets::new(size, Bucket::empty));
        if let Growth::Split = growth {
            self.segments
                .split(hash, Segment::new(depth + 1, SPLIT_BUCKETS));
            self.buckets += SPLIT_BUCKETS;
        }
        self.replace_all(&mut scratch, now, values);
        self.scratch = scratch;
    }

    #[cold]
    #[inline(never)]
    fn make_room(
        &mut self,
        hash: Hash,
        id: usize,
        now: Time,
        values: &mut Values,
        eviction: Eviction,
        pinned: &Pinned,
    ) -> bool {
        let mut scratch = mem::take(&mut self.scratch);
        let mut scores = mem::take(&mut self.scores);
        let Some((size, _)) = self.drain(hash, id, now, values, &mut scratch) else {
            self.scratch = scratch;
            self.scores = scores;
            return false;
        };
        let limit = Segment::<E, L, H>::limit(size);
        let excess = scratch.len().saturating_sub(limit * 3 / 4);
        if excess > 0 && eviction == Eviction::Count {
            scores.clear();
            scores.extend(
                scratch
                    .iter()
                    .zip(0u32..)
                    .filter(|(entry, _)| !pinned.contains(entry.key()))
                    .map(|(entry, index)| {
                        let (count, until) = score(entry);
                        (count, until, index)
                    }),
            );
            let victims = excess.min(scores.len());
            if victims > 0 && victims < scores.len() {
                scores.select_nth_unstable(victims);
            }
            if let Some(doomed) = scores.get_mut(..victims) {
                doomed.sort_unstable_by_key(|&(_, _, index)| Reverse(index));
                for &(_, _, index) in doomed.iter() {
                    let index = index as usize;
                    if index < scratch.len() {
                        let mut victim = scratch.swap_remove(index);
                        victim.value_mut().release(values);
                        self.heap -= victim.heap_bytes();
                        self.len -= 1;
                    }
                }
            }
        }
        let room = scratch.len() < limit;
        self.segments
            .replace(hash, Buckets::new(size, Bucket::empty));
        self.replace_all(&mut scratch, now, values);
        self.scratch = scratch;
        self.scores = scores;
        room
    }

    fn insert_free(&mut self, hash: Hash, mut entry: E, now: Time, values: &mut Values) {
        loop {
            let id = self.segments.route(hash);
            let Some(segment) = self.segments.get_mut(id) else {
                return;
            };
            if segment.has_room() {
                let heap = entry.heap_bytes();
                match segment.place(hash, entry, now, values, &self.hasher) {
                    Ok(Placed::Free) => {
                        self.heap += heap;
                        self.len += 1;
                        return;
                    }
                    Ok(Placed::OverExpired { freed }) => {
                        self.heap = self.heap + heap - freed;
                        return;
                    }
                    Err(back) => entry = back,
                }
            }
            self.rebuild(hash, id, now, values);
        }
    }

    fn insert_capped(
        &mut self,
        hash: Hash,
        mut entry: E,
        now: Time,
        values: &mut Values,
        eviction: Eviction,
        pinned: &Pinned,
    ) -> Result<(), E> {
        let pinned_entry = pinned.contains(entry.key());
        loop {
            let id = self.segments.route(hash);
            let Some(segment) = self.segments.get_mut(id) else {
                return Err(entry);
            };
            if segment.has_room() {
                let heap = entry.heap_bytes();
                match segment.place(hash, entry, now, values, &self.hasher) {
                    Ok(Placed::Free) => {
                        self.heap += heap;
                        self.len += 1;
                        return Ok(());
                    }
                    Ok(Placed::OverExpired { freed }) => {
                        self.heap = self.heap + heap - freed;
                        return Ok(());
                    }
                    Err(back) => entry = back,
                }
            }
            let room = self.make_room(hash, id, now, values, eviction, pinned);
            if !room {
                if !pinned_entry {
                    return Err(entry);
                }
                self.rebuild(hash, id, now, values);
            }
        }
    }

    pub fn new(hasher: KeyHasher) -> Self {
        const {
            assert!(L >= 2 && L <= 8 && H > L);
        }
        LineTable {
            segments: Segments::new(Segment::new(0, MIN_BUCKETS)),
            hasher,
            len: 0,
            heap: 0,
            buckets: MIN_BUCKETS,
            cursor: (0, 0),
            scratch: Vec::new(),
            scores: Vec::new(),
        }
    }

    #[inline]
    pub fn find(&self, hash: Hash, probe: &E::Probe<'_>) -> Option<&E> {
        let buckets = self.segments.buckets(hash);
        let (index, lane) = locate(buckets, hash, probe)?;
        buckets.get(index)?.entry(lane)
    }

    #[inline]
    pub fn find_mut(&mut self, hash: Hash, probe: &E::Probe<'_>) -> Option<&mut E> {
        let buckets = self.segments.buckets_mut(hash);
        let (index, lane) = locate(buckets, hash, probe)?;
        buckets.get_mut(index)?.entry_mut(lane)
    }

    #[inline]
    pub fn insert(
        &mut self,
        hash: Hash,
        entry: E,
        now: Time,
        values: &mut Values,
        room: &Room,
    ) -> Result<(), E> {
        match room {
            Room::Free => {
                self.insert_free(hash, entry, now, values);
                Ok(())
            }
            Room::Capped(eviction, pinned) => {
                self.insert_capped(hash, entry, now, values, *eviction, pinned)
            }
        }
    }

    #[cold]
    #[inline(never)]
    pub fn evict_bytes(
        &mut self,
        hash: Hash,
        need: usize,
        class: Option<u16>,
        values: &mut Values,
        pinned: &Pinned,
    ) -> usize {
        let id = self.segments.route(hash);
        let Some(segment) = self.segments.get_mut(id) else {
            return 0;
        };
        let mut scores = mem::take(&mut self.scores);
        scores.clear();
        for (bucket, index) in segment.buckets().iter().zip(0u32..) {
            for lane in lanes(bucket.occupied()) {
                if let Some(entry) = bucket.entry(lane)
                    && !pinned.contains(entry.key())
                {
                    let (count, until) = score(entry);
                    scores.push((count, until, index * L as u32 + lane as u32));
                }
            }
        }
        scores.sort_unstable();
        let target = need.saturating_mul(EVICT_BATCH);
        let (mut freed, mut slots) = (0, 0);
        for &(_, _, at) in &scores {
            let (index, lane) = ((at / L as u32) as usize, (at % L as u32) as usize);
            let Some(entry) = segment
                .buckets()
                .get(index)
                .and_then(|bucket| bucket.entry(lane))
            else {
                continue;
            };
            let (gain, slot) = values.frees(entry.value(), class);
            if gain == 0 && !slot {
                continue;
            }
            let home = self.hasher.hash(entry.key());
            if home == hash {
                continue;
            }
            if let Some(mut victim) = segment.remove(home, index, lane) {
                victim.value_mut().release(values);
                self.heap -= victim.heap_bytes();
                self.len -= 1;
            }
            freed += gain;
            slots += usize::from(slot);
            if freed + slots * need >= target {
                break;
            }
        }
        self.scores = scores;
        freed + need * slots.min(1)
    }

    #[inline]
    pub fn remove(&mut self, hash: Hash, probe: &E::Probe<'_>) -> Option<E> {
        let id = self.segments.route(hash);
        let segment = self.segments.get_mut(id)?;
        let (index, lane) = locate(segment.buckets(), hash, probe)?;
        let removed = segment.remove(hash, index, lane)?;
        self.heap -= removed.heap_bytes();
        self.len -= 1;
        Some(removed)
    }

    pub fn maintain(&mut self, now: Time, budget: usize, values: &mut Values) -> Sweep {
        let mut sweep = Sweep::default();
        let mut spent = 0;
        let mut visited = 0;
        let mut freed = 0;
        while spent < budget && visited < self.buckets {
            let (id, index) = self.cursor;
            let count = self.segments.count();
            let Some(segment) = self.segments.get_mut(id) else {
                self.cursor = (0, 0);
                visited += 1;
                continue;
            };
            let scanned = sweep.scanned;
            let (next, freed_here) =
                segment.sweep(index, budget - spent, now, values, &self.hasher, &mut sweep);
            freed += freed_here;
            let buckets = next.saturating_sub(index).max(1);
            spent += buckets + (sweep.scanned - scanned);
            visited += buckets;
            self.cursor = if next < segment.len_buckets() {
                (id, next)
            } else if id + 1 < count {
                (id + 1, 0)
            } else {
                (0, 0)
            };
        }
        self.len -= sweep.reclaimed;
        self.heap -= freed;
        sweep
    }

    pub fn purge(
        &mut self,
        cursor: &mut usize,
        budget: usize,
        doomed: &impl Fn(&[u8]) -> bool,
        values: &mut Values,
    ) -> Walked {
        let mut step = Walked {
            visited: 0,
            removed: 0,
            done: false,
        };
        while step.visited < budget {
            let Some(segment) = self.segments.get_mut(*cursor) else {
                break;
            };
            let Purged {
                visited,
                removed,
                freed,
            } = segment.purge(doomed, values, &self.hasher);
            self.heap -= freed;
            step.visited += visited.max(1);
            step.removed += removed;
            *cursor += 1;
        }
        step.done = *cursor >= self.segments.count();
        self.len -= step.removed;
        step
    }

    pub fn visit(&self, cursor: &mut usize, budget: usize, f: &mut impl FnMut(&E)) -> Walked {
        let mut step = Walked {
            visited: 0,
            removed: 0,
            done: false,
        };
        while step.visited < budget {
            let Some(segment) = self.segments.get(*cursor) else {
                break;
            };
            let mut visited = 0;
            segment.for_each(&mut |entry| {
                visited += 1;
                f(entry);
            });
            step.visited += visited.max(1);
            *cursor += 1;
        }
        step.done = *cursor >= self.segments.count();
        step
    }

    pub fn for_each(&self, f: &mut impl FnMut(&E)) {
        for segment in self.segments.iter() {
            segment.for_each(f);
        }
    }

    #[inline]
    pub fn prefetch(&self, hash: Hash) {
        touch_home(self.segments.buckets(hash), hash);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn memory(&self) -> usize {
        self.segments.memory()
            + self.heap
            + self.buckets * size_of::<Bucket<E, L, H>>()
            + self.scratch.capacity() * size_of::<E>()
            + self.scores.capacity() * size_of::<Score>()
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-engine/table/mod.rs"]
mod tests;
