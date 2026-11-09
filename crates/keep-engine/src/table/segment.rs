/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::entry::Stored;
use crate::hash::{Hash, KeyHasher};
use crate::time::Time;
use crate::value::Values;

use super::bucket::{Bucket, lanes};
use super::buckets::Buckets;

pub enum Placed {
    Free,
    OverExpired { freed: usize },
}

#[derive(Clone, Copy, Default)]
pub struct Purged {
    pub visited: usize,
    pub removed: usize,
    pub freed: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Sweep {
    pub scanned: usize,
    pub reclaimed: usize,
}

#[inline]
fn home(count: usize, hash: Hash) -> usize {
    ((u64::from(hash.low()) * count as u64) >> 32) as usize
}

#[inline]
fn next(index: usize, count: usize) -> usize {
    if index + 1 >= count { 0 } else { index + 1 }
}

#[inline]
fn distance(home: usize, index: usize, count: usize) -> usize {
    if index >= home {
        index - home
    } else {
        index + count - home
    }
}

#[inline]
pub fn locate<E: Stored, const L: usize, const H: usize>(
    buckets: &[Bucket<E, L, H>],
    hash: Hash,
    probe: &E::Probe<'_>,
) -> Option<(usize, usize)> {
    let tag = hash.tag();
    let count = buckets.len();
    let mut index = home(count, hash);
    for _ in 0..count {
        let bucket = buckets.get(index)?;
        bucket.touch_lines();
        if let Some(lane) = bucket.find(tag, probe) {
            return Some((index, lane));
        }
        if bucket.outbound() == 0 {
            return None;
        }
        index = next(index, count);
    }
    None
}

#[inline]
pub fn touch_home<E: Stored, const L: usize, const H: usize>(
    buckets: &[Bucket<E, L, H>],
    hash: Hash,
) {
    if let Some(bucket) = buckets.get(home(buckets.len(), hash)) {
        std::hint::black_box(bucket.outbound());
        bucket.touch_lines();
    }
}

pub struct Segment<E, const L: usize, const H: usize> {
    depth: u32,
    len: usize,
    buckets: Buckets<Bucket<E, L, H>>,
}

impl<E: Stored, const L: usize, const H: usize> Segment<E, L, H> {
    const LOAD: (usize, usize) = if L >= 3 { (7, 8) } else { (13, 16) };

    pub fn new(depth: u32, buckets: usize) -> Self {
        Segment {
            depth,
            len: 0,
            buckets: Buckets::new(buckets, Bucket::empty),
        }
    }

    pub fn limit(buckets: usize) -> usize {
        buckets * L * Self::LOAD.0 / Self::LOAD.1
    }

    #[inline]
    pub fn len_buckets(&self) -> usize {
        self.buckets.len()
    }

    #[inline]
    pub fn depth(&self) -> u32 {
        self.depth
    }

    pub(super) fn set_depth(&mut self, depth: u32) {
        self.depth = depth;
    }

    #[inline]
    pub fn has_room(&self) -> bool {
        self.len < Self::limit(self.buckets.len())
    }

    #[inline]
    pub fn buckets(&self) -> &[Bucket<E, L, H>] {
        self.buckets.as_slice()
    }

    #[inline]
    pub fn buckets_mut(&mut self) -> &mut [Bucket<E, L, H>] {
        self.buckets.as_mut_slice()
    }

    pub(super) fn swap(&mut self, buckets: Buckets<Bucket<E, L, H>>) -> Buckets<Bucket<E, L, H>> {
        self.len = 0;
        std::mem::replace(&mut self.buckets, buckets)
    }

    pub fn place(
        &mut self,
        hash: Hash,
        entry: E,
        now: Time,
        values: &mut Values,
        hasher: &KeyHasher,
    ) -> Result<Placed, E> {
        let count = self.buckets.len();
        let start = home(count, hash);
        let mut index = start;
        for distance in 0..count {
            let Some(bucket) = self.buckets.as_slice().get(index) else {
                break;
            };
            let room = match lanes(bucket.tagged(0)).next() {
                Some(lane) => Some((lane, false)),
                None => bucket.expired_lane(now).map(|lane| (lane, true)),
            };
            if let Some((lane, expired)) = room {
                let placed = if expired {
                    Placed::OverExpired {
                        freed: self.evict(index, lane, values, hasher),
                    }
                } else {
                    Placed::Free
                };
                if let Some(bucket) = self.buckets.as_mut_slice().get_mut(index) {
                    bucket.put(lane, hash.tag(), entry);
                }
                self.walk(start, distance, Bucket::bump);
                self.len += 1;
                return Ok(placed);
            }
            index = next(index, count);
        }
        Err(entry)
    }

    fn walk(&mut self, start: usize, distance: usize, step: fn(&mut Bucket<E, L, H>)) {
        let buckets = self.buckets.as_mut_slice();
        let count = buckets.len();
        let mut index = start;
        for _ in 0..distance {
            if let Some(bucket) = buckets.get_mut(index) {
                step(bucket);
            }
            index = next(index, count);
        }
    }

    fn evict(
        &mut self,
        index: usize,
        lane: usize,
        values: &mut Values,
        hasher: &KeyHasher,
    ) -> usize {
        let Some(mut entry) = self
            .buckets
            .as_mut_slice()
            .get_mut(index)
            .and_then(|bucket| bucket.take(lane))
        else {
            return 0;
        };
        entry.value_mut().release(values);
        let count = self.buckets.len();
        let start = home(count, hasher.hash(entry.key()));
        self.walk(start, distance(start, index, count), Bucket::unbump);
        self.len -= 1;
        entry.heap_bytes()
    }

    pub fn remove(&mut self, hash: Hash, index: usize, lane: usize) -> Option<E> {
        let entry = self.buckets.as_mut_slice().get_mut(index)?.take(lane)?;
        let count = self.buckets.len();
        let start = home(count, hash);
        self.walk(start, distance(start, index, count), Bucket::unbump);
        self.len -= 1;
        Some(entry)
    }

    pub fn sweep(
        &mut self,
        from: usize,
        budget: usize,
        now: Time,
        values: &mut Values,
        hasher: &KeyHasher,
        sweep: &mut Sweep,
    ) -> (usize, usize) {
        let mut freed = 0;
        let mut index = from;
        let mut spent = 0;
        while spent < budget {
            let Some(bucket) = self.buckets.as_slice().get(index) else {
                break;
            };
            let occupied = bucket.occupied();
            let expired = bucket.expired(occupied, now);
            sweep.scanned += occupied.count_ones() as usize;
            spent += 1 + occupied.count_ones() as usize;
            for lane in lanes(expired) {
                freed += self.evict(index, lane, values, hasher);
                sweep.reclaimed += 1;
            }
            index += 1;
        }
        (index, freed)
    }

    pub fn purge(
        &mut self,
        doomed: &impl Fn(&[u8]) -> bool,
        values: &mut Values,
        hasher: &KeyHasher,
    ) -> Purged {
        let mut purged = Purged::default();
        for index in 0..self.buckets.len() {
            let Some(bucket) = self.buckets.as_slice().get(index) else {
                break;
            };
            let occupied = bucket.occupied();
            let mut matched = 0u32;
            for lane in lanes(occupied) {
                if bucket.entry(lane).is_some_and(|entry| doomed(entry.key())) {
                    matched |= 1 << lane;
                }
            }
            purged.visited += occupied.count_ones() as usize;
            for lane in lanes(matched) {
                purged.freed += self.evict(index, lane, values, hasher);
                purged.removed += 1;
            }
        }
        purged
    }

    pub fn for_each(&self, f: &mut impl FnMut(&E)) {
        for bucket in self.buckets.as_slice() {
            bucket.for_each(f);
        }
    }
}
