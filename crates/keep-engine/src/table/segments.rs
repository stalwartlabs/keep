/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::entry::Stored;
use crate::hash::Hash;

use super::bucket::Bucket;
use super::buckets::Buckets;
use super::segment::Segment;

pub const PREFIX_BITS: u32 = 24;

pub struct Segments<E, const L: usize, const H: usize> {
    list: Vec<Segment<E, L, H>>,
    ids: Vec<u32>,
    global_depth: u32,
}

impl<E: Stored, const L: usize, const H: usize> Segments<E, L, H> {
    pub fn new(first: Segment<E, L, H>) -> Self {
        Segments {
            list: vec![first],
            ids: vec![0],
            global_depth: 0,
        }
    }

    #[inline]
    fn index(&self, hash: Hash) -> usize {
        (hash.middle() >> (PREFIX_BITS - self.global_depth)) as usize
    }

    #[inline]
    pub fn route(&self, hash: Hash) -> usize {
        self.ids.get(self.index(hash)).copied().unwrap_or(0) as usize
    }

    #[inline]
    pub fn buckets(&self, hash: Hash) -> &[Bucket<E, L, H>] {
        self.ids
            .get(self.index(hash))
            .and_then(|&id| self.list.get(id as usize))
            .map_or(&[], Segment::buckets)
    }

    #[inline]
    pub fn buckets_mut(&mut self, hash: Hash) -> &mut [Bucket<E, L, H>] {
        match self.ids.get(self.index(hash)) {
            Some(&id) => self
                .list
                .get_mut(id as usize)
                .map_or(&mut [], Segment::buckets_mut),
            None => &mut [],
        }
    }

    #[inline]
    pub fn get(&self, id: usize) -> Option<&Segment<E, L, H>> {
        self.list.get(id)
    }

    #[inline]
    pub fn get_mut(&mut self, id: usize) -> Option<&mut Segment<E, L, H>> {
        self.list.get_mut(id)
    }

    pub fn count(&self) -> usize {
        self.list.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Segment<E, L, H>> {
        self.list.iter()
    }

    fn span(&self, hash: Hash, depth: u32) -> (usize, usize) {
        let shift = self.global_depth - depth;
        let start = (self.index(hash) >> shift) << shift;
        (start, start + (1usize << shift))
    }

    pub fn replace(
        &mut self,
        hash: Hash,
        buckets: Buckets<Bucket<E, L, H>>,
    ) -> Option<Buckets<Bucket<E, L, H>>> {
        let id = self.route(hash);
        Some(self.list.get_mut(id)?.swap(buckets))
    }

    pub fn split(&mut self, hash: Hash, mut upper: Segment<E, L, H>) {
        let id = self.route(hash);
        let Some(depth) = self.list.get(id).map(Segment::depth) else {
            return;
        };
        if depth == self.global_depth {
            self.ids = self.ids.iter().flat_map(|&id| [id, id]).collect();
            self.global_depth += 1;
        }
        upper.set_depth(depth + 1);
        if let Some(segment) = self.list.get_mut(id) {
            segment.set_depth(depth + 1);
        }
        let new_id = self.list.len();
        self.list.push(upper);
        let (start, end) = self.span(hash, depth);
        let middle = start + (end - start) / 2;
        if let Some(ids) = self.ids.get_mut(middle..end) {
            ids.fill(new_id as u32);
        }
    }

    pub fn memory(&self) -> usize {
        self.ids.capacity() * size_of::<u32>()
            + self.list.capacity() * size_of::<Segment<E, L, H>>()
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-engine/table/segments/test_support.rs"]
mod test_support;
