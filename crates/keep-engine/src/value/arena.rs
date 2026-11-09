/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::region::{HUGE_PAGE, PAGE, Pages, Region};

pub const ARENA_MIN: usize = 1 << 10;
pub const LARGE_MIN: usize = TRIM_MIN;
const MAX_CLASS_SHIFT: u32 = 22;
const STEPS: usize = 8;
const MIN_SHIFT: u32 = ARENA_MIN.trailing_zeros();
const CLASSES: usize = (MAX_CLASS_SHIFT - MIN_SHIFT) as usize * STEPS + 1;
const CHUNK_SLOTS: usize = 16;
const RETAIN_FLOOR: usize = 4 << 20;
const RETAIN_SHARE: usize = 16;
const SPARE_SLOTS: usize = 2;
const STEP_BYTES: usize = 1 << 20;
const TRIM_MIN: usize = 2 * PAGE;
const TRIM_CLASS: usize = (TRIM_MIN.trailing_zeros() - MIN_SHIFT) as usize * STEPS;
const _: () = assert!(TRIM_MIN.is_power_of_two() && TRIM_MIN >= ARENA_MIN);
const BASE_PAGES_MAX: usize = HUGE_PAGE / CHUNK_SLOTS;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Place {
    pub(super) class: u16,
    chunk: u32,
    index: u32,
}

#[derive(Clone, Copy)]
struct Slot {
    chunk: u32,
    index: u32,
}

struct Filling {
    slot: Slot,
    done: usize,
}

struct Class {
    size: usize,
    per_chunk: u32,
    chunks: Vec<Region>,
    fresh: u32,
    warm: Vec<Slot>,
    cold: Vec<Slot>,
    filling: Option<Filling>,
    live: usize,
}

#[derive(Default)]
pub struct Arena {
    classes: Vec<Class>,
    live_bytes: usize,
    warm_bytes: usize,
    stranded_bytes: usize,
}

fn class_of(len: usize) -> Option<usize> {
    if !(ARENA_MIN..=1 << MAX_CLASS_SHIFT).contains(&len) {
        return None;
    }
    let shift = usize::BITS - 1 - len.leading_zeros();
    let base = 1usize << shift;
    let step = base / STEPS;
    let index = (shift - MIN_SHIFT) as usize * STEPS + (len - base).div_ceil(step);
    (index < CLASSES).then_some(index)
}

fn size_of_class(index: usize) -> usize {
    let base = ARENA_MIN << (index / STEPS);
    base + base / STEPS * (index % STEPS)
}

pub fn slot_size(len: usize) -> Option<usize> {
    class_of(len).map(size_of_class)
}

impl Class {
    fn new(size: usize) -> Class {
        let chunk = (size * CHUNK_SLOTS)
            .max(HUGE_PAGE)
            .next_multiple_of(HUGE_PAGE);
        Class {
            size,
            per_chunk: (chunk / size) as u32,
            chunks: Vec::new(),
            fresh: 0,
            warm: Vec::new(),
            cold: Vec::new(),
            filling: None,
            live: 0,
        }
    }

    #[inline]
    fn offset(&self, slot: Slot) -> usize {
        slot.index as usize * self.size
    }

    fn stranded(&self, slot: Slot) -> usize {
        self.size - Region::whole_pages(self.offset(slot), self.size).1
    }

    fn faulted_tail(&self) -> usize {
        let Some(newest) = self.chunks.len().checked_sub(1) else {
            return 0;
        };
        let filled = self.per_chunk as usize * self.size;
        let sealed = match newest {
            0 => 0,
            _ => self.tail(0, filled) + (newest - 1) * self.tail(1, filled),
        };
        sealed + self.tail(newest, self.fresh as usize * self.size)
    }

    fn tail(&self, chunk: usize, used: usize) -> usize {
        used.next_multiple_of(self.pages(chunk).size()) - used
    }

    fn pages(&self, chunk: usize) -> Pages {
        if chunk == 0 && self.size <= BASE_PAGES_MAX {
            Pages::Base
        } else {
            Pages::Huge
        }
    }

    fn untouched(&mut self) -> Option<Slot> {
        if self.chunks.is_empty() || self.fresh == self.per_chunk {
            let pages = self.pages(self.chunks.len());
            let region = Region::map(self.per_chunk as usize * self.size, pages).ok()?;
            self.chunks.push(region);
            self.fresh = 0;
        }
        let slot = Slot {
            chunk: (self.chunks.len() - 1) as u32,
            index: self.fresh,
        };
        self.fresh += 1;
        Some(slot)
    }

    #[inline]
    fn region(&self, chunk: u32) -> Option<&Region> {
        self.chunks.get(chunk as usize)
    }

    #[inline]
    fn region_mut(&mut self, chunk: u32) -> Option<&mut Region> {
        self.chunks.get_mut(chunk as usize)
    }
}

impl Arena {
    #[inline(always)]
    pub fn store(&mut self, value: &[u8]) -> Option<Place> {
        let class = class_of(value.len())?;
        if self.classes.len() <= class {
            self.classes
                .extend((self.classes.len()..=class).map(|index| Class::new(size_of_class(index))));
        }
        let entry = self.classes.get_mut(class)?;
        let slot = match entry.warm.pop() {
            Some(slot) => {
                self.warm_bytes -= entry.size;
                slot
            }
            None => match entry.cold.pop() {
                Some(slot) => {
                    self.stranded_bytes -= entry.stranded(slot);
                    slot
                }
                None => entry.untouched()?,
            },
        };
        let size = entry.size;
        let offset = slot.index as usize * size;
        if let Some(region) = entry.region_mut(slot.chunk) {
            region.bytes_mut(offset, value.len()).copy_from_slice(value);
        }
        entry.live += 1;
        self.live_bytes += size;
        Some(Place {
            class: class as u16,
            chunk: slot.chunk,
            index: slot.index,
        })
    }

    #[inline]
    pub fn get(&self, place: Place, len: usize) -> &[u8] {
        self.classes
            .get(usize::from(place.class))
            .and_then(|class| {
                let offset = place.index as usize * class.size;
                class
                    .region(place.chunk)
                    .map(|region| region.bytes(offset, len))
            })
            .unwrap_or_default()
    }

    #[inline]
    pub fn free(&mut self, place: Place) {
        if let Some(class) = self.classes.get_mut(usize::from(place.class)) {
            class.warm.push(Slot {
                chunk: place.chunk,
                index: place.index,
            });
            class.live -= 1;
            self.live_bytes -= class.size;
            self.warm_bytes += class.size;
        }
    }

    pub fn retain_cap(&self) -> usize {
        RETAIN_FLOOR.max(self.live_bytes / RETAIN_SHARE)
    }

    pub fn warm_bytes(&self) -> usize {
        self.warm_bytes
    }

    pub fn stuck_warm(&self) -> usize {
        self.classes
            .iter()
            .take_while(|class| class.size < TRIM_MIN)
            .map(|class| class.warm.len() * class.size)
            .sum()
    }

    pub fn fresh_class(&self, len: usize) -> Option<u16> {
        let class = class_of(len)?;
        match self.classes.get(class) {
            Some(entry) if !entry.warm.is_empty() => None,
            _ => u16::try_from(class).ok(),
        }
    }

    pub fn slot_bytes(&self, place: Place) -> usize {
        self.classes
            .get(usize::from(place.class))
            .map_or(0, |class| class.size)
    }

    pub fn maintain(&mut self) {
        let cap = self.retain_cap();
        self.trim(cap);
        self.populate(cap);
    }

    pub fn release(&mut self) {
        self.trim(self.retain_cap());
    }

    fn trim(&mut self, cap: usize) {
        let mut released = 0;
        for spare in [SPARE_SLOTS, 0] {
            for class in self.classes.iter_mut().skip(TRIM_CLASS) {
                let size = class.size;
                while self.warm_bytes > cap
                    && class.warm.len() > spare
                    && (released == 0 || released + size <= STEP_BYTES)
                {
                    let Some(slot) = class.warm.pop() else {
                        break;
                    };
                    let offset = class.offset(slot);
                    let freed = class
                        .region_mut(slot.chunk)
                        .map_or(0, |region| region.release(offset, size));
                    if freed == 0 {
                        class.warm.push(slot);
                        break;
                    }
                    class.cold.push(slot);
                    self.warm_bytes -= size;
                    self.stranded_bytes += size - freed;
                    released += size;
                }
            }
        }
    }

    fn populate(&mut self, cap: usize) {
        let mut populated = 0;
        for class in self.classes.iter_mut() {
            while populated < STEP_BYTES
                && (class.filling.is_some()
                    || (class.live > 0
                        && class.warm.len() < SPARE_SLOTS
                        && self.warm_bytes + class.size <= cap))
            {
                let size = class.size;
                let mut filling = match class.filling.take() {
                    Some(filling) => filling,
                    None => {
                        let slot = match class.cold.pop() {
                            Some(slot) => {
                                self.stranded_bytes -= class.stranded(slot);
                                slot
                            }
                            None => match class.untouched() {
                                Some(slot) => slot,
                                None => break,
                            },
                        };
                        self.warm_bytes += size;
                        Filling { slot, done: 0 }
                    }
                };
                let piece = (size - filling.done).min(STEP_BYTES - populated);
                let offset = class.offset(filling.slot) + filling.done;
                if let Some(region) = class.region_mut(filling.slot.chunk) {
                    region.populate(offset, piece);
                }
                filling.done += piece;
                populated += piece;
                if filling.done == size {
                    class.warm.push(filling.slot);
                } else {
                    class.filling = Some(filling);
                }
            }
        }
    }

    pub fn memory(&self) -> usize {
        self.live_bytes
            + self.warm_bytes
            + self.stranded_bytes
            + self
                .classes
                .iter()
                .map(|class| {
                    class.faulted_tail()
                        + (class.warm.capacity() + class.cold.capacity()) * size_of::<Slot>()
                        + class.chunks.capacity() * size_of::<Region>()
                })
                .sum::<usize>()
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-engine/value/arena.rs"]
mod tests;
