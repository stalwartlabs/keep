/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct Handle {
    index: u32,
    generation: u32,
}

impl Handle {
    pub const fn new(index: u32, generation: u32) -> Handle {
        Handle { index, generation }
    }

    #[inline]
    pub const fn index(self) -> u32 {
        self.index
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        self.generation
    }
}

struct Slot<T> {
    generation: u32,
    value: Option<T>,
}

pub struct Slab<T> {
    slots: Vec<Slot<T>>,
    free: Vec<u32>,
    len: usize,
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Slab {
            slots: Vec::new(),
            free: Vec::new(),
            len: 0,
        }
    }
}

impl<T> Slab<T> {
    pub fn next_handle(&self) -> Option<Handle> {
        match self.free.last() {
            Some(&index) => self
                .slots
                .get(index as usize)
                .map(|slot| Handle::new(index, slot.generation)),
            None => u32::try_from(self.slots.len())
                .ok()
                .map(|index| Handle::new(index, 0)),
        }
    }

    pub fn insert(&mut self, value: T) -> Option<Handle> {
        if let Some(&index) = self.free.last()
            && let Some(slot) = self.slots.get_mut(index as usize)
        {
            self.free.pop();
            slot.value = Some(value);
            self.len += 1;
            return Some(Handle::new(index, slot.generation));
        }
        let index = u32::try_from(self.slots.len()).ok()?;
        self.slots.push(Slot {
            generation: 0,
            value: Some(value),
        });
        self.len += 1;
        Some(Handle::new(index, 0))
    }

    #[inline]
    pub fn get_mut(&mut self, index: u32) -> Option<&mut T> {
        self.slots.get_mut(index as usize)?.value.as_mut()
    }

    #[inline]
    pub fn get_at(&self, handle: Handle) -> Option<&T> {
        let slot = self.slots.get(handle.index as usize)?;
        if slot.generation == handle.generation {
            slot.value.as_ref()
        } else {
            None
        }
    }

    #[inline]
    pub fn get_mut_at(&mut self, handle: Handle) -> Option<&mut T> {
        let slot = self.slots.get_mut(handle.index as usize)?;
        if slot.generation == handle.generation {
            slot.value.as_mut()
        } else {
            None
        }
    }

    #[inline]
    pub fn handle(&self, index: u32) -> Option<Handle> {
        let slot = self.slots.get(index as usize)?;
        slot.value
            .as_ref()
            .map(|_| Handle::new(index, slot.generation))
    }

    pub fn remove(&mut self, index: u32) -> Option<T> {
        let slot = self.slots.get_mut(index as usize)?;
        let value = slot.value.take()?;
        slot.generation = slot.generation.wrapping_add(1);
        self.free.push(index);
        self.len -= 1;
        Some(value)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn end(&self) -> u32 {
        u32::try_from(self.slots.len()).unwrap_or(u32::MAX)
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-rt/slab.rs"]
mod tests;
