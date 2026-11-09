/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::mem;
use std::task::Waker;

use crate::answer::{Answer, Expect};
use crate::error::Error;

const INDEX_BITS: u32 = 20;
const INDEX_MASK: u32 = (1 << INDEX_BITS) - 1;
const GENERATION_MASK: u32 = (1 << (u32::BITS - INDEX_BITS)) - 1;

pub(super) struct Slots {
    entries: Vec<Entry>,
    free: Vec<u32>,
    outstanding: usize,
}

struct Entry {
    generation: u32,
    expect: Expect,
    state: EntryState,
}

enum EntryState {
    Free,
    Waiting(Option<Waker>),
    Done(Result<Answer, Error>),
    Abandoned,
}

pub(super) enum Completion {
    Wake(Option<Waker>),
    Discarded,
    Unknown,
}

impl Slots {
    pub fn new() -> Self {
        Slots {
            entries: Vec::new(),
            free: Vec::new(),
            outstanding: 0,
        }
    }

    pub fn outstanding(&self) -> usize {
        self.outstanding
    }

    pub fn insert(&mut self, expect: Expect) -> Option<u32> {
        let index = match self.free.pop() {
            Some(index) => index,
            None => {
                let index = u32::try_from(self.entries.len())
                    .ok()
                    .filter(|index| *index <= INDEX_MASK)?;
                self.entries.push(Entry {
                    generation: 0,
                    expect,
                    state: EntryState::Free,
                });
                index
            }
        };
        let entry = self.entries.get_mut(index as usize)?;
        entry.generation = match (entry.generation + 1) & GENERATION_MASK {
            0 => 1,
            generation => generation,
        };
        entry.expect = expect;
        entry.state = EntryState::Waiting(None);
        self.outstanding += 1;
        Some((entry.generation << INDEX_BITS) | index)
    }

    fn entry(&mut self, id: u32) -> Option<(&mut Entry, u32)> {
        let index = id & INDEX_MASK;
        let generation = id >> INDEX_BITS;
        self.entries
            .get_mut(index as usize)
            .filter(|entry| entry.generation == generation)
            .map(|entry| (entry, index))
    }

    pub fn complete(
        &mut self,
        id: u32,
        answer: impl FnOnce(Expect) -> Result<Answer, Error>,
    ) -> Completion {
        let Some((entry, index)) = self.entry(id) else {
            return Completion::Unknown;
        };
        match mem::replace(&mut entry.state, EntryState::Free) {
            EntryState::Waiting(waker) => {
                entry.state = EntryState::Done(answer(entry.expect));
                self.outstanding -= 1;
                Completion::Wake(waker)
            }
            EntryState::Abandoned => {
                self.outstanding -= 1;
                self.free.push(index);
                Completion::Discarded
            }
            state => {
                entry.state = state;
                Completion::Unknown
            }
        }
    }

    pub fn take(&mut self, id: u32) -> Option<Result<Answer, Error>> {
        let (entry, index) = self.entry(id)?;
        if !matches!(entry.state, EntryState::Done(_)) {
            return None;
        }
        let EntryState::Done(result) = mem::replace(&mut entry.state, EntryState::Free) else {
            return None;
        };
        self.free.push(index);
        Some(result)
    }

    pub fn park(&mut self, id: u32, waker: &Waker) {
        if let Some((entry, _)) = self.entry(id)
            && let EntryState::Waiting(slot) = &mut entry.state
        {
            match slot {
                Some(current) if current.will_wake(waker) => {}
                _ => *slot = Some(waker.clone()),
            }
        }
    }

    pub fn abandon(&mut self, id: u32) {
        let Some((entry, index)) = self.entry(id) else {
            return;
        };
        match entry.state {
            EntryState::Waiting(_) => entry.state = EntryState::Abandoned,
            EntryState::Done(_) => {
                entry.state = EntryState::Free;
                self.free.push(index);
            }
            EntryState::Free | EntryState::Abandoned => {}
        }
    }

    pub fn fail_all(&mut self, error: &Error, wakers: &mut Vec<Waker>) {
        for (index, entry) in (0u32..).zip(self.entries.iter_mut()) {
            match mem::replace(&mut entry.state, EntryState::Free) {
                EntryState::Waiting(waker) => {
                    entry.state = EntryState::Done(Err(error.clone()));
                    wakers.extend(waker);
                }
                EntryState::Abandoned => self.free.push(index),
                state => entry.state = state,
            }
        }
        self.outstanding = 0;
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-client/conn/slots.rs"]
mod tests;
