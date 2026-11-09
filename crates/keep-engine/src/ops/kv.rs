/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::cap::Eviction;
use crate::engine::{Engine, Hit, with_table};
use crate::entry::Stored;
use crate::hash::Hash;
use crate::key::Key;
use crate::table::{LineTable, Room, probe};
use crate::time::{Deadline, Time, Ttl};
use crate::value::{ARENA_MIN, InlineValue, LARGE_MIN, ValueKind, Values, heap_size, slot_size};

use super::{SetMode, SetOutcome};

#[derive(Clone, Copy)]
pub(crate) struct Write {
    pub deadline: Deadline,
    pub mode: SetMode,
}

impl Engine {
    #[inline]
    pub fn get(&mut self, key: Key<'_>, now: Time) -> Option<Hit<'_>> {
        let hash = self.hasher.hash(&key);
        let values = &self.values;
        with_table!(self, key.class(), |table| get(
            table, values, hash, key, now
        ))
    }

    #[inline]
    pub fn exists(&mut self, key: Key<'_>, now: Time) -> bool {
        self.get(key, now).is_some()
    }

    #[inline]
    pub fn set(
        &mut self,
        key: Key<'_>,
        value: &[u8],
        ttl: Option<Ttl>,
        mode: SetMode,
        now: Time,
    ) -> SetOutcome {
        let write = Write {
            deadline: Deadline::after(now, ttl),
            mode,
        };
        self.store(key, value, ValueKind::Bytes, write, now)
    }

    pub(crate) fn store(
        &mut self,
        key: Key<'_>,
        value: &[u8],
        kind: ValueKind,
        write: Write,
        now: Time,
    ) -> SetOutcome {
        if value.len() >= self.cold_from
            && let Some(outcome) = self.store_big(key, value, write, now)
        {
            return outcome;
        }
        let hash = self.hasher.hash(&key);
        let room = &self.room;
        let values = &mut self.values;
        with_table!(self, key.class(), |table| {
            if let Some(entry) = table.find_mut(hash, &probe(&*table, key)) {
                if write.mode == SetMode::IfAbsent && !entry.deadline().is_expired(now) {
                    return SetOutcome::Exists;
                }
                entry.value_mut().replace(kind, value, values);
                entry.set_deadline(write.deadline);
                return SetOutcome::Stored;
            }
            let stored = InlineValue::of_kind(kind, value, values);
            let entry = Stored::create(key, stored, write.deadline);
            if insert(table, values, hash, entry, now, room) {
                SetOutcome::Stored
            } else {
                SetOutcome::Full
            }
        })
    }

    #[cold]
    #[inline(never)]
    fn store_big(
        &mut self,
        key: Key<'_>,
        value: &[u8],
        write: Write,
        now: Time,
    ) -> Option<SetOutcome> {
        if value.len() >= LARGE_MIN
            && let Some(size) = self.large_size(key, value.len())
        {
            return Some(self.store_large(key, value, size, write, now));
        }
        if self.over_line() {
            return self.store_over(key, value.len(), write.mode, now);
        }
        None
    }

    fn store_large(
        &mut self,
        key: Key<'_>,
        value: &[u8],
        size: usize,
        write: Write,
        now: Time,
    ) -> SetOutcome {
        let hash = self.hasher.hash(&key);
        if write.mode == SetMode::IfAbsent && self.live(key, hash, now) {
            return SetOutcome::Exists;
        }
        if !self.make_large_room(size, now) {
            return SetOutcome::Full;
        }
        self.delete_hashed(key, hash, now);
        let room = &self.room;
        let values = &mut self.values;
        with_table!(self, key.class(), |table| {
            let stored = InlineValue::large(key.as_bytes(), value, values);
            let entry = Stored::create(key, stored, write.deadline);
            if insert_out_of_line(table, values, hash, entry, now, room) {
                SetOutcome::Stored
            } else {
                SetOutcome::Full
            }
        })
    }

    #[cold]
    #[inline(never)]
    fn store_over(
        &mut self,
        key: Key<'_>,
        len: usize,
        mode: SetMode,
        now: Time,
    ) -> Option<SetOutcome> {
        let cap = self.cap?;
        if cap.pinned.contains(&key) {
            return None;
        }
        let hash = self.hasher.hash(&key);
        let (need, class) = if len < ARENA_MIN {
            let values = &self.values;
            let held = with_table!(self, key.class(), |table| table
                .find(hash, &probe(&*table, key))
                .map(|entry| {
                    let live = !entry.deadline().is_expired(now);
                    (values.frees(entry.value(), None).0, live)
                }));
            if mode == SetMode::IfAbsent && held.is_some_and(|(_, live)| live) {
                return None;
            }
            let need = heap_size(len).saturating_sub(held.map_or(0, |(freed, _)| freed));
            if need == 0 {
                return None;
            }
            (need, None)
        } else {
            self.values.fresh_class(len)?;
            if mode == SetMode::IfAbsent && self.live(key, hash, now) {
                return Some(SetOutcome::Exists);
            }
            self.delete_hashed(key, hash, now);
            (slot_size(len)?, Some(self.values.fresh_class(len)?))
        };
        if self.credit < need && cap.eviction == Eviction::Count {
            let values = &mut self.values;
            self.credit += with_table!(self, key.class(), |table| table.evict_bytes(
                hash,
                need,
                class,
                values,
                &cap.pinned
            ));
        }
        match self.credit.checked_sub(need) {
            Some(rest) => {
                self.credit = rest;
                None
            }
            None => Some(SetOutcome::Full),
        }
    }

    fn live(&mut self, key: Key<'_>, hash: Hash, now: Time) -> bool {
        let values = &self.values;
        with_table!(self, key.class(), |table| get(
            table, values, hash, key, now
        ))
        .is_some()
    }

    #[inline]
    pub fn delete(&mut self, key: Key<'_>, now: Time) -> bool {
        let hash = self.hasher.hash(&key);
        let values = &mut self.values;
        with_table!(self, key.class(), |table| remove(
            table, values, hash, key, now
        ))
    }

    fn delete_hashed(&mut self, key: Key<'_>, hash: Hash, now: Time) -> bool {
        let values = &mut self.values;
        with_table!(self, key.class(), |table| remove(
            table, values, hash, key, now
        ))
    }

    #[inline]
    pub fn renew(&mut self, key: Key<'_>, ttl: Ttl, now: Time) -> bool {
        let hash = self.hasher.hash(&key);
        let deadline = Deadline::after(now, Some(ttl));
        with_table!(self, key.class(), |table| {
            match table.find_mut(hash, &probe(&*table, key)) {
                Some(entry) if !entry.deadline().is_expired(now) => {
                    entry.set_deadline(deadline);
                    true
                }
                _ => false,
            }
        })
    }
}

#[inline]
fn get<'t, E: Stored, const L: usize, const H: usize>(
    table: &'t LineTable<E, L, H>,
    values: &'t Values,
    hash: Hash,
    key: Key<'_>,
    now: Time,
) -> Option<Hit<'t>> {
    let entry = table.find(hash, &E::probe(key))?;
    let deadline = entry.deadline();
    (!deadline.is_expired(now)).then(|| Hit {
        value: entry.value().get(values),
        deadline,
    })
}

#[inline(always)]
fn remove<E: Stored, const L: usize, const H: usize>(
    table: &mut LineTable<E, L, H>,
    values: &mut Values,
    hash: Hash,
    key: Key<'_>,
    now: Time,
) -> bool {
    match table.remove(hash, &E::probe(key)) {
        Some(mut entry) => {
            entry.value_mut().release(values);
            !entry.deadline().is_expired(now)
        }
        None => false,
    }
}

#[inline(never)]
pub(super) fn insert_out_of_line<E: Stored, const L: usize, const H: usize>(
    table: &mut LineTable<E, L, H>,
    values: &mut Values,
    hash: Hash,
    entry: E,
    now: Time,
    room: &Room,
) -> bool {
    insert(table, values, hash, entry, now, room)
}

#[inline(always)]
pub(super) fn insert<E: Stored, const L: usize, const H: usize>(
    table: &mut LineTable<E, L, H>,
    values: &mut Values,
    hash: Hash,
    entry: E,
    now: Time,
    room: &Room,
) -> bool {
    match table.insert(hash, entry, now, values, room) {
        Ok(()) => true,
        Err(mut refused) => {
            refused.value_mut().release(values);
            false
        }
    }
}
