/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fmt;

use crate::engine::{Engine, with_table};
use crate::entry::Stored;
use crate::key::Key;
use crate::table::probe;
use crate::time::{Deadline, Time, Ttl};
use crate::value::InlineValue;

use super::kv::insert;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum IncrError {
    WrongType,
    Overflow,
    Full,
}

impl fmt::Display for IncrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            IncrError::WrongType => "value is not a counter",
            IncrError::Overflow => "counter overflow",
            IncrError::Full => "memory cap reached",
        })
    }
}

impl std::error::Error for IncrError {}

impl Engine {
    #[inline]
    pub fn incr(
        &mut self,
        key: Key<'_>,
        delta: i64,
        ttl: Option<Ttl>,
        now: Time,
    ) -> Result<i64, IncrError> {
        let hash = self.hasher.hash(&key);
        let deadline = Deadline::after(now, ttl);
        let room = &self.room;
        let values = &mut self.values;
        with_table!(self, key.class(), |table| {
            if let Some(entry) = table.find_mut(hash, &probe(&*table, key)) {
                if !entry.deadline().is_expired(now) {
                    let current = entry.value().as_counter().ok_or(IncrError::WrongType)?;
                    let next = current.checked_add(delta).ok_or(IncrError::Overflow)?;
                    *entry.value_mut() = InlineValue::counter(next);
                    return Ok(next);
                }
                entry.value_mut().release(values);
                *entry.value_mut() = InlineValue::counter(delta);
                entry.set_deadline(deadline);
                return Ok(delta);
            }
            let entry = Stored::create(key, InlineValue::counter(delta), deadline);
            if insert(table, values, hash, entry, now, room) {
                Ok(delta)
            } else {
                Err(IncrError::Full)
            }
        })
    }
}
