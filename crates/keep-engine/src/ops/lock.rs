/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::engine::{Engine, with_table};
use crate::entry::Stored;
use crate::key::Key;
use crate::lock::{Gate, LockOutcome, Token, TokenOutcome};
use crate::table::probe;
use crate::time::{Deadline, Time, Ttl};
use crate::value::InlineValue;

use super::kv::insert_out_of_line;

impl Engine {
    #[inline]
    pub fn try_lock(
        &mut self,
        key: Key<'_>,
        token: Token,
        ttl: Ttl,
        now: Time,
        gate: Gate,
    ) -> LockOutcome {
        let hash = self.hasher.hash(&key);
        let deadline = Deadline::after(now, Some(ttl));
        let room = &self.room;
        let values = &mut self.values;
        with_table!(self, key.class(), |table| {
            if let Some(entry) = table.find_mut(hash, &probe(&*table, key)) {
                if !entry.deadline().is_expired(now) {
                    if !entry.value().holds(token) {
                        return LockOutcome::Held;
                    }
                    entry.set_deadline(deadline);
                    return LockOutcome::Granted;
                }
                if gate == Gate::Quarantined {
                    return LockOutcome::Held;
                }
                entry.value_mut().release(values);
                *entry.value_mut() = InlineValue::token(token);
                entry.set_deadline(deadline);
                return LockOutcome::Granted;
            }
            if gate == Gate::Quarantined {
                return LockOutcome::Held;
            }
            let entry = Stored::create(key, InlineValue::token(token), deadline);
            if insert_out_of_line(table, values, hash, entry, now, room) {
                LockOutcome::Granted
            } else {
                LockOutcome::Full
            }
        })
    }

    #[inline]
    pub fn unlock(&mut self, key: Key<'_>, token: Token, now: Time) -> TokenOutcome {
        let hash = self.hasher.hash(&key);
        let values = &mut self.values;
        with_table!(self, key.class(), |table| {
            let probe = probe(&*table, key);
            match table.find(hash, &probe) {
                Some(entry) if !entry.deadline().is_expired(now) => {
                    if !entry.value().holds(token) {
                        return TokenOutcome::Conflict;
                    }
                }
                _ => return TokenOutcome::NotFound,
            }
            if let Some(mut entry) = table.remove(hash, &probe) {
                entry.value_mut().release(values);
            }
            TokenOutcome::Done
        })
    }

    #[inline]
    pub fn renew_lock(&mut self, key: Key<'_>, token: Token, ttl: Ttl, now: Time) -> TokenOutcome {
        let hash = self.hasher.hash(&key);
        let deadline = Deadline::after(now, Some(ttl));
        with_table!(self, key.class(), |table| {
            match table.find_mut(hash, &probe(&*table, key)) {
                Some(entry) if !entry.deadline().is_expired(now) => {
                    if !entry.value().holds(token) {
                        return TokenOutcome::Conflict;
                    }
                    entry.set_deadline(deadline);
                    TokenOutcome::Done
                }
                _ => TokenOutcome::NotFound,
            }
        })
    }
}
