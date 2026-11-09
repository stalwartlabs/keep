/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::engine::{Engine, with_table_ref};
use crate::entry::Stored;
use crate::key::{Key, KeyClass};
use crate::ops::{SetMode, SetOutcome, Write};
use crate::time::{Deadline, Time, Ttl};
use crate::value::ValueKind;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Record<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub ttl: Option<Ttl>,
    pub kind: ValueKind,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct ExportCursor {
    pub(crate) class: usize,
    pub(crate) part: usize,
}

impl Engine {
    pub fn export(
        &self,
        cursor: &mut ExportCursor,
        budget: usize,
        now: Time,
        select: impl Fn(&[u8]) -> bool,
        mut sink: impl FnMut(Record<'_>),
    ) -> bool {
        let mut left = budget.max(1);
        let values = &self.values;
        while left > 0 {
            let Some(&class) = KeyClass::ALL.get(cursor.class) else {
                return false;
            };
            let step = with_table_ref!(self, class, |table| {
                table.visit(&mut cursor.part, left, &mut |entry| {
                    let deadline = entry.deadline();
                    if !deadline.is_expired(now) && select(entry.key()) {
                        sink(Record {
                            key: entry.key(),
                            value: entry.value().get(values),
                            ttl: deadline.remaining(now),
                            kind: entry.value().kind(),
                        });
                    }
                })
            });
            left = left.saturating_sub(step.visited.max(1));
            if step.done {
                cursor.class += 1;
                cursor.part = 0;
            }
        }
        cursor.class < KeyClass::ALL.len()
    }

    pub fn import(&mut self, record: Record<'_>, mode: SetMode, now: Time) -> Option<SetOutcome> {
        let key = Key::new(record.key)?;
        let write = Write {
            deadline: Deadline::after(now, record.ttl),
            mode,
        };
        Some(self.store(key, record.value, record.kind, write, now))
    }
}
