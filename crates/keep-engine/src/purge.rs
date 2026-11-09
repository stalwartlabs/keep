/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::engine::{Engine, with_table};
use crate::key::KeyClass;
use crate::snapshot::ExportCursor;

pub(crate) struct PendingPurge {
    prefix: Box<[u8]>,
    class: usize,
    cursor: usize,
}

impl Engine {
    pub fn delete_prefix(&mut self, prefix: &[u8]) {
        self.purges.push_back(PendingPurge {
            prefix: prefix.into(),
            class: 0,
            cursor: 0,
        });
    }

    pub fn purge(&mut self, budget: usize) -> bool {
        let mut left = budget.max(1);
        while left > 0 {
            let Some(pending) = self.purges.front_mut() else {
                return false;
            };
            let Some(&class) = KeyClass::ALL.get(pending.class) else {
                self.purges.pop_front();
                continue;
            };
            let prefix = &pending.prefix;
            let doomed = |key: &[u8]| key.starts_with(prefix);
            let values = &mut self.values;
            let step = with_table!(self, class, |table| {
                table.purge(&mut pending.cursor, left, &doomed, values)
            });
            left = left.saturating_sub(step.visited.max(1));
            if step.done {
                pending.class += 1;
                pending.cursor = 0;
            }
        }
        !self.purges.is_empty()
    }

    pub fn purge_where(
        &mut self,
        cursor: &mut ExportCursor,
        budget: usize,
        doomed: impl Fn(&[u8]) -> bool,
    ) -> bool {
        let mut left = budget.max(1);
        while left > 0 {
            let Some(&class) = KeyClass::ALL.get(cursor.class) else {
                return false;
            };
            let values = &mut self.values;
            let step = with_table!(self, class, |table| {
                table.purge(&mut cursor.part, left, &doomed, values)
            });
            left = left.saturating_sub(step.visited.max(1));
            if step.done {
                cursor.class += 1;
                cursor.part = 0;
            }
        }
        cursor.class < KeyClass::ALL.len()
    }
}
