/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::engine::Engine;
use crate::entry::Stored;
use crate::table::{LineTable, Sweep};
use crate::time::Time;
use crate::value::Values;

#[derive(Clone, Copy, Debug)]
pub struct SweepPace {
    period_ms: u64,
    carry: u64,
}

impl SweepPace {
    pub const DEFAULT_PERIOD_MS: u64 = 120_000;

    pub fn new(period_ms: u64) -> SweepPace {
        SweepPace {
            period_ms: period_ms.max(1),
            carry: 0,
        }
    }

    pub fn budget(&mut self, len: usize, elapsed_ms: u64) -> usize {
        let due = (len as u64)
            .saturating_mul(elapsed_ms)
            .saturating_add(self.carry);
        self.carry = due % self.period_ms;
        usize::try_from(due / self.period_ms).unwrap_or(usize::MAX)
    }
}

impl Default for SweepPace {
    fn default() -> Self {
        SweepPace::new(Self::DEFAULT_PERIOD_MS)
    }
}

impl Engine {
    pub fn maintain(&mut self, now: Time, budget: usize) -> Sweep {
        self.enforce_cap(now);
        self.values.maintain(!self.over_line());
        let mut sweep = Sweep::default();
        if budget == 0 {
            return sweep;
        }
        let total = self.len().max(1);
        let values = &mut self.values;
        let share = Share { now, budget, total };
        share.step(&mut self.k8, values, &mut sweep);
        share.step(&mut self.k16, values, &mut sweep);
        share.step(&mut self.k24, values, &mut sweep);
        share.step(&mut self.k32, values, &mut sweep);
        share.step(&mut self.k40, values, &mut sweep);
        share.step(&mut self.k48, values, &mut sweep);
        share.step(&mut self.long, values, &mut sweep);
        sweep
    }
}

struct Share {
    now: Time,
    budget: usize,
    total: usize,
}

impl Share {
    fn step<E: Stored, const L: usize, const H: usize>(
        &self,
        table: &mut LineTable<E, L, H>,
        values: &mut Values,
        sweep: &mut Sweep,
    ) {
        let len = table.len();
        if len > 0 {
            let share = self.budget.saturating_mul(len) / self.total;
            let done = table.maintain(self.now, share.max(1), values);
            sweep.scanned += done.scanned;
            sweep.reclaimed += done.reclaimed;
        }
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-engine/sweep.rs"]
mod tests;
