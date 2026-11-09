/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::time::Duration;

use keep_engine::{KeepEngine, SweepPace};
use keep_rt::Ctx;
use keep_rt::clock::Nanos;

use crate::clock::EngineClock;
use crate::metrics::ShardStats;

use super::forward::Msg;

const PURGE_STEP: usize = 1024;

const STEP: Duration = Duration::from_millis(10);

const NS_PER_MS: u64 = 1_000_000;

pub(super) struct Upkeep {
    pace: SweepPace,
    last: Nanos,
    purging: bool,
}

impl Upkeep {
    pub(super) fn new(now: Nanos) -> Upkeep {
        Upkeep {
            pace: SweepPace::default(),
            last: now,
            purging: false,
        }
    }

    pub(super) fn purge_started(&mut self) {
        self.purging = true;
    }

    pub(super) fn run(
        &mut self,
        engine: &mut KeepEngine,
        clock: EngineClock,
        stats: &ShardStats,
        cx: &mut Ctx<'_, Msg>,
    ) {
        let now = cx.now();
        if self.purging {
            self.purging = engine.purge(PURGE_STEP);
            if self.purging {
                cx.wake_at(now);
            }
        }
        if now < self.last.after(STEP) {
            return;
        }
        let elapsed_ms = now.raw().saturating_sub(self.last.raw()) / NS_PER_MS;
        let budget = self.pace.budget(engine.len(), elapsed_ms);
        engine.maintain(clock.time(now), budget);
        stats.record_memory(engine.memory_use());
        self.last = now;
    }
}
