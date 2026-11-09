/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{args::Arrival, rng::SplitMix64};
use std::{
    hint::spin_loop,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const SLEEP_MARGIN_NS: u64 = 100_000;
const NANOS_PER_SEC: f64 = 1e9;

#[derive(Clone, Copy)]
pub struct Clock {
    epoch: Instant,
}

impl Clock {
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }

    #[inline]
    pub fn now(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    pub fn wait_until(&self, deadline: u64) {
        loop {
            let left = deadline.saturating_sub(self.now());
            if left == 0 {
                return;
            }
            if left > SLEEP_MARGIN_NS {
                thread::sleep(Duration::from_nanos(left - SLEEP_MARGIN_NS));
            } else {
                spin_loop();
            }
        }
    }
}

pub fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

pub fn secs_to_ns(secs: f64) -> u64 {
    (secs * NANOS_PER_SEC) as u64
}

pub fn ns_to_secs(ns: u64) -> f64 {
    ns as f64 / NANOS_PER_SEC
}

pub struct Schedule {
    origin: u64,
    interval: f64,
    offset: f64,
    arrival: Arrival,
    rng: SplitMix64,
}

impl Schedule {
    pub fn new(origin: u64, rate: f64, arrival: Arrival, seed: u64) -> Self {
        Self {
            origin,
            interval: NANOS_PER_SEC / rate,
            offset: 0.0,
            arrival,
            rng: SplitMix64::new(seed),
        }
    }

    #[inline]
    pub fn next_deadline(&mut self) -> u64 {
        let deadline = self.origin + self.offset as u64;
        self.offset += match self.arrival {
            Arrival::Uniform => self.interval,
            Arrival::Poisson => self.interval * self.rng.exponential(),
        };
        deadline
    }
}
