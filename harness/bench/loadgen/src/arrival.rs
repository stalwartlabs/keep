/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use rand_distr::Exp1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Arrival {
    Poisson,
    Constant,
}

pub struct Schedule {
    arrival: Arrival,
    rng: SmallRng,
    gap_ns: f64,
    next_ns: f64,
    phase_offset: f64,
}

impl Schedule {
    pub fn new(arrival: Arrival, seed: u64, phase_offset: f64) -> Self {
        Schedule {
            arrival,
            rng: SmallRng::seed_from_u64(seed),
            gap_ns: f64::INFINITY,
            next_ns: f64::INFINITY,
            phase_offset: phase_offset.clamp(0.0, 1.0),
        }
    }

    pub fn start(&mut self, at_ns: u64, rate_per_sec: f64) {
        self.gap_ns = if rate_per_sec > 0.0 {
            1e9 / rate_per_sec
        } else {
            f64::INFINITY
        };
        self.next_ns = at_ns as f64
            + match self.arrival {
                Arrival::Poisson => self.draw_gap(),
                Arrival::Constant => self.gap_ns * self.phase_offset,
            };
    }

    #[inline]
    pub fn due_ns(&self) -> u64 {
        if self.next_ns.is_finite() {
            self.next_ns as u64
        } else {
            u64::MAX
        }
    }

    #[inline]
    pub fn advance(&mut self) {
        self.next_ns += self.draw_gap();
    }

    #[inline]
    fn draw_gap(&mut self) -> f64 {
        match self.arrival {
            Arrival::Poisson => {
                let exp: f64 = self.rng.sample(Exp1);
                self.gap_ns * exp
            }
            Arrival::Constant => self.gap_ns,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Poisson arrivals have the requested mean rate and an exponential coefficient of variation.
    #[test]
    fn poisson_rate() {
        let mut schedule = Schedule::new(Arrival::Poisson, 9, 0.0);
        schedule.start(0, 100_000.0);
        let n = 200_000;
        let mut prev = 0u64;
        let mut gaps = Vec::with_capacity(n);
        for _ in 0..n {
            let due = schedule.due_ns();
            gaps.push((due - prev) as f64);
            prev = due;
            schedule.advance();
        }
        let mean = gaps.iter().sum::<f64>() / n as f64;
        let var = gaps.iter().map(|g| (g - mean).powi(2)).sum::<f64>() / n as f64;
        assert!((mean - 10_000.0).abs() < 100.0, "mean {mean}");
        assert!((var.sqrt() / mean - 1.0).abs() < 0.02);
    }

    // Constant arrivals are evenly spaced from the phase offset.
    #[test]
    fn constant_spacing() {
        let mut schedule = Schedule::new(Arrival::Constant, 0, 0.5);
        schedule.start(1_000, 1_000_000.0);
        assert_eq!(schedule.due_ns(), 1_500);
        schedule.advance();
        assert_eq!(schedule.due_ns(), 2_500);
    }
}
