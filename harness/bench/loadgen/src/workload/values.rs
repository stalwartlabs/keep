/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use rand::RngExt;
use rand::rngs::SmallRng;

use super::keys::ALPHABET;

#[derive(Clone, Debug)]
pub enum ValueSize {
    Fixed(u32),
    Choice(Box<[(u64, u32)]>),
    LogUniform { ln_min: f64, ln_max: f64, max: u32 },
}

impl ValueSize {
    pub fn choice(weighted: &[(u32, f64)]) -> Self {
        let total: f64 = weighted.iter().map(|(_, w)| w).sum();
        let mut acc = 0.0;
        let mut table: Vec<(u64, u32)> = weighted
            .iter()
            .map(|&(size, weight)| {
                acc += weight;
                (threshold(acc / total), size)
            })
            .collect();
        if let Some(last) = table.last_mut() {
            last.0 = u64::MAX;
        }
        ValueSize::Choice(table.into_boxed_slice())
    }

    pub fn log_uniform(min: u32, max: u32) -> Self {
        ValueSize::LogUniform {
            ln_min: f64::from(min.max(1)).ln(),
            ln_max: f64::from(max.max(1)).ln(),
            max,
        }
    }

    pub fn only(&self, allowed: &[u32]) -> bool {
        match self {
            ValueSize::Fixed(size) => allowed.contains(size),
            ValueSize::Choice(table) => table.iter().all(|(_, size)| allowed.contains(size)),
            ValueSize::LogUniform { .. } => false,
        }
    }

    pub fn max(&self) -> u32 {
        match self {
            ValueSize::Fixed(size) => *size,
            ValueSize::Choice(table) => table.iter().map(|&(_, size)| size).max().unwrap_or(0),
            ValueSize::LogUniform { max, .. } => *max,
        }
    }

    #[inline]
    pub fn sample(&self, rng: &mut SmallRng) -> usize {
        match self {
            ValueSize::Fixed(size) => *size as usize,
            ValueSize::Choice(table) => {
                let r: u64 = rng.random();
                table
                    .iter()
                    .find(|(bound, _)| r < *bound)
                    .map_or(0, |&(_, size)| size as usize)
            }
            ValueSize::LogUniform {
                ln_min,
                ln_max,
                max,
            } => {
                let u: f64 = rng.random();
                ((ln_min + u * (ln_max - ln_min)).exp() as usize).min(*max as usize)
            }
        }
    }
}

pub fn threshold(fraction: f64) -> u64 {
    if fraction >= 1.0 {
        u64::MAX
    } else {
        (fraction * u64::MAX as f64) as u64
    }
}

pub fn value_pool(len: usize, seed: u64) -> Box<[u8]> {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ALPHABET[(state & 63) as usize]
        })
        .collect()
}
