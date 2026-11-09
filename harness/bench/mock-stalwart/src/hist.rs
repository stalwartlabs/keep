/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::clock::NANOS_PER_SEC;
use hdrhistogram::{CreationError, Histogram};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

pub const SIGNIFICANT_DIGITS: u8 = 3;
pub const MAX_TRACKABLE: u64 = 60 * NANOS_PER_SEC;

const SUB_BUCKET_HALF_COUNT_MAGNITUDE: u32 = 10;
const SUB_BUCKET_HALF_COUNT: usize = 1 << SUB_BUCKET_HALF_COUNT_MAGNITUDE;
const SUB_BUCKET_MASK: u64 = (1 << (SUB_BUCKET_HALF_COUNT_MAGNITUDE + 1)) - 1;
const LEADING_ZERO_COUNT_BASE: u32 = 64 - (SUB_BUCKET_HALF_COUNT_MAGNITUDE + 1);
const BUCKET_COUNT: usize = 26;
const COUNTS_LEN: usize = (BUCKET_COUNT + 1) * SUB_BUCKET_HALF_COUNT;

const _: () = assert!(index_of(MAX_TRACKABLE) < COUNTS_LEN);
const _: () = assert!(index_of(MAX_TRACKABLE << 1) < COUNTS_LEN);

pub struct AtomicHistogram {
    counts: Box<[AtomicU64]>,
}

impl Default for AtomicHistogram {
    fn default() -> Self {
        AtomicHistogram {
            counts: (0..COUNTS_LEN).map(|_| AtomicU64::new(0)).collect(),
        }
    }
}

impl AtomicHistogram {
    #[inline]
    pub fn record(&self, value: u64) {
        if let Some(count) = self.counts.get(index_of(value)) {
            count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn to_histogram(&self) -> Result<Histogram<u64>, CreationError> {
        let mut histogram = empty_histogram()?;
        for (index, count) in self.counts.iter().enumerate() {
            let count = count.load(Ordering::Relaxed);
            if count > 0 {
                histogram.saturating_record_n(value_at(index), count);
            }
        }
        Ok(histogram)
    }

    pub fn count(&self) -> u64 {
        self.counts
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .sum()
    }
}

pub fn empty_histogram() -> Result<Histogram<u64>, CreationError> {
    Histogram::new_with_bounds(1, MAX_TRACKABLE, SIGNIFICANT_DIGITS)
}

pub fn merge_into(target: &mut Histogram<u64>, source: &Histogram<u64>) {
    for value in source.iter_recorded() {
        target.saturating_record_n(value.value_iterated_to(), value.count_at_value());
    }
}

const fn index_of(value: u64) -> usize {
    let value = if value > MAX_TRACKABLE {
        MAX_TRACKABLE
    } else {
        value
    };
    let bucket = LEADING_ZERO_COUNT_BASE - (value | SUB_BUCKET_MASK).leading_zeros();
    let sub_bucket = (value >> bucket) as usize;
    ((bucket as usize + 1) << SUB_BUCKET_HALF_COUNT_MAGNITUDE) + sub_bucket - SUB_BUCKET_HALF_COUNT
}

fn value_at(index: usize) -> u64 {
    let bucket = index >> SUB_BUCKET_HALF_COUNT_MAGNITUDE;
    let sub_bucket = (index & (SUB_BUCKET_HALF_COUNT - 1)) as u64;
    match bucket {
        0 => sub_bucket,
        _ => (sub_bucket + SUB_BUCKET_HALF_COUNT as u64) << (bucket - 1),
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct Percentiles {
    pub count: u64,
    pub mean: f64,
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
    pub p9999: u64,
    pub max: u64,
}

impl Percentiles {
    pub fn of(histogram: &Histogram<u64>) -> Self {
        if histogram.is_empty() {
            return Percentiles::default();
        }
        Percentiles {
            count: histogram.len(),
            mean: histogram.mean(),
            p50: histogram.value_at_quantile(0.5),
            p90: histogram.value_at_quantile(0.9),
            p99: histogram.value_at_quantile(0.99),
            p999: histogram.value_at_quantile(0.999),
            p9999: histogram.value_at_quantile(0.9999),
            max: histogram.max(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AtomicHistogram, MAX_TRACKABLE, empty_histogram, index_of, value_at};
    use rand::{RngExt, SeedableRng, rngs::SmallRng};

    // Every bucket maps back to a value inside the same bucket, as in HdrHistogram.
    #[test]
    fn index_and_value_round_trip() {
        let mut last = 0;
        for index in 0..=index_of(MAX_TRACKABLE) {
            let value = value_at(index);
            assert_eq!(index_of(value), index, "index {index} value {value}");
            assert!(index == 0 || value > last);
            last = value;
        }
    }

    // Recording through the atomic histogram matches recording into HdrHistogram directly.
    #[test]
    fn matches_hdrhistogram() {
        let mut rng = SmallRng::seed_from_u64(7);
        let atomic = AtomicHistogram::default();
        let mut reference = empty_histogram().expect("bounds");
        for _ in 0..200_000 {
            let magnitude = rng.random_range(0..36u32);
            let value = rng.random_range(0..=(1u64 << magnitude)).min(MAX_TRACKABLE);
            atomic.record(value);
            reference.record(value).expect("in range");
        }
        let exported = atomic.to_histogram().expect("bounds");
        assert_eq!(exported.len(), reference.len());
        for quantile in [0.0, 0.1, 0.5, 0.9, 0.99, 0.999, 0.9999, 1.0] {
            assert_eq!(
                exported.value_at_quantile(quantile),
                reference.value_at_quantile(quantile)
            );
        }
    }
}
