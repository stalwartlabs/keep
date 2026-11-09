/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use hdrhistogram::Histogram;
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use rand_distr::{Beta, Distribution};

pub struct Arm {
    cum: Vec<Vec<u64>>,
    totals: Vec<u64>,
    rates: Vec<f64>,
}

pub struct Sample {
    grid: Vec<u64>,
    a: Arm,
    b: Arm,
}

#[derive(Clone, Copy, Debug)]
pub struct Interval {
    pub a: f64,
    pub b: f64,
    pub ratio: f64,
    pub low: f64,
    pub high: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pairing {
    Paired,
    Independent,
}

impl Sample {
    pub fn new(a: &[(&Histogram<u64>, f64)], b: &[(&Histogram<u64>, f64)]) -> Self {
        let mut grid: Vec<u64> = a
            .iter()
            .chain(b.iter())
            .flat_map(|(hist, _)| hist.iter_recorded().map(|v| v.value_iterated_to()))
            .collect();
        grid.sort_unstable();
        grid.dedup();
        let arm = |runs: &[(&Histogram<u64>, f64)]| Arm {
            cum: runs
                .iter()
                .map(|(hist, _)| cumulative(hist, &grid))
                .collect(),
            totals: runs.iter().map(|(hist, _)| hist.len()).collect(),
            rates: runs.iter().map(|&(_, rate)| rate).collect(),
        };
        let a = arm(a);
        let b = arm(b);
        Sample { grid, a, b }
    }

    pub fn quantile_ratio(
        &self,
        quantile: f64,
        floor_ns: f64,
        replicates: usize,
        pairing: Pairing,
        seed: u64,
    ) -> Option<Interval> {
        if self.grid.is_empty() || self.a.totals.is_empty() || self.b.totals.is_empty() {
            return None;
        }
        let excess = |v: u64| (v as f64 - floor_ns).max(1.0);
        let ones_a = vec![1u32; self.a.totals.len()];
        let ones_b = vec![1u32; self.b.totals.len()];
        let a = self.a.exact_quantile(&self.grid, &ones_a, quantile)?;
        let b = self.b.exact_quantile(&self.grid, &ones_b, quantile)?;
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut wa = vec![0u32; self.a.totals.len()];
        let mut wb = vec![0u32; self.b.totals.len()];
        let mut ratios = Vec::with_capacity(replicates);
        for _ in 0..replicates {
            resample(&mut rng, &mut wa, &mut wb, pairing);
            let (Some(qa), Some(qb)) = (
                self.a.drawn_quantile(&self.grid, &wa, quantile, &mut rng),
                self.b.drawn_quantile(&self.grid, &wb, quantile, &mut rng),
            ) else {
                continue;
            };
            ratios.push(excess(qb) / excess(qa));
        }
        let (low, high) = percentile_interval(&mut ratios)?;
        Some(Interval {
            a: a as f64,
            b: b as f64,
            ratio: excess(b) / excess(a),
            low,
            high,
        })
    }

    pub fn throughput_ratio(
        &self,
        replicates: usize,
        pairing: Pairing,
        seed: u64,
    ) -> Option<Interval> {
        let mean = |rates: &[f64], weights: &[u32]| -> f64 {
            let n: u32 = weights.iter().sum();
            rates
                .iter()
                .zip(weights)
                .map(|(r, &w)| r * f64::from(w))
                .sum::<f64>()
                / f64::from(n.max(1))
        };
        if self.a.rates.is_empty() || self.b.rates.is_empty() {
            return None;
        }
        let a = mean(&self.a.rates, &vec![1; self.a.rates.len()]);
        let b = mean(&self.b.rates, &vec![1; self.b.rates.len()]);
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut wa = vec![0u32; self.a.rates.len()];
        let mut wb = vec![0u32; self.b.rates.len()];
        let mut ratios: Vec<f64> = (0..replicates)
            .map(|_| {
                resample(&mut rng, &mut wa, &mut wb, pairing);
                mean(&self.b.rates, &wb) / mean(&self.a.rates, &wa).max(f64::MIN_POSITIVE)
            })
            .collect();
        let (low, high) = percentile_interval(&mut ratios)?;
        Some(Interval {
            a,
            b,
            ratio: b / a.max(f64::MIN_POSITIVE),
            low,
            high,
        })
    }
}

impl Arm {
    fn total(&self, weights: &[u32]) -> u64 {
        self.totals
            .iter()
            .zip(weights)
            .map(|(&t, &w)| t * u64::from(w))
            .sum()
    }

    fn exact_quantile(&self, grid: &[u64], weights: &[u32], quantile: f64) -> Option<u64> {
        let total = self.total(weights);
        if total == 0 {
            return None;
        }
        let rank = ((quantile * total as f64).ceil() as u64).clamp(1, total);
        Some(self.value_at_rank(grid, weights, rank as f64))
    }

    fn drawn_quantile(
        &self,
        grid: &[u64],
        weights: &[u32],
        quantile: f64,
        rng: &mut SmallRng,
    ) -> Option<u64> {
        let total = self.total(weights);
        if total == 0 {
            return None;
        }
        let rank = ((quantile * total as f64).ceil() as u64).clamp(1, total);
        let order = Beta::new(rank as f64, (total - rank + 1) as f64).ok()?;
        let u: f64 = order.sample(rng);
        Some(self.value_at_rank(grid, weights, u * total as f64))
    }

    fn value_at_rank(&self, grid: &[u64], weights: &[u32], rank: f64) -> u64 {
        let reached = |j: usize| -> bool {
            let count: u64 = self
                .cum
                .iter()
                .zip(weights)
                .map(|(cum, &w)| cum.get(j).copied().unwrap_or(0) * u64::from(w))
                .sum();
            count as f64 >= rank
        };
        let (mut lo, mut hi) = (0, grid.len().saturating_sub(1));
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if reached(mid) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        grid.get(lo).copied().unwrap_or(0)
    }
}

fn cumulative(hist: &Histogram<u64>, grid: &[u64]) -> Vec<u64> {
    let mut counts = vec![0u64; grid.len()];
    for value in hist.iter_recorded() {
        if let Ok(pos) = grid.binary_search(&value.value_iterated_to()) {
            counts[pos] += value.count_at_value();
        }
    }
    let mut acc = 0;
    counts.iter_mut().for_each(|c| {
        acc += *c;
        *c = acc;
    });
    counts
}

fn resample(rng: &mut SmallRng, wa: &mut [u32], wb: &mut [u32], pairing: Pairing) {
    wa.fill(0);
    wb.fill(0);
    match pairing {
        Pairing::Paired => {
            let n = wa.len().min(wb.len());
            for _ in 0..n {
                let i = rng.random_range(0..n);
                wa[i] += 1;
                wb[i] += 1;
            }
        }
        Pairing::Independent => {
            for _ in 0..wa.len() {
                wa[rng.random_range(0..wa.len())] += 1;
            }
            for _ in 0..wb.len() {
                wb[rng.random_range(0..wb.len())] += 1;
            }
        }
    }
}

fn percentile_interval(values: &mut [f64]) -> Option<(f64, f64)> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable_by(f64::total_cmp);
    let n = values.len();
    let low = values.get((n as f64 * 0.025).floor() as usize)?;
    let high = values.get(
        ((n as f64 * 0.975).ceil() as usize)
            .saturating_sub(1)
            .min(n - 1),
    )?;
    Some((*low, *high))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hist(values: impl Iterator<Item = u64>) -> Histogram<u64> {
        let mut hist = Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("bounds");
        values.for_each(|v| hist.saturating_record(v));
        hist
    }

    // The exact quantile of pooled runs matches HdrHistogram on the merged histogram.
    #[test]
    fn pooled_quantile_matches_hdr() {
        let runs: Vec<Histogram<u64>> = (0..3u64)
            .map(|r| hist((0..10_000u64).map(move |i| 1_000 + (i * 7919 + r * 13) % 50_000)))
            .collect();
        let mut merged = runs[0].clone();
        merged.add(&runs[1]).expect("add");
        merged.add(&runs[2]).expect("add");
        let refs: Vec<(&Histogram<u64>, f64)> = runs.iter().map(|h| (h, 1.0)).collect();
        let sample = Sample::new(&refs, &refs);
        for q in [0.5, 0.9, 0.99, 0.999] {
            let ours = sample
                .a
                .exact_quantile(&sample.grid, &[1, 1, 1], q)
                .expect("value");
            let hdr = merged.value_at_quantile(q);
            assert_eq!(
                merged.highest_equivalent(ours),
                merged.highest_equivalent(hdr),
                "q={q}"
            );
        }
    }

    // Identical arms give a ratio of one with a CI that covers one.
    #[test]
    fn identical_arms_cover_one() {
        let runs: Vec<Histogram<u64>> = (0..5u64)
            .map(|r| hist((0..20_000u64).map(move |i| 10_000 + (i * 104_729 + r * 31) % 90_000)))
            .collect();
        let refs: Vec<(&Histogram<u64>, f64)> = runs.iter().map(|h| (h, 100.0)).collect();
        let sample = Sample::new(&refs, &refs);
        let ci = sample
            .quantile_ratio(0.99, 0.0, 2000, Pairing::Paired, 1)
            .expect("interval");
        assert!((ci.ratio - 1.0).abs() < 1e-12);
        assert!(ci.low <= 1.0 && ci.high >= 1.0, "{ci:?}");
    }

    // A 20% shift in latency is detected with a CI that excludes one.
    #[test]
    fn detects_shift() {
        let base: Vec<Histogram<u64>> = (0..5u64)
            .map(|r| hist((0..20_000u64).map(move |i| 10_000 + (i * 104_729 + r * 31) % 90_000)))
            .collect();
        let shifted: Vec<Histogram<u64>> = (0..5u64)
            .map(|r| {
                hist(
                    (0..20_000u64).map(move |i| (10_000 + (i * 104_729 + r * 31) % 90_000) * 6 / 5),
                )
            })
            .collect();
        let a: Vec<(&Histogram<u64>, f64)> = base.iter().map(|h| (h, 100.0)).collect();
        let b: Vec<(&Histogram<u64>, f64)> = shifted.iter().map(|h| (h, 120.0)).collect();
        let sample = Sample::new(&a, &b);
        let ci = sample
            .quantile_ratio(0.5, 0.0, 2000, Pairing::Independent, 7)
            .expect("interval");
        assert!(ci.low > 1.15 && ci.high < 1.25, "{ci:?}");
        let tput = sample
            .throughput_ratio(2000, Pairing::Paired, 7)
            .expect("interval");
        assert!((tput.ratio - 1.2).abs() < 1e-9);
    }
}
