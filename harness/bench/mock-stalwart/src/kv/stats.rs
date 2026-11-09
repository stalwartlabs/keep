/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::op::KvOp;
use crate::{
    hist::{AtomicHistogram, Percentiles, empty_histogram, merge_into},
    stats::{Tagged, counters},
};
use hdrhistogram::CreationError;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct OpStats {
    pub total: AtomicHistogram,
    pub queue: AtomicHistogram,
    pub service: AtomicHistogram,
    pub errors: AtomicU64,
    pub checked: AtomicU64,
    pub mismatches: AtomicU64,
    pub unchecked: AtomicU64,
}

counters!(
    KvCounters,
    KvCounterSnapshot {
        dispatched,
        dispatched_in_window,
        follow_ups,
        follow_ups_lost,
        locks_refused,
        leases_lost,
        preload_keys,
        preload_errors,
        errors_logged,
    }
);

pub struct KvStats {
    pub ops: [OpStats; KvOp::COUNT],
    pub checkout: AtomicHistogram,
    pub lag: AtomicHistogram,
    pub completed: AtomicU64,
    pub counters: KvCounters,
}

impl Default for KvStats {
    fn default() -> Self {
        KvStats {
            ops: std::array::from_fn(|_| OpStats::default()),
            checkout: AtomicHistogram::default(),
            lag: AtomicHistogram::default(),
            completed: AtomicU64::new(0),
            counters: KvCounters::default(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct OpReport {
    pub op: &'static str,
    pub total: Percentiles,
    pub queue: Percentiles,
    pub service: Percentiles,
    pub errors: u64,
    pub checked: u64,
    pub mismatches: u64,
    pub unchecked: u64,
}

#[derive(Debug, Serialize)]
pub struct KvReport {
    pub ops: Vec<OpReport>,
    pub all: Percentiles,
    pub lag: Percentiles,
    pub checkout: Percentiles,
    pub completed_ops: u64,
    pub errors: u64,
    pub mismatches: u64,
    pub checked: u64,
    pub counters: KvCounterSnapshot,
    pub client_counters: Option<BTreeMap<&'static str, u64>>,
    pub model_entries: Option<usize>,
}

impl KvStats {
    pub fn report(&self, model_entries: Option<usize>) -> Result<KvReport, CreationError> {
        let mut all = empty_histogram()?;
        let mut ops = Vec::with_capacity(KvOp::COUNT);
        for (op, stats) in KvOp::ALL.iter().zip(self.ops.iter()) {
            let total = stats.total.to_histogram()?;
            if total.is_empty() && stats.errors.load(Ordering::Relaxed) == 0 {
                continue;
            }
            merge_into(&mut all, &total);
            ops.push(OpReport {
                op: op.name(),
                total: Percentiles::of(&total),
                queue: Percentiles::of(&stats.queue.to_histogram()?),
                service: Percentiles::of(&stats.service.to_histogram()?),
                errors: stats.errors.load(Ordering::Relaxed),
                checked: stats.checked.load(Ordering::Relaxed),
                mismatches: stats.mismatches.load(Ordering::Relaxed),
                unchecked: stats.unchecked.load(Ordering::Relaxed),
            });
        }
        Ok(KvReport {
            completed_ops: all.len(),
            errors: ops.iter().map(|op| op.errors).sum(),
            mismatches: ops.iter().map(|op| op.mismatches).sum(),
            checked: ops.iter().map(|op| op.checked).sum(),
            all: Percentiles::of(&all),
            lag: Percentiles::of(&self.lag.to_histogram()?),
            checkout: Percentiles::of(&self.checkout.to_histogram()?),
            ops,
            counters: self.counters.snapshot(),
            client_counters: None,
            model_entries,
        })
    }

    pub fn histograms(&self, out: &mut Vec<Tagged>) -> Result<(), CreationError> {
        for (op, stats) in KvOp::ALL.iter().zip(self.ops.iter()) {
            if stats.total.count() == 0 {
                continue;
            }
            out.push(Tagged::of(format!("kv.{}.total", op.name()), &stats.total)?);
            out.push(Tagged::of(format!("kv.{}.queue", op.name()), &stats.queue)?);
            out.push(Tagged::of(
                format!("kv.{}.service", op.name()),
                &stats.service,
            )?);
        }
        out.push(Tagged::of("kv.lag", &self.lag)?);
        out.push(Tagged::of("kv.checkout", &self.checkout)?);
        Ok(())
    }
}
