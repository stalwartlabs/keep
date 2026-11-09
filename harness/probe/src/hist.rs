/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::error::{Context, Error, Result};
use hdrhistogram::{
    Histogram,
    serialization::{
        V2DeflateSerializer,
        interval_log::{IntervalLogWriterBuilder, Tag},
    },
};
use serde::Serialize;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    time::{Duration, SystemTime},
};

const MAX_TRACKABLE_NS: u64 = 60_000_000_000;
const SIGNIFICANT_DIGITS: u8 = 3;

pub struct LatencyHist(Histogram<u64>);

#[derive(Serialize)]
pub struct Summary {
    pub count: u64,
    pub min: u64,
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
    pub p9999: u64,
    pub max: u64,
    pub mean: f64,
    pub stdev: f64,
}

impl LatencyHist {
    pub fn new() -> Result<Self> {
        Histogram::new_with_bounds(1, MAX_TRACKABLE_NS, SIGNIFICANT_DIGITS)
            .map(Self)
            .map_err(|_| Error::Internal("histogram bounds rejected"))
    }

    #[inline]
    pub fn record(&mut self, ns: u64) {
        self.0.saturating_record(ns);
    }

    pub fn merge(&mut self, other: &Self) {
        for value in other.0.iter_recorded() {
            self.0
                .saturating_record_n(value.value_iterated_to(), value.count_at_value());
        }
    }

    pub fn count(&self) -> u64 {
        self.0.len()
    }

    pub fn summary(&self) -> Summary {
        let h = &self.0;
        Summary {
            count: h.len(),
            min: h.min(),
            p50: h.value_at_quantile(0.5),
            p90: h.value_at_quantile(0.9),
            p99: h.value_at_quantile(0.99),
            p999: h.value_at_quantile(0.999),
            p9999: h.value_at_quantile(0.9999),
            max: h.max(),
            mean: h.mean(),
            stdev: h.stdev(),
        }
    }

    pub fn write_hlog(
        &self,
        path: &Path,
        start: SystemTime,
        length: Duration,
        tag: &str,
    ) -> Result<()> {
        let file = File::create(path).context("create hlog file")?;
        let mut out = BufWriter::new(file);
        let mut serializer = V2DeflateSerializer::new();
        let mut log = IntervalLogWriterBuilder::new()
            .with_start_time(start)
            .begin_log_with(&mut out, &mut serializer)
            .context("write hlog header")?;
        let since_epoch = start
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        log.write_histogram(&self.0, since_epoch, length, Tag::new(tag))
            .map_err(|_| Error::Protocol("hlog histogram serialization failed"))?;
        drop(log);
        out.flush().context("flush hlog file")
    }
}
