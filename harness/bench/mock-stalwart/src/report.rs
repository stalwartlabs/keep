/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    cpu::ProcessUsage,
    dispatch::Marks,
    hist::{Percentiles, empty_histogram, merge_into},
    kv::stats::KvReport,
    stats::Tagged,
    stream::StreamReport,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use hdrhistogram::{
    Histogram,
    serialization::{
        Deserializer, V2DeflateSerializer,
        interval_log::{IntervalLogIterator, IntervalLogWriterBuilder, LogEntry, Tag},
    },
};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, btree_map::Entry},
    fs,
    io::{self, BufWriter, Write},
    path::Path,
    time::{Duration, UNIX_EPOCH},
};

#[derive(Debug, Default, Serialize)]
pub struct CpuReport {
    pub window_ns: u64,
    pub process: ProcessUsage,
    pub dispatcher_ns: u64,
    pub synthetic_load_ns: u64,
    pub client_ns: u64,
    pub client_cores: f64,
    pub achieved_load: f64,
    pub client_ns_per_kv_op: Option<f64>,
    pub client_ns_per_event_published: Option<f64>,
    pub client_ns_per_event_received: Option<f64>,
}

#[derive(Debug, Default, Serialize)]
pub struct Validity {
    pub kv_errors: u64,
    pub kv_mismatches: u64,
    pub kv_unfinished: u64,
    pub kv_lag_p999_ns: u64,
    pub event_lag_p999_ns: u64,
    pub stream_gaps: u64,
    pub stream_verify_failures: u64,
    pub stream_publish_errors: u64,
    pub notes: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub node: u16,
    pub incarnation: u16,
    pub window_start_unix_ns: u64,
    pub window_ns: u64,
    pub settings: Value,
    pub cpu: CpuReport,
    pub kv: Option<KvReport>,
    pub stream: Option<StreamReport>,
    pub validity: Validity,
    #[serde(skip)]
    pub histograms: Vec<Tagged>,
}

pub struct CpuInputs {
    pub marks: Marks,
    pub workers: usize,
    pub kv_ops: Option<u64>,
    pub events_published: Option<u64>,
    pub events_received: Option<u64>,
}

impl CpuReport {
    pub fn from_marks(inputs: CpuInputs) -> CpuReport {
        let (Some(start), Some(end)) = (inputs.marks.start, inputs.marks.end) else {
            return CpuReport::default();
        };
        let process = end.usage.since(&start.usage);
        let window_ns = end.at.saturating_sub(start.at).max(1);
        let dispatcher_ns = end.thread_ns.saturating_sub(start.thread_ns);
        let synthetic_load_ns = end.burn_ns.saturating_sub(start.burn_ns);
        let client_ns = process
            .cpu_ns
            .saturating_sub(dispatcher_ns)
            .saturating_sub(synthetic_load_ns);
        let per = |count: Option<u64>| {
            count
                .filter(|count| *count > 0)
                .map(|count| client_ns as f64 / count as f64)
        };
        CpuReport {
            window_ns,
            process,
            dispatcher_ns,
            synthetic_load_ns,
            client_ns,
            client_cores: client_ns as f64 / window_ns as f64,
            achieved_load: synthetic_load_ns as f64 / (window_ns as f64 * inputs.workers as f64),
            client_ns_per_kv_op: per(inputs.kv_ops),
            client_ns_per_event_published: per(inputs.events_published),
            client_ns_per_event_received: per(inputs.events_received),
        }
    }
}

impl Report {
    pub fn write(&self, dir: &Path) -> io::Result<()> {
        fs::create_dir_all(dir)?;
        let json = serde_json::to_vec_pretty(self).map_err(io::Error::other)?;
        fs::write(dir.join(format!("node-{}.json", self.node)), json)?;
        let file = fs::File::create(dir.join(format!("node-{}.hlog", self.node)))?;
        let mut file = BufWriter::new(file);
        let mut serializer = V2DeflateSerializer::new();
        let start = UNIX_EPOCH + Duration::from_nanos(self.window_start_unix_ns);
        let mut writer = IntervalLogWriterBuilder::new()
            .add_comment(&format!("mock-stalwart node {}", self.node))
            .with_start_time(start)
            .with_base_time(start)
            .begin_log_with(&mut file, &mut serializer)
            .map_err(io::Error::other)?;
        let length = Duration::from_nanos(self.window_ns);
        for tagged in &self.histograms {
            writer
                .write_histogram(
                    &tagged.histogram,
                    Duration::ZERO,
                    length,
                    Tag::new(&tagged.tag),
                )
                .map_err(|err| io::Error::other(format!("{err:?}")))?;
        }
        drop(writer);
        file.flush()
    }

    pub fn print(&self) {
        println!(
            "node {} window {:.1}s",
            self.node,
            self.window_ns as f64 / 1e9
        );
        if let Some(kv) = &self.kv {
            println!(
                "  kv: {} ops, {} errors, {} checked, {} mismatches, lag p99.9 {}",
                kv.completed_ops,
                kv.errors,
                kv.checked,
                kv.mismatches,
                us(kv.lag.p999)
            );
            println!(
                "  {:<14}{:>10}{:>10}{:>10}{:>10}{:>10}{:>10}",
                "op", "count", "p50", "p99", "p99.9", "max", "service50"
            );
            for op in &kv.ops {
                println!(
                    "  {:<14}{:>10}{:>10}{:>10}{:>10}{:>10}{:>10}",
                    op.op,
                    op.total.count,
                    us(op.total.p50),
                    us(op.total.p99),
                    us(op.total.p999),
                    us(op.total.max),
                    us(op.service.p50),
                );
            }
        }
        if let Some(stream) = &self.stream {
            let counters = &stream.counters;
            println!(
                "  stream: published {} events in {} messages, received {} events in {} messages, gaps {}, verify failures {}",
                counters.events_published,
                counters.messages_published,
                counters.events_received,
                counters.messages_received,
                counters.gaps,
                counters.verify_failures
            );
            println!(
                "  stream: publish p50 {} p99 {}, one-way p50 {} p99 {}, echo p50 {} p99 {} p99.9 {}, last-delivery echo p99 {}",
                us(stream.publish.p50),
                us(stream.publish.p99),
                us(stream.oneway.p50),
                us(stream.oneway.p99),
                us(stream.echo.p50),
                us(stream.echo.p99),
                us(stream.echo.p999),
                us(stream.echo_last.p99),
            );
        }
        println!(
            "  cpu: client {:.3} cores, {} per kv op, {} per event received, synthetic load {:.2}",
            self.cpu.client_cores,
            optional_ns(self.cpu.client_ns_per_kv_op),
            optional_ns(self.cpu.client_ns_per_event_received),
            self.cpu.achieved_load
        );
        for note in &self.validity.notes {
            println!("  note: {note}");
        }
    }
}

fn us(ns: u64) -> String {
    format!("{:.1}us", ns as f64 / 1e3)
}

fn optional_ns(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_string(), |ns| format!("{ns:.0}ns"))
}

#[derive(Debug)]
pub enum MergeError {
    Io(io::Error),
    Log(String),
}

impl std::fmt::Display for MergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MergeError::Io(err) => write!(f, "{err}"),
            MergeError::Log(reason) => f.write_str(reason),
        }
    }
}

pub fn merge(
    files: &[impl AsRef<Path>],
) -> Result<(BTreeMap<String, Percentiles>, Value), MergeError> {
    let mut merged: BTreeMap<String, Histogram<u64>> = BTreeMap::new();
    let mut counters = Value::Object(Default::default());
    let mut deserializer = Deserializer::new();
    for file in files {
        let path = file.as_ref();
        match path
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or_default()
        {
            "hlog" => {
                let bytes = fs::read(path).map_err(MergeError::Io)?;
                for entry in IntervalLogIterator::new(&bytes) {
                    let entry = entry
                        .map_err(|err| MergeError::Log(format!("{}: {err:?}", path.display())))?;
                    let LogEntry::Interval(interval) = entry else {
                        continue;
                    };
                    let encoded = STANDARD
                        .decode(interval.encoded_histogram())
                        .map_err(|err| MergeError::Log(err.to_string()))?;
                    let histogram: Histogram<u64> = deserializer
                        .deserialize(&mut encoded.as_slice())
                        .map_err(|err| MergeError::Log(format!("{err:?}")))?;
                    let tag = interval
                        .tag()
                        .map(|tag| tag.as_str().to_string())
                        .unwrap_or_default();
                    let target = match merged.entry(tag) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => entry.insert(
                            empty_histogram().map_err(|err| MergeError::Log(format!("{err:?}")))?,
                        ),
                    };
                    merge_into(target, &histogram);
                }
            }
            "json" => {
                let bytes = fs::read(path).map_err(MergeError::Io)?;
                let value: Value = serde_json::from_slice(&bytes)
                    .map_err(|err| MergeError::Log(err.to_string()))?;
                for (section, field, target) in [
                    ("kv", "counters", "kv"),
                    ("kv", "client_counters", "kv.client"),
                    ("stream", "counters", "stream"),
                    ("stream", "client_counters", "stream.client"),
                ] {
                    if let Some(source) = value.get(section).and_then(|value| value.get(field)) {
                        sum_into(&mut counters, target, source);
                    }
                }
            }
            _ => {}
        }
    }
    Ok((
        merged
            .iter()
            .map(|(tag, histogram)| (tag.clone(), Percentiles::of(histogram)))
            .collect(),
        counters,
    ))
}

fn sum_into(target: &mut Value, section: &str, source: &Value) {
    let Value::Object(source) = source else {
        return;
    };
    let Value::Object(root) = target else {
        return;
    };
    let entry = root
        .entry(section)
        .or_insert_with(|| Value::Object(Default::default()));
    let Value::Object(entry) = entry else {
        return;
    };
    for (key, value) in source {
        let add = value.as_u64().unwrap_or_default();
        let current = entry.get(key).and_then(Value::as_u64).unwrap_or_default();
        entry.insert(key.clone(), Value::from(current + add));
    }
}
