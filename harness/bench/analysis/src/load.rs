/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hdrhistogram::Histogram;
use hdrhistogram::serialization::Deserializer;
use hdrhistogram::serialization::interval_log::{IntervalLogIterator, LogEntry};
use serde::Deserialize;

pub const SUMMARY: &str = "summary.json";
pub const HLOG: &str = "hdr.hlog";

#[derive(Debug)]
pub struct LoadError(pub String);

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for LoadError {}

#[derive(Deserialize)]
struct RawSummary {
    #[serde(default)]
    label: String,
    #[serde(default)]
    host: RawHost,
    points: Vec<RawPoint>,
}

#[derive(Deserialize, Default)]
struct RawHost {
    #[serde(default)]
    hostname: String,
}

#[derive(Deserialize)]
struct RawPoint {
    point: usize,
    offered_rate: f64,
    completed_rate: f64,
    validity: RawValidity,
    generator: RawGenerator,
}

#[derive(Deserialize)]
struct RawValidity {
    valid: bool,
}

#[derive(Deserialize)]
struct RawGenerator {
    util_max: f64,
}

#[derive(Clone, Debug)]
pub struct Point {
    pub offered_rate: f64,
    pub completed_rate: f64,
    pub valid: bool,
    pub util_max: f64,
    pub hosts: usize,
    pub hists: BTreeMap<String, Histogram<u64>>,
}

#[derive(Clone, Debug)]
pub struct Run {
    pub name: String,
    pub labels: Vec<String>,
    pub hosts: Vec<String>,
    pub points: BTreeMap<usize, Point>,
}

pub fn load_run(dir: &Path) -> Result<Run, LoadError> {
    let host_dirs = host_dirs(dir)?;
    let mut run = Run {
        name: dir.display().to_string(),
        labels: Vec::new(),
        hosts: Vec::new(),
        points: BTreeMap::new(),
    };
    for host_dir in &host_dirs {
        add_host(&mut run, host_dir)?;
    }
    for point in run.points.values_mut() {
        for metric in ["latency", "service"] {
            let mut all: Option<Histogram<u64>> = None;
            for (name, hist) in &point.hists {
                if name.split_once('.').is_some_and(|(_, m)| m == metric) {
                    match &mut all {
                        Some(all) => all
                            .add(hist)
                            .map_err(|e| LoadError(format!("{}: {e:?}", run.name)))?,
                        None => all = Some(hist.clone()),
                    }
                }
            }
            if let Some(all) = all {
                point.hists.insert(format!("all.{metric}"), all);
            }
        }
    }
    Ok(run)
}

fn host_dirs(dir: &Path) -> Result<Vec<PathBuf>, LoadError> {
    if dir.join(SUMMARY).is_file() {
        return Ok(vec![dir.to_path_buf()]);
    }
    let entries =
        fs::read_dir(dir).map_err(|e| LoadError(format!("cannot read {}: {e}", dir.display())))?;
    let mut dirs: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.join(SUMMARY).is_file())
        .collect();
    dirs.sort_unstable();
    if dirs.is_empty() {
        return Err(LoadError(format!(
            "{} has no {SUMMARY}, neither directly nor in a host subdirectory",
            dir.display()
        )));
    }
    Ok(dirs)
}

fn add_host(run: &mut Run, dir: &Path) -> Result<(), LoadError> {
    let summary_path = dir.join(SUMMARY);
    let text = fs::read_to_string(&summary_path)
        .map_err(|e| LoadError(format!("cannot read {}: {e}", summary_path.display())))?;
    let summary: RawSummary = serde_json::from_str(&text)
        .map_err(|e| LoadError(format!("invalid {}: {e}", summary_path.display())))?;
    run.labels.push(summary.label);
    run.hosts.push(summary.host.hostname);
    for raw in summary.points {
        let point = run.points.entry(raw.point).or_insert_with(|| Point {
            offered_rate: 0.0,
            completed_rate: 0.0,
            valid: true,
            util_max: 0.0,
            hosts: 0,
            hists: BTreeMap::new(),
        });
        point.offered_rate += raw.offered_rate;
        point.completed_rate += raw.completed_rate;
        point.valid &= raw.validity.valid;
        point.util_max = point.util_max.max(raw.generator.util_max);
        point.hosts += 1;
    }
    let hlog_path = dir.join(HLOG);
    let bytes = fs::read(&hlog_path)
        .map_err(|e| LoadError(format!("cannot read {}: {e}", hlog_path.display())))?;
    let mut deserializer = Deserializer::new();
    for entry in IntervalLogIterator::new(&bytes) {
        let entry = entry.map_err(|e| LoadError(format!("{}: {e:?}", hlog_path.display())))?;
        let LogEntry::Interval(interval) = entry else {
            continue;
        };
        let Some(tag) = interval.tag() else { continue };
        let Some((point, metric)) = parse_tag(tag.as_str()) else {
            continue;
        };
        let decoded = decode_base64(interval.encoded_histogram())
            .ok_or_else(|| LoadError(format!("{}: bad base64 histogram", hlog_path.display())))?;
        let hist: Histogram<u64> = deserializer
            .deserialize(&mut decoded.as_slice())
            .map_err(|e| LoadError(format!("{}: {e:?}", hlog_path.display())))?;
        let Some(target) = run.points.get_mut(&point) else {
            continue;
        };
        match target.hists.get_mut(metric) {
            Some(merged) => merged
                .add(&hist)
                .map_err(|e| LoadError(format!("{}: {e:?}", hlog_path.display())))?,
            None => {
                target.hists.insert(metric.to_string(), hist);
            }
        }
    }
    Ok(())
}

fn parse_tag(tag: &str) -> Option<(usize, &str)> {
    let (point, metric) = tag.strip_prefix('p')?.split_once('.')?;
    Some((point.parse().ok()?, metric))
}

fn decode_base64(text: &str) -> Option<Vec<u8>> {
    STANDARD.decode(text.trim()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tags carry the point index and the metric name.
    #[test]
    fn tags() {
        assert_eq!(parse_tag("p3.incr.latency"), Some((3, "incr.latency")));
        assert_eq!(parse_tag("p0.lag"), Some((0, "lag")));
        assert_eq!(parse_tag("x"), None);
    }

    // Interval log payloads are padded standard base64.
    #[test]
    fn base64() {
        assert_eq!(decode_base64("aGVsbG8=\n"), Some(b"hello".to_vec()));
        assert_eq!(decode_base64("a$"), None);
    }
}
