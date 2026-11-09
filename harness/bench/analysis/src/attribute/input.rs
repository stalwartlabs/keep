/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fs;
use std::path::Path;

use serde::Deserialize;

#[derive(Deserialize)]
pub struct RunInfo {
    pub clock: Clock,
    pub flows: Vec<Flow>,
    #[serde(default)]
    pub worker_tids: Vec<i64>,
    #[serde(default)]
    pub worker_cpus: Vec<u32>,
    pub points: Vec<Point>,
}

#[derive(Deserialize)]
pub struct Clock {
    pub mono_base_ns: u64,
}

#[derive(Deserialize)]
pub struct Flow {
    pub thread: usize,
    pub conn: usize,
    pub local: String,
    pub peer: String,
}

#[derive(Deserialize)]
pub struct Point {
    pub point: usize,
    pub all: PointOp,
}

#[derive(Deserialize)]
pub struct PointOp {
    pub latency: Percentiles,
}

#[derive(Deserialize)]
pub struct Percentiles {
    pub p50_us: f64,
    pub p999_us: f64,
}

#[derive(Deserialize)]
pub struct Outlier {
    pub point: usize,
    pub thread: usize,
    pub conn: usize,
    pub op: String,
    pub intended_ns: u64,
    pub lag_ns: u64,
    pub latency_ns: u64,
}

#[derive(Deserialize)]
pub struct RawEvent {
    pub ev: String,
    pub mono: u64,
    #[serde(default)]
    pub cpu: Option<u32>,
    #[serde(default)]
    pub sport: Option<u16>,
    #[serde(default)]
    pub dport: Option<u16>,
    #[serde(default)]
    pub tid: Option<i64>,
    #[serde(default)]
    pub dur_ns: Option<u64>,
    #[serde(default)]
    pub ticks: Option<u64>,
    #[serde(default)]
    pub real: Option<u64>,
    #[serde(default)]
    pub cport: Option<u16>,
    #[serde(default)]
    pub queue_ns: Option<u64>,
    #[serde(default)]
    pub hop_ns: Option<u64>,
    #[serde(default)]
    pub exec_ns: Option<u64>,
    #[serde(default)]
    pub send_ns: Option<u64>,
    #[serde(default)]
    pub cause: Option<String>,
}

pub fn read_run(dir: &Path) -> Result<(RunInfo, Vec<Outlier>), String> {
    let summary = dir.join("summary.json");
    let bytes = fs::read(&summary).map_err(|e| format!("{}: {e}", summary.display()))?;
    let info: RunInfo =
        serde_json::from_slice(&bytes).map_err(|e| format!("{}: {e}", summary.display()))?;
    let outliers = read_lines(&dir.join("outliers.jsonl"))?;
    Ok((info, outliers))
}

pub fn read_lines<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Vec<T>, String> {
    let text = fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    text.lines()
        .enumerate()
        .filter(|(_, line)| line.starts_with('{'))
        .map(|(n, line)| {
            serde_json::from_str(line).map_err(|e| format!("{}:{}: {e}", path.display(), n + 1))
        })
        .collect()
}

pub fn port_of(addr: &str) -> Option<u16> {
    addr.rsplit_once(':')?.1.parse().ok()
}
