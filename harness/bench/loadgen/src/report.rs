/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fmt::Write as _;
use std::io;
use std::time::Duration;

use hdrhistogram::Histogram;
use hdrhistogram::serialization::Serializer;
use hdrhistogram::serialization::interval_log::{IntervalLogWriter, Tag};
use serde::Serialize;

use crate::op::OpKind;
use crate::stats::{CpuWindow, IoCounters, Outlier, Percentiles, PhaseStats, add_into};

pub struct OpAgg {
    latency: Histogram<u64>,
    service: Histogram<u64>,
    ok: u64,
    miss: u64,
    error: u64,
}

pub struct PointAgg {
    pub workers: usize,
    template: Histogram<u64>,
    io: IoCounters,
    ops: Vec<Option<OpAgg>>,
    lag: Histogram<u64>,
    loop_gap: Histogram<u64>,
    issued: u64,
    completed: u64,
    failed: u64,
    timeouts: u64,
    cpu: Vec<CpuWindow>,
    pub outliers: Vec<(usize, Outlier)>,
    pub outliers_dropped: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Thresholds {
    pub max_util: f64,
    pub max_lag_p99_ns: u64,
    pub max_lag_p999_ns: u64,
}

pub struct PointContext {
    pub point: usize,
    pub offered_rate: f64,
    pub measure_ns: u64,
    pub threads: usize,
    pub thresholds: Thresholds,
    pub tcp_retrans: Option<u64>,
    pub process_cpu_ns: Option<u64>,
}

#[derive(Serialize, Clone, Copy, Debug, Default)]
pub struct ErrorCounts {
    pub replies: u64,
    pub timeouts: u64,
    pub failed: u64,
}

impl ErrorCounts {
    pub fn total(&self) -> u64 {
        self.replies + self.timeouts + self.failed
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct OpSummary {
    pub op: &'static str,
    pub count: u64,
    pub ok: u64,
    pub miss: u64,
    pub error: u64,
    pub latency: Percentiles,
    pub service: Percentiles,
}

#[derive(Serialize, Clone, Debug)]
pub struct GeneratorUse {
    pub threads: usize,
    pub util_max: f64,
    pub util_mean: f64,
    pub busy_ns_per_op: f64,
    pub thread_cpu_ns_per_op: f64,
    pub process_cpu_util: Option<f64>,
}

#[derive(Serialize, Clone, Debug)]
pub struct IoPerOp {
    pub syscalls: f64,
    pub writes: f64,
    pub reads: f64,
    pub ready_polls: f64,
    pub idle_polls: f64,
    pub bytes_out: f64,
    pub bytes_in: f64,
    pub recv_rearms: f64,
}

#[derive(Serialize, Clone, Debug)]
pub struct Validity {
    pub valid: bool,
    pub saturated: bool,
    pub reasons: Vec<String>,
}

#[derive(Serialize, Clone, Debug)]
pub struct PointSummary {
    pub point: usize,
    pub meas_start_unix_ns: u64,
    pub meas_end_unix_ns: u64,
    pub offered_rate: f64,
    pub measure_s: f64,
    pub issued: u64,
    pub completed: u64,
    pub issued_rate: f64,
    pub completed_rate: f64,
    pub errors: ErrorCounts,
    pub all: OpSummary,
    pub ops: Vec<OpSummary>,
    pub lag: Percentiles,
    pub loop_gap: Percentiles,
    pub generator: GeneratorUse,
    pub io_per_op: IoPerOp,
    pub tcp_retrans: Option<u64>,
    pub outliers: u64,
    pub outliers_dropped: u64,
    pub validity: Validity,
}

impl PointAgg {
    pub fn new(template: &Histogram<u64>) -> Self {
        PointAgg {
            workers: 0,
            template: template.clone(),
            io: IoCounters::default(),
            ops: (0..OpKind::COUNT).map(|_| None).collect(),
            lag: template.clone(),
            loop_gap: template.clone(),
            issued: 0,
            completed: 0,
            failed: 0,
            timeouts: 0,
            cpu: Vec::new(),
            outliers: Vec::new(),
            outliers_dropped: 0,
        }
    }

    pub fn add(&mut self, worker: usize, stats: &mut PhaseStats) {
        self.workers += 1;
        self.outliers
            .extend(stats.outliers.drain(..).map(|outlier| (worker, outlier)));
        self.outliers_dropped += stats.outliers_dropped;
        self.io.writes += stats.io.writes;
        self.io.write_bytes += stats.io.write_bytes;
        self.io.reads += stats.io.reads;
        self.io.read_bytes += stats.io.read_bytes;
        self.io.polls += stats.io.polls;
        self.io.ready_polls += stats.io.ready_polls;
        self.io.syscalls += stats.io.syscalls;
        self.io.rearms += stats.io.rearms;
        self.issued += stats.issued;
        self.completed += stats.completed;
        self.failed += stats.failed;
        self.timeouts += stats.timeouts;
        self.cpu.push(stats.cpu);
        add_into(&mut self.lag, &stats.lag);
        add_into(&mut self.loop_gap, &stats.loop_gap);
        for (agg, op) in self.ops.iter_mut().zip(stats.ops.iter()) {
            let Some(op) = op else { continue };
            let agg = agg.get_or_insert_with(|| OpAgg {
                latency: self.template.clone(),
                service: self.template.clone(),
                ok: 0,
                miss: 0,
                error: 0,
            });
            add_into(&mut agg.latency, &op.latency);
            add_into(&mut agg.service, &op.service);
            agg.ok += op.ok;
            agg.miss += op.miss;
            agg.error += op.error;
        }
    }

    fn used_ops(&self) -> impl Iterator<Item = (OpKind, &OpAgg)> {
        OpKind::ALL
            .iter()
            .zip(self.ops.iter())
            .filter_map(|(&kind, agg)| {
                agg.as_ref()
                    .filter(|agg| !agg.latency.is_empty())
                    .map(|agg| (kind, agg))
            })
    }

    pub fn merged(&self) -> (Histogram<u64>, Histogram<u64>) {
        let mut latency = self.template.clone();
        let mut service = self.template.clone();
        for (_, agg) in self.used_ops() {
            let _ = latency.add(&agg.latency);
            let _ = service.add(&agg.service);
        }
        (latency, service)
    }

    pub fn summarize(&self, ctx: &PointContext) -> PointSummary {
        let measure_s = ctx.measure_ns as f64 / 1e9;
        let ops: Vec<OpSummary> = self
            .used_ops()
            .map(|(kind, agg)| OpSummary {
                op: kind.name(),
                count: agg.latency.len(),
                ok: agg.ok,
                miss: agg.miss,
                error: agg.error,
                latency: Percentiles::of(&agg.latency),
                service: Percentiles::of(&agg.service),
            })
            .collect();
        let (latency, service) = self.merged();
        let all = OpSummary {
            op: "all",
            count: latency.len(),
            ok: ops.iter().map(|op| op.ok).sum(),
            miss: ops.iter().map(|op| op.miss).sum(),
            error: ops.iter().map(|op| op.error).sum(),
            latency: Percentiles::of(&latency),
            service: Percentiles::of(&service),
        };
        let errors = ErrorCounts {
            replies: all.error,
            timeouts: self.timeouts,
            failed: self.failed,
        };
        let utils: Vec<f64> = self
            .cpu
            .iter()
            .map(|cpu| cpu.busy_ns() as f64 / cpu.wall_ns.max(1) as f64)
            .collect();
        let util_max = utils.iter().copied().fold(0.0, f64::max);
        let util_mean = utils.iter().sum::<f64>() / utils.len().max(1) as f64;
        let ops_done = self.completed.max(1) as f64;
        let generator = GeneratorUse {
            threads: ctx.threads,
            util_max,
            util_mean,
            busy_ns_per_op: self.cpu.iter().map(CpuWindow::busy_ns).sum::<u64>() as f64 / ops_done,
            thread_cpu_ns_per_op: self.cpu.iter().map(|c| c.thread_cpu_ns).sum::<u64>() as f64
                / ops_done,
            process_cpu_util: ctx
                .process_cpu_ns
                .map(|ns| ns as f64 / (ctx.measure_ns.max(1) as f64 * ctx.threads.max(1) as f64)),
        };
        let per_op = |n: u64| n as f64 / ops_done;
        let io_per_op = IoPerOp {
            syscalls: per_op(self.io.syscalls),
            writes: per_op(self.io.writes),
            reads: per_op(self.io.reads),
            ready_polls: per_op(self.io.ready_polls),
            idle_polls: per_op(self.io.polls - self.io.ready_polls),
            bytes_out: per_op(self.io.write_bytes),
            bytes_in: per_op(self.io.read_bytes),
            recv_rearms: per_op(self.io.rearms),
        };
        let lag = Percentiles::of(&self.lag);
        let issued_rate = self.issued as f64 / measure_s;
        let completed_rate = self.completed as f64 / measure_s;
        let mut reasons = Vec::new();
        let error_total = errors.total();
        if error_total > 0 {
            reasons.push(format!(
                "{error_total} errors ({} error replies, {} timeouts, {} failed)",
                errors.replies, errors.timeouts, errors.failed
            ));
        }
        if util_max > ctx.thresholds.max_util {
            reasons.push(format!(
                "generator busy {:.0}% above {:.0}%",
                util_max * 100.0,
                ctx.thresholds.max_util * 100.0
            ));
        }
        for (quantile, label, limit) in [
            (0.99, "p99", ctx.thresholds.max_lag_p99_ns),
            (0.999, "p99.9", ctx.thresholds.max_lag_p999_ns),
        ] {
            let lag = self.lag.value_at_quantile(quantile);
            if lag > limit {
                reasons.push(format!(
                    "schedule lag {label} {:.1} us above {:.1} us (loop gap {label} {:.1} us, max {:.1} us)",
                    lag as f64 / 1e3,
                    limit as f64 / 1e3,
                    self.loop_gap.value_at_quantile(quantile) as f64 / 1e3,
                    self.loop_gap.max() as f64 / 1e3
                ));
            }
        }
        if self.workers == 0 || self.issued == 0 {
            reasons.push(String::from("no requests issued"));
        }
        PointSummary {
            point: ctx.point,
            meas_start_unix_ns: 0,
            meas_end_unix_ns: 0,
            offered_rate: ctx.offered_rate,
            measure_s,
            issued: self.issued,
            completed: self.completed,
            issued_rate,
            completed_rate,
            errors,
            all,
            ops,
            lag,
            loop_gap: Percentiles::of(&self.loop_gap),
            generator,
            io_per_op,
            tcp_retrans: ctx.tcp_retrans,
            outliers: self.outliers.len() as u64,
            outliers_dropped: self.outliers_dropped,
            validity: Validity {
                valid: reasons.is_empty(),
                saturated: completed_rate < issued_rate * 0.99,
                reasons,
            },
        }
    }

    pub fn write_hlog<W: io::Write, S: Serializer>(
        &self,
        writer: &mut IntervalLogWriter<'_, '_, W, S>,
        point: usize,
        start: Duration,
        duration: Duration,
    ) -> io::Result<()> {
        let mut tag = String::new();
        let mut write = |name: std::fmt::Arguments<'_>, hist: &Histogram<u64>| -> io::Result<()> {
            tag.clear();
            let _ = tag.write_fmt(name);
            writer
                .write_histogram(hist, start, duration, Tag::new(&tag))
                .map_err(|_| io::Error::other("cannot write histogram"))
        };
        write(format_args!("p{point}.lag"), &self.lag)?;
        write(format_args!("p{point}.loop_gap"), &self.loop_gap)?;
        for (kind, agg) in self.used_ops() {
            write(
                format_args!("p{point}.{}.latency", kind.name()),
                &agg.latency,
            )?;
            write(
                format_args!("p{point}.{}.service", kind.name()),
                &agg.service,
            )?;
        }
        Ok(())
    }
}

pub const CSV_HEADER: &str = "point,offered_rate,issued_rate,completed_rate,op,count,ok,miss,error,p50_us,p90_us,p99_us,p999_us,p9999_us,max_us,mean_us,svc_p50_us,svc_p99_us,svc_p999_us,lag_p99_us,lag_p999_us,util_max,busy_ns_per_op,valid\n";

pub fn csv_rows(summary: &PointSummary, out: &mut String) {
    for op in std::iter::once(&summary.all).chain(summary.ops.iter()) {
        let l = &op.latency;
        let s = &op.service;
        let _ = writeln!(
            out,
            "{},{:.1},{:.1},{:.1},{},{},{},{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.3},{:.1},{}",
            summary.point,
            summary.offered_rate,
            summary.issued_rate,
            summary.completed_rate,
            op.op,
            op.count,
            op.ok,
            op.miss,
            op.error,
            l.p50_us,
            l.p90_us,
            l.p99_us,
            l.p999_us,
            l.p9999_us,
            l.max_us,
            l.mean_us,
            s.p50_us,
            s.p99_us,
            s.p999_us,
            summary.lag.p99_us,
            summary.lag.p999_us,
            summary.generator.util_max,
            summary.generator.busy_ns_per_op,
            summary.validity.valid,
        );
    }
}

pub fn console_line(summary: &PointSummary) -> String {
    let l = &summary.all.latency;
    format!(
        "point {:>3}  offered {:>11.0}/s  done {:>11.0}/s  p50 {:>8.1}  p99 {:>8.1}  p99.9 {:>9.1}  max {:>9.1} us  lag p99.9 {:>6.1} us  gen {:>4.0}% {:>6.0} ns/op  {}",
        summary.point,
        summary.offered_rate,
        summary.completed_rate,
        l.p50_us,
        l.p99_us,
        l.p999_us,
        l.max_us,
        summary.lag.p999_us,
        summary.generator.util_max * 100.0,
        summary.generator.busy_ns_per_op,
        if summary.validity.valid {
            String::from("valid")
        } else {
            format!("INVALID: {}", summary.validity.reasons.join("; "))
        }
    )
}
