/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use hdrhistogram::{Counter, CreationError, Histogram};

use crate::conn::InFlight;
use crate::op::{OpKind, Outcome};

pub const NO_PHASE: u8 = u8::MAX;
pub const MAX_POINTS: usize = NO_PHASE as usize;
pub const HIST_LOW_NS: u64 = 1;
pub const HIST_HIGH_NS: u64 = 60_000_000_000;
pub const HIST_SIGFIG: u8 = 3;
pub const MAX_OUTLIERS: usize = 1 << 20;

pub fn hist_template<T: Counter>() -> Result<Histogram<T>, CreationError> {
    Histogram::new_with_bounds(HIST_LOW_NS, HIST_HIGH_NS, HIST_SIGFIG)
}

pub struct OpStats {
    pub latency: Histogram<u32>,
    pub service: Histogram<u32>,
    pub ok: u64,
    pub miss: u64,
    pub error: u64,
}

impl OpStats {
    fn new(template: &Histogram<u32>) -> Self {
        OpStats {
            latency: template.clone(),
            service: template.clone(),
            ok: 0,
            miss: 0,
            error: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CpuWindow {
    pub wall_ns: u64,
    pub thread_cpu_ns: u64,
    pub idle_ns: u64,
}

impl CpuWindow {
    pub fn busy_ns(&self) -> u64 {
        self.wall_ns.saturating_sub(self.idle_ns)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct IoCounters {
    pub writes: u64,
    pub write_bytes: u64,
    pub reads: u64,
    pub read_bytes: u64,
    pub polls: u64,
    pub ready_polls: u64,
    pub syscalls: u64,
    pub rearms: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Outlier {
    pub conn: u32,
    pub kind: OpKind,
    pub intended_ns: u64,
    pub lag_ns: u32,
    pub latency_ns: u64,
}

pub struct PhaseStats {
    pub io: IoCounters,
    pub outliers: Vec<Outlier>,
    pub outliers_dropped: u64,
    pub ops: Box<[Option<OpStats>]>,
    pub lag: Histogram<u32>,
    pub loop_gap: Histogram<u32>,
    pub issued: u64,
    pub completed: u64,
    pub failed: u64,
    pub timeouts: u64,
    pub cpu: CpuWindow,
}

impl PhaseStats {
    fn new(kinds: &[bool; OpKind::COUNT], template: &Histogram<u32>) -> Self {
        PhaseStats {
            io: IoCounters::default(),
            outliers: Vec::new(),
            outliers_dropped: 0,
            ops: kinds
                .iter()
                .map(|&used| used.then(|| OpStats::new(template)))
                .collect(),
            lag: template.clone(),
            loop_gap: template.clone(),
            issued: 0,
            completed: 0,
            failed: 0,
            timeouts: 0,
            cpu: CpuWindow::default(),
        }
    }

    #[cold]
    #[inline(never)]
    fn outlier(&mut self, outlier: Outlier) {
        if self.outliers.len() < MAX_OUTLIERS {
            self.outliers.push(outlier);
        } else {
            self.outliers_dropped += 1;
        }
    }

    fn prefault(&mut self) {
        self.lag.reset();
        self.loop_gap.reset();
        for op in self.ops.iter_mut().flatten() {
            op.latency.reset();
            op.service.reset();
        }
    }
}

pub struct Recorder {
    kinds: [bool; OpKind::COUNT],
    template: Histogram<u32>,
    outlier_ns: u64,
    phases: Vec<Option<Box<PhaseStats>>>,
    outstanding: Vec<u64>,
    total_outstanding: u64,
    current: u8,
}

impl Recorder {
    pub fn new(
        kinds: impl IntoIterator<Item = OpKind>,
        template: Histogram<u32>,
        outlier_ns: u64,
    ) -> Self {
        let mut used = [false; OpKind::COUNT];
        kinds.into_iter().for_each(|kind| used[kind.index()] = true);
        Recorder {
            kinds: used,
            template,
            outlier_ns,
            phases: (0..MAX_POINTS).map(|_| None).collect(),
            outstanding: vec![0; MAX_POINTS + 1],
            total_outstanding: 0,
            current: NO_PHASE,
        }
    }

    pub fn open(&mut self, phase: u8) {
        if let Some(slot) = self.phases.get_mut(phase as usize) {
            let mut stats = Box::new(PhaseStats::new(&self.kinds, &self.template));
            stats.prefault();
            *slot = Some(stats);
        }
    }

    pub fn take(&mut self, phase: u8) -> Option<Box<PhaseStats>> {
        let mut stats = self.phases.get_mut(phase as usize)?.take()?;
        if let Some(left) = self.outstanding.get_mut(phase as usize) {
            stats.timeouts = std::mem::take(left);
            self.total_outstanding = self.total_outstanding.saturating_sub(stats.timeouts);
        }
        Some(stats)
    }

    pub fn phase_mut(&mut self, phase: u8) -> Option<&mut PhaseStats> {
        self.phases.get_mut(phase as usize)?.as_deref_mut()
    }

    pub fn outstanding(&self, phase: u8) -> u64 {
        self.outstanding.get(phase as usize).copied().unwrap_or(0)
    }

    pub fn total_outstanding(&self) -> u64 {
        self.total_outstanding
    }

    #[inline]
    pub fn issued(&mut self, phase: u8) {
        if let Some(count) = self.outstanding.get_mut(phase as usize) {
            *count += 1;
            self.total_outstanding += 1;
        }
        if let Some(stats) = self.phase_mut(phase) {
            stats.issued += 1;
        }
    }

    pub fn set_current(&mut self, phase: u8) {
        self.current = phase;
    }

    #[inline]
    pub fn wrote(&mut self, bytes: usize) {
        if let Some(stats) = self.phase_mut(self.current) {
            stats.io.writes += 1;
            stats.io.write_bytes += bytes as u64;
        }
    }

    #[inline]
    pub fn read(&mut self, bytes: usize) {
        if let Some(stats) = self.phase_mut(self.current) {
            stats.io.reads += 1;
            stats.io.read_bytes += bytes as u64;
        }
    }

    #[inline]
    pub fn syscall(&mut self) {
        if let Some(stats) = self.phase_mut(self.current) {
            stats.io.syscalls += 1;
        }
    }

    #[inline]
    pub fn rearmed(&mut self) {
        if let Some(stats) = self.phase_mut(self.current) {
            stats.io.rearms += 1;
        }
    }

    #[inline]
    pub fn polled(&mut self, ready: bool) {
        if let Some(stats) = self.phase_mut(self.current) {
            stats.io.polls += 1;
            stats.io.ready_polls += u64::from(ready);
        }
    }

    #[inline]
    pub fn loop_gap(&mut self, phase: u8, gap_ns: u64) {
        if let Some(stats) = self.phase_mut(phase) {
            stats.loop_gap.saturating_record(gap_ns.max(HIST_LOW_NS));
        }
    }

    #[inline]
    pub fn lag(&mut self, phase: u8, lag_ns: u64) {
        if let Some(stats) = self.phase_mut(phase) {
            stats.lag.saturating_record(lag_ns.max(HIST_LOW_NS));
        }
    }

    #[inline]
    pub fn complete(&mut self, entry: &InFlight, outcome: Outcome, recv_ns: u64, conn: u32) {
        let phase = entry.phase;
        let outlier_ns = self.outlier_ns;
        self.settle(phase);
        let Some(stats) = self.phase_mut(phase) else {
            return;
        };
        stats.completed += 1;
        let latency = recv_ns.saturating_sub(entry.intended_ns);
        if latency > outlier_ns {
            stats.outlier(Outlier {
                conn,
                kind: entry.kind,
                intended_ns: entry.intended_ns,
                lag_ns: entry.lag_ns,
                latency_ns: latency,
            });
        }
        let Some(Some(op)) = stats.ops.get_mut(entry.kind.index()) else {
            return;
        };
        let service = latency.saturating_sub(u64::from(entry.lag_ns));
        op.latency.saturating_record(latency.max(HIST_LOW_NS));
        op.service.saturating_record(service.max(HIST_LOW_NS));
        match outcome {
            Outcome::Ok => op.ok += 1,
            Outcome::Miss => op.miss += 1,
            Outcome::Error => op.error += 1,
        }
    }

    #[inline]
    fn settle(&mut self, phase: u8) {
        if let Some(count) = self.outstanding.get_mut(phase as usize)
            && *count > 0
        {
            *count -= 1;
            self.total_outstanding -= 1;
        }
    }

    pub fn fail(&mut self, entry: &InFlight) {
        self.settle(entry.phase);
        if let Some(stats) = self.phase_mut(entry.phase) {
            stats.failed += 1;
        }
    }
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct Percentiles {
    pub count: u64,
    pub p50_us: f64,
    pub p90_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub p9999_us: f64,
    pub max_us: f64,
    pub mean_us: f64,
}

impl Percentiles {
    pub fn of(hist: &Histogram<u64>) -> Self {
        let us = |ns: u64| ns as f64 / 1_000.0;
        Percentiles {
            count: hist.len(),
            p50_us: us(hist.value_at_quantile(0.5)),
            p90_us: us(hist.value_at_quantile(0.9)),
            p99_us: us(hist.value_at_quantile(0.99)),
            p999_us: us(hist.value_at_quantile(0.999)),
            p9999_us: us(hist.value_at_quantile(0.9999)),
            max_us: us(hist.max()),
            mean_us: hist.mean() / 1_000.0,
        }
    }
}

pub fn add_into(target: &mut Histogram<u64>, source: &Histogram<u32>) {
    for value in source.iter_recorded() {
        target.saturating_record_n(value.value_iterated_to(), u64::from(value.count_at_value()));
    }
}
