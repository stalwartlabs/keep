/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    clock::{Clock, ns_to_secs, unix_now},
    error::Result,
    report::emit,
    rng::SplitMix64,
    sys::{self, CpuTimes},
};
use clap::Args;
use serde::Serialize;
use std::hint::black_box;

const CACHE_LINE_WORDS: usize = 16;
const KIB: usize = 1024;
const MIB: usize = 1024 * 1024;
const ALU_MULTIPLIER: u64 = 0xBF58_476D_1CE4_E5B9;
const CHASE_SEED: u64 = 0x5EED;

#[derive(Args)]
pub struct CpuArgs {
    #[arg(long, help = "Pin to this CPU")]
    cpu: Option<usize>,
    #[arg(long, default_value_t = 10)]
    reps: u32,
    #[arg(
        long,
        default_value_t = 100_000_000,
        help = "Dependent multiply-xorshift steps per repetition"
    )]
    alu_iters: u64,
    #[arg(long, default_value_t = 512, value_parser = clap::value_parser!(u32).range(1..=1_048_576), help = "Pointer chase working set held in L2, KiB")]
    l2_kib: u32,
    #[arg(long, default_value_t = 20_000_000)]
    l2_steps: u64,
    #[arg(long, default_value_t = 256, value_parser = clap::value_parser!(u32).range(1..=8192), help = "Pointer chase working set in DRAM, MiB")]
    mem_mib: u32,
    #[arg(long, default_value_t = 2_000_000)]
    mem_steps: u64,
    #[arg(long)]
    label: Option<String>,
}

#[derive(Serialize)]
struct KernelStats {
    name: &'static str,
    ops: u64,
    reps: u32,
    min_ns: u64,
    median_ns: u64,
    max_ns: u64,
    mean_ns: f64,
    cv: f64,
    ns_per_op_min: f64,
    ns_per_op_median: f64,
}

#[derive(Serialize)]
struct CpuReport<'a> {
    probe: &'static str,
    label: Option<&'a str>,
    cpu: Option<usize>,
    unix_start: f64,
    elapsed_s: f64,
    steal_pct: Option<f64>,
    kernels: Vec<KernelStats>,
}

struct ChaseRing(Box<[u32]>);

impl ChaseRing {
    fn new(bytes: usize, rng: &mut SplitMix64) -> Self {
        let lines = (bytes / (CACHE_LINE_WORDS * size_of::<u32>())).max(2);
        let mut order: Vec<u32> = (0..u32::try_from(lines).unwrap_or(u32::MAX)).collect();
        for i in (1..order.len()).rev() {
            order.swap(i, rng.below(i));
        }
        let mut words = vec![0u32; order.len() * CACHE_LINE_WORDS].into_boxed_slice();
        let (lines_mut, _) = words.as_chunks_mut::<CACHE_LINE_WORDS>();
        for ([first, ..], next) in lines_mut.iter_mut().zip(&order) {
            *first = next * CACHE_LINE_WORDS as u32;
        }
        Self(words)
    }

    #[inline(never)]
    fn walk(&self, steps: u64) -> u32 {
        let mut at = 0u32;
        for _ in 0..steps {
            at = self.0[at as usize];
        }
        at
    }
}

#[inline(never)]
fn alu_chain(iters: u64, seed: u64) -> u64 {
    let mut x = seed;
    for _ in 0..iters {
        x = (x ^ (x >> 29)).wrapping_mul(ALU_MULTIPLIER);
    }
    x
}

fn stats(name: &'static str, ops: u64, mut samples: Vec<u64>) -> KernelStats {
    samples.sort_unstable();
    let reps = u32::try_from(samples.len()).unwrap_or(u32::MAX);
    let mean = samples.iter().sum::<u64>() as f64 / samples.len().max(1) as f64;
    let variance = samples
        .iter()
        .map(|&s| (s as f64 - mean).powi(2))
        .sum::<f64>()
        / samples.len().max(1) as f64;
    let min = samples.first().copied().unwrap_or_default();
    let median = samples.get(samples.len() / 2).copied().unwrap_or_default();
    let per_op = |ns: u64| ns as f64 / ops.max(1) as f64;
    KernelStats {
        name,
        ops,
        reps,
        min_ns: min,
        median_ns: median,
        max_ns: samples.last().copied().unwrap_or_default(),
        mean_ns: mean,
        cv: if mean > 0.0 {
            variance.sqrt() / mean
        } else {
            0.0
        },
        ns_per_op_min: per_op(min),
        ns_per_op_median: per_op(median),
    }
}

fn time_reps(clock: &Clock, reps: u32, mut work: impl FnMut()) -> Vec<u64> {
    (0..reps)
        .map(|_| {
            let t0 = clock.now();
            work();
            clock.now() - t0
        })
        .collect()
}

pub fn run(args: &CpuArgs) -> Result<()> {
    let cpu = match args.cpu {
        Some(cpu) => Some(sys::pin_to(cpu).map(|()| cpu)?),
        None => None,
    };
    let mut rng = SplitMix64::new(CHASE_SEED);
    let l2 = ChaseRing::new(args.l2_kib as usize * KIB, &mut rng);
    let mem = ChaseRing::new(args.mem_mib as usize * MIB, &mut rng);
    black_box(l2.walk(black_box(args.l2_steps.min(1_000_000))));
    let clock = Clock::new();
    let unix_start = unix_now();
    let before = CpuTimes::read();
    let alu = time_reps(&clock, args.reps, || {
        black_box(alu_chain(black_box(args.alu_iters), black_box(1)));
    });
    let l2_samples = time_reps(&clock, args.reps, || {
        black_box(l2.walk(black_box(args.l2_steps)));
    });
    let mem_samples = time_reps(&clock, args.reps, || {
        black_box(mem.walk(black_box(args.mem_steps)));
    });
    let elapsed = clock.now();
    let steal_pct = before
        .zip(CpuTimes::read())
        .and_then(|(b, a)| a.steal_pct_since(&b));
    emit(&CpuReport {
        probe: "cpu-loop",
        label: args.label.as_deref(),
        cpu,
        unix_start,
        elapsed_s: ns_to_secs(elapsed),
        steal_pct,
        kernels: vec![
            stats("alu", args.alu_iters, alu),
            stats("chase-l2", args.l2_steps, l2_samples),
            stats("chase-mem", args.mem_steps, mem_samples),
        ],
    })
}
