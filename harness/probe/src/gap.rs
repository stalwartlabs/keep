/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    args::CpuList,
    clock::{Clock, ns_to_secs, secs_to_ns, unix_now},
    error::{Error, Result},
    hist::{LatencyHist, Summary},
    report::emit,
    sys::{self, CpuTimes},
};
use clap::Args;
use serde::Serialize;
use std::thread;

const TOP_GAPS: usize = 16;
const BUCKETS_NS: [u64; 4] = [1_000, 10_000, 100_000, 1_000_000];

#[derive(Args)]
pub struct GapArgs {
    #[arg(
        long,
        help = "CPUs to measure, one spinning thread each (default: unpinned single thread)"
    )]
    cpus: Option<CpuList>,
    #[arg(long, default_value_t = 10.0)]
    duration: f64,
    #[arg(long)]
    label: Option<String>,
}

struct Top([u64; TOP_GAPS]);

impl Top {
    #[inline]
    fn offer(&mut self, gap: u64) {
        if let Some(min) = self.0.iter_mut().min()
            && gap > *min
        {
            *min = gap;
        }
    }

    fn sorted(mut self) -> Vec<u64> {
        self.0.sort_unstable_by(|a, b| b.cmp(a));
        self.0.into_iter().filter(|g| *g > 0).collect()
    }
}

struct Outcome {
    cpu: Option<usize>,
    hist: LatencyHist,
    iterations: u64,
    over: [u64; BUCKETS_NS.len()],
    top: Vec<u64>,
}

#[derive(Serialize)]
struct CpuGaps {
    cpu: Option<usize>,
    iterations: u64,
    gap_ns: Summary,
    over_1us: u64,
    over_10us: u64,
    over_100us: u64,
    over_1ms: u64,
    top_ns: Vec<u64>,
}

#[derive(Serialize)]
struct GapReport<'a> {
    probe: &'static str,
    label: Option<&'a str>,
    unix_start: f64,
    duration_s: f64,
    steal_pct: Option<f64>,
    cpus: Vec<CpuGaps>,
}

pub fn run(args: &GapArgs) -> Result<()> {
    let clock = Clock::new();
    let unix_start = unix_now();
    let before = CpuTimes::read();
    let threads = args.cpus.as_ref().map_or(1, CpuList::count);
    let until = secs_to_ns(args.duration);
    let outcomes = thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|slot| {
                let clock = &clock;
                scope.spawn(move || spin(args, clock, slot, until))
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or(Err(Error::Protocol("spin thread panicked")))
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let steal_pct = before
        .zip(CpuTimes::read())
        .and_then(|(b, a)| a.steal_pct_since(&b));
    emit(&GapReport {
        probe: "spin-gap",
        label: args.label.as_deref(),
        unix_start,
        duration_s: ns_to_secs(clock.now()),
        steal_pct,
        cpus: outcomes
            .into_iter()
            .map(|o| {
                let [over_1us, over_10us, over_100us, over_1ms] = o.over;
                CpuGaps {
                    cpu: o.cpu,
                    iterations: o.iterations,
                    gap_ns: o.hist.summary(),
                    over_1us,
                    over_10us,
                    over_100us,
                    over_1ms,
                    top_ns: o.top,
                }
            })
            .collect(),
    })
}

fn spin(args: &GapArgs, clock: &Clock, slot: usize, until: u64) -> Result<Outcome> {
    let cpu = sys::pin(args.cpus.as_ref(), slot)?;
    let mut hist = LatencyHist::new()?;
    let mut over = [0u64; BUCKETS_NS.len()];
    let mut top = Top([0; TOP_GAPS]);
    let mut iterations = 0u64;
    let mut prev = clock.now();
    while prev < until {
        let now = clock.now();
        let gap = now - prev;
        hist.record(gap);
        if gap > BUCKETS_NS[0] {
            for (count, limit) in over.iter_mut().zip(BUCKETS_NS) {
                *count += u64::from(gap > limit);
            }
            top.offer(gap);
        }
        iterations += 1;
        prev = now;
    }
    Ok(Outcome {
        cpu,
        hist,
        iterations,
        over,
        top: top.sorted(),
    })
}
