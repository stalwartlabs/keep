/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use mock_stalwart::{
    clock::Clock,
    cpu::{ProcessUsage, thread_cpu_ns},
    hist::{AtomicHistogram, Percentiles},
};
use std::{
    hint::black_box,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::runtime::Runtime;

const JOBS: u64 = 400_000;
const TASKS: usize = 2048;
const WORKERS: usize = 4;
const ROUNDS: usize = 5;

#[derive(Clone, Copy, Debug)]
enum Variant {
    AsyncChannel,
    Flume,
    Spawn,
}

#[derive(Clone, Copy)]
struct Job {
    sent: u64,
}

struct Shared {
    clock: Clock,
    latency: AtomicHistogram,
    done: AtomicU64,
}

struct Outcome {
    latency: Percentiles,
    cpu_ns_per_job: f64,
    dispatcher_ns_per_job: f64,
    switches_per_job: f64,
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("runtime")
}

fn handle(shared: &Shared, job: Job) {
    let now = shared.clock.now();
    shared.latency.record(now.saturating_sub(job.sent));
    black_box(job);
    shared.done.fetch_add(1, Ordering::Relaxed);
}

fn pace(clock: Clock, start: u64, index: u64, gap_ns: u64) {
    let due = start + index * gap_ns;
    while clock.now() < due {
        std::hint::spin_loop();
    }
}

fn run(variant: Variant, rate: f64) -> Outcome {
    let runtime = runtime();
    let shared = Arc::new(Shared {
        clock: Clock::start(),
        latency: AtomicHistogram::default(),
        done: AtomicU64::new(0),
    });
    let gap_ns = (1e9 / rate) as u64;
    let handle_rt = runtime.handle().clone();
    let (async_tx, async_rx) = async_channel::unbounded::<Job>();
    let (flume_tx, flume_rx) = flume::unbounded::<Job>();
    match variant {
        Variant::AsyncChannel => {
            for _ in 0..TASKS {
                let (rx, shared) = (async_rx.clone(), shared.clone());
                handle_rt.spawn(async move {
                    while let Ok(job) = rx.recv().await {
                        handle(&shared, job);
                    }
                });
            }
        }
        Variant::Flume => {
            for _ in 0..TASKS {
                let (rx, shared) = (flume_rx.clone(), shared.clone());
                handle_rt.spawn(async move {
                    while let Ok(job) = rx.recv_async().await {
                        handle(&shared, job);
                    }
                });
            }
        }
        Variant::Spawn => {}
    }
    std::thread::sleep(Duration::from_millis(200));
    let clock = shared.clock;
    let before = ProcessUsage::now();
    let dispatcher = std::thread::spawn({
        let shared = shared.clone();
        move || {
            let thread_start = thread_cpu_ns();
            let start = clock.now();
            for index in 0..JOBS {
                pace(clock, start, index, gap_ns);
                let job = Job { sent: clock.now() };
                match variant {
                    Variant::AsyncChannel => {
                        let _ = async_tx.try_send(job);
                    }
                    Variant::Flume => {
                        let _ = flume_tx.send(job);
                    }
                    Variant::Spawn => {
                        let shared = shared.clone();
                        handle_rt.spawn(async move { handle(&shared, job) });
                    }
                }
            }
            thread_cpu_ns() - thread_start
        }
    });
    let dispatcher_ns = dispatcher.join().expect("dispatcher");
    while shared.done.load(Ordering::Relaxed) < JOBS {
        std::thread::sleep(Duration::from_millis(1));
    }
    let usage = ProcessUsage::now().since(&before);
    drop(runtime);
    Outcome {
        latency: Percentiles::of(&shared.latency.to_histogram().expect("histogram bounds")),
        cpu_ns_per_job: usage.cpu_ns.saturating_sub(dispatcher_ns) as f64 / JOBS as f64,
        dispatcher_ns_per_job: dispatcher_ns as f64 / JOBS as f64,
        switches_per_job: (usage.voluntary_switches + usage.involuntary_switches) as f64
            / JOBS as f64,
    }
}

fn main() {
    let rates = std::env::args()
        .skip(1)
        .filter_map(|arg| arg.parse::<f64>().ok())
        .collect::<Vec<_>>();
    let rates = match rates.is_empty() {
        true => vec![20_000.0, 100_000.0, 400_000.0],
        false => rates,
    };
    println!("jobs {JOBS} tasks {TASKS} workers {WORKERS} rounds {ROUNDS} (interleaved)");
    println!(
        "{:>9} {:<13}{:>11}{:>11}{:>11}{:>11}{:>14}{:>14}{:>12}",
        "rate",
        "variant",
        "p50 ns",
        "p99 ns",
        "p99.9 ns",
        "max ns",
        "worker ns/job",
        "disp ns/job",
        "csw/job"
    );
    for rate in rates {
        for round in 0..ROUNDS {
            let order = match round % 3 {
                0 => [Variant::AsyncChannel, Variant::Flume, Variant::Spawn],
                1 => [Variant::Flume, Variant::Spawn, Variant::AsyncChannel],
                _ => [Variant::Spawn, Variant::AsyncChannel, Variant::Flume],
            };
            for variant in order {
                let outcome = run(variant, rate);
                println!(
                    "{:>9} {:<13}{:>11}{:>11}{:>11}{:>11}{:>14.0}{:>14.0}{:>12.3}",
                    rate,
                    format!("{variant:?}"),
                    outcome.latency.p50,
                    outcome.latency.p99,
                    outcome.latency.p999,
                    outcome.latency.max,
                    outcome.cpu_ns_per_job,
                    outcome.dispatcher_ns_per_job,
                    outcome.switches_per_job
                );
            }
        }
    }
}
