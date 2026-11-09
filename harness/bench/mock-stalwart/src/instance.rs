/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    clock::{Clock, Window, duration_ns},
    config::{Check, Config, Issuers, KvConfig, StreamConfig},
    dispatch::{Dispatcher, EventFeed, EventSink, JobSink, KvFeed, Marks},
    kv::{
        KvBackend, KvError,
        model::Model,
        preload::preload,
        stats::KvStats,
        worker::{KvContext, Worker},
    },
    load,
    pattern::mix64,
    progress::{Progress, Sources},
    report::{CpuInputs, CpuReport, Report, Validity},
    stats::Tagged,
    stream::{
        EventReplay, StreamError, StreamStats,
        ack::{self, AckReceiver, Tracker},
        backend::{Identity, StreamBackend, loopback::Hub},
        frame::Origin,
        publisher::{self, IPC_CHANNEL_BUFFER, PublishContext},
        subscriber::{self, ReceiveContext},
    },
    workload::{EventSource, Workload, WorkloadError, mix::KeyCodec, mix::KeyScope},
};
use hdrhistogram::CreationError;
use rand::{SeedableRng, rngs::SmallRng};
use std::{
    fmt, io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};

const ACK_QUEUE: usize = 4096;
const DRAIN_POLL: Duration = Duration::from_millis(10);

pub struct Shared {
    pub clock: Clock,
    pub hub: Arc<Hub>,
    pub ready: tokio::sync::Barrier,
    pub begin: AtomicU64,
}

impl Shared {
    pub fn new(instances: u16) -> Self {
        Shared {
            clock: Clock::start(),
            hub: Arc::default(),
            ready: tokio::sync::Barrier::new(usize::from(instances)),
            begin: AtomicU64::new(0),
        }
    }

    async fn rendezvous(&self) -> u64 {
        self.begin.fetch_max(self.clock.now(), Ordering::AcqRel);
        self.ready.wait().await;
        self.begin.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub enum RunError {
    Runtime(io::Error),
    Kv(KvError),
    Stream(StreamError),
    Workload(WorkloadError),
    Histogram(CreationError),
}

impl From<CreationError> for RunError {
    fn from(err: CreationError) -> Self {
        RunError::Histogram(err)
    }
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RunError::Runtime(err) => write!(f, "runtime: {err}"),
            RunError::Kv(err) => write!(f, "kv: {err}"),
            RunError::Stream(err) => write!(f, "stream: {err}"),
            RunError::Workload(err) => write!(f, "workload: {err}"),
            RunError::Histogram(err) => write!(f, "histogram: {err:?}"),
        }
    }
}

pub fn run(config: &Config, node: u16, shared: &Shared) -> Result<Report, RunError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.workers)
        .enable_all()
        .thread_name(format!("ms-{node}"))
        .build();
    match runtime {
        Ok(runtime) => runtime.block_on(run_async(config, node, shared)),
        Err(err) => {
            futures::executor::block_on(shared.rendezvous());
            Err(RunError::Runtime(err))
        }
    }
}

struct KvPrepared {
    backend: KvBackend,
    workload: Workload,
    codec: KeyCodec,
    model: Option<Model>,
    stats: KvStats,
}

struct StreamPrepared {
    backend: Arc<StreamBackend>,
    source: Arc<EventSource>,
}

async fn prepare_kv(
    config: &Config,
    kv: &KvConfig,
    node: u16,
    clock: Clock,
    seed: u64,
) -> Result<KvPrepared, RunError> {
    let workload = Workload::load(
        &kv.workload,
        kv.scope,
        kv.deployment,
        config.replay.select.nodes(node),
    )
    .map_err(RunError::Workload)?;
    let backend = KvBackend::connect(&kv.target, kv.mapping, kv.shape, clock)
        .await
        .map_err(RunError::Kv)?;
    let codec = KeyCodec::new(kv.scope, mix64(seed ^ 0x6b65_7973), node);
    let model = match (kv.check, kv.scope, backend.incr_ttl()) {
        (Check::Model, KeyScope::Node, Some(incr_ttl)) => Some(Model::new(kv.unseen, incr_ttl)),
        _ => None,
    };
    let stats = KvStats::default();
    if let Workload::Mix(mix) = &workload {
        preload(
            &backend,
            mix,
            &codec,
            model.as_ref(),
            clock,
            kv.preload_scale,
            &stats.counters,
        )
        .await;
    }
    Ok(KvPrepared {
        backend,
        workload,
        codec,
        model,
        stats,
    })
}

async fn prepare_stream(
    stream: &StreamConfig,
    config: &Config,
    identity: Identity,
    shared: &Shared,
) -> Result<StreamPrepared, RunError> {
    let source = EventSource::load(&stream.events, config.replay.select.nodes(identity.node))
        .map_err(RunError::Workload)?;
    let backend = StreamBackend::connect(&stream.target, identity, &shared.hub)
        .await
        .map_err(RunError::Stream)?;
    Ok(StreamPrepared {
        backend: Arc::new(backend),
        source: Arc::new(source),
    })
}

async fn prepare(
    config: &Config,
    node: u16,
    clock: Clock,
    seed: u64,
    identity: Identity,
    shared: &Shared,
) -> Result<(Option<KvPrepared>, Option<StreamPrepared>), RunError> {
    let kv = match &config.kv {
        Some(kv) => Some(prepare_kv(config, kv, node, clock, seed).await?),
        None => None,
    };
    let stream = match &config.stream {
        Some(stream) => Some(prepare_stream(stream, config, identity, shared).await?),
        None => None,
    };
    Ok((kv, stream))
}

async fn run_async(config: &Config, node: u16, shared: &Shared) -> Result<Report, RunError> {
    let clock = shared.clock;
    let seed = match config.seed {
        0 => mix64(clock.now() ^ u64::from(node)),
        seed => mix64(seed ^ u64::from(node)),
    };
    let incarnation = (mix64(seed ^ clock.now()) & 0xffff) as u16;
    let identity = Identity { node, incarnation };

    let prepared = prepare(config, node, clock, seed, identity, shared).await;
    let ready = shared.rendezvous().await;
    let (kv, stream) = prepared?;

    let begin = config.start_at.unwrap_or(0).max(ready);
    let window = Window::new(begin + duration_ns(config.warmup), config.duration);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = tokio::runtime::Handle::current();
    let burn = Arc::new(AtomicU64::new(0));

    let mut kv_feed = None;
    let mut kv_workers = Vec::new();
    let mut kv_ctx = None;
    if let (Some(prepared), Some(kv_config)) = (kv, &config.kv) {
        let ctx = Arc::new(KvContext {
            backend: prepared.backend,
            workload: prepared.workload,
            codec: prepared.codec,
            clock,
            window,
            model: prepared.model,
            stats: prepared.stats,
            node,
        });
        let (follow_tx, follow_rx) = mpsc::channel();
        let jobs = match kv_config.issuers {
            Issuers::Pool(tasks) => {
                let (jobs_tx, jobs_rx) = async_channel::unbounded();
                for task in 0..tasks {
                    let worker = Worker::new(
                        SmallRng::seed_from_u64(mix64(seed ^ task as u64)),
                        follow_tx.clone(),
                    );
                    let ctx = ctx.clone();
                    let jobs = jobs_rx.clone();
                    kv_workers.push(handle.spawn(async move { worker.run(&ctx, jobs).await }));
                }
                JobSink::Pool(jobs_tx)
            }
            Issuers::TaskPerOp => JobSink::Spawn {
                handle: handle.clone(),
                follow: follow_tx,
            },
        };
        kv_feed = Some(KvFeed {
            arrival: kv_config.arrival,
            ctx: ctx.clone(),
            jobs,
            follow_ups: follow_rx,
        });
        kv_ctx = Some(ctx);
    }

    let mut event_feed = None;
    let mut stream_tasks = StreamTasks::default();
    let mut stream_parts = None;
    if let (Some(prepared), Some(stream_config)) = (stream, &config.stream) {
        let stats = Arc::new(StreamStats::default());
        let tracker = Arc::new(Tracker::new(
            u32::from(stream_config.expected_nodes.saturating_sub(1)),
            stream_config.ack_every,
        ));
        let acks = match stream_config.ack_every {
            0 => None,
            every => {
                let (ack_tx, ack_rx) = tokio::sync::mpsc::channel(ACK_QUEUE);
                let publisher = prepared
                    .backend
                    .ack_publisher()
                    .await
                    .map_err(RunError::Stream)?;
                let acker_stats = stats.clone();
                stream_tasks.acker = Some(handle.spawn(async move {
                    ack::run_acker(publisher, ack_rx, node, &acker_stats).await
                }));
                let backend = prepared.backend.clone();
                let stats = stats.clone();
                let tracker = tracker.clone();
                let shutdown = shutdown_rx.clone();
                stream_tasks.ack_receiver = Some(handle.spawn(async move {
                    let receiver = AckReceiver {
                        node,
                        clock,
                        window,
                        stats: &stats,
                        tracker: &tracker,
                    };
                    ack::run_receiver(&backend, receiver, shutdown).await
                }));
                Some((ack_tx, every))
            }
        };
        {
            let backend = prepared.backend.clone();
            let stats = stats.clone();
            let shutdown = shutdown_rx.clone();
            let (verify, apply, catch_up) = (
                stream_config.verify,
                stream_config.apply,
                stream_config.catch_up,
            );
            stream_tasks.subscriber = Some(handle.spawn(async move {
                let ctx = ReceiveContext {
                    node,
                    clock,
                    window,
                    stats: &stats,
                    verify,
                    apply,
                    catch_up,
                    acks,
                };
                subscriber::run(&backend, ctx, shutdown).await
            }));
        }
        let sink = {
            let publisher = prepared
                .backend
                .publisher()
                .await
                .map_err(RunError::Stream)?;
            let stats = stats.clone();
            let tracker = tracker.clone();
            let track = stream_config.ack_every > 0;
            match stream_config.replay {
                EventReplay::Events => {
                    let (tx, rx) = tokio::sync::mpsc::channel(IPC_CHANNEL_BUFFER);
                    stream_tasks.publisher = Some(handle.spawn(async move {
                        let tracker = track.then_some(tracker.as_ref());
                        let ctx = publish_context(identity, clock, window, &stats, tracker);
                        publisher::run(publisher, rx, ctx).await
                    }));
                    EventSink::Events(tx)
                }
                EventReplay::Messages => {
                    let (tx, rx) = tokio::sync::mpsc::channel(IPC_CHANNEL_BUFFER);
                    stream_tasks.publisher = Some(handle.spawn(async move {
                        let tracker = track.then_some(tracker.as_ref());
                        let ctx = publish_context(identity, clock, window, &stats, tracker);
                        publisher::run_messages(publisher, rx, ctx).await
                    }));
                    EventSink::Messages(tx)
                }
            }
        };
        event_feed = Some(EventFeed {
            arrival: stream_config.arrival,
            source: prepared.source.clone(),
            stats: stats.clone(),
            sink,
        });
        stream_parts = Some((stats, tracker, prepared.source, prepared.backend));
    }

    let burners = match config.load {
        Some(load) => load::spawn(&handle, load, config.workers, &burn, &shutdown_rx),
        None => Vec::new(),
    };
    let progress = match config.progress {
        Some(interval) => {
            let sources = Sources {
                node,
                clock,
                window,
                kv: kv_ctx.clone(),
                stream: stream_parts.as_ref().map(|(stats, ..)| stats.clone()),
            };
            Some(Progress::spawn(interval, sources, io::stderr()).map_err(RunError::Runtime)?)
        }
        None => None,
    };

    tokio::time::sleep_until(tokio::time::Instant::from_std(clock.instant_at(begin))).await;
    let dispatcher = Dispatcher {
        clock,
        begin: clock.now(),
        window,
        spin: config.spin,
        seed: mix64(seed ^ 0x6469_7370),
        kv: kv_feed,
        events: event_feed,
        burn: burn.clone(),
    };
    let marks = tokio::task::spawn_blocking(move || dispatcher.run())
        .await
        .unwrap_or_else(|_| Marks::default());

    let mut notes = Vec::new();
    let drained = match kv_ctx.as_deref() {
        Some(ctx) if kv_workers.is_empty() => drain(ctx, config.grace).await,
        _ => join_all(kv_workers, config.grace).await,
    };
    if !drained {
        notes.push("KV ops did not drain within the grace period".to_string());
    }
    if let Some(publisher) = stream_tasks.publisher.take()
        && !join_all(vec![publisher], config.grace).await
    {
        notes.push("publisher did not drain within the grace period".to_string());
    }
    tokio::time::sleep(match config.stream {
        Some(_) => config.settle,
        None => Duration::ZERO,
    })
    .await;
    let _ = shutdown_tx.send(true);
    if let Some(progress) = progress {
        progress.stop();
    }
    let tail = [
        stream_tasks.subscriber.take(),
        stream_tasks.ack_receiver.take(),
        stream_tasks.acker.take(),
    ]
    .into_iter()
    .flatten()
    .chain(burners)
    .collect::<Vec<_>>();
    join_all(tail, config.grace).await;

    let mut histograms: Vec<Tagged> = Vec::new();
    let mut validity = Validity::default();
    let kv_report = match kv_ctx.as_ref() {
        None => None,
        Some(ctx) => {
            ctx.stats.histograms(&mut histograms)?;
            let mut report = ctx.stats.report(ctx.model.as_ref().map(Model::entries))?;
            report.client_counters = marks.client_deltas();
            validity.kv_errors = report.errors;
            validity.kv_mismatches = report.mismatches;
            validity.kv_lag_p999_ns = report.lag.p999;
            validity.kv_unfinished = ctx
                .stats
                .counters
                .dispatched
                .load(Ordering::Relaxed)
                .saturating_sub(ctx.stats.completed.load(Ordering::Relaxed));
            Some(report)
        }
    };
    let stream_report = match stream_parts.as_ref() {
        None => None,
        Some((stats, tracker, source, backend)) => {
            tracker.finish(stats);
            stats.histograms(&mut histograms)?;
            let mut report = stats.report(match source.as_ref() {
                EventSource::Trace(trace) => Some(trace.published),
                EventSource::Mix(_) => None,
            })?;
            let mut client = Vec::new();
            backend.client_counters(&mut client);
            report.client_counters = (!client.is_empty()).then(|| client.into_iter().collect());
            validity.stream_gaps = report.counters.gaps;
            validity.stream_verify_failures = report.counters.verify_failures;
            validity.stream_publish_errors = report.counters.publish_errors;
            validity.event_lag_p999_ns = report.lag.p999;
            Some(report)
        }
    };
    if config.instances > 1 {
        notes.push("process CPU is shared by every instance in this process".to_string());
    }
    if kv_report.is_some() && stream_report.is_some() {
        notes.push(
            "KV and stream share the process: CPU per op and per event both include the other"
                .to_string(),
        );
    }
    validity.notes = notes;
    let cpu = CpuReport::from_marks(CpuInputs {
        marks,
        workers: config.workers,
        kv_ops: kv_report.as_ref().map(|report| report.completed_ops),
        events_published: stream_report
            .as_ref()
            .map(|report| report.counters.events_published),
        events_received: stream_report
            .as_ref()
            .map(|report| report.counters.events_received),
    });
    Ok(Report {
        node,
        incarnation,
        window_start_unix_ns: window.start,
        window_ns: window.length_ns(),
        settings: settings(config),
        cpu,
        kv: kv_report,
        stream: stream_report,
        validity,
        histograms,
    })
}

fn publish_context<'a>(
    identity: Identity,
    clock: Clock,
    window: Window,
    stats: &'a StreamStats,
    tracker: Option<&'a Tracker>,
) -> PublishContext<'a> {
    PublishContext {
        origin: Origin {
            node: identity.node,
            incarnation: identity.incarnation,
        },
        clock,
        window,
        stats,
        tracker,
    }
}

#[derive(Default)]
struct StreamTasks {
    publisher: Option<JoinHandle<()>>,
    subscriber: Option<JoinHandle<()>>,
    acker: Option<JoinHandle<()>>,
    ack_receiver: Option<JoinHandle<()>>,
}

async fn drain(ctx: &KvContext, grace: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        let dispatched = ctx.stats.counters.dispatched.load(Ordering::Relaxed);
        if ctx.stats.completed.load(Ordering::Relaxed) >= dispatched {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(DRAIN_POLL).await;
    }
}

async fn join_all(handles: Vec<JoinHandle<()>>, grace: Duration) -> bool {
    let aborts = handles
        .iter()
        .map(JoinHandle::abort_handle)
        .collect::<Vec<_>>();
    match tokio::time::timeout(grace, futures::future::join_all(handles)).await {
        Ok(_) => true,
        Err(_) => {
            aborts.iter().for_each(|handle| handle.abort());
            false
        }
    }
}

fn settings(config: &Config) -> serde_json::Value {
    serde_json::json!({
        "instances": config.instances,
        "worker_threads": config.workers,
        "warmup_s": config.warmup.as_secs_f64(),
        "duration_s": config.duration.as_secs_f64(),
        "spin_us": config.spin.as_secs_f64() * 1e6,
        "kv": config.kv.as_ref().map(|kv| serde_json::json!({
            "target": format!("{:?}", kv.target),
            "mapping": kv.mapping,
            "conn": kv.shape,
            "arrival": kv.arrival,
            "issuers": kv.issuers,
            "workload": kv.workload,
            "scope": format!("{:?}", kv.scope),
            "check": kv.check,
            "unseen": format!("{:?}", kv.unseen),
            "preload_scale": kv.preload_scale,
        })),
        "stream": config.stream.as_ref().map(|stream| serde_json::json!({
            "target": format!("{:?}", stream.target),
            "arrival": stream.arrival,
            "events": stream.events,
            "replay": stream.replay,
            "expected_nodes": stream.expected_nodes,
            "ack_every": stream.ack_every,
            "apply_us": stream.apply.as_secs_f64() * 1e6,
            "catch_up_us": stream.catch_up.as_secs_f64() * 1e6,
            "verify": stream.verify,
        })),
        "load": config.load,
    })
}
