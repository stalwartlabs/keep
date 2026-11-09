/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::{
    KvBackend, KvError, Probe,
    model::{Effect, Model, ValueTag, Verdict},
    op::KvOp,
    stats::{KvStats, OpStats},
};
use crate::{
    clock::{Clock, Window},
    pattern::{fill, fingerprint, mix64},
    stats::bump,
    workload::{
        Workload,
        mix::{ActionKind, Family, KeyCodec, KeyRef},
    },
};
use rand::{Rng, SeedableRng, rngs::SmallRng};
use std::sync::{atomic::Ordering, mpsc};

const LOGGED_ERRORS: u64 = 10;

pub struct KvContext {
    pub backend: KvBackend,
    pub workload: Workload,
    pub codec: KeyCodec,
    pub clock: Clock,
    pub window: Window,
    pub model: Option<Model>,
    pub stats: KvStats,
    pub node: u16,
}

#[derive(Clone, Copy, Debug)]
pub enum KvTask {
    Action(u16),
    Trace(u32),
    Unlock {
        action: u16,
        key: KeyRef,
    },
    Renew {
        action: u16,
        key: KeyRef,
        remaining: u8,
        interval: u64,
        unlock_at: u64,
    },
    Redeem {
        action: u16,
        key: KeyRef,
    },
    RenewLease {
        action: u16,
        key: KeyRef,
        token: u64,
        remaining: u8,
        interval: u64,
        unlock_at: u64,
    },
    ReleaseLease {
        action: u16,
        key: KeyRef,
        token: u64,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct KvJob {
    pub intended: u64,
    pub dispatched: u64,
    pub task: KvTask,
}

#[derive(Clone, Copy, Debug)]
pub struct FollowUp {
    pub due: u64,
    pub task: KvTask,
}

const _: () = assert!(size_of::<KvJob>() <= 64);

#[derive(Clone, Copy)]
struct Timing {
    intended: u64,
    dispatched: u64,
    started: u64,
}

#[derive(Clone, Copy)]
enum Call<'a> {
    Get,
    Exists,
    Set { value: &'a [u8], ttl: Option<u64> },
    Delete,
    Incr { delta: i64, ttl: Option<u64> },
    TryLock { ttl: u64 },
    Unlock,
    Renew { ttl: u64 },
    DeletePrefix,
    TryLease { token: u64, ttl: u64 },
    RenewLease { token: u64, ttl: u64 },
    ReleaseLease { token: u64 },
}

enum Reply {
    Value(Option<Vec<u8>>),
    Flag(bool),
    Count(i64),
    Done,
}

pub struct Worker {
    rng: SmallRng,
    key: Vec<u8>,
    value: Vec<u8>,
    batch_keys: Vec<u8>,
    batch_lens: Vec<(usize, usize)>,
    follow: mpsc::Sender<FollowUp>,
}

impl Call<'_> {
    fn op(&self) -> KvOp {
        match self {
            Call::Get => KvOp::Get,
            Call::Exists => KvOp::Exists,
            Call::Set { .. } => KvOp::Set,
            Call::Delete => KvOp::Delete,
            Call::Incr { .. } => KvOp::Incr,
            Call::TryLock { .. } => KvOp::TryLock,
            Call::Unlock => KvOp::Unlock,
            Call::Renew { .. } => KvOp::Renew,
            Call::DeletePrefix => KvOp::DeletePrefix,
            Call::TryLease { .. } => KvOp::TryLease,
            Call::RenewLease { .. } => KvOp::RenewLease,
            Call::ReleaseLease { .. } => KvOp::ReleaseLease,
        }
    }

    fn mutates(&self) -> bool {
        !matches!(self, Call::Get | Call::Exists)
    }
}

impl Timing {
    fn inline(at: u64) -> Self {
        Timing {
            intended: at,
            dispatched: at,
            started: at,
        }
    }
}

impl Worker {
    pub fn new(rng: SmallRng, follow: mpsc::Sender<FollowUp>) -> Self {
        Worker {
            rng,
            key: Vec::with_capacity(128),
            value: Vec::new(),
            batch_keys: Vec::new(),
            batch_lens: Vec::new(),
            follow,
        }
    }

    pub async fn once(ctx: &KvContext, job: KvJob, follow: mpsc::Sender<FollowUp>) {
        let started = ctx.clock.now();
        let mut worker = Worker::new(SmallRng::seed_from_u64(mix64(job.intended)), follow);
        worker.execute(ctx, job, started).await;
        ctx.stats.completed.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn run(mut self, ctx: &KvContext, jobs: async_channel::Receiver<KvJob>) {
        while let Ok(job) = jobs.recv().await {
            let started = ctx.clock.now();
            self.execute(ctx, job, started).await;
            ctx.stats.completed.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn execute(&mut self, ctx: &KvContext, job: KvJob, started: u64) {
        let timing = Timing {
            intended: job.intended,
            dispatched: job.dispatched,
            started,
        };
        match (job.task, &ctx.workload) {
            (KvTask::Action(index), Workload::Mix(mix)) => {
                if let Some((action, family)) = mix.action(index) {
                    let key = family.key_ref(family.sample_index(&mut self.rng), started);
                    self.action(ctx, index, action.kind, family, key, timing)
                        .await;
                }
            }
            (KvTask::Unlock { action, key }, Workload::Mix(mix)) => {
                if let Some((_, family)) = mix.action(action) {
                    family.write_key(&ctx.codec, key, &mut self.key);
                    call(ctx, Call::Unlock, &self.key, timing).await;
                }
            }
            (
                KvTask::Renew {
                    action,
                    key,
                    remaining,
                    interval,
                    unlock_at,
                },
                Workload::Mix(mix),
            ) => {
                if let Some((_, family)) = mix.action(action) {
                    family.write_key(&ctx.codec, key, &mut self.key);
                    let ttl = family.ttl_at(key, started).unwrap_or(1);
                    call(ctx, Call::Renew { ttl }, &self.key, timing).await;
                    let now = ctx.clock.now();
                    let next = match remaining {
                        0 | 1 => FollowUp {
                            due: unlock_at.max(now),
                            task: KvTask::Unlock { action, key },
                        },
                        _ => FollowUp {
                            due: now + interval,
                            task: KvTask::Renew {
                                action,
                                key,
                                remaining: remaining - 1,
                                interval,
                                unlock_at,
                            },
                        },
                    };
                    self.schedule(ctx, next);
                }
            }
            (
                KvTask::RenewLease {
                    action,
                    key,
                    token,
                    remaining,
                    interval,
                    unlock_at,
                },
                Workload::Mix(mix),
            ) => {
                if let Some((_, family)) = mix.action(action) {
                    family.write_key(&ctx.codec, key, &mut self.key);
                    let ttl = family.ttl_at(key, started).unwrap_or(1);
                    let renew = Call::RenewLease { token, ttl };
                    if let Some(Reply::Flag(false)) = call(ctx, renew, &self.key, timing).await {
                        bump(&ctx.stats.counters.leases_lost);
                        let retry = Call::TryLease { token, ttl };
                        call(ctx, retry, &self.key, Timing::inline(ctx.clock.now())).await;
                    }
                    let now = ctx.clock.now();
                    let next = match remaining {
                        0 | 1 => FollowUp {
                            due: unlock_at.max(now),
                            task: KvTask::ReleaseLease { action, key, token },
                        },
                        _ => FollowUp {
                            due: now + interval,
                            task: KvTask::RenewLease {
                                action,
                                key,
                                token,
                                remaining: remaining - 1,
                                interval,
                                unlock_at,
                            },
                        },
                    };
                    self.schedule(ctx, next);
                }
            }
            (KvTask::ReleaseLease { action, key, token }, Workload::Mix(mix)) => {
                if let Some((_, family)) = mix.action(action) {
                    family.write_key(&ctx.codec, key, &mut self.key);
                    call(ctx, Call::ReleaseLease { token }, &self.key, timing).await;
                }
            }
            (KvTask::Redeem { action, key }, Workload::Mix(mix)) => {
                if let Some((_, family)) = mix.action(action) {
                    family.write_key(&ctx.codec, key, &mut self.key);
                    call(ctx, Call::Get, &self.key, timing).await;
                    let now = ctx.clock.now();
                    call(ctx, Call::Delete, &self.key, Timing::inline(now)).await;
                }
            }
            (KvTask::Trace(index), Workload::Trace(trace)) => {
                if let Some((record, key)) = trace.record(index as usize) {
                    self.key.clear();
                    self.key.extend_from_slice(key);
                    ctx.codec.scope_trace_key(&mut self.key);
                    let value_len = record.value_len as usize;
                    let ttl = (record.ttl > 0).then_some(u64::from(record.ttl));
                    let call_args = match record.op {
                        KvOp::Get => Call::Get,
                        KvOp::Exists => Call::Exists,
                        KvOp::Set => {
                            self.fill_value(value_len);
                            Call::Set {
                                value: &self.value,
                                ttl,
                            }
                        }
                        KvOp::Incr => Call::Incr {
                            delta: record.delta,
                            ttl,
                        },
                        KvOp::TryLock => Call::TryLock {
                            ttl: ttl.unwrap_or(1),
                        },
                        KvOp::Unlock => Call::Unlock,
                        KvOp::Renew => Call::Renew {
                            ttl: ttl.unwrap_or(1),
                        },
                        KvOp::Delete => Call::Delete,
                        KvOp::DeletePrefix => Call::DeletePrefix,
                        KvOp::TryLease => Call::TryLease {
                            token: fingerprint(&self.key),
                            ttl: ttl.unwrap_or(1),
                        },
                        KvOp::RenewLease => Call::RenewLease {
                            token: fingerprint(&self.key),
                            ttl: ttl.unwrap_or(1),
                        },
                        KvOp::ReleaseLease => Call::ReleaseLease {
                            token: fingerprint(&self.key),
                        },
                        KvOp::SetMany => {
                            self.fill_value(value_len);
                            Call::Set {
                                value: &self.value,
                                ttl,
                            }
                        }
                    };
                    call(ctx, call_args, &self.key, timing).await;
                }
            }
            _ => {}
        }
    }

    async fn action(
        &mut self,
        ctx: &KvContext,
        index: u16,
        kind: ActionKind,
        family: &Family,
        key: KeyRef,
        timing: Timing,
    ) {
        family.write_key(&ctx.codec, key, &mut self.key);
        let ttl = family.ttl_at(key, timing.started);
        match kind {
            ActionKind::Rate => {
                call(ctx, Call::Incr { delta: 1, ttl }, &self.key, timing).await;
            }
            ActionKind::Counter => {
                call(
                    ctx,
                    Call::Incr {
                        delta: 1,
                        ttl: None,
                    },
                    &self.key,
                    timing,
                )
                .await;
            }
            ActionKind::Get => {
                call(ctx, Call::Get, &self.key, timing).await;
            }
            ActionKind::Exists => {
                call(ctx, Call::Exists, &self.key, timing).await;
            }
            ActionKind::Delete => {
                call(ctx, Call::Delete, &self.key, timing).await;
            }
            ActionKind::Set => {
                let len = family.value.sample(&mut self.rng) as usize;
                self.fill_value(len);
                let set = Call::Set {
                    value: &self.value,
                    ttl,
                };
                call(ctx, set, &self.key, timing).await;
            }
            ActionKind::CheckSet => {
                let found = call(ctx, Call::Exists, &self.key, timing).await;
                if let Some(Reply::Flag(false)) = found {
                    let len = family.value.sample(&mut self.rng) as usize;
                    self.fill_value(len);
                    let now = ctx.clock.now();
                    let set = Call::Set {
                        value: &self.value,
                        ttl,
                    };
                    call(ctx, set, &self.key, Timing::inline(now)).await;
                }
            }
            ActionKind::Lock { hold, renews } => {
                let ttl = ttl.unwrap_or(1);
                match call(ctx, Call::TryLock { ttl }, &self.key, timing).await {
                    Some(Reply::Flag(true)) => {
                        let now = ctx.clock.now();
                        let hold = hold.sample(&mut self.rng);
                        let task = match renews {
                            0 => KvTask::Unlock { action: index, key },
                            renews => KvTask::Renew {
                                action: index,
                                key,
                                remaining: renews,
                                interval: hold / (u64::from(renews) + 1),
                                unlock_at: now + hold,
                            },
                        };
                        let due = match task {
                            KvTask::Renew { interval, .. } => now + interval,
                            _ => now + hold,
                        };
                        self.schedule(ctx, FollowUp { due, task });
                    }
                    Some(Reply::Flag(false)) => bump(&ctx.stats.counters.locks_refused),
                    _ => {}
                }
            }
            ActionKind::Lease { hold, renews } => {
                let ttl = ttl.unwrap_or(1);
                let token = self.rng.next_u64();
                match call(ctx, Call::TryLease { token, ttl }, &self.key, timing).await {
                    Some(Reply::Flag(true)) => {
                        let now = ctx.clock.now();
                        let hold = hold.sample(&mut self.rng);
                        let follow_up = match renews {
                            0 => FollowUp {
                                due: now + hold,
                                task: KvTask::ReleaseLease {
                                    action: index,
                                    key,
                                    token,
                                },
                            },
                            renews => {
                                let interval = hold / (u64::from(renews) + 1);
                                FollowUp {
                                    due: now + interval,
                                    task: KvTask::RenewLease {
                                        action: index,
                                        key,
                                        token,
                                        remaining: renews,
                                        interval,
                                        unlock_at: now + hold,
                                    },
                                }
                            }
                        };
                        self.schedule(ctx, follow_up);
                    }
                    Some(Reply::Flag(false)) => bump(&ctx.stats.counters.locks_refused),
                    _ => {}
                }
            }
            ActionKind::SetMany { batch } => {
                let count = batch.sample(&mut self.rng);
                self.set_many(ctx, family, key, count, ttl, timing).await;
            }
            ActionKind::Token { redeem } => {
                let len = family.value.sample(&mut self.rng) as usize;
                self.fill_value(len);
                let set = Call::Set {
                    value: &self.value,
                    ttl,
                };
                if call(ctx, set, &self.key, timing).await.is_some() {
                    let due = ctx.clock.now() + redeem.sample(&mut self.rng);
                    self.schedule(
                        ctx,
                        FollowUp {
                            due,
                            task: KvTask::Redeem { action: index, key },
                        },
                    );
                }
            }
            ActionKind::Purge => {
                family.write_purge_prefix(&ctx.codec, &mut self.key);
                call(ctx, Call::DeletePrefix, &self.key, timing).await;
            }
        }
    }

    async fn set_many(
        &mut self,
        ctx: &KvContext,
        family: &Family,
        key: KeyRef,
        count: u64,
        ttl: Option<u64>,
        timing: Timing,
    ) {
        self.batch_keys.clear();
        self.batch_lens.clear();
        self.value.clear();
        for offset in 0..count {
            family.write_key(&ctx.codec, family.neighbour(key, offset), &mut self.key);
            self.batch_keys.extend_from_slice(&self.key);
            let start = self.value.len();
            let len = family.value.sample(&mut self.rng) as usize;
            fill(&mut self.value, mix64(self.rng.next_u64()), len);
            self.batch_lens
                .push((self.key.len(), self.value.len() - start));
        }
        let mut items = Vec::with_capacity(self.batch_lens.len());
        let (mut keys, mut values) = (self.batch_keys.as_slice(), self.value.as_slice());
        for &(key_len, value_len) in &self.batch_lens {
            let (Some((key, next_keys)), Some((value, next_values))) = (
                keys.split_at_checked(key_len),
                values.split_at_checked(value_len),
            ) else {
                break;
            };
            items.push((key, value));
            (keys, values) = (next_keys, next_values);
        }
        call_many(ctx, &items, ttl, timing).await;
    }

    fn fill_value(&mut self, len: usize) {
        self.value.clear();
        fill(&mut self.value, mix64(self.rng.next_u64()), len);
    }

    fn schedule(&self, ctx: &KvContext, follow_up: FollowUp) {
        bump(&ctx.stats.counters.follow_ups);
        if self.follow.send(follow_up).is_err() {
            bump(&ctx.stats.counters.follow_ups_lost);
        }
    }
}

async fn call(ctx: &KvContext, call: Call<'_>, key: &[u8], timing: Timing) -> Option<Reply> {
    let op = call.op();
    let ticket = match (&ctx.model, call) {
        (Some(model), Call::DeletePrefix) => {
            model.purge_begin();
            None
        }
        (Some(model), _) => Some((model, model.begin(fingerprint(key)))),
        (None, _) => None,
    };
    let mut probe = Probe::default();
    let start = ctx.clock.now().max(timing.started);
    let result = match call {
        Call::Get => ctx.backend.get(key, &mut probe).await.map(Reply::Value),
        Call::Exists => ctx.backend.exists(key, &mut probe).await.map(Reply::Flag),
        Call::Set { value, ttl } => ctx
            .backend
            .set(key, value, ttl, &mut probe)
            .await
            .map(|()| Reply::Done),
        Call::Delete => ctx
            .backend
            .delete(key, &mut probe)
            .await
            .map(|()| Reply::Done),
        Call::Unlock => ctx
            .backend
            .unlock(key, &mut probe)
            .await
            .map(|()| Reply::Done),
        Call::Incr { delta, ttl } => ctx
            .backend
            .incr(key, delta, ttl, &mut probe)
            .await
            .map(Reply::Count),
        Call::TryLock { ttl } => ctx
            .backend
            .try_lock(key, ttl, &mut probe)
            .await
            .map(Reply::Flag),
        Call::Renew { ttl } => ctx
            .backend
            .renew(key, ttl, &mut probe)
            .await
            .map(|()| Reply::Done),
        Call::DeletePrefix => ctx
            .backend
            .delete_prefix(key, &mut probe)
            .await
            .map(|()| Reply::Done),
        Call::TryLease { token, ttl } => ctx
            .backend
            .try_lease(key, token, ttl, &mut probe)
            .await
            .map(Reply::Flag),
        Call::RenewLease { token, ttl } => ctx
            .backend
            .renew_lease(key, token, ttl, &mut probe)
            .await
            .map(Reply::Flag),
        Call::ReleaseLease { token } => ctx
            .backend
            .release_lease(key, token, &mut probe)
            .await
            .map(Reply::Flag),
    };
    let end = ctx.clock.now();
    let stats = &ctx.stats.ops[op.index()];
    let in_window = record_latency(ctx, stats, timing, start, end, &probe);
    if let Some((model, ticket)) = ticket {
        let effect = match (&result, call) {
            (Ok(Reply::Value(value)), _) => Effect::Get(value.as_deref()),
            (Ok(Reply::Flag(exists)), Call::Exists) => Effect::Exists(*exists),
            (Ok(Reply::Flag(acquired)), Call::TryLock { ttl }) => Effect::TryLock {
                ttl,
                acquired: *acquired,
            },
            (Ok(Reply::Count(result)), Call::Incr { delta, ttl }) => Effect::Incr {
                delta,
                ttl,
                result: *result,
            },
            (Ok(_), Call::Set { value, ttl }) => Effect::Set {
                value: ValueTag::of(value),
                ttl,
            },
            (Ok(_), Call::Delete | Call::Unlock) => Effect::Delete,
            (Ok(_), Call::Renew { ttl }) => Effect::Renew { ttl },
            (Ok(Reply::Flag(acquired)), Call::TryLease { token, ttl }) => Effect::TryLease {
                token,
                ttl,
                acquired: *acquired,
            },
            (Ok(Reply::Flag(renewed)), Call::RenewLease { token, ttl }) => Effect::RenewLease {
                token,
                ttl,
                renewed: *renewed,
            },
            (Ok(Reply::Flag(released)), Call::ReleaseLease { token }) => Effect::ReleaseLease {
                token,
                released: *released,
            },
            _ => Effect::Failed {
                mutates: call.mutates(),
            },
        };
        record_verdict(
            ctx,
            op,
            model.complete(ticket, effect, start, end),
            in_window,
        );
    }
    if let Some(model) = ctx
        .model
        .as_ref()
        .filter(|_| matches!(call, Call::DeletePrefix))
    {
        model.purge_end();
    }
    match result {
        Ok(reply) => Some(reply),
        Err(err) => {
            bump(&stats.errors);
            log_error(ctx, op, &err);
            None
        }
    }
}

async fn call_many(
    ctx: &KvContext,
    items: &[(&[u8], &[u8])],
    ttl: Option<u64>,
    timing: Timing,
) -> bool {
    let op = KvOp::SetMany;
    let tickets = ctx.model.as_ref().map(|model| {
        items
            .iter()
            .map(|(key, _)| model.begin(fingerprint(key)))
            .collect::<Vec<_>>()
    });
    let mut probe = Probe::default();
    let start = ctx.clock.now().max(timing.started);
    let result = ctx.backend.set_many(items, ttl, &mut probe).await;
    let end = ctx.clock.now();
    let stats = &ctx.stats.ops[op.index()];
    let in_window = record_latency(ctx, stats, timing, start, end, &probe);
    if let (Some(model), Some(tickets)) = (&ctx.model, tickets) {
        for (ticket, (_, value)) in tickets.into_iter().zip(items) {
            let effect = match result {
                Ok(()) => Effect::Set {
                    value: ValueTag::of(value),
                    ttl,
                },
                Err(_) => Effect::Failed { mutates: true },
            };
            record_verdict(
                ctx,
                op,
                model.complete(ticket, effect, start, end),
                in_window,
            );
        }
    }
    match result {
        Ok(()) => true,
        Err(err) => {
            bump(&stats.errors);
            log_error(ctx, op, &err);
            false
        }
    }
}

fn record_latency(
    ctx: &KvContext,
    stats: &OpStats,
    timing: Timing,
    start: u64,
    end: u64,
    probe: &Probe,
) -> bool {
    let in_window = ctx.window.contains(timing.intended);
    if in_window {
        stats.total.record(end - timing.intended);
        stats
            .queue
            .record(timing.started.saturating_sub(timing.dispatched));
        stats.service.record(end - start);
        if probe.checkout_ns > 0 {
            ctx.stats.checkout.record(probe.checkout_ns);
        }
    }
    in_window
}

fn record_verdict(ctx: &KvContext, op: KvOp, verdict: Verdict, in_window: bool) {
    let stats = &ctx.stats.ops[op.index()];
    match verdict {
        Verdict::Match if in_window => bump(&stats.checked),
        Verdict::Unchecked if in_window => bump(&stats.unchecked),
        Verdict::Mismatch(reason) => {
            bump(&stats.mismatches);
            log_problem(ctx, op, &reason);
        }
        _ => {}
    }
}

#[cold]
#[inline(never)]
fn log_error(ctx: &KvContext, op: KvOp, err: &KvError) {
    log_problem(ctx, op, &err.to_string());
}

#[cold]
#[inline(never)]
fn log_problem(ctx: &KvContext, op: KvOp, reason: &str) {
    if ctx
        .stats
        .counters
        .errors_logged
        .fetch_add(1, Ordering::Relaxed)
        < LOGGED_ERRORS
    {
        eprintln!("node {} {}: {reason}", ctx.node, op.name());
    }
}

#[cfg(test)]
mod tests {
    use super::{KvContext, KvJob, KvTask, Worker};
    use crate::{
        clock::{Clock, Window},
        kv::{KvBackend, stats::KvStats},
        workload::{
            Workload,
            mix::{KeyCodec, KeyScope},
            trace::TraceNodes,
        },
    };
    use std::{sync::mpsc, time::Duration};

    // The per-op task future stays small enough to spawn once per operation.
    #[test]
    fn op_future_size() {
        let clock = Clock::start();
        let ctx = KvContext {
            backend: KvBackend::Null,
            workload: Workload::load(
                "mixed",
                KeyScope::Node,
                &crate::workload::deployment::LAB,
                TraceNodes::All,
            )
            .expect("profile"),
            codec: KeyCodec::new(KeyScope::Node, 1, 1),
            clock,
            window: Window::new(0, Duration::from_secs(1)),
            model: None,
            stats: KvStats::default(),
            node: 1,
        };
        let job = KvJob {
            intended: 0,
            dispatched: 0,
            task: KvTask::Action(0),
        };
        let future = Worker::once(&ctx, job, mpsc::channel().0);
        let size = size_of_val(&future);
        assert!(size <= 4096, "{size}");
    }
}
