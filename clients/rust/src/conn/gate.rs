/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use keep_proto::Status;
use tokio::sync::Notify;
use tokio::sync::futures::Notified;

use super::Signals;
use crate::answer::Answer;
use crate::client::{BACKOFF_MAX, BACKOFF_START};
use crate::error::Error;
use crate::stats::Stats;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Gated {
    Open,
    Closed,
}

#[derive(Default)]
pub(crate) struct Gate {
    closed: AtomicBool,
    refusals: AtomicU64,
    state: Mutex<Option<Closed>>,
    notify: Notify,
    waits: AtomicU64,
    probes: AtomicU64,
}

struct Closed {
    retry_at: Instant,
    backoff: Duration,
    claimed: bool,
}

pub(crate) struct Waiting<F>(Pin<Box<F>>);

impl<F: Future> Future for Waiting<F> {
    type Output = F::Output;

    #[cold]
    #[inline(never)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

pub(crate) enum Pass<'a> {
    Retry,
    Probe(Claim<'a>),
    Expired,
}

pub(crate) struct Claim<'a> {
    gate: &'a Gate,
    refusals: u64,
    outcome: Outcome,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Outcome {
    Lost,
    Refused,
    Answered,
}

enum Step<'a> {
    Retry,
    Expired,
    Probe(Claim<'a>),
    Sleep(Instant),
}

impl Gate {
    #[inline]
    pub fn state(&self) -> Gated {
        match self.closed.load(Ordering::Relaxed) {
            true => Gated::Closed,
            false => Gated::Open,
        }
    }

    #[cold]
    pub fn trip(&self) {
        self.refusals.fetch_add(1, Ordering::Relaxed);
        let mut closed = self.lock();
        if closed.is_none() {
            *closed = Some(Closed {
                retry_at: Instant::now() + BACKOFF_START,
                backoff: BACKOFF_START,
                claimed: false,
            });
            self.closed.store(true, Ordering::Relaxed);
        }
    }

    pub fn wake(&self) {
        self.notify.notify_waiters();
    }

    #[cold]
    #[inline(never)]
    pub fn pass<'a>(
        &'a self,
        until: Instant,
        seen: u64,
        signals: &'a Signals,
    ) -> Waiting<impl Future<Output = Pass<'a>>> {
        Waiting(Box::pin(self.wait(until, seen, signals)))
    }

    async fn wait(&self, until: Instant, seen: u64, signals: &Signals) -> Pass<'_> {
        self.waits.fetch_add(1, Ordering::Relaxed);
        let mut claim = None;
        loop {
            let mut notified = pin!(self.notify.notified());
            let fresh = signals.views() == seen;
            let wake_at = match self.step(&mut claim, fresh, until, notified.as_mut()) {
                Step::Sleep(at) => at,
                Step::Retry => return Pass::Retry,
                Step::Expired => return Pass::Expired,
                Step::Probe(probe) => return Pass::Probe(probe),
            };
            tokio::select! {
                () = notified => {}
                () = tokio::time::sleep_until(wake_at.into()) => {}
            }
        }
    }

    fn step<'a>(
        &'a self,
        claim: &mut Option<Claim<'a>>,
        fresh: bool,
        until: Instant,
        notified: Pin<&mut Notified<'_>>,
    ) -> Step<'a> {
        let now = Instant::now();
        let mut state = self.lock();
        let Some(closed) = state.as_mut() else {
            return match now >= until {
                true => Step::Expired,
                false => Step::Retry,
            };
        };
        if now >= until {
            let unclaimed = claim.is_none() && !closed.claimed;
            drop(state);
            if unclaimed {
                self.notify.notify_one();
            }
            return Step::Expired;
        }
        if !fresh {
            return Step::Retry;
        }
        if claim.is_none() && !closed.claimed {
            closed.claimed = true;
            *claim = Some(Claim {
                gate: self,
                refusals: 0,
                outcome: Outcome::Lost,
            });
        }
        let wake_at = match claim {
            Some(_) if now >= closed.retry_at => {
                self.probes.fetch_add(1, Ordering::Relaxed);
                return claim.take().map_or(Step::Retry, |mut probe| {
                    probe.refusals = self.refusals.load(Ordering::Relaxed);
                    Step::Probe(probe)
                });
            }
            Some(_) => closed.retry_at.min(until),
            None => until,
        };
        notified.enable();
        Step::Sleep(wake_at)
    }

    fn release(&self, outcome: Outcome) {
        let mut state = self.lock();
        let Some(closed) = state.as_mut() else {
            return;
        };
        match outcome {
            Outcome::Lost => closed.claimed = false,
            Outcome::Refused => {
                closed.backoff = (closed.backoff * 2).min(BACKOFF_MAX);
                closed.retry_at = Instant::now() + closed.backoff;
                closed.claimed = false;
            }
            Outcome::Answered => {
                *state = None;
                self.closed.store(false, Ordering::Relaxed);
                drop(state);
                self.notify.notify_waiters();
                return;
            }
        }
        drop(state);
        self.notify.notify_one();
    }

    pub fn stats(&self) -> Stats {
        Stats {
            gate_waits: self.waits.load(Ordering::Relaxed),
            gate_probes: self.probes.load(Ordering::Relaxed),
            ..Stats::default()
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<Closed>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Claim<'_> {
    pub fn settle(mut self, result: &Result<Answer, Error>) {
        self.outcome = match result {
            Err(Error::Unavailable | Error::Unknown(_)) => Outcome::Lost,
            Err(Error::Server {
                status: Status::Busy,
                ..
            }) if self.gate.refusals.load(Ordering::Relaxed) != self.refusals => Outcome::Refused,
            _ => Outcome::Answered,
        };
    }
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        self.gate.release(self.outcome);
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-client/conn/gate.rs"]
mod tests;
