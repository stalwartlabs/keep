/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::mem;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use keep_proto::Topic;
use tokio::sync::Notify;

use crate::conn::Signals;

const INBOX_BUDGET: usize = 16 << 20;
const RECORD_COST: usize = size_of::<Record>();

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Kind {
    Event,
    Heartbeat,
    End,
    Lost,
    Resubscribed,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Record {
    pub seq: u32,
    pub end: u32,
    pub lane: u16,
    pub kind: Kind,
}

const _: () = assert!(size_of::<Record>() <= 12);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Life {
    Open,
    ClientGone,
    Dropped,
}

pub(crate) struct Shared {
    pub topic: Topic,
    inbox: Mutex<Inbox>,
    pub changed: Notify,
    pub signals: Arc<Signals>,
    pub dropped_events: AtomicU64,
}

pub(crate) struct Inbox {
    pub records: Vec<Record>,
    pub payloads: Vec<u8>,
    pub lanes: Vec<Lane>,
    waker: Option<Waker>,
    started: bool,
    life: Life,
}

pub(crate) struct Lane {
    pub server: SocketAddr,
    pub live: bool,
}

impl Shared {
    pub fn new(topic: Topic, signals: Arc<Signals>) -> Shared {
        Shared {
            topic,
            inbox: Mutex::new(Inbox {
                records: Vec::new(),
                payloads: Vec::new(),
                lanes: Vec::new(),
                waker: None,
                started: false,
                life: Life::Open,
            }),
            changed: Notify::new(),
            signals,
            dropped_events: AtomicU64::new(0),
        }
    }

    pub fn lock(&self) -> MutexGuard<'_, Inbox> {
        self.inbox.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn lane(&self, server: SocketAddr) -> Option<u16> {
        let mut inbox = self.lock();
        if let Some(index) = inbox.lanes.iter().position(|lane| lane.server == server) {
            return u16::try_from(index).ok();
        }
        let index = u16::try_from(inbox.lanes.len()).ok()?;
        inbox.lanes.push(Lane {
            server,
            live: false,
        });
        Some(index)
    }

    pub fn is_dropped(&self) -> bool {
        self.lock().life == Life::Dropped
    }

    pub fn is_open(&self) -> bool {
        self.lock().life == Life::Open
    }

    pub fn start(&self) -> usize {
        let mut inbox = self.lock();
        inbox.started = true;
        inbox.lanes.iter().filter(|lane| lane.live).count()
    }

    pub fn live_on(&self, servers: &[SocketAddr]) -> bool {
        let inbox = self.lock();
        let live = inbox
            .lanes
            .iter()
            .filter(|lane| lane.live && servers.binary_search(&lane.server).is_ok())
            .count();
        !servers.is_empty() && live == servers.len()
    }

    pub fn close(&self) {
        let mut inbox = self.lock();
        inbox.life = Life::Dropped;
        inbox.records = Vec::new();
        inbox.payloads = Vec::new();
        inbox.waker = None;
    }

    pub fn client_gone(&self) {
        let waker = {
            let mut inbox = self.lock();
            if inbox.life == Life::Open {
                inbox.life = Life::ClientGone;
            }
            inbox.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        self.changed.notify_waiters();
    }

    pub fn dropped(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    pub fn take(
        &self,
        cx: &mut Context<'_>,
        records: &mut Vec<Record>,
        payloads: &mut Vec<u8>,
        lanes: &mut Vec<SocketAddr>,
    ) -> Poll<bool> {
        let mut inbox = self.lock();
        if inbox.records.is_empty() {
            if inbox.life != Life::Open {
                return Poll::Ready(false);
            }
            match &inbox.waker {
                Some(waker) if waker.will_wake(cx.waker()) => {}
                _ => inbox.waker = Some(cx.waker().clone()),
            }
            return Poll::Pending;
        }
        records.clear();
        payloads.clear();
        mem::swap(&mut inbox.records, records);
        mem::swap(&mut inbox.payloads, payloads);
        if let Some(new) = inbox.lanes.get(lanes.len()..) {
            lanes.extend(new.iter().map(|lane| lane.server));
        }
        Poll::Ready(true)
    }
}

impl Inbox {
    fn used(&self) -> usize {
        self.payloads.len() + self.records.len() * RECORD_COST
    }

    fn push(&mut self, record: Record) -> Option<Waker> {
        self.records.push(record);
        self.waker.take()
    }

    pub fn event(&mut self, lane: u16, seq: u32, payload: &[u8], shared: &Shared) -> Option<Waker> {
        if self.life != Life::Open {
            return None;
        }
        let end = self.payloads.len() + payload.len();
        let end = match u32::try_from(end) {
            Ok(end) if self.used() + payload.len() + RECORD_COST <= INBOX_BUDGET => end,
            _ => {
                shared.dropped_events.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        self.payloads.extend_from_slice(payload);
        self.push(Record {
            seq,
            end,
            lane,
            kind: Kind::Event,
        })
    }

    pub fn mark(&mut self, lane: u16, kind: Kind, seq: u32) -> Option<Waker> {
        if self.life != Life::Open
            || (kind == Kind::Heartbeat && self.used() + RECORD_COST > INBOX_BUDGET)
        {
            return None;
        }
        let end = u32::try_from(self.payloads.len()).unwrap_or(u32::MAX);
        self.push(Record {
            seq,
            end,
            lane,
            kind,
        })
    }

    pub fn live(&mut self, lane: u16, again: bool) -> Option<Waker> {
        if let Some(entry) = self.lanes.get_mut(usize::from(lane)) {
            entry.live = true;
        }
        if again || self.started {
            self.mark(lane, Kind::Resubscribed, 0)
        } else {
            None
        }
    }

    pub fn down(&mut self, lane: u16) {
        if let Some(entry) = self.lanes.get_mut(usize::from(lane)) {
            entry.live = false;
        }
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-client/stream/inbox.rs"]
mod tests;
