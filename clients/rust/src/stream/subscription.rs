/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::future::poll_fn;
use std::net::SocketAddr;
use std::sync::Arc;

use keep_proto::Topic;

use super::inbox::{Kind, Record, Shared};

/// What a subscription delivers, in order per server.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Delivery<'a> {
    /// A published payload; `seq` counts the events of this subscription on
    /// `server`, from 1.
    Event {
        /// The server that delivered the event.
        server: SocketAddr,
        /// The event's sequence number on `server`.
        seq: u32,
        /// The published payload.
        payload: &'a [u8],
    },
    /// `missed` events from `server` were lost: a sequence jump, or a
    /// Heartbeat or End ahead of the last event. Run catch-up.
    Gap {
        /// The server whose events were lost.
        server: SocketAddr,
        /// How many events were lost.
        missed: u32,
    },
    /// The connection to `server` closed without End: events published there
    /// may be lost. Run catch-up; `Resubscribed` follows when it is back.
    Lost {
        /// The server whose connection closed.
        server: SocketAddr,
    },
    /// The subscription is live on `server` again, or for the first time after `subscribe`
    /// returned: events published there before this point may be missing, so run catch-up.
    Resubscribed {
        /// The server the subscription is live on.
        server: SocketAddr,
    },
    /// `server` ended the subscription cleanly. After a drain no event was lost; after an
    /// unsubscribe, events in flight from the server's other shards may be missing.
    Ended {
        /// The server that ended the subscription.
        server: SocketAddr,
    },
}

impl Delivery<'_> {
    /// Whether the caller must catch up from its source of truth before trusting later events.
    pub fn needs_catch_up(&self) -> bool {
        matches!(
            self,
            Delivery::Gap { .. } | Delivery::Lost { .. } | Delivery::Resubscribed { .. }
        )
    }
}

/// What `Subscription::next_with_heartbeats` yields: every delivery, and
/// every Heartbeat a server sent, in order per server.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Received<'a> {
    /// A delivery, as `Subscription::next` yields it.
    Delivery(Delivery<'a>),
    /// `server` has sent this subscription `seq` events so far. One ahead of
    /// the last event comes after the `Gap` it shows.
    Heartbeat {
        /// The server that sent the heartbeat.
        server: SocketAddr,
        /// Events `server` has sent this subscription so far.
        seq: u32,
    },
}

/// A topic subscription on every server the client's stream mode selects;
/// each server's part ends and resumes on its own. Dropping it
/// unsubscribes.
pub struct Subscription {
    shared: Arc<Shared>,
    records: Vec<Record>,
    payloads: Vec<u8>,
    next: usize,
    offset: usize,
    servers: Vec<SocketAddr>,
    last: Vec<u32>,
}

const RETAIN_FACTOR: usize = 4;
const RETAIN_FLOOR: usize = 64 << 10;

fn recycle<T>(buffer: &mut Vec<T>) {
    let keep = buffer
        .len()
        .saturating_mul(RETAIN_FACTOR)
        .max(RETAIN_FLOOR / size_of::<T>().max(1));
    buffer.clear();
    if buffer.capacity() > keep {
        buffer.shrink_to(keep);
    }
}

enum Step {
    Event {
        lane: usize,
        seq: u32,
        start: usize,
        end: usize,
    },
    Gap {
        lane: usize,
        missed: u32,
    },
    Lost(usize),
    Resubscribed(usize),
    Ended(usize),
}

enum Read {
    Step(Step),
    Heartbeat { lane: usize, seq: u32 },
}

impl Subscription {
    pub(crate) fn new(shared: Arc<Shared>) -> Subscription {
        Subscription {
            shared,
            records: Vec::new(),
            payloads: Vec::new(),
            next: 0,
            offset: 0,
            servers: Vec::new(),
            last: Vec::new(),
        }
    }

    /// The subscribed topic.
    pub fn topic(&self) -> Topic {
        self.shared.topic
    }

    /// Events this client dropped because the subscriber fell more than
    /// 16 MiB behind (they show as a `Gap`).
    pub fn dropped(&self) -> u64 {
        self.shared.dropped()
    }

    /// The next delivery; `None` once the client is gone.
    pub async fn next(&mut self) -> Option<Delivery<'_>> {
        let step = loop {
            if self.next == self.records.len() {
                if !self.refill().await {
                    return None;
                }
                continue;
            }
            if let Some(step) = self.step() {
                break step;
            }
        };
        Some(self.deliver(step))
    }

    /// As `next`, and also every Heartbeat, as history checkers need. Use it
    /// or `next` on a subscription, not both.
    pub async fn next_with_heartbeats(&mut self) -> Option<Received<'_>> {
        let read = loop {
            if self.next == self.records.len() {
                if !self.refill().await {
                    return None;
                }
                continue;
            }
            if let Some(read) = self.step_reporting() {
                break read;
            }
        };
        Some(match read {
            Read::Step(step) => Received::Delivery(self.deliver(step)),
            Read::Heartbeat { lane, seq } => Received::Heartbeat {
                server: self.server(lane),
                seq,
            },
        })
    }

    async fn refill(&mut self) -> bool {
        self.next = 0;
        self.offset = 0;
        recycle(&mut self.records);
        recycle(&mut self.payloads);
        let Subscription {
            shared,
            records,
            payloads,
            servers,
            ..
        } = self;
        if !poll_fn(|cx| shared.take(cx, records, payloads, servers)).await {
            return false;
        }
        self.last.resize(self.servers.len(), 0);
        true
    }

    fn step_reporting(&mut self) -> Option<Read> {
        let record = *self.records.get(self.next)?;
        if record.kind != Kind::Heartbeat {
            return self.step().map(Read::Step);
        }
        let lane = usize::from(record.lane);
        let end = usize::try_from(record.end).unwrap_or(usize::MAX);
        let Some(last) = self.last.get_mut(lane) else {
            self.next += 1;
            self.offset = end;
            return None;
        };
        if record.seq != *last {
            let missed = record.seq.wrapping_sub(*last);
            *last = record.seq;
            return Some(Read::Step(Step::Gap { lane, missed }));
        }
        self.next += 1;
        self.offset = end;
        Some(Read::Heartbeat {
            lane,
            seq: record.seq,
        })
    }

    fn step(&mut self) -> Option<Step> {
        let record = *self.records.get(self.next)?;
        let lane = usize::from(record.lane);
        let end = usize::try_from(record.end).unwrap_or(usize::MAX);
        let Some(last) = self.last.get_mut(lane) else {
            self.next += 1;
            self.offset = end;
            return None;
        };
        let advance = |this: &mut Subscription| {
            this.next += 1;
            this.offset = end;
        };
        let step = match record.kind {
            Kind::Event => {
                let expected = last.wrapping_add(1);
                if record.seq != expected {
                    *last = record.seq.wrapping_sub(1);
                    return Some(Step::Gap {
                        lane,
                        missed: record.seq.wrapping_sub(expected),
                    });
                }
                *last = record.seq;
                let start = self.offset;
                advance(self);
                Step::Event {
                    lane,
                    seq: record.seq,
                    start,
                    end,
                }
            }
            Kind::Heartbeat | Kind::End if record.seq != *last => {
                let missed = record.seq.wrapping_sub(*last);
                *last = record.seq;
                if record.kind == Kind::Heartbeat {
                    advance(self);
                }
                Step::Gap { lane, missed }
            }
            Kind::Heartbeat => {
                advance(self);
                return None;
            }
            Kind::End => {
                *last = 0;
                advance(self);
                Step::Ended(lane)
            }
            Kind::Lost => {
                advance(self);
                Step::Lost(lane)
            }
            Kind::Resubscribed => {
                *last = 0;
                advance(self);
                Step::Resubscribed(lane)
            }
        };
        Some(step)
    }

    fn server(&self, lane: usize) -> SocketAddr {
        self.servers
            .get(lane)
            .copied()
            .unwrap_or(SocketAddr::from(([0, 0, 0, 0], 0)))
    }

    fn deliver(&self, step: Step) -> Delivery<'_> {
        let server = |lane: usize| self.server(lane);
        match step {
            Step::Event {
                lane,
                seq,
                start,
                end,
            } => Delivery::Event {
                server: server(lane),
                seq,
                payload: self.payloads.get(start..end).unwrap_or_default(),
            },
            Step::Gap { lane, missed } => Delivery::Gap {
                server: server(lane),
                missed,
            },
            Step::Lost(lane) => Delivery::Lost {
                server: server(lane),
            },
            Step::Resubscribed(lane) => Delivery::Resubscribed {
                server: server(lane),
            },
            Step::Ended(lane) => Delivery::Ended {
                server: server(lane),
            },
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.shared.close();
        self.shared.signals.streams.notify_one();
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-client/stream/subscription.rs"]
mod tests;
