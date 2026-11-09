/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::collections::HashMap;
use std::time::Duration;

use foldhash::fast::RandomState;
use keep_proto::{Mark, ServerFrame, Topic};
use keep_rt::clock::Nanos;
use keep_rt::{ConnId, Conns, Io};

use super::records::Run;
use super::{COALESCE_BYTES, Counts, SUBSCRIBER_CAP};

#[derive(Debug)]
struct Sub {
    conn: ConnId,
    seq: u32,
    skipping: bool,
    gap: u16,
    last: Nanos,
}

const _: () = assert!(size_of::<Sub>() <= 24);

const GAP_UNIT_SHIFT: u32 = 10;

const GAP_UNKNOWN: u16 = u16::MAX;

const GAP_WEIGHT_SHIFT: u32 = 3;

impl Sub {
    #[inline]
    fn observe(&mut self, now: Nanos) -> u16 {
        if now > self.last {
            let sample =
                u16::try_from((now.raw() - self.last.raw()) >> GAP_UNIT_SHIFT).unwrap_or(u16::MAX);
            self.gap = self.gap - (self.gap >> GAP_WEIGHT_SHIFT) + (sample >> GAP_WEIGHT_SHIFT);
        }
        self.gap
    }

    #[inline]
    fn write<C>(&mut self, io: Io<'_, C>, run: &Run<'_>, now: Nanos, counts: &mut Counts) -> usize {
        if io.pending() > SUBSCRIBER_CAP {
            self.skip(io.out, run.topic, run.count, Over::Cap, now, counts);
            return io.pending();
        }
        self.seq = run.write(io.out, self.seq);
        self.skipping = false;
        self.last = now;
        counts.delivered += u64::from(run.count);
        io.pending()
    }

    #[cold]
    fn skip(
        &mut self,
        out: &mut Vec<u8>,
        topic: Topic,
        count: u32,
        over: Over,
        now: Nanos,
        counts: &mut Counts,
    ) {
        self.seq = self.seq.wrapping_add(count);
        counts.skipped += u64::from(count);
        if over == Over::Cap {
            if self.skipping {
                return;
            }
            self.skipping = true;
            counts.skip_runs += 1;
        }
        self.mark(out, topic, now);
    }

    fn mark(&mut self, out: &mut Vec<u8>, topic: Topic, now: Nanos) {
        ServerFrame::Heartbeat(self.last_mark(topic)).encode(out);
        self.last = now;
    }

    fn last_mark(&self, topic: Topic) -> Mark {
        Mark {
            last_seq: self.seq,
            topic,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Over {
    Cap,
    Under,
}

impl Over {
    fn of<C>(io: &Io<'_, C>) -> Over {
        if io.pending() > SUBSCRIBER_CAP {
            Over::Cap
        } else {
            Over::Under
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct Subscriptions {
    topics: HashMap<Topic, Vec<Sub>, RandomState>,
}

impl Subscriptions {
    pub(super) fn insert(&mut self, topic: Topic, conn: ConnId, now: Nanos) {
        self.topics.entry(topic).or_default().push(Sub {
            conn,
            seq: 0,
            skipping: false,
            gap: GAP_UNKNOWN,
            last: now,
        });
    }

    pub(super) fn remove(&mut self, topic: Topic, conn: ConnId) -> Option<Mark> {
        let subs = self.topics.get_mut(&topic)?;
        let at = subs.iter().position(|sub| sub.conn == conn)?;
        let sub = subs.swap_remove(at);
        if subs.is_empty() {
            self.topics.remove(&topic);
        }
        Some(sub.last_mark(topic))
    }

    #[inline]
    pub(super) fn deliver<C>(
        &mut self,
        run: &Run<'_>,
        conns: &mut Conns<'_, C>,
        now: Nanos,
        counts: &mut Counts,
        window: &mut Vec<ConnId>,
        span: Duration,
    ) {
        let Some(subs) = self.topics.get_mut(&run.topic) else {
            return;
        };
        let span = u16::try_from(span.as_nanos() >> GAP_UNIT_SHIFT).unwrap_or(u16::MAX);
        subs.retain_mut(|sub| {
            let conn = sub.conn;
            if span == 0 || sub.observe(now) >= span {
                let Some(io) = conns.get(conn) else {
                    return false;
                };
                sub.write(io, run, now, counts);
                return true;
            }
            let Some(io) = conns.get_unqueued(conn) else {
                return false;
            };
            let idle = io.pending() == 0;
            if sub.write(io, run, now, counts) >= COALESCE_BYTES {
                conns.get(conn);
            } else if idle && window.last() != Some(&conn) {
                window.push(conn);
            }
            true
        });
        if subs.is_empty() {
            self.topics.remove(&run.topic);
        }
    }

    #[cold]
    pub(super) fn skip<C>(
        &mut self,
        topic: Topic,
        count: u32,
        conns: &mut Conns<'_, C>,
        now: Nanos,
        counts: &mut Counts,
    ) {
        let Some(subs) = self.topics.get_mut(&topic) else {
            return;
        };
        subs.retain_mut(|sub| match conns.get(sub.conn) {
            Some(io) => {
                let over = Over::of(&io);
                sub.skip(io.out, topic, count, over, now, counts);
                true
            }
            None => false,
        });
        if subs.is_empty() {
            self.topics.remove(&topic);
        }
    }

    pub(super) fn beat<C>(
        &mut self,
        conns: &mut Conns<'_, C>,
        now: Nanos,
        interval: Duration,
        slack: Duration,
        slow: &mut u64,
    ) -> Option<Nanos> {
        let horizon = now.after(slack);
        let mut next: Option<Nanos> = None;
        self.topics.retain(|&topic, subs| {
            subs.retain_mut(|sub| {
                if sub.last.after(interval) <= horizon {
                    let Some(io) = conns.get(sub.conn) else {
                        return false;
                    };
                    if Over::of(&io) == Over::Cap {
                        sub.last = now;
                    } else {
                        sub.skipping = false;
                        sub.mark(io.out, topic, now);
                    }
                }
                *slow += u64::from(sub.skipping);
                let due = sub.last.after(interval);
                next = Some(next.map_or(due, |next| next.min(due)));
                true
            });
            !subs.is_empty()
        });
        next
    }

    pub(super) fn end_all<C>(&mut self, conns: &mut Conns<'_, C>) {
        for (topic, subs) in self.topics.drain() {
            for sub in subs {
                if let Some(io) = conns.get(sub.conn) {
                    ServerFrame::End(sub.last_mark(topic)).encode(io.out);
                }
            }
        }
    }
}
