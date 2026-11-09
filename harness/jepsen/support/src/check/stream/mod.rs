/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

#[cfg(test)]
mod tests;

use foldhash::{HashMap, HashMapExt, HashSet, HashSetExt};

use crate::history::{Op, Outcome, Record};

const BACKWARDS: u32 = 1 << 31;

const TAIL_MARGIN_NS: u64 = 1_000_000_000;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StreamFault {
    SequenceBack,
    Reordered { first: usize },
    Duplicate,
    Phantom,
    NotExecuted,
    Early,
    SilentLoss { publish: usize },
    AfterEnd,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct StreamViolation {
    pub record: usize,
    pub fault: StreamFault,
}

#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct StreamReport {
    pub subscriptions: usize,
    pub events: usize,
    pub confirmed: usize,
    pub unplaced: usize,
    pub violations: Vec<StreamViolation>,
}

pub fn check_stream(history: &[Record]) -> StreamReport {
    let publishes = Publishes::new(history);
    let mut report = StreamReport {
        confirmed: publishes.confirmed,
        unplaced: publishes.unplaced,
        ..StreamReport::default()
    };
    let mut covers = Vec::new();
    let mut groups: HashMap<(u32, u32), Vec<usize>> = HashMap::new();
    for (index, record) in history.iter().enumerate() {
        if is_subscriber(record.op)
            && let Some(topic) = record.topic
        {
            groups
                .entry((record.client, topic))
                .or_default()
                .push(index);
        }
    }
    let mut groups: Vec<((u32, u32), Vec<usize>)> = groups.into_iter().collect();
    groups.sort_unstable_by_key(|(group, _)| *group);
    for ((_, topic), records) in &groups {
        let mut subscriber = Subscriber {
            history,
            publishes: &publishes,
            topic: *topic,
            lanes: HashMap::new(),
            start: None,
            unsubscribed: u64::MAX,
            missing: Vec::new(),
            covers: &mut covers,
            report: &mut report,
        };
        for &index in records {
            subscriber.record(index);
        }
        subscriber.close_all();
    }
    unplaced_losses(history, &publishes, &covers, &mut report);
    report
        .violations
        .sort_unstable_by_key(|violation| violation.record);
    report
}

fn unplaced_losses(
    history: &[Record],
    publishes: &Publishes<'_>,
    covers: &[Cover<'_>],
    report: &mut StreamReport,
) {
    if publishes.unplaced_list.is_empty() {
        return;
    }
    let servers: HashSet<&str> = history
        .iter()
        .filter_map(|record| record.server.as_deref())
        .collect();
    let mut covering: HashSet<&str> = HashSet::new();
    for &(send, reply, publish) in &publishes.unplaced_list {
        covering.clear();
        let mut witness = None;
        for cover in covers
            .iter()
            .filter(|cover| cover.from < send && reply < cover.until)
        {
            covering.insert(cover.server);
            witness.get_or_insert(cover.witness);
        }
        if let Some(witness) = witness
            && covering.len() == servers.len()
        {
            report.violations.push(StreamViolation {
                record: witness,
                fault: StreamFault::SilentLoss { publish },
            });
        }
    }
}

struct Cover<'a> {
    server: &'a str,
    from: u64,
    until: u64,
    witness: usize,
}

fn is_subscriber(op: Op) -> bool {
    matches!(
        op,
        Op::Subscribe
            | Op::Unsubscribe
            | Op::Event
            | Op::Heartbeat
            | Op::End
            | Op::Gap
            | Op::Lost
            | Op::Resubscribed
            | Op::Ended
    )
}

#[derive(Clone, Copy)]
enum Carrier {
    One(usize),
    Several,
}

struct Publishes<'a> {
    by_payload: HashMap<(u32, &'a str), Carrier>,
    topics: HashSet<u32>,
    unplaced_list: Vec<(u64, u64, usize)>,
    confirmed_on: HashMap<&'a str, Vec<(u64, u64, usize)>>,
    confirmed: usize,
    unplaced: usize,
}

impl<'a> Publishes<'a> {
    fn new(history: &'a [Record]) -> Publishes<'a> {
        let mut by_payload: HashMap<(u32, &str), Carrier> = HashMap::new();
        let mut topics = HashSet::new();
        let mut servers: HashMap<usize, &str> = HashMap::new();
        for (index, record) in history.iter().enumerate() {
            let (Op::Publish, Some(topic), Some(payload)) =
                (record.op, record.topic, &record.value)
            else {
                continue;
            };
            topics.insert(topic);
            by_payload
                .entry((topic, payload.as_str()))
                .and_modify(|carrier| *carrier = Carrier::Several)
                .or_insert(Carrier::One(index));
            if let Some(server) = &record.server {
                servers.insert(index, server.as_str());
            }
        }
        for record in history.iter().filter(|record| record.op == Op::Event) {
            let (Some(topic), Some(payload), Some(server)) =
                (record.topic, &record.value, &record.server)
            else {
                continue;
            };
            if let Some(Carrier::One(publish)) = by_payload.get(&(topic, payload.as_str())) {
                servers.entry(*publish).or_insert(server.as_str());
            }
        }
        let mut confirmed_on: HashMap<&str, Vec<(u64, u64, usize)>> = HashMap::new();
        let mut confirmed = 0;
        let mut unplaced_list = Vec::new();
        for (index, record) in history.iter().enumerate() {
            let (Op::Publish, Some(Outcome::Ok), Some(send), Some(reply)) =
                (record.op, record.outcome, record.send_ns, record.reply_ns)
            else {
                continue;
            };
            confirmed += 1;
            match servers.get(&index) {
                Some(server) => confirmed_on
                    .entry(server)
                    .or_default()
                    .push((send, reply, index)),
                None => unplaced_list.push((send, reply, index)),
            }
        }
        for list in confirmed_on.values_mut() {
            list.sort_unstable();
        }
        Publishes {
            by_payload,
            topics,
            unplaced: unplaced_list.len(),
            unplaced_list,
            confirmed_on,
            confirmed,
        }
    }
}

#[derive(Clone, Copy)]
struct Segment {
    from: Option<u64>,
    until: u64,
    witness: usize,
}

impl Segment {
    fn starting(from: Option<u64>) -> Segment {
        Segment {
            from,
            until: 0,
            witness: 0,
        }
    }

    fn missing(&self, list: &[(u64, u64, usize)], seen: &HashSet<usize>, out: &mut Vec<usize>) {
        let Some(from) = self.from.filter(|&from| self.until > from) else {
            return;
        };
        let first = list.partition_point(|&(send, ..)| send <= from);
        let candidates = list.get(first..).unwrap_or_default();
        out.extend(
            candidates
                .iter()
                .take_while(|&&(send, ..)| send < self.until)
                .filter(|&&(_, reply, publish)| reply < self.until && !seen.contains(&publish))
                .map(|&(.., publish)| publish),
        );
    }
}

struct Life {
    seen: HashSet<usize>,
    latest: Option<(u64, usize)>,
    segment: Segment,
}

impl Life {
    fn new(start: u64) -> Life {
        Life {
            seen: HashSet::new(),
            latest: None,
            segment: Segment::starting(Some(start)),
        }
    }
}

enum Phase {
    Fresh,
    Live(Life),
    Lost,
    Ended,
}

struct Lane {
    last: u32,
    phase: Phase,
}

struct Subscriber<'a, 'r> {
    history: &'a [Record],
    publishes: &'r Publishes<'a>,
    topic: u32,
    lanes: HashMap<&'a str, Lane>,
    start: Option<u64>,
    unsubscribed: u64,
    missing: Vec<usize>,
    covers: &'r mut Vec<Cover<'a>>,
    report: &'r mut StreamReport,
}

impl<'a> Subscriber<'a, '_> {
    fn record(&mut self, index: usize) {
        let history = self.history;
        let Some(record) = history.get(index) else {
            return;
        };
        let at = record.reply_ns.or(record.send_ns).unwrap_or_default();
        match record.op {
            Op::Subscribe => {
                if record.outcome == Some(Outcome::Ok) {
                    self.close_all();
                    self.lanes.clear();
                    self.start = Some(at);
                    self.unsubscribed = u64::MAX;
                    self.report.subscriptions += 1;
                }
                return;
            }
            Op::Unsubscribe => {
                let send = record.send_ns.unwrap_or(at);
                self.unsubscribed = self.unsubscribed.min(send);
                self.tail(index, send.saturating_sub(TAIL_MARGIN_NS));
                return;
            }
            _ => {}
        }
        let (Some(server), Some(start)) = (record.server.as_deref(), self.start) else {
            return;
        };
        let lane = self.lanes.entry(server).or_insert(Lane {
            last: 0,
            phase: Phase::Fresh,
        });
        if matches!(lane.phase, Phase::Fresh) && record.op != Op::Resubscribed {
            lane.phase = Phase::Live(Life::new(start));
        }
        let barrier = at < self.unsubscribed;
        match record.op {
            Op::Event => self.event(server, index, record, at),
            Op::Gap => {
                let missed = record.missed.unwrap_or_default();
                if missed >= BACKWARDS {
                    self.fault(index, StreamFault::SequenceBack);
                }
                self.advance(server, missed);
            }
            Op::Heartbeat | Op::End => {
                let seq = record.seq.unwrap_or_default();
                let missed = seq.wrapping_sub(self.lane(server).last);
                if missed >= BACKWARDS {
                    self.fault(index, StreamFault::SequenceBack);
                } else if missed > 0 {
                    self.advance(server, missed);
                }
                if record.op == Op::End {
                    self.end(server, index, barrier, at);
                }
            }
            Op::Ended => self.end(server, index, barrier, at),
            Op::Lost => self.finish(server, Phase::Lost),
            Op::Resubscribed => {
                self.finish(server, Phase::Live(Life::new(at)));
                self.lane(server).last = 0;
            }
            _ => {}
        }
    }

    fn lane(&mut self, server: &'a str) -> &mut Lane {
        self.lanes.entry(server).or_insert(Lane {
            last: 0,
            phase: Phase::Fresh,
        })
    }

    fn fault(&mut self, record: usize, fault: StreamFault) {
        self.report
            .violations
            .push(StreamViolation { record, fault });
    }

    fn event(&mut self, server: &'a str, index: usize, record: &Record, at: u64) {
        self.report.events += 1;
        if matches!(self.lane(server).phase, Phase::Ended) {
            self.fault(index, StreamFault::AfterEnd);
        }
        let seq = record.seq.unwrap_or_default();
        let jump = seq.wrapping_sub(self.lane(server).last.wrapping_add(1));
        if jump >= BACKWARDS {
            self.fault(index, StreamFault::SequenceBack);
        } else if jump > 0 {
            self.advance(server, jump);
        }
        self.lane(server).last = seq;
        let carrier = record
            .value
            .as_deref()
            .and_then(|payload| self.publishes.by_payload.get(&(self.topic, payload)))
            .copied();
        let publish = match carrier {
            Some(Carrier::One(publish)) => publish,
            Some(Carrier::Several) => return,
            None => {
                if self.publishes.topics.contains(&self.topic) {
                    self.fault(index, StreamFault::Phantom);
                }
                return;
            }
        };
        let Some(published) = self.history.get(publish) else {
            return;
        };
        if published.outcome.is_some_and(Outcome::not_executed) {
            self.fault(index, StreamFault::NotExecuted);
        }
        let send = published.send_ns.unwrap_or_default();
        if at < send {
            self.fault(index, StreamFault::Early);
        }
        let confirmed = match (published.outcome, published.reply_ns) {
            (Some(Outcome::Ok), Some(reply)) => Some(reply),
            _ => None,
        };
        let Phase::Live(life) = &mut self.lane(server).phase else {
            return;
        };
        let duplicate = !life.seen.insert(publish);
        let reordered = match (duplicate, confirmed, life.latest) {
            (false, Some(reply), Some((latest, first))) if latest > reply => Some(first),
            _ => None,
        };
        if life.latest.is_none_or(|(latest, _)| send > latest) {
            life.latest = Some((send, index));
        }
        let segment = &mut life.segment;
        if segment.from.is_none() {
            segment.from = confirmed;
        }
        if segment.from.is_some() && send > segment.until {
            segment.until = send;
            segment.witness = index;
        }
        if duplicate {
            self.fault(index, StreamFault::Duplicate);
        }
        if let Some(first) = reordered {
            self.fault(index, StreamFault::Reordered { first });
        }
    }

    fn advance(&mut self, server: &'a str, missed: u32) {
        self.check(server);
        let lane = self.lane(server);
        lane.last = lane.last.wrapping_add(missed);
        if let Phase::Live(life) = &mut lane.phase {
            life.segment = Segment::starting(None);
        }
    }

    fn end(&mut self, server: &'a str, index: usize, barrier: bool, at: u64) {
        if barrier
            && let Phase::Live(life) = &mut self.lane(server).phase
            && life.segment.from.is_some()
            && at > life.segment.until
        {
            life.segment.until = at;
            life.segment.witness = index;
        }
        self.finish(server, Phase::Ended);
        self.lane(server).last = 0;
    }

    fn finish(&mut self, server: &'a str, next: Phase) {
        self.check(server);
        self.lane(server).phase = next;
    }

    fn tail(&mut self, index: usize, until: u64) {
        let servers: Vec<&'a str> = self.lanes.keys().copied().collect();
        for server in servers {
            if let Phase::Live(life) = &mut self.lane(server).phase
                && life.segment.from.is_some()
                && until > life.segment.until
            {
                life.segment.until = until;
                life.segment.witness = index;
                self.check(server);
                if let Phase::Live(life) = &mut self.lane(server).phase {
                    life.segment = Segment::starting(None);
                }
            }
        }
    }

    fn check(&mut self, server: &'a str) {
        let Some(Lane {
            phase: Phase::Live(life),
            ..
        }) = self.lanes.get(server)
        else {
            return;
        };
        if let Some(from) = life.segment.from
            && life.segment.until > from
        {
            self.covers.push(Cover {
                server,
                from,
                until: life.segment.until,
                witness: life.segment.witness,
            });
        }
        let Some(list) = self.publishes.confirmed_on.get(server) else {
            return;
        };
        let witness = life.segment.witness;
        life.segment.missing(list, &life.seen, &mut self.missing);
        for publish in self.missing.drain(..) {
            self.report.violations.push(StreamViolation {
                record: witness,
                fault: StreamFault::SilentLoss { publish },
            });
        }
    }

    fn close_all(&mut self) {
        let servers: Vec<&'a str> = self.lanes.keys().copied().collect();
        for server in servers {
            self.check(server);
        }
    }
}
