/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::collections::HashMap;

use super::input::RawEvent;

const STEAL_TICK_NS: u64 = 10_000_000;

#[derive(Clone, Copy, Debug)]
pub struct Span {
    pub start: u64,
    pub end: u64,
}

#[derive(Default)]
pub struct Spans {
    spans: Vec<Span>,
    longest: u64,
}

impl Spans {
    fn push(&mut self, span: Span) {
        self.longest = self.longest.max(span.end - span.start);
        self.spans.push(span);
    }

    fn sort(&mut self) {
        self.spans.sort_unstable_by_key(|span| span.start);
    }

    pub fn overlap(&self, from: u64, to: u64) -> u64 {
        let first = self
            .spans
            .partition_point(|span| span.start < from.saturating_sub(self.longest));
        let last = self.spans.partition_point(|span| span.start <= to);
        self.spans
            .get(first..last)
            .unwrap_or_default()
            .iter()
            .map(|span| span.end.min(to).saturating_sub(span.start.max(from)))
            .max()
            .unwrap_or(0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Loss {
    Retransmit,
    Timeout,
}

impl Loss {
    pub fn name(self) -> &'static str {
        match self {
            Loss::Retransmit => "retransmit",
            Loss::Timeout => "rto",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Retransmit {
    pub mono: u64,
    pub timeout: bool,
}

#[derive(Clone, Debug)]
pub struct Stage {
    pub mono: u64,
    pub queue_ns: u64,
    pub hop_ns: u64,
    pub exec_ns: u64,
    pub send_ns: u64,
    pub cause: Option<Box<str>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EventKind {
    Retransmit,
    Loss,
    Sched,
    Irq,
    Steal,
    Clock,
    Stage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Client,
    Server,
}

#[derive(Default)]
pub struct Host {
    offsets: Vec<i128>,
    retransmits: HashMap<(u16, u16), Vec<Retransmit>>,
    sched_by_tid: HashMap<i64, Spans>,
    sched_all: Spans,
    irq_by_cpu: HashMap<u32, Spans>,
    irq_all: Spans,
    steal_by_cpu: HashMap<u32, Spans>,
    steal_all: Spans,
    stages: HashMap<u16, Vec<Stage>>,
}

impl Host {
    pub fn from_events(events: Vec<RawEvent>, side: Side) -> Host {
        let mut host = Host::default();
        for event in events {
            host.add(event, side);
        }
        host.sort();
        host
    }

    fn add(&mut self, event: RawEvent, side: Side) {
        let Some(kind) = hashify::map!(event.ev.as_bytes(), EventKind,
            "rtx" => EventKind::Retransmit,
            "loss" => EventKind::Loss,
            "preempt" => EventKind::Sched,
            "runq" => EventKind::Sched,
            "softirq" => EventKind::Irq,
            "hardirq" => EventKind::Irq,
            "steal" => EventKind::Steal,
            "clock" => EventKind::Clock,
            "stage" => EventKind::Stage,
        )
        .copied() else {
            return;
        };
        let span = Span {
            start: event.mono,
            end: event.mono + event.dur_ns.unwrap_or(0),
        };
        match kind {
            EventKind::Retransmit | EventKind::Loss => {
                if let (Some(sport), Some(dport)) = (event.sport, event.dport) {
                    let key = match side {
                        Side::Client => (sport, dport),
                        Side::Server => (dport, sport),
                    };
                    self.retransmits.entry(key).or_default().push(Retransmit {
                        mono: event.mono,
                        timeout: kind == EventKind::Loss,
                    });
                }
            }
            EventKind::Sched => {
                if let Some(tid) = event.tid {
                    self.sched_by_tid.entry(tid).or_default().push(span);
                }
                self.sched_all.push(span);
            }
            EventKind::Irq => {
                if let Some(cpu) = event.cpu {
                    self.irq_by_cpu.entry(cpu).or_default().push(span);
                }
                self.irq_all.push(span);
            }
            EventKind::Steal => {
                let start = event.mono.saturating_sub(STEAL_TICK_NS);
                let span = Span {
                    start,
                    end: start + event.ticks.unwrap_or(1) * STEAL_TICK_NS,
                };
                if let Some(cpu) = event.cpu {
                    self.steal_by_cpu.entry(cpu).or_default().push(span);
                }
                self.steal_all.push(span);
            }
            EventKind::Clock => {
                if let Some(real) = event.real {
                    self.offsets.push(i128::from(real) - i128::from(event.mono));
                }
            }
            EventKind::Stage => {
                if let Some(port) = event.cport {
                    self.stages.entry(port).or_default().push(Stage {
                        mono: event.mono,
                        queue_ns: event.queue_ns.unwrap_or(0),
                        hop_ns: event.hop_ns.unwrap_or(0),
                        exec_ns: event.exec_ns.unwrap_or(0),
                        send_ns: event.send_ns.unwrap_or(0),
                        cause: event.cause.map(String::into_boxed_str),
                    });
                }
            }
        }
    }

    fn sort(&mut self) {
        self.offsets.sort_unstable();
        self.retransmits
            .values_mut()
            .for_each(|list| list.sort_unstable_by_key(|r| r.mono));
        self.stages
            .values_mut()
            .for_each(|list| list.sort_unstable_by_key(|s| s.mono));
        self.sched_by_tid.values_mut().for_each(Spans::sort);
        self.irq_by_cpu.values_mut().for_each(Spans::sort);
        self.steal_by_cpu.values_mut().for_each(Spans::sort);
        self.sched_all.sort();
        self.irq_all.sort();
        self.steal_all.sort();
    }

    pub fn offset(&self) -> Option<i128> {
        self.offsets.get(self.offsets.len() / 2).copied()
    }

    pub fn retransmit(&self, flow: (u16, u16), from: u64, to: u64) -> Option<Loss> {
        let list = self.retransmits.get(&flow)?;
        let first = list.partition_point(|r| r.mono < from);
        list.get(first..)?
            .iter()
            .take_while(|r| r.mono <= to)
            .map(|r| match r.timeout {
                true => Loss::Timeout,
                false => Loss::Retransmit,
            })
            .max()
    }

    pub fn sched(&self, tid: Option<i64>, from: u64, to: u64) -> u64 {
        match tid {
            Some(tid) => self
                .sched_by_tid
                .get(&tid)
                .map_or(0, |spans| spans.overlap(from, to)),
            None => self.sched_all.overlap(from, to),
        }
    }

    pub fn irq(&self, cpu: Option<u32>, from: u64, to: u64) -> u64 {
        match cpu {
            Some(cpu) => self
                .irq_by_cpu
                .get(&cpu)
                .map_or(0, |spans| spans.overlap(from, to)),
            None => self.irq_all.overlap(from, to),
        }
    }

    pub fn steal(&self, cpu: Option<u32>, from: u64, to: u64) -> u64 {
        match cpu {
            Some(cpu) => self
                .steal_by_cpu
                .get(&cpu)
                .map_or(0, |spans| spans.overlap(from, to)),
            None => self.steal_all.overlap(from, to),
        }
    }

    pub fn stage(&self, client_port: u16, from: u64, to: u64) -> Option<&Stage> {
        let list = self.stages.get(&client_port)?;
        let first = list.partition_point(|s| s.mono < from);
        list.get(first..)?
            .iter()
            .take_while(|s| s.mono <= to)
            .max_by_key(|s| s.queue_ns + s.hop_ns + s.exec_ns + s.send_ns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(json: &str) -> RawEvent {
        serde_json::from_str(json).expect("event")
    }

    // Spans report the longest overlap with a window, including spans that start before it.
    #[test]
    fn overlap_finds_long_spans_starting_early() {
        let mut spans = Spans::default();
        spans.push(Span {
            start: 0,
            end: 1_000,
        });
        spans.push(Span {
            start: 5_000,
            end: 5_010,
        });
        spans.sort();
        assert_eq!(spans.overlap(900, 2_000), 100);
        assert_eq!(spans.overlap(5_005, 6_000), 5);
        assert_eq!(spans.overlap(2_000, 4_000), 0);
    }

    // Server retransmits are keyed by the client's port first, like client ones.
    #[test]
    fn retransmits_match_flows_from_both_sides() {
        let client = Host::from_events(
            vec![event(
                r#"{"ev":"rtx","mono":100,"sport":40000,"dport":6379}"#,
            )],
            Side::Client,
        );
        let server = Host::from_events(
            vec![event(
                r#"{"ev":"loss","mono":200,"sport":6379,"dport":40000}"#,
            )],
            Side::Server,
        );
        assert_eq!(
            client.retransmit((40000, 6379), 0, 1_000),
            Some(Loss::Retransmit)
        );
        assert_eq!(
            server.retransmit((40000, 6379), 0, 1_000),
            Some(Loss::Timeout)
        );
        assert_eq!(server.retransmit((40001, 6379), 0, 1_000), None);
    }
}
