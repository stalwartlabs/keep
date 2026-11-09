/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

mod model;
mod search;
mod segments;
#[cfg(test)]
mod tests;

use std::cmp::Reverse;
use std::thread;
use std::time::Duration;

use foldhash::{HashMap, HashMapExt};

use crate::history::{Op, Outcome, Record};

use model::{Action, Pins, ValueInfo};
use search::{Budget, Call, Search, Verdict};
use segments::{Segment, Windows};

pub use segments::{OwnerRule, Owners, OwnersError};

pub const DEFAULT_BUDGET: usize = 16_000_000;

pub const DEFAULT_SETTLE: Duration = Duration::from_millis(1_500);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum KvClaim {
    Linearizable,
    Failover { settle: Duration },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum KvFault {
    InventedValue,
    PresentBeforeWrite,
    ImpossibleCount,
    NotLinearizable { linearized: usize },
    Undecided { states: usize },
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct KvViolation {
    pub key: String,
    pub record: usize,
    pub fault: KvFault,
}

#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct KvReport {
    pub keys: usize,
    pub segments: usize,
    pub owners: OwnerRule,
    pub ops: usize,
    pub malformed: usize,
    pub violations: Vec<KvViolation>,
}

impl KvReport {
    pub fn undecided(&self) -> bool {
        self.violations
            .iter()
            .any(|violation| matches!(violation.fault, KvFault::Undecided { .. }))
    }
}

pub fn check_kv(
    history: &[Record],
    claim: KvClaim,
    budget: usize,
    owners: Option<&Owners>,
) -> KvReport {
    let mut keys: HashMap<&str, Vec<usize>> = HashMap::new();
    let mut malformed = 0;
    let windows = Windows::of(history, claim, &mut malformed);
    for (index, record) in history.iter().enumerate() {
        if !is_kv(record.op) {
            continue;
        }
        match &record.key {
            Some(key) => keys.entry(key.as_str()).or_default().push(index),
            None => malformed += 1,
        }
    }
    let used = owners.filter(|_| !windows.list_change);
    let mut plans: Vec<(Option<u32>, Vec<Segment>)> = vec![(None, windows.segments(None))];
    let mut unowned = 0;
    let mut keys: Vec<(&str, Vec<usize>, usize)> = keys
        .into_iter()
        .map(|(key, records)| {
            let owner = used.and_then(|owners| owners.get(key));
            if owner.is_none() {
                unowned += 1;
            }
            let plan = match plans.iter().position(|(of, _)| *of == owner) {
                Some(plan) => plan,
                None => {
                    plans.push((owner, windows.segments(owner)));
                    plans.len() - 1
                }
            };
            (key, records, plan)
        })
        .collect();
    keys.sort_unstable_by_key(|(key, records, _)| (Reverse(records.len()), *key));
    let threads = thread::available_parallelism()
        .map_or(1, |n| n.get())
        .min(keys.len().max(1));
    let mut report = KvReport {
        keys: keys.len(),
        segments: keys
            .iter()
            .filter_map(|(_, _, plan)| plans.get(*plan))
            .chain(plans.first().filter(|_| keys.is_empty()))
            .map(|(_, segments)| segments.len())
            .max()
            .unwrap_or(0),
        owners: match (owners, used) {
            (None, _) => OwnerRule::Everyone,
            (Some(_), None) => OwnerRule::ListChange,
            (Some(_), Some(_)) => OwnerRule::Owners { unowned },
        },
        malformed,
        ..KvReport::default()
    };
    let budget = Budget::new(budget);
    let parts: Vec<KvReport> = thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|worker| {
                let (keys, plans, budget) = (&keys, &plans, &budget);
                scope.spawn(move || {
                    let mut part = KvReport::default();
                    let mut buffer = Vec::new();
                    for (key, records, plan) in keys.iter().skip(worker).step_by(threads) {
                        let Some((_, segments)) = plans.get(*plan) else {
                            continue;
                        };
                        let key = Key {
                            history,
                            key,
                            records,
                        };
                        key.check(segments, budget, &mut buffer, &mut part);
                    }
                    part
                })
            })
            .collect();
        workers
            .into_iter()
            .filter_map(|worker| worker.join().ok())
            .collect()
    });
    for part in parts {
        report.ops += part.ops;
        report.malformed += part.malformed;
        report.violations.extend(part.violations);
    }
    report
        .violations
        .sort_unstable_by_key(|violation| violation.record);
    report
}

fn is_kv(op: Op) -> bool {
    matches!(
        op,
        Op::Get | Op::Exists | Op::Set | Op::SetNx | Op::Delete | Op::Incr
    )
}

struct Key<'a> {
    history: &'a [Record],
    key: &'a str,
    records: &'a [usize],
}

impl Key<'_> {
    fn check(
        &self,
        segments: &[Segment],
        budget: &Budget,
        buffer: &mut Vec<Call>,
        report: &mut KvReport,
    ) {
        let mut values = Values::default();
        let mut calls = Vec::with_capacity(self.records.len());
        for &index in self.records {
            let Some(record) = self.history.get(index) else {
                continue;
            };
            match Converted::of(index, record, &mut values) {
                Converted::Call(call) => calls.push(call),
                Converted::Skip => {}
                Converted::Malformed => report.malformed += 1,
            }
        }
        report.ops += calls.len();
        note_writes(&calls, &mut values.info);
        let before = report.violations.len();
        check_reads(self.key, &calls, &values.info, &mut report.violations);
        if report.violations.len() > before {
            return;
        }
        unify_unread(&mut calls, values.info.len());
        for segment in segments {
            segment.calls(&calls, buffer);
            if !buffer.iter().any(|call| call.required) {
                continue;
            }
            let pins = Pins {
                values: &values.info,
                since: segment.start,
            };
            let (record, fault) = match Search::new(buffer, pins).run(segment.from, budget) {
                Verdict::Linearizable => continue,
                Verdict::Violated { record, linearized } => {
                    (record, KvFault::NotLinearizable { linearized })
                }
                Verdict::Undecided { states } => (
                    buffer.first().map_or(0, |call| call.record),
                    KvFault::Undecided { states },
                ),
            };
            report.violations.push(KvViolation {
                key: self.key.to_owned(),
                record,
                fault,
            });
            return;
        }
    }
}

fn unify_unread(calls: &mut [Call], values: usize) {
    let mut read = vec![false; values];
    for call in calls.iter() {
        if let Action::Get(Some(value)) = call.action
            && let Some(seen) = read.get_mut(value as usize)
        {
            *seen = true;
        }
    }
    let canonical = |value: u32| {
        if read.get(value as usize).copied().unwrap_or(false) {
            value
        } else {
            UNREAD
        }
    };
    for call in calls.iter_mut() {
        call.action = match call.action {
            Action::Set(value) => Action::Set(canonical(value)),
            Action::SetNx { value, set } => Action::SetNx {
                value: canonical(value),
                set,
            },
            other => other,
        };
    }
}

const UNREAD: u32 = u32::MAX;

#[derive(Default)]
struct Values<'a> {
    ids: HashMap<&'a str, u32>,
    info: Vec<ValueInfo>,
}

impl<'a> Values<'a> {
    fn intern(&mut self, hex: &'a str) -> u32 {
        let next = u32::try_from(self.info.len()).unwrap_or(u32::MAX);
        let info = &mut self.info;
        *self.ids.entry(hex).or_insert_with(|| {
            info.push(ValueInfo {
                counter: counter_of(hex),
                first_write: u64::MAX,
            });
            next
        })
    }
}

fn counter_of(hex: &str) -> Option<i64> {
    let digits: &[u8; 16] = hex.as_bytes().try_into().ok()?;
    let (pairs, _) = digits.as_chunks::<2>();
    let mut bytes = [0u8; 8];
    for (byte, &[high, low]) in bytes.iter_mut().zip(pairs) {
        *byte = (nibble(high)? << 4) | nibble(low)?;
    }
    Some(i64::from_le_bytes(bytes))
}

fn nibble(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        b'A'..=b'F' => Some(digit - b'A' + 10),
        _ => None,
    }
}

enum Converted {
    Call(Call),
    Skip,
    Malformed,
}

impl Converted {
    fn of<'a>(index: usize, record: &'a Record, values: &mut Values<'a>) -> Converted {
        let Some(outcome) = record.outcome else {
            return Converted::Malformed;
        };
        if outcome.not_executed() {
            return Converted::Skip;
        }
        let known = outcome != Outcome::Unknown;
        let value = record.value.as_deref();
        let action = match (record.op, outcome) {
            (Op::Get | Op::Exists, Outcome::Unknown) => return Converted::Skip,
            (Op::Get, Outcome::Ok) => match value {
                Some(value) => Action::Get(Some(values.intern(value))),
                None => return Converted::Malformed,
            },
            (Op::Get, Outcome::NotFound) => Action::Get(None),
            (Op::Exists, Outcome::Ok) => Action::Exists(true),
            (Op::Exists, Outcome::NotFound) => Action::Exists(false),
            (Op::Set, Outcome::Ok | Outcome::Unknown) => match value {
                Some(value) => Action::Set(values.intern(value)),
                None => return Converted::Malformed,
            },
            (Op::SetNx, Outcome::Ok | Outcome::Conflict | Outcome::Unknown) => match value {
                Some(value) => Action::SetNx {
                    value: values.intern(value),
                    set: known.then_some(outcome == Outcome::Ok),
                },
                None => return Converted::Malformed,
            },
            (Op::Delete, Outcome::Ok | Outcome::NotFound | Outcome::Unknown) => {
                Action::Delete(known.then_some(outcome == Outcome::Ok))
            }
            (Op::Incr, Outcome::WrongType | Outcome::Overflow) => match record.delta {
                Some(delta) => Action::IncrRefused {
                    delta,
                    overflow: outcome == Outcome::Overflow,
                },
                None => return Converted::Malformed,
            },
            (Op::Incr, Outcome::Ok | Outcome::Unknown) => match (record.delta, record.count) {
                (Some(delta), count) if !known || count.is_some() => Action::Incr {
                    delta,
                    count: count.filter(|_| known),
                },
                _ => return Converted::Malformed,
            },
            _ => return Converted::Malformed,
        };
        let ret = match (known, record.reply_ns) {
            (true, Some(reply)) => reply,
            (true, None) => return Converted::Malformed,
            (false, _) => u64::MAX,
        };
        let Some(call) = record.send_ns.or(record.call_ns) else {
            return Converted::Malformed;
        };
        Converted::Call(Call {
            record: index,
            call,
            ret,
            action,
            required: known,
        })
    }
}

fn note_writes(calls: &[Call], values: &mut [ValueInfo]) {
    for call in calls {
        if let Some(info) = call
            .action
            .written()
            .and_then(|value| values.get_mut(value as usize))
        {
            info.first_write = info.first_write.min(call.call);
        }
    }
}

fn check_reads(key: &str, calls: &[Call], values: &[ValueInfo], out: &mut Vec<KvViolation>) {
    let mut first_create = u64::MAX;
    let mut increments: Vec<(u64, i64)> = Vec::new();
    for call in calls {
        if call.action.creates() {
            first_create = first_create.min(call.call);
        }
        if let Action::Incr { delta, .. } = call.action {
            increments.push((call.call, delta));
        }
    }
    increments.sort_unstable();
    let sums = Sums::new(&increments);
    for call in calls.iter().filter(|call| call.required) {
        let fault = match call.action {
            Action::Get(Some(value)) => {
                let info = values.get(value as usize);
                let written = info.is_some_and(|info| info.first_write <= call.ret);
                let counted = info
                    .and_then(|info| info.counter)
                    .is_some_and(|count| sums.reachable(call.ret, None, count));
                (!written && !counted).then_some(KvFault::InventedValue)
            }
            Action::Incr {
                delta,
                count: Some(count),
            } => (!sums.reachable(call.ret.max(call.call), Some(delta), count))
                .then_some(KvFault::ImpossibleCount),
            action if action.saw_present() && first_create > call.ret => {
                Some(KvFault::PresentBeforeWrite)
            }
            _ => None,
        };
        if let Some(fault) = fault {
            out.push(KvViolation {
                key: key.to_owned(),
                record: call.record,
                fault,
            });
        }
    }
}

struct Sums<'a> {
    sends: &'a [(u64, i64)],
    positive: Vec<i128>,
    negative: Vec<i128>,
}

impl<'a> Sums<'a> {
    fn new(sends: &'a [(u64, i64)]) -> Sums<'a> {
        let mut positive = Vec::with_capacity(sends.len() + 1);
        let mut negative = Vec::with_capacity(sends.len() + 1);
        let (mut up, mut down) = (0i128, 0i128);
        positive.push(up);
        negative.push(down);
        for &(_, delta) in sends {
            if delta > 0 {
                up += i128::from(delta);
            } else {
                down += i128::from(delta);
            }
            positive.push(up);
            negative.push(down);
        }
        Sums {
            sends,
            positive,
            negative,
        }
    }

    fn reachable(&self, before: u64, own: Option<i64>, count: i64) -> bool {
        let sent = self.sends.partition_point(|&(at, _)| at <= before);
        if sent == 0 {
            return false;
        }
        let (Some(&up), Some(&down)) = (self.positive.get(sent), self.negative.get(sent)) else {
            return false;
        };
        let count = i128::from(count);
        match own {
            None => (down..=up).contains(&count),
            Some(own) => {
                let own = i128::from(own);
                let (up, down) = if own > 0 {
                    (up - own, down)
                } else {
                    (up, down - own)
                };
                (own + down..=own + up).contains(&count)
            }
        }
    }
}
