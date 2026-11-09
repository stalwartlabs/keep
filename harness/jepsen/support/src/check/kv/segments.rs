/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fmt::{self, Write};

use foldhash::HashMap;

use crate::history::{FaultKind, Op, Record};

use super::KvClaim;
use super::model::State;
use super::search::Call;

#[derive(Clone, Default, Debug)]
pub struct Owners(HashMap<Box<str>, u32>);

impl Owners {
    pub fn insert(&mut self, key: &str, owner: u32) {
        self.0.insert(key.into(), owner);
    }

    pub fn get(&self, key: &str) -> Option<u32> {
        self.0.get(key).copied()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn write_tsv(&self, out: &mut String) {
        out.push_str("kind\tkey\tslot\towner\n");
        let mut rows: Vec<_> = self.0.iter().collect();
        rows.sort_unstable();
        for (key, owner) in rows {
            let _ = writeln!(out, "kv\t{key}\t-\t{owner}");
        }
    }

    pub fn parse(text: &str) -> Result<Owners, OwnersError> {
        let mut owners = Owners::default();
        for (index, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with("kind\t") {
                continue;
            }
            let error = OwnersError { line: index + 1 };
            let mut fields = line.split('\t');
            let (Some(_), Some(key), Some(_), Some(owner), None) = (
                fields.next(),
                fields.next(),
                fields.next(),
                fields.next(),
                fields.next(),
            ) else {
                return Err(error);
            };
            owners.insert(key, owner.parse().map_err(|_| error)?);
        }
        Ok(owners)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct OwnersError {
    pub line: usize,
}

impl fmt::Display for OwnersError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "line {}: expected kind, key, slot and owner separated by tabs",
            self.line
        )
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub enum OwnerRule {
    #[default]
    Everyone,
    ListChange,
    Owners {
        unowned: usize,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Scope {
    Every,
    Member(u32),
    Away,
}

#[derive(Clone, Copy, Debug)]
struct Window {
    start: u64,
    end: u64,
    scope: Scope,
}

#[derive(Clone, Copy, Debug)]
struct Absence {
    member: u32,
    from: u64,
    to: u64,
}

#[derive(Clone, Copy, Debug)]
struct Fault {
    kind: FaultKind,
    node: Option<u32>,
    start: u64,
    end: u64,
}

#[derive(Default, Debug)]
pub(super) struct Windows {
    failovers: Vec<Window>,
    absences: Vec<Absence>,
    pub(super) list_change: bool,
}

impl Windows {
    pub(super) fn of(history: &[Record], claim: KvClaim, malformed: &mut usize) -> Windows {
        let mut windows = Windows::default();
        let KvClaim::Failover { settle } = claim else {
            return windows;
        };
        let settle = u64::try_from(settle.as_nanos()).unwrap_or(u64::MAX);
        let mut faults = Vec::new();
        for record in history.iter().filter(|record| record.op == Op::Fault) {
            match (record.fault, record.send_ns, record.reply_ns) {
                (Some(kind), Some(start), end) if end.is_none_or(|end| end >= start) => {
                    faults.push(Fault {
                        kind,
                        node: record.node,
                        start,
                        end: end.unwrap_or(u64::MAX),
                    });
                }
                _ => *malformed += 1,
            }
        }
        windows.list_change = faults
            .iter()
            .any(|fault| matches!(fault.kind, FaultKind::MemberAdd | FaultKind::MemberRemove));
        for fault in &faults {
            let Some(member) = fault.node else {
                continue;
            };
            if fault.kind == FaultKind::RunOut
                || !(fault.kind.fails_over() || fault.kind.changes_membership())
            {
                continue;
            }
            windows.absences.push(Absence {
                member,
                from: fault.start,
                to: fault.end.saturating_add(settle),
            });
        }
        for fault in faults.iter().filter(|fault| fault.kind.fails_over()) {
            let scope = match fault.node {
                _ if fault.kind == FaultKind::RunOut => {
                    windows.run_out_scope(&faults, fault.start, settle)
                }
                Some(node) => Scope::Member(node),
                None => Scope::Every,
            };
            windows.failovers.push(Window {
                start: fault.start,
                end: fault.end.saturating_add(settle),
                scope,
            });
        }
        let failovers = windows.failovers.len();
        let changes: Vec<&Fault> = faults
            .iter()
            .filter(|fault| fault.kind.changes_membership())
            .collect();
        for (index, change) in changes.iter().enumerate() {
            let overlaps = |from: u64, to: u64| from < change.end && change.start < to;
            let crossed = windows
                .failovers
                .iter()
                .take(failovers)
                .any(|window| overlaps(window.start, window.end))
                || changes
                    .iter()
                    .enumerate()
                    .any(|(other, fault)| other != index && overlaps(fault.start, fault.end));
            if crossed {
                let scope = match change.node {
                    Some(node) if change.kind != FaultKind::Leave => Scope::Member(node),
                    _ => Scope::Every,
                };
                windows.failovers.push(Window {
                    start: change.start,
                    end: change.end.saturating_add(settle),
                    scope,
                });
            }
        }
        windows
            .failovers
            .sort_unstable_by_key(|window| (window.start, window.end));
        windows
    }

    fn run_out_scope(&self, faults: &[Fault], at: u64, settle: u64) -> Scope {
        let global = faults.iter().any(|fault| {
            fault.node.is_none()
                && fault.kind != FaultKind::RunOut
                && (fault.kind.fails_over() || fault.kind.changes_membership())
                && fault.start <= at
                && at < fault.end.saturating_add(settle)
        });
        let away = self
            .absences
            .iter()
            .any(|absence| absence.from <= at && at < absence.to);
        if global || !away {
            Scope::Every
        } else {
            Scope::Away
        }
    }

    fn away(&self, member: u32, at: u64) -> bool {
        self.absences
            .iter()
            .any(|absence| absence.member == member && absence.from <= at && at < absence.to)
    }

    pub(super) fn segments(&self, owner: Option<u32>) -> Vec<Segment> {
        let restarts = |window: &&Window| match (window.scope, owner) {
            (Scope::Every, _) | (_, None) => true,
            (Scope::Member(member), Some(owner)) => {
                member == owner || self.away(owner, window.start)
            }
            (Scope::Away, Some(owner)) => self.away(owner, window.start),
        };
        Segment::split(self.failovers.iter().filter(restarts))
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Segment {
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) from: State,
}

impl Segment {
    fn split<'a>(windows: impl Iterator<Item = &'a Window>) -> Vec<Segment> {
        let mut segments = vec![Segment {
            start: 0,
            end: u64::MAX,
            from: State::Absent,
        }];
        for window in windows {
            let Some(last) = segments.last_mut() else {
                break;
            };
            if window.start < last.start {
                last.start = last.start.max(window.end);
                continue;
            }
            last.end = window.start;
            segments.push(Segment {
                start: window.end,
                end: u64::MAX,
                from: State::Any,
            });
        }
        segments.retain(|segment| segment.start < segment.end);
        segments
    }

    pub(super) fn calls(self, calls: &[Call], out: &mut Vec<Call>) {
        out.clear();
        out.extend(calls.iter().filter_map(|call| {
            if call.call >= self.start && call.ret < self.end {
                return Some(*call);
            }
            let overlaps = call.call < self.end && call.ret >= self.start;
            (overlaps && !call.action.reads()).then_some(Call {
                call: call.call.max(self.start),
                ret: u64::MAX,
                required: false,
                ..*call
            })
        }));
    }
}
