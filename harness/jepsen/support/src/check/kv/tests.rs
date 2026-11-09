/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::model::{Action, Pins, State, ValueInfo};
use super::search::{Budget, Call, Search, Verdict};
use super::*;
use crate::history::FaultKind;

const FAILOVER: KvClaim = KvClaim::Failover {
    settle: DEFAULT_SETTLE,
};

const UNWRITTEN: u64 = u64::MAX;

const SINCE: u64 = 50;

const VALUES: [ValueInfo; 4] = [
    ValueInfo {
        counter: None,
        first_write: 0,
    },
    ValueInfo {
        counter: None,
        first_write: 0,
    },
    ValueInfo {
        counter: Some(5),
        first_write: 0,
    },
    ValueInfo {
        counter: Some(2),
        first_write: UNWRITTEN,
    },
];

fn pins(values: &[ValueInfo]) -> Pins<'_> {
    Pins {
        values,
        since: SINCE,
    }
}

struct Xor(u64);

impl Xor {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn reference(calls: &[Call], pins: Pins<'_>, start: State) -> bool {
    fn place(calls: &[Call], pins: Pins<'_>, used: &mut Vec<bool>, state: State) -> bool {
        let pending = calls
            .iter()
            .zip(used.iter())
            .any(|(call, &used)| call.required && !used);
        if !pending {
            return true;
        }
        for index in 0..calls.len() {
            if used[index] {
                continue;
            }
            let call = calls[index];
            let blocked = calls
                .iter()
                .zip(used.iter())
                .any(|(other, &used)| !used && other.ret < call.call);
            if blocked {
                continue;
            }
            let Some(next) = call.action.step(state, pins) else {
                continue;
            };
            used[index] = true;
            if place(calls, pins, used, next) {
                return true;
            }
            used[index] = false;
        }
        false
    }
    place(calls, pins, &mut vec![false; calls.len()], start)
}

fn random_action(rng: &mut Xor) -> Action {
    match rng.below(6) {
        0 => Action::Get(None),
        1 => Action::Exists(false),
        2 => Action::Set(rng.below(3) as u32),
        3 => Action::SetNx {
            value: rng.below(3) as u32,
            set: None,
        },
        4 => Action::Delete(None),
        _ => Action::Incr {
            delta: rng.below(3) as i64 - 1,
            count: None,
        },
    }
}

fn answered(action: Action, state: State) -> Action {
    match action {
        Action::Get(_) => match state {
            State::Absent => Action::Get(None),
            State::Value(value) => Action::Get(Some(value)),
            State::Counter(2) => Action::Get(Some(3)),
            State::Counter(_) => Action::Get(Some(0)),
            State::Present | State::Any => action,
        },
        Action::Exists(_) => Action::Exists(state != State::Absent),
        Action::SetNx { value, .. } => Action::SetNx {
            value,
            set: Some(state == State::Absent),
        },
        Action::Delete(_) => Action::Delete(Some(state != State::Absent)),
        Action::Incr { delta, .. } => match state {
            State::Absent => Action::Incr {
                delta,
                count: Some(delta),
            },
            State::Counter(count) => Action::Incr {
                delta,
                count: Some(count + delta),
            },
            State::Value(_) => Action::IncrRefused {
                delta,
                overflow: false,
            },
            State::Present | State::Any => action,
        },
        Action::Set(_) | Action::IncrRefused { .. } => action,
    }
}

fn history(rng: &mut Xor, len: usize, spread: u64, corrupt: bool, start: State) -> Vec<Call> {
    let mut state = start;
    let mut calls = Vec::with_capacity(len);
    for index in 0..len {
        let at = 100 + 10 * index as u64;
        let action = random_action(rng);
        let replied = answered(action, state);
        let required = matches!(replied, Action::Get(_) | Action::Exists(_)) || rng.below(5) != 0;
        let action = if required { replied } else { action };
        let Some(next) = action.step(state, pins(&VALUES)) else {
            continue;
        };
        if required || rng.below(2) == 0 {
            state = next;
        }
        let call = at - rng.below(spread);
        let ret = if required {
            at + rng.below(spread)
        } else {
            u64::MAX
        };
        calls.push(Call {
            record: index,
            call,
            ret,
            action,
            required,
        });
    }
    let required = calls.iter().filter(|call| call.required).count() as u64;
    let pick = rng.below(required.max(1)) as usize;
    if corrupt && let Some(victim) = calls.iter_mut().filter(|call| call.required).nth(pick) {
        victim.action = match victim.action {
            Action::Get(Some(value)) => Action::Get(Some((value + 1) % 3)),
            Action::Get(None) => Action::Get(Some(1 + rng.below(2) as u32)),
            Action::Exists(present) => Action::Exists(!present),
            Action::SetNx { value, set } => Action::SetNx {
                value,
                set: set.map(|set| !set),
            },
            Action::Delete(removed) => Action::Delete(removed.map(|removed| !removed)),
            Action::Incr { delta, count } => Action::Incr {
                delta,
                count: count.map(|count| count + 1),
            },
            Action::IncrRefused { delta, overflow } => Action::IncrRefused {
                delta,
                overflow: !overflow,
            },
            Action::Set(value) => Action::Set(value),
        };
    }
    calls
}

fn search(calls: &[Call], values: &[ValueInfo], start: State) -> Verdict {
    Search::new(calls, pins(values)).run(start, &Budget::new(DEFAULT_BUDGET))
}

fn written(calls: &[Call], before: u64) -> [ValueInfo; 4] {
    let mut values = VALUES;
    for (index, info) in (0..).zip(values.iter_mut()) {
        info.first_write = if before & (1 << index) != 0 {
            0
        } else {
            UNWRITTEN
        };
    }
    note_writes(calls, &mut values);
    values
}

fn checked(calls: &[Call]) -> bool {
    let mut violations = Vec::new();
    let values = written(calls, 0);
    check_reads("6b", calls, &values, &mut violations);
    let searched = search(calls, &values, State::Absent);
    let reads_hold = violations.is_empty();
    assert!(
        reads_hold || searched != Verdict::Linearizable,
        "the read checks flagged a linearizable history: {calls:?} {violations:?}"
    );
    reads_hold && searched == Verdict::Linearizable
}

#[test]
fn the_search_matches_the_exhaustive_reference() {
    // Random small histories, linearizable by construction or with one reply changed, decide as every-order enumeration does.
    let mut rng = Xor(0x2545_f491_4f6c_dd1d);
    let (mut held, mut broken) = (0, 0);
    for round in 0..6_000 {
        let len = 2 + rng.below(6) as usize;
        let spread = 1 + rng.below(25);
        let calls = history(&mut rng, len, spread, round % 2 == 1, State::Absent);
        let expected = reference(&calls, pins(&VALUES), State::Absent);
        assert_eq!(checked(&calls), expected, "{calls:?}");
        if expected {
            held += 1;
        } else {
            broken += 1;
        }
    }
    assert!(
        held > 1_000 && broken > 1_000,
        "{held} held, {broken} broken"
    );
}

#[test]
fn a_search_from_any_matches_every_concrete_start() {
    // From Any, a history is linearizable when some concrete start a failover could leave makes it so (values written before the segment; counters, unless a write wrote their bytes and a read returns them); only a refused Incr may widen the verdict.
    let mut rng = Xor(0x9e37_79b9_7f4a_7c15);
    let (mut held, mut broken, mut widened) = (0, 0, 0);
    for round in 0..6_000 {
        let before = rng.below(8);
        let left = (0..3u32).filter(|value| before & (1 << value) != 0);
        let drawn: Vec<State> = [State::Absent]
            .into_iter()
            .chain(left.clone().map(State::Value))
            .chain((-4..=4).map(State::Counter))
            .collect();
        let len = 2 + rng.below(6) as usize;
        let spread = 1 + rng.below(25);
        let start = drawn
            .get(rng.below(drawn.len() as u64) as usize)
            .copied()
            .unwrap_or(State::Absent);
        let calls = history(&mut rng, len, spread, round % 2 == 1, start);
        let values = written(&calls, before);
        let shadowed = |count: i64| {
            (0..).zip(&values).any(|(value, info)| {
                info.first_write != UNWRITTEN
                    && info.counter == Some(count)
                    && calls
                        .iter()
                        .any(|call| call.action == Action::Get(Some(value)))
            })
        };
        let expected = [State::Absent]
            .into_iter()
            .chain(left.map(State::Value))
            .chain(
                (-30..=30)
                    .filter(|&count| !shadowed(count))
                    .map(State::Counter),
            )
            .any(|from| reference(&calls, pins(&values), from));
        let found = search(&calls, &values, State::Any) == Verdict::Linearizable;
        assert!(
            found || !expected,
            "a linearizable history was rejected: {calls:?}"
        );
        if found && !expected {
            assert!(
                calls
                    .iter()
                    .any(|call| matches!(call.action, Action::IncrRefused { .. })),
                "widened without a refused Incr: {calls:?}"
            );
            widened += 1;
        } else if found {
            held += 1;
        } else {
            broken += 1;
        }
    }
    assert!(
        held > 1_000 && broken > 1_000 && widened < 200,
        "{held} held, {broken} broken, {widened} widened"
    );
}

fn record(client: u32, op: Op, send: u64, reply: u64, outcome: Outcome) -> Record {
    let mut record = Record::new(client, op);
    record.key = Some("6b".to_owned());
    record.send_ns = Some(send);
    record.reply_ns = (outcome != Outcome::Unknown).then_some(reply);
    record.outcome = Some(outcome);
    record
}

fn with_value(mut record: Record, value: &str) -> Record {
    record.value = Some(value.to_owned());
    record
}

fn incr(client: u32, send: u64, reply: u64, delta: i64, count: Option<i64>) -> Record {
    let outcome = if count.is_some() {
        Outcome::Ok
    } else {
        Outcome::Unknown
    };
    let mut record = record(client, Op::Incr, send, reply, outcome);
    record.delta = Some(delta);
    record.count = count;
    record
}

fn faults(history: &[Record], claim: KvClaim) -> Vec<(usize, KvFault)> {
    check_kv(history, claim, DEFAULT_BUDGET, None)
        .violations
        .into_iter()
        .map(|violation| (violation.record, violation.fault))
        .collect()
}

#[test]
fn a_stale_read_after_an_acknowledged_write_is_not_linearizable() {
    // Set a, then Set b, then a Get that starts after b's reply still sees a.
    let history = [
        with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
        with_value(record(1, Op::Set, 20, 30, Outcome::Ok), "bb"),
        with_value(record(2, Op::Get, 40, 50, Outcome::Ok), "aa"),
    ];
    assert!(matches!(
        faults(&history, KvClaim::Linearizable).as_slice(),
        [(2, KvFault::NotLinearizable { .. })]
    ));
    assert!(!faults(&history, FAILOVER).is_empty());
}

const SETTLED: u64 = 1_600_000_000;

fn window(kind: FaultKind, start: u64, end: u64) -> Record {
    let mut record = Record::new(0, Op::Fault);
    record.fault = Some(kind);
    record.send_ns = Some(start);
    record.reply_ns = Some(end);
    record
}

#[test]
fn a_failover_window_allows_a_stale_read_after_it() {
    // A kill between Set b and the Get lets the Get see a: K3 allows it, K1 does not.
    let history = [
        with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
        with_value(record(1, Op::Set, 20, 30, Outcome::Ok), "bb"),
        window(FaultKind::Kill, 40, 50),
        with_value(
            record(2, Op::Get, 50 + SETTLED, 60 + SETTLED, Outcome::Ok),
            "aa",
        ),
    ];
    assert!(!faults(&history, KvClaim::Linearizable).is_empty());
    assert!(faults(&history, FAILOVER).is_empty());
    assert_eq!(
        check_kv(&history, FAILOVER, DEFAULT_BUDGET, None).segments,
        2
    );
}

#[test]
fn a_graceful_window_does_not_split_the_history() {
    // A leave hands slots over, so the stale read after it still breaks the claim.
    let history = [
        with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
        with_value(record(1, Op::Set, 20, 30, Outcome::Ok), "bb"),
        window(FaultKind::Leave, 40, 50),
        with_value(
            record(2, Op::Get, 50 + SETTLED, 60 + SETTLED, Outcome::Ok),
            "aa",
        ),
    ];
    assert!(matches!(
        faults(&history, FAILOVER).as_slice(),
        [(3, KvFault::NotLinearizable { .. })]
    ));
}

#[test]
fn a_read_before_the_settle_time_is_not_searched() {
    // A Get 1 s after a heal may still meet the cut-off owner, so only the read checks apply to it.
    let history = [
        with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
        with_value(record(1, Op::Set, 20, 30, Outcome::Ok), "bb"),
        window(FaultKind::Isolate, 40, 50),
        with_value(
            record(2, Op::Get, 1_000_000_000, 1_000_000_010, Outcome::Ok),
            "aa",
        ),
        with_value(
            record(2, Op::Get, 50 + SETTLED, 60 + SETTLED, Outcome::Ok),
            "bb",
        ),
    ];
    assert!(faults(&history, FAILOVER).is_empty());
}

#[test]
fn inside_a_segment_after_a_failover_reads_stay_linearizable() {
    // After the window the key may hold a or b, but once c is acknowledged a read of b is stale.
    let history = [
        with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
        with_value(record(1, Op::Set, 20, 30, Outcome::Ok), "bb"),
        window(FaultKind::Pause, 40, 50),
        with_value(record(2, Op::Get, SETTLED, SETTLED + 10, Outcome::Ok), "aa"),
        with_value(
            record(1, Op::Set, SETTLED + 20, SETTLED + 30, Outcome::Ok),
            "cc",
        ),
        with_value(
            record(2, Op::Get, SETTLED + 40, SETTLED + 50, Outcome::Ok),
            "bb",
        ),
    ];
    assert!(matches!(
        faults(&history, FAILOVER).as_slice(),
        [(5, KvFault::NotLinearizable { .. })]
    ));
}

#[test]
fn a_write_across_a_window_edge_may_land_on_either_side() {
    // Set b is sent before a split and answered after it settled; reads around it may see a, then b.
    let history = [
        with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
        with_value(record(1, Op::Set, 20, SETTLED + 100, Outcome::Ok), "bb"),
        with_value(record(2, Op::Get, 25, 35, Outcome::Ok), "aa"),
        window(FaultKind::Split, 40, 50),
        with_value(
            record(2, Op::Get, SETTLED + 10, SETTLED + 20, Outcome::Ok),
            "aa",
        ),
        with_value(
            record(2, Op::Get, SETTLED + 200, SETTLED + 210, Outcome::Ok),
            "bb",
        ),
        with_value(
            record(2, Op::Get, SETTLED + 300, SETTLED + 310, Outcome::Ok),
            "bb",
        ),
    ];
    assert!(faults(&history, FAILOVER).is_empty());
    let mut flipped = history.to_vec();
    flipped.push(with_value(
        record(3, Op::Get, SETTLED + 400, SETTLED + 410, Outcome::Ok),
        "aa",
    ));
    assert!(!faults(&flipped, FAILOVER).is_empty());
}

#[test]
fn a_counter_may_restart_at_a_failover_but_not_inside_a_segment() {
    // Counts 1, 2, 3, then a kill: the count may restart at 1, but may not fall back again after that.
    let mut history = vec![
        incr(1, 0, 10, 1, Some(1)),
        incr(1, 20, 30, 1, Some(2)),
        incr(1, 40, 50, 1, Some(3)),
        window(FaultKind::Kill, 60, 70),
        incr(1, SETTLED + 100, SETTLED + 110, 1, Some(1)),
        incr(1, SETTLED + 120, SETTLED + 130, 1, Some(2)),
    ];
    assert!(faults(&history, FAILOVER).is_empty());
    history.push(incr(1, SETTLED + 140, SETTLED + 150, 1, Some(2)));
    assert!(matches!(
        faults(&history, FAILOVER).as_slice(),
        [(6, KvFault::NotLinearizable { .. })]
    ));
}

#[test]
fn overlapping_windows_merge_and_malformed_fault_records_count() {
    // Two overlapping cuts form one window; a fault without a kind and one ending before it starts are malformed.
    let mut no_kind = window(FaultKind::Kill, 5, 6);
    no_kind.fault = None;
    let history = [
        window(FaultKind::Isolate, 100, 200),
        window(FaultKind::Netem, 150, 2_000_000_000),
        window(FaultKind::Clients, 3_000_000_000, 3_000_000_100),
        window(FaultKind::Kill, 9_000_000_000, 8_000_000_000),
        no_kind,
        with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
    ];
    let report = check_kv(&history, FAILOVER, DEFAULT_BUDGET, None);
    assert_eq!(report.segments, 2);
    assert_eq!(report.malformed, 2);
}

#[test]
fn an_unknown_write_may_land_late_or_never() {
    // An unknown Set of b is read long after; a second key's unknown Set is never read.
    let mut history = vec![
        with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
        with_value(record(2, Op::Set, 5, 0, Outcome::Unknown), "bb"),
        with_value(record(1, Op::Get, 20, 30, Outcome::Ok), "aa"),
        with_value(record(1, Op::Get, 1_000, 1_010, Outcome::Ok), "bb"),
    ];
    assert!(faults(&history, KvClaim::Linearizable).is_empty());
    history.push(with_value(
        record(1, Op::Get, 2_000, 2_010, Outcome::Ok),
        "aa",
    ));
    assert!(matches!(
        faults(&history, KvClaim::Linearizable).as_slice(),
        [(4, KvFault::NotLinearizable { .. })]
    ));
}

#[test]
fn reads_of_values_nobody_wrote_are_invented_under_every_claim() {
    // A Get returns cc, which no write carried; a SetNx conflicts before any write was sent.
    let history = [
        with_value(record(1, Op::SetNx, 0, 10, Outcome::Conflict), "aa"),
        with_value(record(1, Op::Set, 20, 30, Outcome::Ok), "bb"),
        with_value(record(2, Op::Get, 40, 50, Outcome::Ok), "cc"),
    ];
    let expected = vec![
        (0, KvFault::PresentBeforeWrite),
        (2, KvFault::InventedValue),
    ];
    assert_eq!(faults(&history, FAILOVER), expected);
    assert_eq!(faults(&history, KvClaim::Linearizable), expected);
}

#[test]
fn counters_count_each_increment_once() {
    // Two increments of 1 and a Get of the counter's 8 bytes; then a count that skips one.
    let two = "0200000000000000";
    let history = vec![
        incr(1, 0, 10, 1, Some(1)),
        incr(2, 5, 15, 1, Some(2)),
        with_value(record(3, Op::Get, 20, 30, Outcome::Ok), two),
        incr(4, 40, 50, 1, None),
        incr(1, 60, 70, 1, Some(4)),
    ];
    assert!(faults(&history, KvClaim::Linearizable).is_empty());
    let mut doubled = history.clone();
    doubled.push(incr(2, 80, 90, 1, Some(4)));
    assert!(matches!(
        faults(&doubled, KvClaim::Linearizable).as_slice(),
        [(5, KvFault::NotLinearizable { .. })]
    ));
    let mut impossible = history;
    impossible.push(incr(2, 80, 90, 1, Some(9)));
    assert_eq!(
        faults(&impossible, FAILOVER),
        vec![(5, KvFault::ImpossibleCount)]
    );
}

#[test]
fn not_executed_attempts_are_ignored_and_malformed_records_counted() {
    // A Busy Set of b and a Moved Get never count; a Set without a value is malformed.
    let history = [
        with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
        with_value(record(2, Op::Set, 20, 30, Outcome::Busy), "bb"),
        with_value(record(2, Op::Get, 20, 30, Outcome::Moved), "bb"),
        record(2, Op::Set, 40, 50, Outcome::Ok),
        with_value(record(1, Op::Get, 60, 70, Outcome::Ok), "aa"),
    ];
    let report = check_kv(&history, KvClaim::Linearizable, DEFAULT_BUDGET, None);
    assert!(report.violations.is_empty(), "{report:?}");
    assert_eq!((report.keys, report.ops, report.malformed), (1, 2, 1));
}

#[test]
fn a_long_concurrent_history_decides_quickly() {
    // 8 clients, 20,000 operations on one key with overlapping intervals and 1% unknown writes: linearizable, found without backtracking far.
    let mut rng = Xor(0x9e37_79b9_7f4a_7c15);
    let mut state = State::Absent;
    let mut calls = Vec::new();
    for index in 0..20_000u64 {
        let action = random_action(&mut rng);
        let replied = answered(action, state);
        let unknown = !matches!(replied, Action::Get(_) | Action::Exists(_)) && rng.below(100) == 0;
        let action = if unknown { action } else { replied };
        let Some(next) = action.step(state, pins(&VALUES)) else {
            continue;
        };
        if !unknown || rng.below(2) == 0 {
            state = next;
        }
        let at = 1_000 + 100 * index;
        calls.push(Call {
            record: index as usize,
            call: at - rng.below(400),
            ret: if unknown {
                u64::MAX
            } else {
                at + rng.below(400)
            },
            action,
            required: !unknown,
        });
    }
    assert_eq!(
        search(&calls, &VALUES, State::Absent),
        Verdict::Linearizable
    );
}

#[test]
fn lost_acknowledged_write_without_any_failover_fails_k1() {
    // b is acknowledged, then every later read sees a: a lost write; under K1 it must fail.
    let history = [
        with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
        with_value(record(2, Op::Get, 15, 18, Outcome::Ok), "aa"),
        with_value(record(1, Op::Set, 20, 30, Outcome::Ok), "bb"),
        with_value(record(2, Op::Get, 40, 50, Outcome::Ok), "aa"),
        with_value(record(3, Op::Get, 60, 70, Outcome::Ok), "aa"),
    ];
    assert!(!faults(&history, KvClaim::Linearizable).is_empty());
}

#[test]
fn failover_mode_checks_linearizability_between_failovers() {
    // DESIGN 1.6 (K3): segments between failovers must be linearizable; with no failover at all a stale read must fail.
    let history = [
        with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
        with_value(record(1, Op::Set, 20, 30, Outcome::Ok), "bb"),
        with_value(record(2, Op::Get, 40, 50, Outcome::Ok), "aa"),
    ];
    assert!(!faults(&history, FAILOVER).is_empty());
}

#[test]
fn failover_mode_catches_a_counter_that_falls_back_without_a_failover() {
    // One client increments 1, 2, 3, then reads 1 from the same server with no failover in the history.
    let history = [
        incr(1, 0, 10, 1, Some(1)),
        incr(1, 20, 30, 1, Some(2)),
        incr(1, 40, 50, 1, Some(3)),
        incr(1, 60, 70, 1, Some(2)),
    ];
    assert!(!faults(&history, FAILOVER).is_empty());
}

#[test]
fn a_known_outcome_without_a_reply_time_is_malformed() {
    // An Ok Set with no reply_ns becomes a call that may land at any time; it must be counted malformed, not checked loosely.
    let mut late = with_value(record(1, Op::Set, 20, 30, Outcome::Ok), "bb");
    late.reply_ns = None;
    let history = [
        with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
        late,
        with_value(record(2, Op::Get, 40, 50, Outcome::Ok), "aa"),
    ];
    let report = check_kv(&history, KvClaim::Linearizable, DEFAULT_BUDGET, None);
    assert!(
        report.malformed > 0 || !report.violations.is_empty(),
        "{report:?}"
    );
}

#[test]
fn a_request_without_send_or_call_time_is_malformed() {
    // A Get with no send_ns and no call_ns is placed at time 0, so a read that predates the write it saw passes.
    let mut get = with_value(record(2, Op::Get, 0, 5, Outcome::Ok), "bb");
    get.send_ns = None;
    let history = [
        with_value(record(1, Op::Set, 1, 10, Outcome::Ok), "bb"),
        get,
    ];
    let report = check_kv(&history, KvClaim::Linearizable, DEFAULT_BUDGET, None);
    assert!(
        report.malformed > 0 || !report.violations.is_empty(),
        "{report:?}"
    );
}

#[test]
fn unknown_writes_do_not_hide_a_stale_read_behind_the_budget() {
    // 60 unknown-outcome Sets of unread values on the key, then a stale read: must be NotLinearizable, not Undecided.
    let mut history = vec![
        with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
        with_value(record(1, Op::Set, 20, 30, Outcome::Ok), "bb"),
    ];
    for n in 0..60u64 {
        let value = format!("{:016x}", 0x1000 + n);
        history.push(with_value(
            record(3, Op::Set, 40 + 10 * n, 0, Outcome::Unknown),
            &value,
        ));
        history.push(with_value(
            record(2, Op::Get, 45 + 10 * n, 48 + 10 * n, Outcome::Ok),
            "bb",
        ));
    }
    history.push(with_value(
        record(2, Op::Get, 2_000, 2_010, Outcome::Ok),
        "aa",
    ));
    let report = check_kv(&history, KvClaim::Linearizable, 200_000, None);
    assert!(!report.undecided(), "{report:?}");
    assert!(!report.violations.is_empty(), "{report:?}");
}

#[test]
fn any_pins_only_values_written_before_the_segment() {
    // After a kill, w is written, then v; a read pinned to v before v's write would explain a later read of w, so v's loss inside the segment must fail.
    let base = 2 * SETTLED;
    let history = [
        window(FaultKind::Kill, 0, 10),
        with_value(record(2, Op::Get, base, base + 110, Outcome::Ok), "76"),
        with_value(
            record(1, Op::Set, base + 102, base + 104, Outcome::Ok),
            "77",
        ),
        with_value(
            record(3, Op::Set, base + 105, base + 200, Outcome::Ok),
            "76",
        ),
        with_value(
            record(4, Op::Get, base + 112, base + 113, Outcome::Ok),
            "77",
        ),
    ];
    assert!(!faults(&history, KvClaim::Linearizable).is_empty());
    assert!(!faults(&history, FAILOVER).is_empty());
}

#[test]
fn a_value_written_before_the_failover_may_be_read_first() {
    // v was written before the kill, so a failover may have left it: the read of v ahead of the segment's writes is legal.
    let base = 2 * SETTLED;
    let history = [
        with_value(record(1, Op::Set, 0, 5, Outcome::Ok), "76"),
        window(FaultKind::Kill, 10, 20),
        with_value(record(2, Op::Get, base, base + 110, Outcome::Ok), "76"),
        with_value(
            record(1, Op::Set, base + 102, base + 104, Outcome::Ok),
            "77",
        ),
        with_value(
            record(4, Op::Get, base + 112, base + 113, Outcome::Ok),
            "77",
        ),
    ];
    assert!(faults(&history, FAILOVER).is_empty());
}

#[test]
fn a_present_key_pins_only_values_written_before_the_segment() {
    // Exists widens Any to Present, which may hold u from before the kill but must not pin v ahead of v's own write.
    let base = 2 * SETTLED;
    let history = [
        with_value(record(1, Op::Set, 0, 5, Outcome::Ok), "75"),
        window(FaultKind::Kill, 10, 20),
        record(5, Op::Exists, base, base + 1, Outcome::Ok),
        with_value(record(2, Op::Get, base + 2, base + 110, Outcome::Ok), "76"),
        with_value(
            record(1, Op::Set, base + 102, base + 104, Outcome::Ok),
            "77",
        ),
        with_value(
            record(3, Op::Set, base + 105, base + 200, Outcome::Ok),
            "76",
        ),
        with_value(
            record(4, Op::Get, base + 112, base + 113, Outcome::Ok),
            "77",
        ),
    ];
    assert!(!faults(&history, FAILOVER).is_empty());
}

#[test]
fn overlapping_membership_changes_are_checked_as_failovers() {
    // Two leaves in flight at once are under K3: the stale read after them passes, while after one leave it still fails.
    let history = |second: Option<Record>| {
        let mut history = vec![
            with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
            with_value(record(1, Op::Set, 20, 30, Outcome::Ok), "bb"),
            window(FaultKind::Leave, 40, 50),
        ];
        history.extend(second);
        history.push(with_value(
            record(2, Op::Get, 60 + SETTLED, 70 + SETTLED, Outcome::Ok),
            "aa",
        ));
        history
    };
    assert!(faults(&history(Some(window(FaultKind::Leave, 45, 60))), FAILOVER).is_empty());
    assert!(
        faults(
            &history(Some(window(FaultKind::WallRestart, 49, 55))),
            FAILOVER
        )
        .is_empty()
    );
    assert!(!faults(&history(Some(window(FaultKind::Leave, 50, 60))), FAILOVER).is_empty());
    assert!(!faults(&history(None), FAILOVER).is_empty());
    assert!(
        !faults(
            &history(Some(window(FaultKind::Leave, 45, 60))),
            KvClaim::Linearizable
        )
        .is_empty()
    );
}

#[test]
fn a_membership_change_inside_a_failover_window_is_a_failover() {
    // A leave that starts inside a kill's window and outlasts it is a second change in flight, so its whole window is a failover.
    let history = |leave_end: u64| {
        [
            with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
            window(FaultKind::Kill, 40, 50),
            window(FaultKind::Leave, 45, leave_end),
            with_value(
                record(1, Op::Set, 2 * SETTLED, 2 * SETTLED + 10, Outcome::Ok),
                "bb",
            ),
            with_value(
                record(2, Op::Get, 6 * SETTLED, 6 * SETTLED + 10, Outcome::Ok),
                "aa",
            ),
        ]
    };
    assert!(faults(&history(5 * SETTLED), FAILOVER).is_empty());
    assert!(!faults(&history(48), FAILOVER).is_empty());
}

#[test]
fn an_unknown_set_if_absent_after_a_failover_may_have_written() {
    // After a kill the key may be absent, so an unknown SetNx of v may have set it and a later read of v is legal.
    let base = 2 * SETTLED;
    let history = [
        with_value(record(1, Op::Set, 0, 5, Outcome::Ok), "75"),
        window(FaultKind::Kill, 10, 20),
        with_value(
            record(2, Op::SetNx, base, base + 10, Outcome::Unknown),
            "76",
        ),
        with_value(record(3, Op::Get, base + 20, base + 30, Outcome::Ok), "76"),
    ];
    assert!(faults(&history, FAILOVER).is_empty());
}

#[test]
fn an_eight_byte_value_written_in_the_segment_does_not_pin_as_a_counter() {
    // The earlier stale-read history with the workload's 8-byte values: a value written only inside the segment must not pin from Any as a counter either.
    let base = 2 * SETTLED;
    let v = "0000000100000002";
    let w = "0000000100000003";
    let history = [
        window(FaultKind::Kill, 0, 10),
        with_value(record(2, Op::Get, base, base + 110, Outcome::Ok), v),
        with_value(record(1, Op::Set, base + 102, base + 104, Outcome::Ok), w),
        with_value(record(3, Op::Set, base + 105, base + 200, Outcome::Ok), v),
        with_value(record(4, Op::Get, base + 112, base + 113, Outcome::Ok), w),
    ];
    assert!(!faults(&history, KvClaim::Linearizable).is_empty());
    assert!(!faults(&history, FAILOVER).is_empty());
}

fn on(mut record: Record, node: u32) -> Record {
    record.node = Some(node);
    record
}

fn owned_by(owner: u32) -> Owners {
    let mut owners = Owners::default();
    owners.insert("6b", owner);
    owners
}

fn owned_faults(history: &[Record], owners: &Owners) -> Vec<(usize, KvFault)> {
    check_kv(history, FAILOVER, DEFAULT_BUDGET, Some(owners))
        .violations
        .into_iter()
        .map(|violation| (violation.record, violation.fault))
        .collect()
}

fn stale_after(faults: impl IntoIterator<Item = Record>, read_at: u64) -> Vec<Record> {
    let mut history = vec![
        with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
        with_value(record(1, Op::Set, 20, 30, Outcome::Ok), "bb"),
    ];
    history.extend(faults);
    history.push(with_value(
        record(2, Op::Get, read_at, read_at + 10, Outcome::Ok),
        "aa",
    ));
    history
}

#[test]
fn a_window_on_another_member_keeps_the_owners_keys_linearizable() {
    // A kill of member 2 cannot move a key member 1 owns, so a stale read after it breaks K1 with owners and passes without.
    let history = stale_after([on(window(FaultKind::Kill, 40, 50), 2)], 60 + SETTLED);
    assert!(faults(&history, FAILOVER).is_empty());
    assert!(matches!(
        owned_faults(&history, &owned_by(1)).as_slice(),
        [(3, KvFault::NotLinearizable { .. })]
    ));
    assert!(owned_faults(&history, &owned_by(2)).is_empty());
    let report = check_kv(&history, FAILOVER, DEFAULT_BUDGET, Some(&owned_by(1)));
    assert_eq!(
        (report.owners, report.segments),
        (OwnerRule::Owners { unowned: 0 }, 1)
    );
}

#[test]
fn a_cut_before_the_owners_restart_does_not_hide_a_loss_after_the_heal() {
    // Case 33: member 2 is cut, member 1 restarts and serves again; a write acknowledged then and read back absent after the heal is a loss.
    let heal = 10 * SETTLED;
    let history = [
        on(window(FaultKind::Isolate, 40, heal), 2),
        on(window(FaultKind::Kill, 50, 2 * SETTLED), 1),
        with_value(
            record(1, Op::Set, 4 * SETTLED, 4 * SETTLED + 10, Outcome::Ok),
            "bb",
        ),
        record(
            2,
            Op::Get,
            12 * SETTLED,
            12 * SETTLED + 10,
            Outcome::NotFound,
        ),
    ];
    assert!(faults(&history, FAILOVER).is_empty());
    assert!(matches!(
        owned_faults(&history, &owned_by(1)).as_slice(),
        [(3, KvFault::NotLinearizable { .. })]
    ));
}

#[test]
fn a_window_that_starts_while_the_owner_is_away_restarts_its_keys() {
    // A kill record spans until its member is back and settled (or the run's end); member 2 may hold member 1's slot only within that span or a rejoin window.
    let owners = owned_by(1);
    let away = on(window(FaultKind::Kill, 40, 50), 1);
    let taker = |start: u64| on(window(FaultKind::Kill, start, start + 10), 2);
    let read = 20 * SETTLED;
    let before = |extra: Vec<Record>| {
        let mut history = vec![
            with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
            away.clone(),
            with_value(
                record(1, Op::Set, 2 * SETTLED, 2 * SETTLED + 10, Outcome::Ok),
                "bb",
            ),
        ];
        history.extend(extra);
        history.push(with_value(
            record(2, Op::Get, read, read + 10, Outcome::Ok),
            "aa",
        ));
        history
    };
    assert!(!owned_faults(&before(vec![taker(3 * SETTLED)]), &owners).is_empty());
    let mut gone = away.clone();
    gone.reply_ns = None;
    let mut history = before(vec![taker(3 * SETTLED)]);
    if let Some(slot) = history.iter_mut().find(|record| **record == away) {
        *slot = gone;
    }
    assert!(owned_faults(&history, &owners).is_empty());
    let rejoin = on(window(FaultKind::Restart, SETTLED, 3 * SETTLED), 1);
    assert!(owned_faults(&before(vec![rejoin.clone(), taker(5_000_000_000)]), &owners).is_empty());
    assert!(!owned_faults(&before(vec![rejoin, taker(7 * SETTLED)]), &owners).is_empty());
}

#[test]
fn windows_without_one_member_restart_every_key() {
    // A split without a node, a run-out and a crossed leave may move any slot, whichever member the record names.
    let owners = owned_by(1);
    let read = 60 + 4 * SETTLED;
    for faults in [
        vec![window(FaultKind::Split, 40, 50)],
        vec![on(window(FaultKind::RunOut, 40, 50), 2)],
        vec![
            on(window(FaultKind::Kill, 40, 50), 3),
            on(window(FaultKind::Leave, 45, 60), 2),
        ],
    ] {
        let history = stale_after(faults, read);
        assert!(owned_faults(&history, &owners).is_empty(), "{history:?}");
    }
    let crossed_restart = stale_after(
        [
            on(window(FaultKind::Kill, 40, 50), 3),
            on(window(FaultKind::Restart, 45, 60), 2),
        ],
        read,
    );
    assert!(!owned_faults(&crossed_restart, &owners).is_empty());
}

#[test]
fn a_list_change_or_a_key_without_owner_takes_every_window() {
    // Owners describe the starting list only, and a key missing from the map keeps the suite-wide rule.
    let history = stale_after(
        [
            on(window(FaultKind::Kill, 40, 50), 2),
            on(
                window(FaultKind::MemberAdd, 10 * SETTLED, 10 * SETTLED + 10),
                4,
            ),
        ],
        60 + SETTLED,
    );
    let report = check_kv(&history, FAILOVER, DEFAULT_BUDGET, Some(&owned_by(1)));
    assert_eq!(report.owners, OwnerRule::ListChange);
    assert!(report.violations.is_empty());
    let history = stale_after([on(window(FaultKind::Kill, 40, 50), 2)], 60 + SETTLED);
    let mut other = Owners::default();
    other.insert("6c", 1);
    let report = check_kv(&history, FAILOVER, DEFAULT_BUDGET, Some(&other));
    assert_eq!(report.owners, OwnerRule::Owners { unowned: 1 });
    assert!(report.violations.is_empty());
}

#[test]
fn owners_tsv_round_trips_and_rejects_short_rows() {
    // The drive and suite files carry a header and four tab-separated fields per row.
    let owners = Owners::parse("kind\tkey\tslot\towner\nkv\t6b\t17\t3\nkv\t6c\t-\t0\n")
        .expect("valid owners");
    assert_eq!(
        (owners.get("6b"), owners.get("6c"), owners.len()),
        (Some(3), Some(0), 2)
    );
    let mut text = String::new();
    owners.write_tsv(&mut text);
    let again = Owners::parse(&text).expect("written owners");
    assert_eq!((again.get("6b"), again.get("6c")), (Some(3), Some(0)));
    assert_eq!(
        Owners::parse("kind\tkey\tslot\towner\nkv\t6b\t3\n").map(|owners| owners.len()),
        Err(OwnersError { line: 2 })
    );
    assert!(Owners::parse("kv\t6b\t1\tx\n").is_err());
}

#[test]
fn a_run_out_restarts_the_keys_of_members_away_when_it_starts() {
    // Case 33: a run-out over member 2's cut moves member 2's slots only; with nobody away it may move any slot.
    let cut = |node: u32| on(window(FaultKind::Isolate, 40, 50), node);
    let run_out = window(FaultKind::RunOut, 40, 60);
    let read = 60 + 4 * SETTLED;
    let history = stale_after([cut(2), run_out.clone()], read);
    assert!(matches!(
        owned_faults(&history, &owned_by(1)).as_slice(),
        [(4, KvFault::NotLinearizable { .. })]
    ));
    assert!(owned_faults(&history, &owned_by(2)).is_empty());
    let alone = stale_after([run_out], read);
    assert!(owned_faults(&alone, &owned_by(1)).is_empty());
}

#[test]
fn a_later_restart_does_not_extend_a_kill_that_already_rejoined() {
    // Fault suite: member 1's kill record spans its restart, so a later stop and restart of member 1 must not hide a loss of its key during member 2's cut in between.
    let history = [
        with_value(record(1, Op::Set, 0, 10, Outcome::Ok), "aa"),
        on(window(FaultKind::Kill, 40, 2 * SETTLED), 1),
        with_value(
            record(1, Op::Set, 4 * SETTLED, 4 * SETTLED + 10, Outcome::Ok),
            "bb",
        ),
        on(window(FaultKind::Isolate, 6 * SETTLED, 7 * SETTLED), 2),
        with_value(
            record(2, Op::Get, 10 * SETTLED, 10 * SETTLED + 10, Outcome::Ok),
            "aa",
        ),
        on(window(FaultKind::Restart, 20 * SETTLED, 21 * SETTLED), 1),
    ];
    let without_restart = &history[..5];
    assert!(matches!(
        owned_faults(without_restart, &owned_by(1)).as_slice(),
        [(4, KvFault::NotLinearizable { .. })]
    ));
    assert!(matches!(
        owned_faults(&history, &owned_by(1)).as_slice(),
        [(4, KvFault::NotLinearizable { .. })]
    ));
}
