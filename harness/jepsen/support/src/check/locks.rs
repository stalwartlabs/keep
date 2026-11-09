/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::collections::HashMap;

use crate::history::{Op, Outcome, Record};

pub const CLOCK_MARGIN_PPM: u64 = 833;

const NANOS_PER_SEC: u64 = 1_000_000_000;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Hold {
    pub client: u32,
    pub token: String,
    pub start_ns: u64,
    pub end_ns: u64,
    pub record: usize,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LockViolation {
    pub key: String,
    pub earlier: Hold,
    pub later: Hold,
}

#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct LockReport {
    pub holds: usize,
    pub malformed: usize,
    pub violations: Vec<LockViolation>,
}

pub fn check_locks(history: &[Record], margin_ppm: u64) -> LockReport {
    let malformed = history.iter().filter(|record| malformed(record)).count();
    let mut tokens: HashMap<(&str, &str), Sends> = HashMap::new();
    for record in history {
        let (Some(key), Some(token), Some(send), Some(outcome)) =
            (&record.key, &record.token, record.send_ns, record.outcome)
        else {
            continue;
        };
        let (limit, attempt) = match record.op {
            Op::Unlock if !outcome.not_executed() => (send, None),
            Op::TryLock | Op::Renew if matches!(outcome, Outcome::Ok | Outcome::Unknown) => (
                record
                    .ttl_s
                    .map_or(send, |ttl| bound(send, ttl, margin_ppm)),
                (record.op == Op::TryLock).then(|| (send, record.reply_ns.unwrap_or(u64::MAX))),
            ),
            _ => continue,
        };
        let sends = tokens.entry((key, token)).or_default();
        sends.limits.push((send, limit));
        sends.attempts.extend(attempt);
    }
    for sends in tokens.values_mut() {
        sends.limits.sort_unstable();
        sends.attempts.sort_unstable();
    }
    let mut holds: HashMap<&str, Vec<Hold>> = HashMap::new();
    for (index, record) in history.iter().enumerate() {
        let (Op::TryLock | Op::Renew) = record.op else {
            continue;
        };
        let (Some(Outcome::Ok), Some(key), Some(token), Some(ttl), Some(send), Some(reply)) = (
            record.outcome,
            &record.key,
            &record.token,
            record.ttl_s,
            record.send_ns,
            record.reply_ns,
        ) else {
            continue;
        };
        let mut start = reply;
        let mut end = bound(send, ttl, margin_ppm);
        if let Some(sends) = tokens.get(&(key.as_str(), token.as_str())) {
            let sent = sends.attempts.partition_point(|&(at, _)| at < reply);
            if record.op == Op::Renew
                && !sends
                    .attempts
                    .get(..sent)
                    .unwrap_or_default()
                    .iter()
                    .any(|&(_, answered)| answered > send)
            {
                start = send;
            }
            let first = sends.limits.partition_point(|&(at, _)| at < reply);
            for &(at, limit) in sends.limits.get(first..).unwrap_or_default() {
                if at >= end {
                    break;
                }
                end = end.min(limit);
            }
        }
        if end > start {
            holds.entry(key).or_default().push(Hold {
                client: record.client,
                token: token.clone(),
                start_ns: start,
                end_ns: end,
                record: index,
            });
        }
    }
    let mut violations = Vec::new();
    let mut count = 0;
    for (key, mut holds) in holds {
        count += holds.len();
        holds.sort_unstable_by_key(|hold| (hold.start_ns, hold.end_ns, hold.record));
        sweep(key, &holds, &mut violations);
    }
    violations.sort_unstable_by_key(|violation| violation.later.start_ns);
    LockReport {
        holds: count,
        malformed,
        violations,
    }
}

#[derive(Default)]
struct Sends {
    limits: Vec<(u64, u64)>,
    attempts: Vec<(u64, u64)>,
}

fn bound(send: u64, ttl_s: u32, margin_ppm: u64) -> u64 {
    let ttl = u64::from(ttl_s).saturating_mul(NANOS_PER_SEC);
    send.saturating_add(ttl - ttl.saturating_mul(margin_ppm) / 1_000_000)
}

fn malformed(record: &Record) -> bool {
    if !matches!(record.op, Op::TryLock | Op::Renew | Op::Unlock) {
        return false;
    }
    let Some(outcome) = record.outcome else {
        return true;
    };
    record.key.is_none()
        || record.send_ns.is_none()
        || match record.op {
            Op::TryLock | Op::Renew => {
                matches!(outcome, Outcome::Ok | Outcome::Unknown)
                    && (record.token.is_none()
                        || record.ttl_s.is_none()
                        || (outcome == Outcome::Ok && record.reply_ns.is_none()))
            }
            _ => !outcome.not_executed() && record.token.is_none(),
        }
}

fn sweep(key: &str, holds: &[Hold], violations: &mut Vec<LockViolation>) {
    let mut last: Option<&Hold> = None;
    let mut other: Option<&Hold> = None;
    for hold in holds {
        let overlapped = [last, other]
            .into_iter()
            .flatten()
            .find(|seen| seen.token != hold.token && seen.end_ns > hold.start_ns);
        if let Some(seen) = overlapped {
            violations.push(LockViolation {
                key: key.to_owned(),
                earlier: seen.clone(),
                later: hold.clone(),
            });
        }
        match last {
            Some(seen) if seen.end_ns >= hold.end_ns => {
                if seen.token != hold.token && other.is_none_or(|o| o.end_ns < hold.end_ns) {
                    other = Some(hold);
                }
            }
            Some(seen) => {
                if seen.token != hold.token {
                    other = Some(seen);
                }
                last = Some(hold);
            }
            None => last = Some(hold),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: u64 = NANOS_PER_SEC;
    const MS: u64 = SEC / 1000;

    fn violations(history: &[Record], margin_ppm: u64) -> Vec<LockViolation> {
        check_locks(history, margin_ppm).violations
    }

    fn grant(client: u32, op: Op, token: &str, send: u64, reply: u64) -> Record {
        let mut record = Record::new(client, op);
        record.key = Some("6b".to_owned());
        record.token = Some(token.to_owned());
        record.ttl_s = Some(10);
        record.send_ns = Some(send);
        record.reply_ns = Some(reply);
        record.outcome = Some(Outcome::Ok);
        record
    }

    fn unlock(client: u32, token: &str, send: u64) -> Record {
        let mut record = Record::new(client, Op::Unlock);
        record.key = Some("6b".to_owned());
        record.token = Some(token.to_owned());
        record.send_ns = Some(send);
        record.reply_ns = Some(send + 1);
        record.outcome = Some(Outcome::Ok);
        record
    }

    fn with_ttl(mut record: Record, ttl_s: u32) -> Record {
        record.ttl_s = Some(ttl_s);
        record
    }

    fn with_outcome(mut record: Record, outcome: Outcome) -> Record {
        record.outcome = Some(outcome);
        if outcome == Outcome::Unknown {
            record.reply_ns = None;
        }
        record
    }

    #[test]
    fn a_grant_inside_another_tokens_ttl_is_a_violation() {
        // Token b is granted 9 s into a's 10 s hold.
        let history = [
            grant(1, Op::TryLock, "a", 0, SEC / 1000),
            grant(2, Op::TryLock, "b", 9 * SEC, 9 * SEC + 1000),
        ];
        let violations = violations(&history, CLOCK_MARGIN_PPM);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].earlier.token, "a");
        assert_eq!(violations[0].later.token, "b");
    }

    #[test]
    fn an_unlock_ends_the_hold_at_its_send() {
        // a unlocks at 2 s; b is granted at 3 s.
        let history = [
            grant(1, Op::TryLock, "a", 0, 1000),
            unlock(1, "a", 2 * SEC),
            grant(2, Op::TryLock, "b", 2 * SEC + 10, 3 * SEC),
        ];
        assert!(violations(&history, CLOCK_MARGIN_PPM).is_empty());
    }

    #[test]
    fn an_unlock_that_did_not_execute_keeps_the_hold() {
        // a's Unlock at 2 s is answered Busy; b's grant at 3 s overlaps a's hold.
        let mut busy = unlock(1, "a", 2 * SEC);
        busy.outcome = Some(Outcome::Busy);
        let history = [
            grant(1, Op::TryLock, "a", 0, 1000),
            busy,
            grant(2, Op::TryLock, "b", 2 * SEC + 10, 3 * SEC),
        ];
        assert_eq!(violations(&history, CLOCK_MARGIN_PPM).len(), 1);
        let mut unknown = history.clone();
        unknown[1].outcome = Some(Outcome::Unknown);
        unknown[1].reply_ns = None;
        assert!(violations(&unknown, CLOCK_MARGIN_PPM).is_empty());
    }

    #[test]
    fn the_clock_margin_shortens_every_hold() {
        // b's grant lands 1 ms before a's send + TTL: inside the 8.33 ms margin of a 10 s TTL, outside without it.
        let history = [
            grant(1, Op::TryLock, "a", 0, 1000),
            grant(
                2,
                Op::TryLock,
                "b",
                10 * SEC - 2_000_000,
                10 * SEC - 1_000_000,
            ),
        ];
        assert!(violations(&history, CLOCK_MARGIN_PPM).is_empty());
        assert_eq!(violations(&history, 0).len(), 1);
    }

    #[test]
    fn renewals_by_the_same_token_never_conflict() {
        // a holds, renews and reacquires; only b's grant inside a renewal overlaps.
        let history = [
            grant(1, Op::TryLock, "a", 0, 1000),
            grant(1, Op::Renew, "a", 5 * SEC, 5 * SEC + 1000),
            grant(1, Op::TryLock, "a", 12 * SEC, 12 * SEC + 1000),
            grant(2, Op::TryLock, "b", 14 * SEC, 14 * SEC + 1000),
        ];
        let violations = violations(&history, CLOCK_MARGIN_PPM);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].later.token, "b");
    }

    #[test]
    fn an_overlap_hidden_behind_a_longer_hold_of_the_same_token_is_found() {
        // c starts while a short hold of b runs, behind a long hold of c's own token: b's hold must still be seen.
        let history = [
            grant(1, Op::TryLock, "c", 0, 1000),
            grant(2, Op::TryLock, "b", SEC, SEC + 1000),
            grant(3, Op::Renew, "c", 2 * SEC, 2 * SEC + 1000),
        ];
        let violations = violations(&history, CLOCK_MARGIN_PPM);
        assert_eq!(violations.len(), 2);
    }

    #[test]
    fn the_sweep_matches_the_quadratic_reference() {
        // Pseudo-random holds over three tokens: the sweep flags exactly the holds that overlap an earlier-starting hold of another token.
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..200 {
            let mut history = Vec::new();
            for client in 0..12u32 {
                let token = ["a", "b", "c"][(next() % 3) as usize];
                let send = (next() % 60) * SEC / 2;
                let reply = send + next() % SEC;
                history.push(grant(client, Op::TryLock, token, send, reply));
            }
            let found: Vec<usize> = violations(&history, CLOCK_MARGIN_PPM)
                .iter()
                .map(|violation| violation.later.record)
                .collect();
            let mut holds: Vec<(usize, &str, u64, u64)> = history
                .iter()
                .enumerate()
                .map(|(index, record)| {
                    let send = record.send_ns.unwrap_or_default();
                    let end = send + 10 * SEC - 10 * SEC * CLOCK_MARGIN_PPM / 1_000_000;
                    (
                        index,
                        record.token.as_deref().unwrap_or_default(),
                        record.reply_ns.unwrap_or_default(),
                        end,
                    )
                })
                .collect();
            holds.sort_unstable_by_key(|&(index, _, start, end)| (start, end, index));
            let mut expected: Vec<usize> = holds
                .iter()
                .enumerate()
                .filter(|(position, (_, token, start, _))| {
                    holds[..*position]
                        .iter()
                        .any(|(_, seen, _, end)| seen != token && end > start)
                })
                .map(|(_, (index, ..))| *index)
                .collect();
            let mut found = found;
            found.sort_unstable();
            expected.sort_unstable();
            assert_eq!(found, expected);
        }
    }

    #[test]
    fn a_renew_with_a_shorter_ttl_ends_the_hold_early() {
        // a renews its 3 s lock with 1 s, the renew after that finds it expired, and b's grant at 18.323 s is legal; b at 17.5 s is not.
        let history = |b_send: u64| {
            [
                with_ttl(grant(1, Op::TryLock, "a", 16_405 * MS, 16_406 * MS), 3),
                with_ttl(grant(1, Op::Renew, "a", 16_758 * MS, 16_759 * MS), 1),
                with_outcome(
                    with_ttl(grant(1, Op::Renew, "a", 18_045 * MS, 18_046 * MS), 3),
                    Outcome::NotFound,
                ),
                with_ttl(grant(2, Op::TryLock, "b", b_send, b_send + MS), 6),
            ]
        };
        assert!(violations(&history(18_323 * MS), CLOCK_MARGIN_PPM).is_empty());
        assert_eq!(violations(&history(17_500 * MS), CLOCK_MARGIN_PPM).len(), 1);
    }

    #[test]
    fn a_chain_of_renews_ends_every_hold_at_the_last_bound() {
        // a renews its 6 s lock with 5 s and then 1 s: every hold of a ends by 3 s, so b at 3.5 s is legal and b at 2.9 s is not.
        let history = |b_send: u64| {
            [
                with_ttl(grant(1, Op::TryLock, "a", 0, MS), 6),
                with_ttl(grant(1, Op::Renew, "a", SEC, SEC + MS), 5),
                with_ttl(grant(1, Op::Renew, "a", 2 * SEC, 2 * SEC + MS), 1),
                grant(2, Op::TryLock, "b", b_send, b_send + MS),
            ]
        };
        assert!(violations(&history(3_500 * MS), CLOCK_MARGIN_PPM).is_empty());
        assert_eq!(violations(&history(2_900 * MS), CLOCK_MARGIN_PPM).len(), 1);
    }

    #[test]
    fn a_renew_of_unknown_outcome_may_shorten_a_hold_but_never_extends_it() {
        // An unknown 1 s renew at 1 s ends a's 6 s hold at 2 s; an unknown 10 s renew leaves it ending at 6 s.
        let history = |renew_ttl: u32, b_send: u64| {
            [
                with_ttl(grant(1, Op::TryLock, "a", 0, MS), 6),
                with_outcome(
                    with_ttl(grant(1, Op::Renew, "a", SEC, SEC + MS), renew_ttl),
                    Outcome::Unknown,
                ),
                grant(2, Op::TryLock, "b", b_send, b_send + MS),
            ]
        };
        assert!(violations(&history(1, 2_500 * MS), CLOCK_MARGIN_PPM).is_empty());
        assert!(violations(&history(10, 7 * SEC), CLOCK_MARGIN_PPM).is_empty());
        assert_eq!(violations(&history(10, 5 * SEC), CLOCK_MARGIN_PPM).len(), 1);
    }

    #[test]
    fn a_renew_that_changed_no_deadline_keeps_the_hold() {
        // A 1 s renew of a answered NotFound, Conflict or Busy changed no deadline, so b at 3 s overlaps a's 6 s hold.
        for outcome in [Outcome::NotFound, Outcome::Conflict, Outcome::Busy] {
            let history = [
                with_ttl(grant(1, Op::TryLock, "a", 0, MS), 6),
                with_outcome(
                    with_ttl(grant(1, Op::Renew, "a", SEC, SEC + MS), 1),
                    outcome,
                ),
                grant(2, Op::TryLock, "b", 3 * SEC, 3 * SEC + MS),
            ];
            assert_eq!(
                violations(&history, CLOCK_MARGIN_PPM).len(),
                1,
                "{outcome:?}"
            );
        }
    }

    #[test]
    fn a_longer_renew_after_a_shortening_one_keeps_the_lock_held() {
        // a shortens its 6 s lock to 1 s, then renews it with 10 s; b at 3 s overlaps the last renewal.
        let history = [
            with_ttl(grant(1, Op::TryLock, "a", 0, MS), 6),
            with_ttl(grant(1, Op::Renew, "a", SEC, SEC + MS), 1),
            grant(1, Op::Renew, "a", 1_500 * MS, 1_501 * MS),
            with_ttl(grant(2, Op::TryLock, "b", 3 * SEC, 3 * SEC + MS), 6),
        ];
        let violations = violations(&history, CLOCK_MARGIN_PPM);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].earlier.record, 2);
    }

    #[test]
    fn a_shortening_renew_that_did_not_execute_keeps_the_hold() {
        // a 1 s renew of a answered Moved or Error never ran, so b at 3 s overlaps a's 6 s hold.
        for outcome in [Outcome::Moved, Outcome::Error] {
            let history = [
                with_ttl(grant(1, Op::TryLock, "a", 0, MS), 6),
                with_outcome(
                    with_ttl(grant(1, Op::Renew, "a", SEC, SEC + MS), 1),
                    outcome,
                ),
                grant(2, Op::TryLock, "b", 3 * SEC, 3 * SEC + MS),
            ];
            assert_eq!(
                violations(&history, CLOCK_MARGIN_PPM).len(),
                1,
                "{outcome:?}"
            );
        }
    }

    #[test]
    fn an_attempt_sent_before_the_grant_reply_does_not_shorten_it() {
        // a's unknown 1 s renew is sent before its own grant's reply arrives, so it does not end that hold; b at 3 s overlaps.
        let history = [
            with_ttl(grant(1, Op::TryLock, "a", 0, 2 * SEC), 6),
            with_outcome(
                with_ttl(grant(1, Op::Renew, "a", SEC, SEC + MS), 1),
                Outcome::Unknown,
            ),
            grant(2, Op::TryLock, "b", 3 * SEC, 3 * SEC + MS),
        ];
        assert_eq!(violations(&history, CLOCK_MARGIN_PPM).len(), 1);
    }

    #[test]
    fn a_shortening_attempt_of_another_token_or_key_keeps_the_hold() {
        // unknown 1 s renews of token c on the key and of a on another key leave a's 6 s hold; b at 3 s overlaps.
        let mut other_key = with_outcome(
            with_ttl(grant(1, Op::Renew, "a", SEC, SEC + MS), 1),
            Outcome::Unknown,
        );
        other_key.key = Some("6c".to_owned());
        let history = [
            with_ttl(grant(1, Op::TryLock, "a", 0, MS), 6),
            with_outcome(
                with_ttl(grant(3, Op::Renew, "c", SEC, SEC + MS), 1),
                Outcome::Unknown,
            ),
            other_key,
            grant(2, Op::TryLock, "b", 3 * SEC, 3 * SEC + MS),
        ];
        assert_eq!(violations(&history, CLOCK_MARGIN_PPM).len(), 1);
    }

    #[test]
    fn a_shortening_renew_sent_after_another_grant_does_not_end_the_hold_before_it() {
        // b is granted at 3 s inside a's 6 s hold; a's 1 s renew at 4 s ends the hold only at 5 s, so b still overlaps.
        let history = [
            with_ttl(grant(1, Op::TryLock, "a", 0, MS), 6),
            grant(2, Op::TryLock, "b", 3 * SEC, 3 * SEC + MS),
            with_ttl(grant(1, Op::Renew, "a", 4 * SEC, 4 * SEC + MS), 1),
        ];
        let violations = violations(&history, CLOCK_MARGIN_PPM);
        assert_eq!(violations[0].earlier.record, 0);
        assert_eq!(violations[0].later.record, 1);
    }

    #[test]
    fn a_grant_inside_a_renew_still_in_flight_is_a_violation() {
        // a's 10 s renew is sent at 1.5 s, inside the 1 s renewal's hold, and answered Ok at 2.5 s; a lock held by a at its execution means b's grant at 2.2 s and unlock at 2.3 s overlapped it.
        let history = [
            with_ttl(grant(1, Op::TryLock, "a", 0, MS), 6),
            with_ttl(grant(1, Op::Renew, "a", SEC, SEC + MS), 1),
            grant(1, Op::Renew, "a", 1_500 * MS, 2_500 * MS),
            with_ttl(grant(2, Op::TryLock, "b", 2_200 * MS, 2_201 * MS), 6),
            unlock(2, "b", 2_300 * MS),
        ];
        assert_eq!(violations(&history, CLOCK_MARGIN_PPM).len(), 1);
    }

    #[test]
    fn a_grant_while_a_renew_that_found_the_lock_was_in_flight_is_a_violation() {
        // a's 10 s renew is sent at 0.5 s inside its 1 s hold and answered Ok at 1.5 s, so a held the lock throughout; b's grant at 1.2 s overlapped it.
        let history = [
            with_ttl(grant(1, Op::TryLock, "a", 0, MS), 1),
            grant(1, Op::Renew, "a", 500 * MS, 1_500 * MS),
            with_ttl(grant(2, Op::TryLock, "b", 1_200 * MS, 1_201 * MS), 6),
            unlock(2, "b", 1_300 * MS),
        ];
        let violations = violations(&history, CLOCK_MARGIN_PPM);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].earlier.record, 1);
        assert_eq!(violations[0].later.record, 2);
    }

    #[test]
    fn a_renew_starts_its_hold_at_its_reply_while_a_trylock_of_its_token_may_be_in_flight() {
        // a's lock ends at 1 s; b holds 1.2 s to 1.3 s; a's unknown TryLock may have created a new lock after b, before a's renew ran, so only a refused TryLock makes b a violation.
        let history = |outcome: Outcome| {
            [
                with_ttl(grant(1, Op::TryLock, "a", 0, MS), 1),
                with_outcome(grant(1, Op::TryLock, "a", 900 * MS, 1_000 * MS), outcome),
                grant(1, Op::Renew, "a", 1_100 * MS, 1_600 * MS),
                with_ttl(grant(2, Op::TryLock, "b", 1_200 * MS, 1_201 * MS), 6),
                unlock(2, "b", 1_300 * MS),
            ]
        };
        assert!(violations(&history(Outcome::Unknown), CLOCK_MARGIN_PPM).is_empty());
        assert_eq!(
            violations(&history(Outcome::Busy), CLOCK_MARGIN_PPM).len(),
            1
        );
    }

    #[test]
    fn an_attempt_that_may_have_executed_without_a_ttl_is_malformed() {
        // A TryLock or Renew answered Ok or unknown without a TTL cannot bound the hold it may have replaced; a refused one can go without.
        let mut unknown = with_outcome(grant(1, Op::Renew, "a", 0, MS), Outcome::Unknown);
        unknown.ttl_s = None;
        let mut ok = grant(1, Op::TryLock, "a", 2 * MS, 3 * MS);
        ok.ttl_s = None;
        let mut busy = with_outcome(grant(1, Op::Renew, "a", 4 * MS, 5 * MS), Outcome::Busy);
        busy.ttl_s = None;
        assert_eq!(
            check_locks(&[unknown, ok, busy], CLOCK_MARGIN_PPM).malformed,
            2
        );
    }

    #[test]
    fn hold_ends_match_the_claim_reference() {
        // pseudo-random lock histories with mixed TTLs and outcomes; the checker flags exactly the holds the literal claim L text flags.
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let outcomes = [
            Outcome::Ok,
            Outcome::Unknown,
            Outcome::NotFound,
            Outcome::Conflict,
            Outcome::Busy,
            Outcome::Moved,
            Outcome::Error,
        ];
        let ops = [Op::TryLock, Op::Renew, Op::Unlock];
        for _ in 0..500 {
            let mut history = Vec::new();
            for client in 0..16u32 {
                let token = ["a", "b", "c"][(next() % 3) as usize];
                let send = (next() % 20_000) * MS;
                let reply = send + next() % SEC;
                let op = ops[(next() % 3) as usize];
                let outcome = outcomes[(next() % 7) as usize];
                let record = match op {
                    Op::Unlock => unlock(client, token, send),
                    _ => with_ttl(
                        grant(client, op, token, send, reply),
                        1 + (next() % 6) as u32,
                    ),
                };
                history.push(with_outcome(record, outcome));
            }
            let bound = |record: &Record| {
                let ttl = u64::from(record.ttl_s.unwrap_or_default()) * SEC;
                record.send_ns.unwrap_or_default() + ttl - ttl * CLOCK_MARGIN_PPM / 1_000_000
            };
            let mut holds: Vec<(usize, &str, u64, u64)> = Vec::new();
            for (index, record) in history.iter().enumerate() {
                if record.op == Op::Unlock || record.outcome != Some(Outcome::Ok) {
                    continue;
                }
                let token = record.token.as_deref().unwrap_or_default();
                let send = record.send_ns.unwrap_or_default();
                let reply = record.reply_ns.unwrap_or_default();
                let in_flight = history.iter().any(|attempt| {
                    attempt.op == Op::TryLock
                        && attempt.token.as_deref() == Some(token)
                        && matches!(attempt.outcome, Some(Outcome::Ok | Outcome::Unknown))
                        && attempt.send_ns.unwrap_or_default() < reply
                        && attempt.reply_ns.unwrap_or(u64::MAX) > send
                });
                let start = if record.op == Op::Renew && !in_flight {
                    send
                } else {
                    reply
                };
                let end = history
                    .iter()
                    .filter(|later| {
                        later.token.as_deref() == Some(token)
                            && later.send_ns.unwrap_or_default() >= reply
                    })
                    .filter_map(|later| match (later.op, later.outcome) {
                        (Op::Unlock, Some(outcome)) if !outcome.not_executed() => later.send_ns,
                        (Op::TryLock | Op::Renew, Some(Outcome::Ok | Outcome::Unknown)) => {
                            Some(bound(later))
                        }
                        _ => None,
                    })
                    .fold(bound(record), u64::min);
                if end > start {
                    holds.push((index, token, start, end));
                }
            }
            holds.sort_unstable_by_key(|&(index, _, start, end)| (start, end, index));
            let mut expected: Vec<usize> = holds
                .iter()
                .enumerate()
                .filter(|(position, (_, token, start, _))| {
                    holds[..*position]
                        .iter()
                        .any(|(_, seen, _, end)| seen != token && end > start)
                })
                .map(|(_, (index, ..))| *index)
                .collect();
            let mut found: Vec<usize> = violations(&history, CLOCK_MARGIN_PPM)
                .iter()
                .map(|violation| violation.later.record)
                .collect();
            found.sort_unstable();
            expected.sort_unstable();
            assert_eq!(found, expected);
        }
    }

    #[test]
    fn grants_missing_fields_are_counted_as_malformed() {
        // a grant without a reply time, an Unlock without a token and a record without an outcome are malformed; a refused attempt is not.
        let mut no_reply = grant(1, Op::TryLock, "a", 0, 10);
        no_reply.reply_ns = None;
        let mut no_token = unlock(1, "a", 20);
        no_token.token = None;
        let mut no_outcome = grant(2, Op::Renew, "b", 30, 40);
        no_outcome.outcome = None;
        let mut refused = grant(2, Op::TryLock, "b", 50, 60);
        refused.outcome = Some(Outcome::Conflict);
        refused.token = None;
        let report = check_locks(
            &[
                no_reply,
                no_token,
                no_outcome,
                refused,
                grant(3, Op::TryLock, "c", 70, 80),
            ],
            CLOCK_MARGIN_PPM,
        );
        assert_eq!(report.malformed, 3);
        assert_eq!(report.holds, 1);
        assert!(report.violations.is_empty());
    }
}
