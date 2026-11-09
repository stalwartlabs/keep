/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::*;

const TOPIC: u32 = 7;
const S: &str = "10.0.0.1:7000";

fn subscribe(client: u32, reply: u64) -> Record {
    let mut record = Record::new(client, Op::Subscribe);
    record.topic = Some(TOPIC);
    record.send_ns = Some(reply - 1);
    record.reply_ns = Some(reply);
    record.outcome = Some(Outcome::Ok);
    record
}

fn publish(client: u32, payload: &str, send: u64, reply: u64, outcome: Outcome) -> Record {
    let mut record = Record::new(client, Op::Publish);
    record.topic = Some(TOPIC);
    record.value = Some(payload.to_owned());
    record.server = Some(S.to_owned());
    record.send_ns = Some(send);
    record.reply_ns = (outcome != Outcome::Unknown).then_some(reply);
    record.outcome = Some(outcome);
    record
}

fn delivery(client: u32, op: Op, at: u64) -> Record {
    let mut record = Record::new(client, op);
    record.topic = Some(TOPIC);
    record.server = Some(S.to_owned());
    record.reply_ns = Some(at);
    record
}

fn event(client: u32, seq: u32, payload: &str, at: u64) -> Record {
    let mut record = delivery(client, Op::Event, at);
    record.seq = Some(seq);
    record.value = Some(payload.to_owned());
    record
}

fn gap(client: u32, missed: u32, at: u64) -> Record {
    let mut record = delivery(client, Op::Gap, at);
    record.missed = Some(missed);
    record
}

fn faults(history: &[Record]) -> Vec<(usize, StreamFault)> {
    check_stream(history)
        .violations
        .into_iter()
        .map(|violation| (violation.record, violation.fault))
        .collect()
}

fn published() -> Vec<Record> {
    vec![
        subscribe(10, 10),
        publish(1, "01", 20, 25, Outcome::Ok),
        publish(1, "02", 40, 45, Outcome::Ok),
        publish(1, "03", 60, 65, Outcome::Ok),
    ]
}

#[test]
fn a_complete_subscription_passes() {
    // Every confirmed publish arrives in order with consecutive sequences.
    let mut history = published();
    history.extend([
        event(10, 1, "01", 26),
        event(10, 2, "02", 46),
        event(10, 3, "03", 66),
    ]);
    let report = check_stream(&history);
    assert!(report.violations.is_empty(), "{report:?}");
    assert_eq!(
        (
            report.subscriptions,
            report.events,
            report.confirmed,
            report.unplaced
        ),
        (1, 3, 3, 0)
    );
}

#[test]
fn a_publish_missing_before_a_later_event_is_a_silent_loss_unless_a_gap_shows_it() {
    // 02 never arrives and 03 carries sequence 2: silent; with a Gap before 03 it is signaled.
    let mut history = published();
    history.extend([event(10, 1, "01", 26), event(10, 2, "03", 66)]);
    assert_eq!(
        faults(&history),
        vec![(5, StreamFault::SilentLoss { publish: 2 })]
    );
    let mut signaled = published();
    signaled.extend([
        event(10, 1, "01", 26),
        gap(10, 1, 66),
        event(10, 3, "03", 66),
    ]);
    assert!(faults(&signaled).is_empty());
}

#[test]
fn a_drain_end_proves_every_earlier_confirmed_publish_was_counted() {
    // 02 is confirmed before the drain End arrives but never delivered; after this client's own Unsubscribe the End proves nothing.
    let mut history = published();
    history.truncate(3);
    history.extend([event(10, 1, "01", 26), delivery(10, Op::Ended, 50)]);
    assert_eq!(
        faults(&history),
        vec![(4, StreamFault::SilentLoss { publish: 2 })]
    );
    let mut unsubscribed = published();
    unsubscribed.truncate(3);
    let mut leave = Record::new(10, Op::Unsubscribe);
    leave.topic = Some(TOPIC);
    leave.send_ns = Some(47);
    unsubscribed.extend([event(10, 1, "01", 26), leave, delivery(10, Op::Ended, 50)]);
    assert!(faults(&unsubscribed).is_empty());
}

#[test]
fn a_publish_confirmed_before_another_was_sent_arrives_first() {
    // 01 was confirmed at 25 and 02 sent at 40, yet 02 arrives first.
    let mut history = published();
    history.extend([event(10, 1, "02", 46), event(10, 2, "01", 47)]);
    assert_eq!(
        faults(&history),
        vec![(5, StreamFault::Reordered { first: 4 })]
    );
}

#[test]
fn sequences_that_step_back_are_reported_at_both_levels() {
    // A wire-level repeat of sequence 2, and a client-level Gap computed from a backward step.
    let mut history = published();
    history.extend([
        event(10, 1, "01", 26),
        event(10, 2, "02", 46),
        event(10, 2, "03", 66),
    ]);
    assert_eq!(faults(&history), vec![(6, StreamFault::SequenceBack)]);
    let mut client = published();
    client.extend([event(10, 1, "01", 26), gap(10, u32::MAX, 30)]);
    assert_eq!(faults(&client), vec![(5, StreamFault::SequenceBack)]);
}

#[test]
fn deliveries_must_come_from_executed_publishes_once_and_not_after_end() {
    // A payload nobody published, one of a Busy publish, 01 twice, 03 before its send, and an event after End.
    let mut history = vec![
        subscribe(10, 10),
        publish(1, "01", 20, 25, Outcome::Ok),
        publish(1, "02", 40, 45, Outcome::Busy),
        publish(1, "03", 60, 65, Outcome::Ok),
        event(10, 1, "01", 26),
        event(10, 2, "ff", 27),
        event(10, 3, "02", 46),
        event(10, 4, "01", 47),
        event(10, 5, "03", 55),
        delivery(10, Op::Ended, 70),
        event(10, 1, "03", 71),
    ];
    let expected = vec![
        (5, StreamFault::Phantom),
        (6, StreamFault::NotExecuted),
        (7, StreamFault::Duplicate),
        (8, StreamFault::Early),
        (10, StreamFault::AfterEnd),
    ];
    assert_eq!(faults(&history), expected);
    history.truncate(5);
    assert!(faults(&history).is_empty());
}

#[test]
fn wire_level_heartbeats_and_ends_carry_the_loss_signals() {
    // A Heartbeat ahead of the last event signals 02's loss; an End equal to the last sequence is a clean barrier, one behind it steps back.
    let mut heartbeat = delivery(10, Op::Heartbeat, 50);
    heartbeat.seq = Some(2);
    let mut end = delivery(10, Op::End, 70);
    end.seq = Some(3);
    let mut history = published();
    history.extend([
        event(10, 1, "01", 26),
        heartbeat,
        event(10, 3, "03", 66),
        end.clone(),
    ]);
    assert!(faults(&history).is_empty(), "{:?}", faults(&history));
    let mut behind = published();
    behind.truncate(3);
    end.seq = Some(1);
    behind.extend([event(10, 1, "01", 26), event(10, 2, "02", 46), end]);
    assert_eq!(faults(&behind), vec![(5, StreamFault::SequenceBack)]);
}

#[test]
fn a_lost_lane_owes_nothing_until_it_is_resubscribed() {
    // Lost after 01; 02 is published while the lane is down; after Resubscribed sequences restart at 1 and only 03 is owed.
    let mut history = published();
    history.extend([
        event(10, 1, "01", 26),
        delivery(10, Op::Lost, 30),
        delivery(10, Op::Resubscribed, 50),
        event(10, 1, "03", 66),
    ]);
    assert!(faults(&history).is_empty(), "{:?}", faults(&history));
    let mut skipped = published();
    skipped.push(publish(1, "04", 80, 85, Outcome::Ok));
    skipped.extend([
        event(10, 1, "01", 26),
        delivery(10, Op::Lost, 30),
        delivery(10, Op::Resubscribed, 50),
        event(10, 1, "04", 86),
    ]);
    assert_eq!(
        faults(&skipped),
        vec![(8, StreamFault::SilentLoss { publish: 3 })]
    );
}

#[test]
fn a_confirmed_publish_no_one_saw_is_counted_unplaced() {
    // 02 reaches no subscriber and names no server, so the loss check cannot place it.
    let mut history = published();
    history[2].server = None;
    history.extend([
        event(10, 1, "01", 26),
        gap(10, 1, 60),
        event(10, 3, "03", 66),
    ]);
    let report = check_stream(&history);
    assert!(report.violations.is_empty(), "{report:?}");
    assert_eq!(report.unplaced, 1);
}

struct Xor(u64);

impl Xor {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

fn simulated(rng: &mut Xor, drop: usize, signal: bool) -> Vec<Record> {
    let subscribers = [10u32, 11];
    let mut history = Vec::new();
    for &client in &subscribers {
        history.push(subscribe(client, 10));
    }
    let mut executions = Vec::new();
    let mut clock = [100u64, 100];
    for n in 0..200u64 {
        let publisher = usize::from(n % 2 == 1);
        let send = clock[publisher] + rng.below(50);
        let executed = send + 1 + rng.below(40);
        let reply = executed + 1 + rng.below(40);
        clock[publisher] = reply;
        let payload = format!("{publisher:02x}{n:04x}");
        history.push(publish(
            publisher as u32 + 1,
            &payload,
            send,
            reply,
            Outcome::Ok,
        ));
        executions.push((executed, payload));
    }
    executions.sort_unstable();
    for &client in &subscribers {
        let mut dropped: Vec<usize> = (0..drop)
            .map(|_| 1 + rng.below(executions.len() as u64 - 2) as usize)
            .collect();
        dropped.sort_unstable();
        dropped.dedup();
        let mut arrived = 0u64;
        let mut seq = 0u32;
        let mut missed = 0u32;
        for (position, (executed, payload)) in executions.iter().enumerate() {
            if signal || !dropped.contains(&position) {
                seq += 1;
            }
            if dropped.contains(&position) {
                missed += 1;
                continue;
            }
            arrived = arrived.max(executed + 1 + rng.below(30));
            if signal && missed > 0 {
                history.push(gap(client, missed, arrived));
            }
            missed = 0;
            history.push(event(client, seq, payload, arrived));
        }
    }
    history
}

#[test]
fn simulated_streams_pass_and_silent_drops_are_found() {
    // A server model with random timing: signaled losses pass, and every subscriber that silently loses events is caught.
    let mut rng = Xor(0x5851_f42d_4c95_7f2d);
    for _ in 0..200 {
        let clean = simulated(&mut rng, 0, false);
        assert!(faults(&clean).is_empty(), "{:?}", faults(&clean));
        let signaled = simulated(&mut rng, 3, true);
        assert!(faults(&signaled).is_empty(), "{:?}", faults(&signaled));
        let silent = simulated(&mut rng, 3, false);
        let found = faults(&silent);
        assert!(
            found
                .iter()
                .all(|(_, fault)| matches!(fault, StreamFault::SilentLoss { .. })),
            "{found:?}"
        );
        let subscribers: HashSet<u32> = found
            .iter()
            .filter_map(|&(record, _)| silent.get(record).map(|record| record.client))
            .collect();
        assert_eq!(subscribers.len(), 2, "{found:?}");
    }
}

fn unplaced(mut record: Record) -> Record {
    record.server = None;
    record
}

#[test]
fn a_confirmed_publish_no_subscriber_received_is_a_silent_loss() {
    // The fault suite logs no server on Publish: 02 is confirmed on S but dropped for every subscriber, 03 arrives with the next sequence, no gap.
    let history = vec![
        subscribe(10, 10),
        subscribe(12, 10),
        unplaced(publish(1, "01", 20, 25, Outcome::Ok)),
        unplaced(publish(1, "02", 40, 45, Outcome::Ok)),
        unplaced(publish(1, "03", 60, 65, Outcome::Ok)),
        event(10, 1, "01", 26),
        event(12, 1, "01", 26),
        event(10, 2, "03", 66),
        event(12, 2, "03", 66),
    ];
    let report = check_stream(&history);
    assert!(!report.violations.is_empty(), "{report:?}");
}

#[test]
fn a_confirmed_publish_lost_at_the_tail_shows_by_the_unsubscribe() {
    // 03 is confirmed at 65 and the subscriber stays subscribed for 2 s (20 heartbeat intervals) without receiving it or any gap.
    let mut history = published();
    history.extend([event(10, 1, "01", 26), event(10, 2, "02", 46)]);
    let mut leave = Record::new(10, Op::Unsubscribe);
    leave.topic = Some(TOPIC);
    leave.send_ns = Some(2_000_000_065);
    history.push(leave);
    let report = check_stream(&history);
    assert!(!report.violations.is_empty(), "{report:?}");
}
