/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::{
    Event, TAG_CACHE_INVALIDATE, TAG_CACHE_INVALIDATE_ALL, TAG_CACHE_INVALIDATE_NEGATIVE,
    TAG_CALENDAR_ALERT, TAG_EMAIL_PUSH, TAG_INDEX_REFRESH, TAG_PUSH_SERVER_UPDATE,
    TAG_QUEUE_PAUSED, TAG_QUEUE_REFRESH, TAG_QUEUE_RUNNING, TAG_REGISTRY_DELETE,
    TAG_REGISTRY_INSERT, TAG_REGISTRY_RELOAD, TAG_STATE_CHANGE, TAG_TASK_REFRESH,
    TAG_VIEWER_STATE_CHANGE,
};
use std::collections::{HashMap, VecDeque};

pub const MAX_BATCH_SIZE: usize = 100;
pub const MAX_PENDING_EVENTS: usize = 65_536;
pub const MAX_PENDING_BYTES: usize = 8 * 1024 * 1024;
const LOW_WATER_NUMERATOR: usize = 7;
const LOW_WATER_DENOMINATOR: usize = 8;
const INVALIDATE_ALL_THRESHOLD: usize = 1024;
const MAX_CHUNK_BYTES: usize = 256 * 1024;
const EVENT_COST: usize = 48;
const INVALIDATION_ITEM_BYTES: u32 = 6;

#[derive(Default)]
pub struct Backlog {
    ready: VecDeque<Event>,
    ready_bytes: usize,
    registry: Vec<Event>,
    queue_status: Option<Event>,
    push_updates: HashMap<u32, Event>,
    invalidate_negative: Option<Event>,
    events: VecDeque<Event>,
    events_bytes: usize,
    states: HashMap<(u8, u32), Event>,
    invalidations: Vec<Event>,
    invalidation_items: usize,
    invalidation_bytes: usize,
    invalidate_all: bool,
    refreshes: [Option<Event>; 3],
    in_flight: usize,
    in_flight_bytes: usize,
    degraded: bool,
    dropped: u64,
    coalesced: u64,
}

pub fn refresh_slot(tag: u8) -> Option<usize> {
    match tag {
        TAG_QUEUE_REFRESH => Some(0),
        TAG_TASK_REFRESH => Some(1),
        TAG_INDEX_REFRESH => Some(2),
        _ => None,
    }
}

pub fn cost(event: &Event) -> usize {
    EVENT_COST
        + match event.tag {
            TAG_CALENDAR_ALERT | TAG_CACHE_INVALIDATE => event.size as usize,
            _ => 0,
        }
}

fn is_push_notification(tag: u8) -> bool {
    matches!(
        tag,
        TAG_STATE_CHANGE | TAG_CALENDAR_ALERT | TAG_EMAIL_PUSH | TAG_VIEWER_STATE_CHANGE
    )
}

fn invalidation_items(event: &Event) -> usize {
    (event.size.saturating_sub(1) / INVALIDATION_ITEM_BYTES).max(1) as usize
}

impl Backlog {
    pub fn push(&mut self, event: Event) {
        match event.tag {
            TAG_STATE_CHANGE | TAG_VIEWER_STATE_CHANGE => self.push_state(event.tag, event),
            TAG_EMAIL_PUSH if self.degraded => self.push_state(
                TAG_STATE_CHANGE,
                Event {
                    tag: TAG_STATE_CHANGE,
                    ..event
                },
            ),
            TAG_CACHE_INVALIDATE => self.push_invalidation(event),
            TAG_CACHE_INVALIDATE_ALL => self.invalidate_all(),
            TAG_CACHE_INVALIDATE_NEGATIVE => match self.invalidate_all {
                true => self.coalesced += 1,
                false => {
                    self.coalesced += u64::from(self.invalidate_negative.replace(event).is_some())
                }
            },
            TAG_REGISTRY_INSERT | TAG_REGISTRY_DELETE | TAG_REGISTRY_RELOAD => {
                self.push_registry(event)
            }
            TAG_QUEUE_RUNNING | TAG_QUEUE_PAUSED => {
                self.coalesced += u64::from(self.queue_status.replace(event).is_some());
            }
            TAG_PUSH_SERVER_UPDATE => {
                self.coalesced +=
                    u64::from(self.push_updates.insert(event.account, event).is_some());
            }
            tag => match refresh_slot(tag) {
                Some(slot) => {
                    self.coalesced += u64::from(self.refreshes[slot].replace(event).is_some());
                }
                None => {
                    self.events_bytes += cost(&event);
                    self.events.push_back(event);
                }
            },
        }
        self.enforce_limits();
    }

    pub fn fill(&mut self, batch: &mut Vec<Event>) -> bool {
        if !batch.is_empty() {
            return true;
        }
        if self.ready.is_empty() {
            self.snapshot();
        }
        let mut chunk_bytes = 0;
        while batch.len() < MAX_BATCH_SIZE {
            let Some(event) = self.ready.front() else {
                break;
            };
            let event_cost = cost(event);
            if !batch.is_empty() && chunk_bytes + event_cost > MAX_CHUNK_BYTES {
                break;
            }
            chunk_bytes += event_cost;
            self.ready_bytes -= event_cost;
            batch.extend(self.ready.pop_front());
        }
        self.in_flight = batch.len();
        self.in_flight_bytes = chunk_bytes;
        !batch.is_empty()
    }

    pub fn release(&mut self) {
        self.in_flight = 0;
        self.in_flight_bytes = 0;
    }

    pub fn take_dropped(&mut self) -> u64 {
        std::mem::take(&mut self.dropped)
    }

    pub fn take_coalesced(&mut self) -> u64 {
        std::mem::take(&mut self.coalesced)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.in_flight
            + self.ready.len()
            + self.registry.len()
            + usize::from(self.queue_status.is_some())
            + self.push_updates.len()
            + usize::from(self.invalidate_negative.is_some())
            + self.events.len()
            + self.states.len()
            + self.invalidations.len()
            + usize::from(self.invalidate_all)
            + self.refreshes.iter().flatten().count()
    }

    pub fn bytes(&self) -> usize {
        self.in_flight_bytes
            + self.ready_bytes
            + (self.registry.len() + self.push_updates.len() + self.states.len()) * EVENT_COST
            + self.events_bytes
            + self.invalidation_bytes
    }

    fn push_state(&mut self, tag: u8, event: Event) {
        self.coalesced += u64::from(self.states.insert((tag, event.account), event).is_some());
    }

    fn push_invalidation(&mut self, event: Event) {
        if self.invalidate_all {
            self.coalesced += 1;
            return;
        }
        self.invalidation_items += invalidation_items(&event);
        self.invalidation_bytes += cost(&event);
        self.invalidations.push(event);
        if self.invalidation_items > INVALIDATE_ALL_THRESHOLD {
            self.invalidate_all();
        }
    }

    fn invalidate_all(&mut self) {
        self.invalidate_all = true;
        self.coalesced += u64::from(self.invalidate_negative.take().is_some());
        self.coalesced += self.invalidations.len() as u64;
        self.invalidations.clear();
        self.invalidation_items = 0;
        self.invalidation_bytes = 0;
    }

    fn push_registry(&mut self, event: Event) {
        match self
            .registry
            .iter_mut()
            .find(|pending| pending.account == event.account)
        {
            Some(pending) => {
                if pending.tag != event.tag {
                    *pending = Event {
                        tag: TAG_REGISTRY_RELOAD,
                        ..event
                    };
                }
                self.coalesced += 1;
            }
            None => self.registry.push(event),
        }
    }

    fn snapshot(&mut self) {
        if std::mem::take(&mut self.invalidate_all) {
            self.push_ready(Event {
                seq: 0,
                created: 0,
                size: 1,
                account: 0,
                tag: TAG_CACHE_INVALIDATE_ALL,
            });
        }
        self.invalidation_items = 0;
        self.invalidation_bytes = 0;
        for event in std::mem::take(&mut self.invalidations) {
            self.push_ready(event);
        }
        if let Some(event) = self.invalidate_negative.take() {
            self.push_ready(event);
        }
        for event in std::mem::take(&mut self.registry) {
            self.push_ready(event);
        }
        if let Some(event) = self.queue_status.take() {
            self.push_ready(event);
        }
        for (_, event) in std::mem::take(&mut self.push_updates) {
            self.push_ready(event);
        }
        self.ready_bytes += std::mem::take(&mut self.events_bytes);
        self.ready.append(&mut self.events);
        for (_, event) in std::mem::take(&mut self.states) {
            self.push_ready(event);
        }
        for slot in self.refreshes.iter_mut() {
            if let Some(event) = slot.take() {
                self.ready_bytes += cost(&event);
                self.ready.push_back(event);
            }
        }
        self.enforce_limits();
    }

    fn push_ready(&mut self, event: Event) {
        self.ready_bytes += cost(&event);
        self.ready.push_back(event);
    }

    fn enforce_limits(&mut self) {
        if self.len() > MAX_PENDING_EVENTS || self.bytes() > MAX_PENDING_BYTES {
            self.relieve();
        }
    }

    #[cold]
    #[inline(never)]
    fn relieve(&mut self) {
        if !self.invalidate_all && !self.invalidations.is_empty() {
            self.invalidate_all();
        }
        if !self.degraded {
            self.degrade_email_pushes();
        }
        if self.len() <= MAX_PENDING_EVENTS && self.bytes() <= MAX_PENDING_BYTES {
            return;
        }
        let low_len = MAX_PENDING_EVENTS / LOW_WATER_DENOMINATOR * LOW_WATER_NUMERATOR;
        let low_bytes = MAX_PENDING_BYTES / LOW_WATER_DENOMINATOR * LOW_WATER_NUMERATOR;
        let is_over = |backlog: &Self| backlog.len() > low_len || backlog.bytes() > low_bytes;
        let mut dropped = 0u64;
        if is_over(self) {
            let ready_bytes = &mut self.ready_bytes;
            self.ready
                .retain(|event| match is_push_notification(event.tag) {
                    true => {
                        *ready_bytes -= cost(event);
                        dropped += 1;
                        false
                    }
                    false => true,
                });
        }
        if is_over(self) {
            let mut excess = self
                .len()
                .saturating_sub(low_len)
                .max(self.bytes().saturating_sub(low_bytes).div_ceil(EVENT_COST));
            let before = self.states.len();
            self.states.retain(|_, _| match excess {
                0 => true,
                _ => {
                    excess -= 1;
                    false
                }
            });
            dropped += (before - self.states.len()) as u64;
        }
        while is_over(self) {
            let Some(event) = self.events.pop_front() else {
                break;
            };
            self.events_bytes -= cost(&event);
            dropped += 1;
        }
        if is_over(self) {
            dropped += self.invalidations.len() as u64;
            self.invalidations.clear();
            self.invalidation_items = 0;
            self.invalidation_bytes = 0;
        }
        if dropped > 0 {
            self.dropped += dropped;
            self.invalidate_all();
        }
    }

    fn degrade_email_pushes(&mut self) {
        self.degraded = true;
        let mut pushes = Vec::new();
        for (events, bytes) in [
            (&mut self.ready, &mut self.ready_bytes),
            (&mut self.events, &mut self.events_bytes),
        ] {
            events.retain(|event| match event.tag {
                TAG_EMAIL_PUSH => {
                    *bytes -= cost(event);
                    pushes.push(*event);
                    false
                }
                _ => true,
            });
        }
        for push in pushes {
            self.push_state(
                TAG_STATE_CHANGE,
                Event {
                    tag: TAG_STATE_CHANGE,
                    ..push
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Backlog, MAX_BATCH_SIZE, MAX_PENDING_BYTES, MAX_PENDING_EVENTS};
    use crate::stream::{
        Event, TAG_CACHE_INVALIDATE, TAG_CACHE_INVALIDATE_ALL, TAG_EMAIL_PUSH, TAG_QUEUE_REFRESH,
        TAG_STATE_CHANGE,
    };

    fn event(seq: u64, tag: u8, account: u32, size: u32) -> Event {
        Event {
            seq,
            created: seq,
            size,
            account,
            tag,
        }
    }

    fn drain(backlog: &mut Backlog) -> Vec<Event> {
        let mut batch = Vec::new();
        let mut out = Vec::new();
        while backlog.fill(&mut batch) {
            assert!(batch.len() <= MAX_BATCH_SIZE);
            out.append(&mut batch);
            backlog.release();
        }
        out
    }

    // State changes coalesce per account and refreshes collapse to one per kind.
    #[test]
    fn coalesces_by_key() {
        let mut backlog = Backlog::default();
        for seq in 0..1000 {
            backlog.push(event(seq, TAG_STATE_CHANGE, (seq % 10) as u32, 10));
            backlog.push(event(seq, TAG_QUEUE_REFRESH, 0, 1));
        }
        let out = drain(&mut backlog);
        assert_eq!(out.len(), 11);
        assert!(
            out.iter()
                .filter(|e| e.tag == TAG_STATE_CHANGE)
                .all(|e| e.seq >= 990)
        );
        assert_eq!(backlog.take_coalesced(), 2000 - 11);
    }

    // Too many invalidations collapse into one invalidate-all marker.
    #[test]
    fn invalidations_collapse() {
        let mut backlog = Backlog::default();
        for seq in 0..300 {
            backlog.push(event(seq, TAG_CACHE_INVALIDATE, 0, 1 + 6 * 4));
        }
        let out = drain(&mut backlog);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].tag, TAG_CACHE_INVALIDATE_ALL);
    }

    // A flood stays within the limits, drops push data first and signals invalidate-all.
    #[test]
    fn flood_stays_bounded() {
        let mut backlog = Backlog::default();
        for seq in 0..400_000u64 {
            let tag = match seq % 3 {
                0 => TAG_EMAIL_PUSH,
                _ => TAG_STATE_CHANGE,
            };
            backlog.push(event(seq, tag, seq as u32, 12));
            assert!(backlog.len() <= MAX_PENDING_EVENTS);
            assert!(backlog.bytes() <= MAX_PENDING_BYTES);
        }
        assert!(backlog.take_dropped() > 0);
        let out = drain(&mut backlog);
        assert_eq!(out[0].tag, TAG_CACHE_INVALIDATE_ALL);
        assert!(out.iter().all(|e| e.tag != TAG_EMAIL_PUSH));
    }
}
