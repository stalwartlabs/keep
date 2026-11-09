/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use foldhash::fast::RandomState;
use keep_proto::Topic;
use keep_rt::mesh::Outbox;

use super::hub::Hub;
use super::records::{Buckets, Records};
use super::{Msg as StreamMsg, WINDOW};
use crate::shard::Msg;

#[derive(Debug, Default)]
struct Lane {
    sent: u64,
    lost: HashMap<Topic, u32, RandomState>,
    marker: bool,
}

impl Lane {
    fn owes(&self) -> bool {
        self.marker || !self.lost.is_empty()
    }

    #[cold]
    fn lose(&mut self, records: &Records, hub: &Hub, target: usize) {
        for run in records.runs() {
            if hub.wants(target, &Buckets::of(run.topic)) {
                let lost = self.lost.entry(run.topic).or_default();
                *lost = lost.wrapping_add(run.count);
            }
        }
    }

    #[cold]
    fn send_lost(&mut self, target: usize, outbox: &mut Outbox<Msg>) -> bool {
        let lost: Box<[(Topic, u32)]> = self.lost.drain().collect();
        match outbox.send(target, Msg::Stream(StreamMsg::Skipped(lost))) {
            Ok(()) => true,
            Err(refused) => {
                if let Msg::Stream(StreamMsg::Skipped(lost)) = refused {
                    self.lost.extend(lost);
                }
                false
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct Lanes {
    me: usize,
    lanes: Box<[Lane]>,
    owing: usize,
}

impl Lanes {
    pub(super) fn new(me: usize, shards: usize) -> Lanes {
        Lanes {
            me,
            lanes: (0..shards).map(|_| Lane::default()).collect(),
            owing: 0,
        }
    }

    pub(super) fn owing(&self) -> bool {
        self.owing != 0
    }

    pub(super) fn seal(&mut self) {
        let me = self.me;
        for (target, lane) in self.lanes.iter_mut().enumerate() {
            lane.marker = target != me;
        }
        self.recount();
    }

    pub(super) fn flush(
        &mut self,
        turn: &mut Records,
        hub: &Hub,
        outbox: &mut Outbox<Msg>,
    ) -> bool {
        if turn.is_empty() && self.owing == 0 {
            return false;
        }
        let me = self.me;
        let events = !turn.is_empty();
        let buckets = *turn.buckets();
        let mut shared: Option<Arc<Records>> = None;
        for (target, lane) in self.lanes.iter_mut().enumerate() {
            let wanted = events && target != me && hub.wants(target, &buckets);
            if !wanted && !lane.owes() {
                continue;
            }
            let lag = lane.sent.wrapping_sub(hub.consumed(target, me));
            let open = lag <= WINDOW as u64;
            let mut clear = lane.lost.is_empty() || (open && lane.send_lost(target, outbox));
            if wanted {
                let sent = clear && open && {
                    let records = shared.get_or_insert_with(|| Arc::new(mem::take(turn)));
                    let len = records.len() as u64;
                    let sent = outbox
                        .send(target, Msg::Stream(StreamMsg::Events(records.clone())))
                        .is_ok();
                    if sent {
                        lane.sent = lane.sent.wrapping_add(len);
                    }
                    sent
                };
                if !sent {
                    lane.lose(shared.as_deref().unwrap_or(&*turn), hub, target);
                    clear = false;
                }
            }
            if clear && lane.marker && outbox.send(target, Msg::Stream(StreamMsg::Marker)).is_ok() {
                lane.marker = false;
            }
        }
        if shared.is_none() {
            turn.clear();
        }
        self.recount();
        self.owing != 0
    }

    fn recount(&mut self) {
        self.owing = self.lanes.iter().filter(|lane| lane.owes()).count();
    }
}
