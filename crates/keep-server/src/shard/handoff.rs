/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;

use keep_cluster::{PeerFrame, SlotSet};
use keep_engine::{ExportCursor, KeepEngine, Pinned, Record, SetMode, Time};
use keep_proto::{Decoded, Frame, Slot};
use keep_rt::Ctx;

use super::forward::Msg;
use super::route::{Placement, Router};

const WALK_STEP: usize = 256;

const WALKS_PER_TURN: usize = 4;

pub const EXPORT_CHUNK: usize = 256 << 10;

const UNMOVED_MIN: usize = 64 << 10;

const IMPORT_STEP: usize = 512;

pub const SLOT_TAG: usize = 2;

#[derive(Debug)]
pub enum HandoffMsg {
    Export {
        id: u32,
        batch: u32,
        slots: Arc<SlotSet>,
    },
    More {
        id: u32,
        buf: Vec<u8>,
    },
    Chunk(ExportChunk),
    Cancel(u32),
    Import(Vec<u8>),
    Imported(Vec<u8>),
    Sync(u32),
    Synced(u32),
    Purge(Arc<SlotSet>),
    Purged,
}

#[derive(Debug)]
pub struct ExportChunk {
    pub id: u32,
    pub bytes: Vec<u8>,
    pub count: u32,
    pub done: bool,
}

struct Walk {
    id: u32,
    batch: u32,
    slots: Arc<SlotSet>,
    cursor: ExportCursor,
    buf: Option<Vec<u8>>,
    count: u32,
}

pub(super) struct ShardHandoff {
    control: usize,
    walks: Vec<Walk>,
    imports: VecDeque<Vec<u8>>,
    offset: usize,
    syncs: Vec<u32>,
    purge: Option<(Arc<SlotSet>, ExportCursor)>,
    outgoing: VecDeque<HandoffMsg>,
}

impl ShardHandoff {
    pub(super) fn new(control: usize) -> ShardHandoff {
        ShardHandoff {
            control,
            walks: Vec::new(),
            imports: VecDeque::new(),
            offset: 0,
            syncs: Vec::new(),
            purge: None,
            outgoing: VecDeque::new(),
        }
    }

    pub(super) fn message(&mut self, msg: HandoffMsg) {
        match msg {
            HandoffMsg::Export { id, batch, slots } => self.walks.push(Walk {
                id,
                batch,
                slots,
                cursor: ExportCursor::default(),
                buf: Some(Vec::with_capacity(EXPORT_CHUNK)),
                count: 0,
            }),
            HandoffMsg::More { id, buf } => {
                if let Some(walk) = self.walks.iter_mut().find(|walk| walk.id == id) {
                    walk.buf = Some(buf);
                }
            }
            HandoffMsg::Cancel(id) => self.walks.retain(|walk| walk.id != id),
            HandoffMsg::Import(buf) => self.imports.push_back(buf),
            HandoffMsg::Sync(token) => self.syncs.push(token),
            HandoffMsg::Purge(slots) => {
                let mut start = self.offset;
                for buf in &mut self.imports {
                    drop_slots(buf, start, &slots);
                    start = 0;
                }
                self.purge = Some((slots, ExportCursor::default()));
            }
            HandoffMsg::Chunk(_)
            | HandoffMsg::Imported(_)
            | HandoffMsg::Synced(_)
            | HandoffMsg::Purged => {}
        }
    }

    #[inline]
    pub(super) fn is_idle(&self) -> bool {
        self.purge.is_none()
            && self.imports.is_empty()
            && self.syncs.is_empty()
            && self.outgoing.is_empty()
            && self.walks.iter().all(|walk| walk.buf.is_none())
    }

    pub(super) fn step(
        &mut self,
        engine: &mut KeepEngine,
        router: &Router,
        placement: &mut Placement,
        now: Time,
        cx: &mut Ctx<'_, Msg>,
    ) {
        self.retry(cx);
        if let Some((slots, cursor)) = &mut self.purge {
            let doomed = |key: &[u8]| slots.contains(router.hash(key).slot());
            if !engine.purge_where(cursor, WALK_STEP, doomed) {
                self.purge = None;
                self.send(cx, HandoffMsg::Purged);
            }
        }
        if self.purge.is_none() {
            self.import(engine, placement, now, cx);
        }
        if self.imports.is_empty() {
            for token in mem::take(&mut self.syncs) {
                self.send(cx, HandoffMsg::Synced(token));
            }
        }
        self.export(engine, router, now, cx);
        if !self.is_idle() {
            cx.wake_at(cx.now());
        }
    }

    fn import(
        &mut self,
        engine: &mut KeepEngine,
        placement: &mut Placement,
        now: Time,
        cx: &mut Ctx<'_, Msg>,
    ) {
        if self.imports.is_empty() {
            return;
        }
        placement.refresh();
        let mut left = IMPORT_STEP;
        while left > 0 {
            let Some(buf) = self.imports.front() else {
                return;
            };
            match next_import(buf.get(self.offset..).unwrap_or_default()) {
                Some((slot, frame, len)) => {
                    if let Ok(PeerFrame::Entry { record, .. }) = PeerFrame::parse(&frame)
                        && placement.importable(slot)
                    {
                        engine.import(record, SetMode::IfAbsent, now);
                    }
                    self.offset += len;
                    left -= 1;
                }
                None => {
                    self.offset = 0;
                    if let Some(mut buf) = self.imports.pop_front() {
                        buf.clear();
                        self.send(cx, HandoffMsg::Imported(buf));
                    }
                }
            }
        }
    }

    fn export(&mut self, engine: &KeepEngine, router: &Router, now: Time, cx: &mut Ctx<'_, Msg>) {
        let mut index = 0;
        while let Some(walk) = self.walks.get_mut(index) {
            let Walk {
                batch,
                slots,
                cursor,
                buf: Some(buf),
                count,
                ..
            } = walk
            else {
                index += 1;
                continue;
            };
            let mut more = true;
            for _ in 0..WALKS_PER_TURN {
                more = engine.export(
                    cursor,
                    WALK_STEP,
                    now,
                    |key| slots.contains(router.hash(key).slot()),
                    |record| {
                        if moves(&record)
                            && (PeerFrame::Entry {
                                batch: *batch,
                                record,
                            })
                            .encode(buf)
                        {
                            *count += 1;
                        }
                    },
                );
                if !more || buf.len() >= EXPORT_CHUNK {
                    break;
                }
            }
            if more && buf.len() < EXPORT_CHUNK {
                index += 1;
                continue;
            }
            let chunk = HandoffMsg::Chunk(ExportChunk {
                id: walk.id,
                bytes: walk.buf.take().unwrap_or_default(),
                count: mem::take(&mut walk.count),
                done: !more,
            });
            self.send(cx, chunk);
            if more {
                index += 1;
            } else {
                self.walks.swap_remove(index);
            }
        }
    }

    fn send(&mut self, cx: &mut Ctx<'_, Msg>, msg: HandoffMsg) {
        if self.outgoing.is_empty() {
            if let Err(Msg::Handoff(msg)) = cx.mesh().send(self.control, Msg::Handoff(msg)) {
                self.outgoing.push_back(msg);
            }
        } else {
            self.outgoing.push_back(msg);
        }
    }

    fn retry(&mut self, cx: &mut Ctx<'_, Msg>) {
        while let Some(msg) = self.outgoing.pop_front() {
            if let Err(Msg::Handoff(msg)) = cx.mesh().send(self.control, Msg::Handoff(msg)) {
                self.outgoing.push_front(msg);
                return;
            }
        }
    }
}

fn moves(record: &Record<'_>) -> bool {
    record.value.len() < UNMOVED_MIN || Pinned::STALWART.contains(record.key)
}

fn drop_slots(buf: &mut Vec<u8>, start: usize, slots: &SlotSet) {
    let mut read = start;
    let mut write = start;
    while let Some((slot, _, len)) = buf.get(read..).and_then(next_import) {
        let end = read + len;
        if !slots.contains(slot) {
            if write != read {
                buf.copy_within(read..end, write);
            }
            write += len;
        }
        read = end;
    }
    buf.truncate(write);
}

fn next_import(bytes: &[u8]) -> Option<(Slot, Frame<'_>, usize)> {
    let ([s0, s1], rest) = bytes.split_first_chunk::<SLOT_TAG>()?;
    let slot = Slot::new(u16::from_le_bytes([*s0, *s1]))?;
    match Frame::decode(rest) {
        Ok(Decoded::Complete { frame, len }) => Some((slot, frame, SLOT_TAG + len)),
        _ => None,
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-server/shard/handoff.rs"]
mod tests;
