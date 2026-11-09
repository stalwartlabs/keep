/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::mem;
use std::num::NonZeroU16;
use std::sync::Arc;

use keep_cluster::{Handshake, PeerFrame, PeerLink, SlotSet};
use keep_proto::{HEADER_LEN, KeyHash};
use keep_rt::clock::Nanos;
use keep_rt::{ConnId, Conns, Ctx};

use crate::shard::{EXPORT_CHUNK, ExportChunk, HandoffMsg, Msg, SLOT_TAG};

pub(super) const HANDSHAKE_FRAME: usize = HEADER_LEN + 64;

const IMPORT_CHUNK: usize = 256 << 10;

const IMPORT_WINDOW: usize = 8;

const EXPORT_LOW: usize = 2 * EXPORT_CHUNK;

pub struct PeerConn {
    pub(super) input: Vec<u8>,
    pub(super) stage: Stage,
    pub(super) seen: Nanos,
}

impl PeerConn {
    pub(super) fn new(stage: Stage, now: Nanos) -> PeerConn {
        PeerConn {
            input: Vec::new(),
            stage,
            seen: now,
        }
    }
}

pub(super) enum Stage {
    Dialing {
        member: usize,
        handshake: Box<Handshake>,
        queued: Queued,
    },
    Accepted {
        handshake: Box<Handshake>,
    },
    Puller {
        link: PeerLink,
    },
    Server {
        link: PeerLink,
        export: Option<u32>,
    },
}

impl Stage {
    pub(super) fn frame_limit(&self) -> usize {
        match self {
            Stage::Dialing { .. } | Stage::Accepted { .. } => HANDSHAKE_FRAME,
            Stage::Puller { .. } | Stage::Server { .. } => usize::MAX,
        }
    }
}

#[derive(Default)]
pub(super) struct Queued {
    pub(super) ack: Option<u32>,
    pub(super) pull: Option<(u32, SlotSet, (u32, u32))>,
}

impl Queued {
    pub(super) fn write(self, out: &mut Vec<u8>) {
        if let Some(batch) = self.ack {
            PeerFrame::Ack { batch }.encode(out);
        }
        if let Some((batch, slots, since)) = self.pull {
            PeerFrame::Pull {
                batch,
                slots,
                since,
            }
            .encode(out);
        }
    }
}

pub(super) struct Fault;

struct Export {
    id: u32,
    conn: ConnId,
    batch: u32,
    slots: Arc<SlotSet>,
    count: u32,
    running: usize,
    parked: Vec<(usize, Vec<u8>)>,
}

pub(super) struct Exports {
    list: Vec<Export>,
    cancelled: Vec<u32>,
    next: u32,
    shards: usize,
}

impl Exports {
    pub(super) fn new(shards: usize) -> Exports {
        Exports {
            list: Vec::new(),
            cancelled: Vec::new(),
            next: 1,
            shards,
        }
    }

    pub(super) fn start(
        &mut self,
        cx: &mut Ctx<'_, Msg>,
        conn: ConnId,
        batch: u32,
        slots: SlotSet,
    ) -> Option<u32> {
        let id = self.next;
        self.next = self.next.wrapping_add(1).max(1);
        let slots = Arc::new(slots);
        let mut running = 0;
        for shard in 0..self.shards {
            let start = HandoffMsg::Export {
                id,
                batch,
                slots: slots.clone(),
            };
            if cx.mesh().send(shard, Msg::Handoff(start)).is_ok() {
                running += 1;
            }
        }
        self.list.push(Export {
            id,
            conn,
            batch,
            slots,
            count: 0,
            running,
            parked: Vec::new(),
        });
        if running < self.shards {
            self.cancel(id);
            return None;
        }
        Some(id)
    }

    pub(super) fn cancel(&mut self, id: u32) {
        if let Some(index) = self.list.iter().position(|export| export.id == id) {
            self.list.swap_remove(index);
            self.cancelled.push(id);
        }
    }

    pub(super) fn running(&self) -> (usize, usize) {
        let slots = self.list.iter().map(|export| export.slots.len()).sum();
        (self.list.len(), slots)
    }

    pub(super) fn intersects(&self, slots: &SlotSet) -> bool {
        self.list
            .iter()
            .any(|export| export.slots.intersects(slots))
    }

    pub(super) fn chunk(
        &mut self,
        cx: &mut Ctx<'_, Msg>,
        conns: &mut Conns<'_, PeerConn>,
        shard: usize,
        chunk: ExportChunk,
    ) {
        let ExportChunk {
            id,
            bytes: mut chunk,
            count,
            done,
        } = chunk;
        let Some(index) = self.list.iter().position(|export| export.id == id) else {
            return;
        };
        let Some(export) = self.list.get_mut(index) else {
            return;
        };
        let Some(io) = conns.get(export.conn) else {
            self.cancel(id);
            return;
        };
        io.out.extend_from_slice(&chunk);
        io.state.seen = cx.now();
        export.count = export.count.wrapping_add(count);
        if done {
            export.running = export.running.saturating_sub(1);
            if export.running == 0 {
                PeerFrame::End {
                    batch: export.batch,
                    count: export.count,
                }
                .encode(io.out);
                if let Stage::Server {
                    export: running, ..
                } = &mut io.state.stage
                {
                    *running = None;
                }
                self.list.swap_remove(index);
            }
            return;
        }
        chunk.clear();
        if io.pending() < EXPORT_LOW {
            let more = HandoffMsg::More { id, buf: chunk };
            if cx.mesh().send(shard, Msg::Handoff(more)).is_err() {
                conns.close(export.conn);
                self.cancel(id);
            }
        } else {
            export.parked.push((shard, chunk));
        }
    }

    pub(super) fn advance(&mut self, cx: &mut Ctx<'_, Msg>, conns: &mut Conns<'_, PeerConn>) {
        for id in self.cancelled.drain(..) {
            for shard in 0..self.shards {
                let _ = cx.mesh().send(shard, Msg::Handoff(HandoffMsg::Cancel(id)));
            }
        }
        let mut refused = None;
        for export in self
            .list
            .iter_mut()
            .filter(|export| !export.parked.is_empty())
        {
            let Some(io) = conns.get(export.conn) else {
                refused = Some(export.id);
                continue;
            };
            if io.pending() >= EXPORT_LOW {
                continue;
            }
            for (shard, buf) in export.parked.drain(..) {
                let more = HandoffMsg::More { id: export.id, buf };
                if cx.mesh().send(shard, Msg::Handoff(more)).is_err() {
                    refused = Some(export.id);
                }
            }
        }
        if let Some(id) = refused
            && let Some(export) = self.list.iter().find(|export| export.id == id)
        {
            conns.close(export.conn);
            self.cancel(id);
        }
    }
}

struct Ending {
    token: u32,
    from: usize,
    batch: u32,
    count: u32,
    waiting: usize,
}

pub(super) struct Ended {
    pub(super) from: usize,
    pub(super) batch: u32,
    pub(super) count: u32,
}

pub(super) struct Imports {
    filling: Box<[Vec<u8>]>,
    queued: Box<[usize]>,
    pool: Vec<Vec<u8>>,
    endings: Vec<Ending>,
    held: Vec<ConnId>,
    next: u32,
    shards: NonZeroU16,
}

impl Imports {
    pub(super) fn new(shards: NonZeroU16) -> Imports {
        let count = usize::from(shards.get());
        Imports {
            filling: (0..count).map(|_| Vec::new()).collect(),
            queued: vec![0; count].into_boxed_slice(),
            pool: Vec::new(),
            endings: Vec::new(),
            held: Vec::new(),
            next: 1,
            shards,
        }
    }

    pub(super) fn push(&mut self, cx: &mut Ctx<'_, Msg>, hash: KeyHash, raw: &[u8]) -> bool {
        let shard = usize::from(hash.shard(self.shards));
        let Some(buf) = self.filling.get_mut(shard) else {
            return true;
        };
        buf.reserve(SLOT_TAG + raw.len());
        buf.extend_from_slice(&hash.slot().get().to_le_bytes());
        buf.extend_from_slice(raw);
        buf.len() < IMPORT_CHUNK || self.flush(cx, shard)
    }

    pub(super) fn flush(&mut self, cx: &mut Ctx<'_, Msg>, shard: usize) -> bool {
        let (Some(buf), Some(queued)) = (self.filling.get_mut(shard), self.queued.get_mut(shard))
        else {
            return true;
        };
        if buf.is_empty() {
            return true;
        }
        let full = mem::take(buf);
        match cx
            .mesh()
            .send(shard, Msg::Handoff(HandoffMsg::Import(full)))
        {
            Ok(()) => {
                *queued += 1;
                *buf = self.pool.pop().unwrap_or_default();
                true
            }
            Err(refused) => {
                if let Msg::Handoff(HandoffMsg::Import(full)) = refused {
                    *buf = full;
                }
                false
            }
        }
    }

    pub(super) fn full(&self) -> bool {
        self.queued.iter().any(|&queued| queued >= IMPORT_WINDOW)
    }

    pub(super) fn hold(&mut self, conn: ConnId) {
        if !self.held.contains(&conn) {
            self.held.push(conn);
        }
    }

    pub(super) fn end(
        &mut self,
        cx: &mut Ctx<'_, Msg>,
        from: usize,
        batch: u32,
        count: u32,
    ) -> bool {
        let token = self.next;
        self.next = self.next.wrapping_add(1).max(1);
        let shards = self.filling.len();
        let mut sent = true;
        for shard in 0..shards {
            sent &= self.flush(cx, shard);
            sent &= cx
                .mesh()
                .send(shard, Msg::Handoff(HandoffMsg::Sync(token)))
                .is_ok();
        }
        if sent {
            self.endings.push(Ending {
                token,
                from,
                batch,
                count,
                waiting: shards,
            });
        }
        sent
    }

    pub(super) fn imported(
        &mut self,
        conns: &mut Conns<'_, PeerConn>,
        shard: usize,
        mut buf: Vec<u8>,
    ) {
        if let Some(queued) = self.queued.get_mut(shard) {
            *queued = queued.saturating_sub(1);
        }
        if self.pool.len() < self.filling.len() && buf.capacity() <= 2 * IMPORT_CHUNK {
            buf.clear();
            self.pool.push(buf);
        }
        if !self.full() {
            for conn in self.held.drain(..) {
                conns.hold(conn, false);
            }
        }
    }

    pub(super) fn synced(&mut self, token: u32) -> Option<Ended> {
        let index = self
            .endings
            .iter()
            .position(|ending| ending.token == token)?;
        let ending = self.endings.get_mut(index)?;
        ending.waiting = ending.waiting.saturating_sub(1);
        if ending.waiting > 0 {
            return None;
        }
        let ending = self.endings.swap_remove(index);
        Some(Ended {
            from: ending.from,
            batch: ending.batch,
            count: ending.count,
        })
    }

    pub(super) fn forget(&mut self, conn: ConnId) {
        self.held.retain(|held| *held != conn);
    }
}
