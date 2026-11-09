/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::mem;

use keep_proto::{EXT_WORD_LEN, HEADER_LEN, MAX_KEY_LEN, MAX_VALUE_LEN, Slot};
use keep_rt::ConnId;
use keep_rt::mesh::Outbox;
use keep_rt::slab::Handle;

use crate::stream;

use super::handoff::HandoffMsg;

const ENTRY_HEAD: usize = 14;
const LEN_AT: usize = 8;
const LEN_END: usize = 12;

const LARGEST_ENTRY: usize = ENTRY_HEAD
    + HEADER_LEN
    + u8::MAX as usize * EXT_WORD_LEN
    + MAX_KEY_LEN
    + MAX_VALUE_LEN as usize;

pub(super) const FORWARD_WINDOW: usize = 16 << 20;

const _: () = assert!(2 * LARGEST_ENTRY <= FORWARD_WINDOW);

const POOL_KEEP_CAPACITY: usize = 256 << 10;

#[derive(Default, Debug)]
pub struct Batch(Vec<u8>);

impl Batch {
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn push(&mut self, conn: ConnId, tag: u16, bytes: &[u8]) {
        self.0.reserve(ENTRY_HEAD + bytes.len());
        let at = self.open(conn, tag);
        self.0.extend_from_slice(bytes);
        self.close(at);
    }

    #[inline]
    pub fn open(&mut self, conn: ConnId, tag: u16) -> usize {
        let handle = conn.handle();
        let at = self.0.len();
        let index = handle.index().to_le_bytes();
        let generation = handle.generation().to_le_bytes();
        self.0.extend_from_slice(&index);
        self.0.extend_from_slice(&generation);
        self.0.extend_from_slice(&[0; 4]);
        self.0.extend_from_slice(&tag.to_le_bytes());
        at
    }

    #[inline]
    pub fn close(&mut self, at: usize) {
        let len = self.0.len().saturating_sub(at + ENTRY_HEAD);
        if let Some(field) = self.0.get_mut(at + LEN_AT..at + LEN_END) {
            field.copy_from_slice(&(len as u32).to_le_bytes());
        }
    }

    #[inline]
    pub fn bytes_mut(&mut self) -> &mut Vec<u8> {
        &mut self.0
    }

    pub fn entries(&self) -> Entries<'_> {
        Entries(&self.0)
    }

    fn reset(&mut self) {
        self.0.clear();
    }

    fn capacity(&self) -> usize {
        self.0.capacity()
    }
}

pub struct Entries<'a>(&'a [u8]);

impl<'a> Iterator for Entries<'a> {
    type Item = (ConnId, u16, &'a [u8]);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let (head, rest) = self.0.split_first_chunk::<ENTRY_HEAD>()?;
        let [i0, i1, i2, i3, g0, g1, g2, g3, l0, l1, l2, l3, t0, t1] = *head;
        let len = u32::from_le_bytes([l0, l1, l2, l3]) as usize;
        let (bytes, rest) = rest.split_at_checked(len)?;
        self.0 = rest;
        let handle = Handle::new(
            u32::from_le_bytes([i0, i1, i2, i3]),
            u32::from_le_bytes([g0, g1, g2, g3]),
        );
        Some((ConnId::new(handle), u16::from_le_bytes([t0, t1]), bytes))
    }
}

#[derive(Debug)]
pub struct Replies {
    pub batch: Batch,
    pub settled: usize,
    pub discarded: bool,
}

#[derive(Debug)]
pub enum Msg {
    Forward(Batch),
    Replies(Replies),
    Purge(Box<[u8]>),
    Drain,
    Freeze,
    Frozen,
    View,
    Stream(stream::Msg),
    Handoff(HandoffMsg),
}

const _: () = assert!(size_of::<Msg>() <= 48);

#[derive(Default)]
struct Lane {
    requests: Batch,
    replies: Batch,
    settled: usize,
    discarded: bool,
    in_flight: usize,
}

impl Lane {
    fn is_idle(&self) -> bool {
        self.requests.is_empty() && self.replies.is_empty()
    }
}

pub(super) struct Lanes {
    lanes: Box<[Lane]>,
    touched: Vec<usize>,
    pool: Vec<Batch>,
    pool_max: usize,
}

impl Lanes {
    pub(super) fn new(endpoints: usize) -> Lanes {
        Lanes {
            lanes: (0..endpoints).map(|_| Lane::default()).collect(),
            touched: Vec::with_capacity(endpoints),
            pool: Vec::new(),
            pool_max: 2 * endpoints + 4,
        }
    }

    #[inline]
    pub(super) fn admits(&self, target: usize, len: usize) -> bool {
        self.lanes.get(target).is_some_and(|lane| {
            lane.in_flight + lane.requests.len() + ENTRY_HEAD + len <= FORWARD_WINDOW
        })
    }

    #[inline]
    pub(super) fn forward(&mut self, target: usize, conn: ConnId, slot: Slot, frame: &[u8]) {
        if let Some(lane) = self.lanes.get_mut(target) {
            if lane.is_idle() {
                self.touched.push(target);
            }
            lane.requests.push(conn, slot.get(), frame);
        }
    }

    pub(super) fn replies(&mut self, target: usize, settled: usize) -> Option<&mut Batch> {
        let lane = self.lanes.get_mut(target)?;
        if lane.is_idle() {
            self.touched.push(target);
        }
        lane.settled += settled;
        Some(&mut lane.replies)
    }

    pub(super) fn discard_replies(&mut self) {
        for lane in &mut self.lanes {
            lane.discarded |= !lane.replies.is_empty();
        }
    }

    pub(super) fn settle(&mut self, from: usize, settled: usize) {
        if let Some(lane) = self.lanes.get_mut(from) {
            lane.in_flight = lane.in_flight.saturating_sub(settled);
        }
    }

    pub(super) fn recycle(&mut self, batch: Batch) {
        spare(&mut self.pool, self.pool_max, batch);
    }

    pub(super) fn flush(&mut self, outbox: &mut Outbox<Msg>) -> bool {
        let Lanes {
            lanes,
            touched,
            pool,
            pool_max,
        } = self;
        touched.retain(|&target| {
            lanes
                .get_mut(target)
                .is_some_and(|lane| !send(lane, target, outbox, pool, *pool_max))
        });
        touched.is_empty()
    }

    pub(super) fn flush_requests(&mut self, target: usize, outbox: &mut Outbox<Msg>) -> bool {
        let Lanes {
            lanes,
            touched,
            pool,
            pool_max,
        } = self;
        let Some(lane) = lanes.get_mut(target) else {
            return true;
        };
        let sent = send_requests(lane, target, outbox, pool, *pool_max);
        if sent && lane.is_idle() {
            touched.retain(|&lane| lane != target);
        }
        sent
    }
}

fn fresh(pool: &mut Vec<Batch>) -> Batch {
    pool.pop().unwrap_or_default()
}

fn spare(pool: &mut Vec<Batch>, pool_max: usize, mut batch: Batch) {
    if batch.capacity() <= POOL_KEEP_CAPACITY && pool.len() < pool_max {
        batch.reset();
        pool.push(batch);
    }
}

fn send(
    lane: &mut Lane,
    target: usize,
    outbox: &mut Outbox<Msg>,
    pool: &mut Vec<Batch>,
    pool_max: usize,
) -> bool {
    let mut sent = send_requests(lane, target, outbox, pool, pool_max);
    if !lane.replies.is_empty() {
        let replies = Replies {
            batch: mem::replace(&mut lane.replies, fresh(pool)),
            settled: mem::take(&mut lane.settled),
            discarded: mem::take(&mut lane.discarded),
        };
        if let Err(refused) = outbox.send(target, Msg::Replies(replies)) {
            if let Msg::Replies(replies) = refused {
                let empty = mem::replace(&mut lane.replies, replies.batch);
                spare(pool, pool_max, empty);
                lane.settled = replies.settled;
                lane.discarded = replies.discarded;
            }
            sent = false;
        }
    }
    sent
}

fn send_requests(
    lane: &mut Lane,
    target: usize,
    outbox: &mut Outbox<Msg>,
    pool: &mut Vec<Batch>,
    pool_max: usize,
) -> bool {
    if lane.requests.is_empty() {
        return true;
    }
    let batch = mem::replace(&mut lane.requests, fresh(pool));
    let len = batch.len();
    match outbox.send(target, Msg::Forward(batch)) {
        Ok(()) => {
            lane.in_flight += len;
            true
        }
        Err(refused) => {
            if let Msg::Forward(batch) = refused {
                let empty = mem::replace(&mut lane.requests, batch);
                spare(pool, pool_max, empty);
            }
            false
        }
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-server/shard/forward.rs"]
mod tests;
