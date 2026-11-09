/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::collections::VecDeque;
use std::fmt;
use std::io;

use crate::op::{OpKind, Outcome, Request};
use crate::protocol::{Matching, Protocol, ProtocolError, resp};
use crate::stats::Recorder;

const READ_CHUNK: usize = 16 << 10;
const INITIAL_QUEUE: usize = 64;
const MAX_WRITE_BACKLOG: usize = 256 << 20;
const FLAG_ERROR: u8 = 1;
const FLAG_FINAL_DEL: u8 = 2;

#[derive(Clone, Copy, Debug)]
pub struct InFlight {
    pub intended_ns: u64,
    pub lag_ns: u32,
    pub kind: OpKind,
    pub parts: u8,
    pub phase: u8,
    pub flags: u8,
}

const _: () = assert!(size_of::<InFlight>() == 16);

#[derive(Debug)]
pub enum ConnError {
    Io(io::Error),
    Closed,
    Protocol(ProtocolError),
    Backlog,
    Tls(rustls::Error),
}

impl fmt::Display for ConnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnError::Io(e) => write!(f, "i/o error: {e}"),
            ConnError::Closed => f.write_str("connection closed by peer"),
            ConnError::Protocol(e) => write!(f, "protocol error: {e}"),
            ConnError::Backlog => f.write_str("write backlog above 256 MiB"),
            ConnError::Tls(e) => write!(f, "tls: {e}"),
        }
    }
}

impl From<ProtocolError> for ConnError {
    fn from(e: ProtocolError) -> Self {
        ConnError::Protocol(e)
    }
}

impl From<io::Error> for ConnError {
    fn from(e: io::Error) -> Self {
        ConnError::Io(e)
    }
}

struct IdSlab {
    slots: Vec<InFlight>,
    free: Vec<u32>,
    new: Vec<u32>,
}

impl IdSlab {
    fn insert(&mut self, entry: InFlight) -> u32 {
        let id = match self.free.pop() {
            Some(id) => {
                self.slots[id as usize] = entry;
                id
            }
            None => {
                self.slots.push(entry);
                u32::try_from(self.slots.len() - 1).unwrap_or(u32::MAX)
            }
        };
        self.new.push(id);
        id
    }

    fn next_id(&self) -> u32 {
        self.free
            .last()
            .copied()
            .unwrap_or_else(|| u32::try_from(self.slots.len()).unwrap_or(u32::MAX))
    }

    fn get_mut(&mut self, id: u32) -> Option<&mut InFlight> {
        self.slots
            .get_mut(id as usize)
            .filter(|entry| entry.parts > 0)
    }

    fn remove(&mut self, id: u32) {
        if let Some(entry) = self.slots.get_mut(id as usize) {
            entry.parts = 0;
            self.free.push(id);
        }
    }

    fn drain(&mut self) -> impl Iterator<Item = InFlight> + '_ {
        self.free.clear();
        self.new.clear();
        self.slots.drain(..).filter(|entry| entry.parts > 0)
    }
}

enum Pending {
    Fifo {
        queue: VecDeque<InFlight>,
        new: usize,
    },
    Ids(IdSlab),
}

pub struct Conn {
    id: u32,
    wbuf: Vec<u8>,
    wpos: usize,
    rbuf: Vec<u8>,
    rpos: usize,
    rlen: usize,
    pending: Pending,
    scans: VecDeque<Box<[u8]>>,
    dirty: bool,
}

impl Conn {
    pub fn new(matching: Matching, id: u32) -> Self {
        let pending = match matching {
            Matching::Fifo => Pending::Fifo {
                queue: VecDeque::with_capacity(INITIAL_QUEUE),
                new: 0,
            },
            Matching::Id => Pending::Ids(IdSlab {
                slots: Vec::with_capacity(INITIAL_QUEUE),
                free: Vec::new(),
                new: Vec::new(),
            }),
        };
        Conn {
            id,
            wbuf: Vec::with_capacity(READ_CHUNK / 4),
            wpos: 0,
            rbuf: vec![0; READ_CHUNK * 2],
            rpos: 0,
            rlen: 0,
            pending,
            scans: VecDeque::new(),
            dirty: false,
        }
    }

    pub fn in_flight(&self) -> usize {
        match &self.pending {
            Pending::Fifo { queue, .. } => queue.len(),
            Pending::Ids(slab) => slab.slots.len() - slab.free.len(),
        }
    }

    pub fn mark_dirty(&mut self) -> bool {
        !std::mem::replace(&mut self.dirty, true)
    }

    #[inline]
    pub fn push(
        &mut self,
        protocol: &Protocol,
        req: &Request<'_>,
        intended_ns: u64,
        phase: u8,
    ) -> Result<(), ConnError> {
        if self.wbuf.len() > MAX_WRITE_BACKLOG {
            return Err(ConnError::Backlog);
        }
        let id = match &self.pending {
            Pending::Fifo { .. } => 0,
            Pending::Ids(slab) => slab.next_id(),
        };
        let parts = protocol.encode(req, id, &mut self.wbuf)?;
        let entry = InFlight {
            intended_ns,
            lag_ns: 0,
            kind: req.kind,
            parts,
            phase,
            flags: 0,
        };
        match &mut self.pending {
            Pending::Fifo { queue, new } => {
                queue.push_back(entry);
                *new += 1;
            }
            Pending::Ids(slab) => {
                slab.insert(entry);
            }
        }
        if req.kind == OpKind::DeletePrefix && protocol.matching() == Matching::Fifo {
            self.scans.push_back(req.key.into());
        }
        Ok(())
    }

    #[inline]
    pub fn stamp_new(&mut self, now: u64, rec: &mut Recorder) {
        self.dirty = false;
        match &mut self.pending {
            Pending::Fifo { queue, new } => {
                queue
                    .iter_mut()
                    .rev()
                    .take(std::mem::take(new))
                    .for_each(|entry| stamp(entry, now, rec));
            }
            Pending::Ids(slab) => {
                let IdSlab { slots, new, .. } = slab;
                for id in new.drain(..) {
                    if let Some(entry) = slots.get_mut(id as usize) {
                        stamp(entry, now, rec);
                    }
                }
            }
        }
    }

    pub fn clear_dirty(&mut self) {
        self.dirty = false;
    }

    #[inline]
    pub fn unsent(&self) -> Option<&[u8]> {
        self.wbuf.get(self.wpos..).filter(|rest| !rest.is_empty())
    }

    #[inline]
    pub fn consume_sent(&mut self, n: usize) {
        self.wpos += n;
        if self.wpos >= self.wbuf.len() {
            self.wbuf.clear();
            self.wpos = 0;
        }
    }

    #[inline]
    pub fn swap_unsent(&mut self, empty: &mut Vec<u8>) {
        empty.clear();
        if self.wpos > 0 {
            empty.extend_from_slice(self.wbuf.get(self.wpos..).unwrap_or_default());
            self.wbuf.clear();
        } else {
            std::mem::swap(&mut self.wbuf, empty);
        }
        self.wpos = 0;
    }

    pub fn has_unsent(&self) -> bool {
        self.wpos < self.wbuf.len()
    }

    #[inline]
    pub fn read_space(&mut self) -> &mut [u8] {
        self.make_room(READ_CHUNK);
        self.rbuf.get_mut(self.rlen..).unwrap_or_default()
    }

    #[inline]
    pub fn received(
        &mut self,
        n: usize,
        protocol: &Protocol,
        now: u64,
        rec: &mut Recorder,
    ) -> Result<(), ConnError> {
        self.rlen = (self.rlen + n).min(self.rbuf.len());
        self.process(protocol, now, rec)
    }

    #[inline]
    pub fn on_data(
        &mut self,
        data: &[u8],
        protocol: &Protocol,
        now: u64,
        rec: &mut Recorder,
    ) -> Result<(), ConnError> {
        self.make_room(data.len());
        if let Some(spare) = self.rbuf.get_mut(self.rlen..self.rlen + data.len()) {
            spare.copy_from_slice(data);
            self.rlen += data.len();
        }
        self.process(protocol, now, rec)
    }

    fn make_room(&mut self, want: usize) {
        if self.rpos == self.rlen {
            self.rpos = 0;
            self.rlen = 0;
        }
        if self.rbuf.len() - self.rlen >= want {
            return;
        }
        if self.rpos > 0 {
            self.rbuf.copy_within(self.rpos..self.rlen, 0);
            self.rlen -= self.rpos;
            self.rpos = 0;
        }
        if self.rbuf.len() - self.rlen < want {
            let grown = (self.rbuf.len() * 2).max(self.rlen + want);
            self.rbuf.resize(grown, 0);
        }
    }

    #[inline]
    fn process(
        &mut self,
        protocol: &Protocol,
        now: u64,
        rec: &mut Recorder,
    ) -> Result<(), ConnError> {
        while let Some(buf) = self
            .rbuf
            .get(self.rpos..self.rlen)
            .filter(|buf| !buf.is_empty())
        {
            let expect = match &self.pending {
                Pending::Fifo { queue, .. } => queue.front().map(|entry| {
                    (
                        entry.kind,
                        entry.parts == 1 && entry.flags & FLAG_FINAL_DEL == 0,
                    )
                }),
                Pending::Ids(_) => None,
            };
            let Some(reply) = protocol.decode(buf, expect)? else {
                return Ok(());
            };
            self.rpos += reply.len;
            match &mut self.pending {
                Pending::Fifo { queue, .. } => {
                    let entry = queue.front_mut().ok_or(ProtocolError::UnexpectedReply)?;
                    if reply.outcome == Outcome::Error {
                        entry.flags |= FLAG_ERROR;
                    }
                    if entry.parts > 1 {
                        entry.parts -= 1;
                        continue;
                    }
                    if let Some((cursor, keys_start)) = reply.cursor {
                        let mut entry = *entry;
                        queue.pop_front();
                        let prefix = self.scans.pop_front().unwrap_or_default();
                        let parts = resp::encode_scan_step(
                            buf,
                            keys_start,
                            cursor,
                            &prefix,
                            &mut self.wbuf,
                        )?;
                        if parts > 0 {
                            entry.parts = parts;
                            if cursor == 0 {
                                entry.flags |= FLAG_FINAL_DEL;
                            } else {
                                self.scans.push_back(prefix);
                            }
                            queue.push_back(entry);
                            continue;
                        }
                        rec.complete(&entry, outcome_of(&entry, reply.outcome), now, self.id);
                        continue;
                    }
                    let entry = *entry;
                    queue.pop_front();
                    rec.complete(&entry, outcome_of(&entry, reply.outcome), now, self.id);
                }
                Pending::Ids(slab) => {
                    let entry = slab.get_mut(reply.id).ok_or(ProtocolError::UnexpectedId)?;
                    if reply.outcome == Outcome::Error {
                        entry.flags |= FLAG_ERROR;
                    }
                    if entry.parts > 1 {
                        entry.parts -= 1;
                        continue;
                    }
                    let entry = *entry;
                    slab.remove(reply.id);
                    rec.complete(&entry, outcome_of(&entry, reply.outcome), now, self.id);
                }
            }
        }
        Ok(())
    }

    pub fn fail_all(&mut self, rec: &mut Recorder) {
        match &mut self.pending {
            Pending::Fifo { queue, new } => {
                *new = 0;
                queue.drain(..).for_each(|entry| rec.fail(&entry));
            }
            Pending::Ids(slab) => slab.drain().for_each(|entry| rec.fail(&entry)),
        }
        self.scans.clear();
    }
}

#[inline]
fn stamp(entry: &mut InFlight, now: u64, rec: &mut Recorder) {
    let lag = now.saturating_sub(entry.intended_ns);
    entry.lag_ns = u32::try_from(lag).unwrap_or(u32::MAX);
    rec.lag(entry.phase, lag);
}

#[inline]
fn outcome_of(entry: &InFlight, last: Outcome) -> Outcome {
    if entry.flags & FLAG_ERROR != 0 {
        Outcome::Error
    } else {
        last
    }
}
