/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::io;
use std::net::SocketAddr;
use std::os::fd::OwnedFd;

use crate::clock::{self, Nanos};
use crate::mesh::Outbox;
use crate::slab::{Handle, Slab};
use crate::tls::Tls;
#[cfg(target_os = "linux")]
use crate::tls::TlsConfig;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct ConnId(Handle);

impl ConnId {
    pub const fn new(handle: Handle) -> ConnId {
        ConnId(handle)
    }

    #[inline]
    pub const fn handle(self) -> Handle {
        self.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Flow {
    Continue,
    Drain,
    Close,
    Hold,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Flush {
    Send,
    Discard,
}

pub(crate) const QUEUED: u8 = 1;
pub(crate) const DRAIN: u8 = 2;
pub(crate) const CLOSE: u8 = 4;
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const PAUSED: u8 = 8;
pub(crate) const HELD: u8 = 16;
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const LINGER: u8 = 32;

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) enum Dialer<'a> {
    #[cfg(target_os = "linux")]
    Epoll(&'a OwnedFd, Option<&'a TlsConfig>),
    #[cfg(any(keep_tests, not(target_os = "linux")))]
    Record(&'a mut Vec<(ConnId, SocketAddr)>),
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn earliest(a: Option<Nanos>, b: Option<Nanos>) -> Option<Nanos> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn discard<C>(conns: &mut Slab<Conn<C>>, dirty: &[u32]) {
    for &index in dirty {
        if let Some(conn) = conns.get_mut(index)
            && conn.out.len() > conn.mark
        {
            conn.out.truncate(conn.mark.max(conn.sent));
            conn.flags |= DRAIN;
        }
    }
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub struct Conn<C> {
    pub(crate) id: ConnId,
    pub(crate) fd: Option<OwnedFd>,
    pub(crate) state: C,
    pub(crate) out: Vec<u8>,
    pub(crate) sent: usize,
    pub(crate) mark: usize,
    pub(crate) flags: u8,
    pub(crate) interest: u32,
    pub(crate) tls: Option<Box<Tls>>,
}

const _: () = assert!(size_of::<Conn<()>>() <= 72);

impl<C> Conn<C> {
    pub(crate) fn new(
        id: ConnId,
        fd: Option<OwnedFd>,
        state: C,
        interest: u32,
        tls: Option<Box<Tls>>,
    ) -> Conn<C> {
        Conn {
            id,
            fd,
            state,
            out: Vec::new(),
            sent: 0,
            mark: 0,
            flags: 0,
            interest,
            tls,
        }
    }

    #[inline]
    pub(crate) fn backlog(&self) -> usize {
        self.tls.as_deref().map_or(0, Tls::backlog)
    }

    #[inline]
    pub(crate) fn touch(&mut self, index: u32, mark: usize, dirty: &mut Vec<u32>) {
        if self.flags & QUEUED == 0 {
            self.flags |= QUEUED;
            self.mark = mark;
            dirty.push(index);
        }
    }

    pub(crate) fn io(&mut self) -> Io<'_, C> {
        Io {
            id: self.id,
            sent: self.sent,
            backlog: self.backlog(),
            state: &mut self.state,
            out: &mut self.out,
        }
    }
}

pub struct Io<'a, C> {
    pub id: ConnId,
    sent: usize,
    backlog: usize,
    pub state: &'a mut C,
    pub out: &'a mut Vec<u8>,
}

impl<C> Io<'_, C> {
    #[inline]
    pub fn pending(&self) -> usize {
        self.out.len().saturating_sub(self.sent) + self.backlog
    }
}

pub struct Conns<'a, C> {
    slab: &'a mut Slab<Conn<C>>,
    dirty: &'a mut Vec<u32>,
    dialer: Dialer<'a>,
}

impl<'a, C> Conns<'a, C> {
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) fn new(
        slab: &'a mut Slab<Conn<C>>,
        dirty: &'a mut Vec<u32>,
        dialer: Dialer<'a>,
    ) -> Conns<'a, C> {
        Conns {
            slab,
            dirty,
            dialer,
        }
    }

    pub fn connect(&mut self, addr: SocketAddr, state: C) -> io::Result<ConnId> {
        let handle = self
            .slab
            .next_handle()
            .ok_or_else(|| io::Error::other("no free connection slot"))?;
        let id = ConnId::new(handle);
        let (fd, interest, tls) = match &mut self.dialer {
            #[cfg(target_os = "linux")]
            Dialer::Epoll(epoll, tls) => {
                let tls = match tls {
                    Some(config) => Some(
                        Tls::client(config, addr)
                            .ok_or_else(|| io::Error::other("no TLS client configuration"))?,
                    ),
                    None => None,
                };
                let (fd, interest) = crate::shard::dial(epoll, addr, handle)?;
                (Some(fd), interest, tls)
            }
            #[cfg(any(keep_tests, not(target_os = "linux")))]
            Dialer::Record(dials) => {
                dials.push((id, addr));
                (None, 0, None)
            }
        };
        let index = self
            .slab
            .insert(Conn::new(id, fd, state, interest, tls))
            .ok_or_else(|| io::Error::other("no free connection slot"))?;
        if let Some(conn) = self.slab.get_mut_at(index) {
            conn.touch(index.index(), 0, self.dirty);
        }
        Ok(id)
    }

    pub fn hold(&mut self, id: ConnId, held: bool) {
        if let Some(conn) = self.slab.get_mut_at(id.0) {
            if held {
                conn.flags |= HELD;
            } else {
                conn.flags &= !HELD;
            }
            conn.touch(id.0.index(), conn.out.len(), self.dirty);
        }
    }

    pub fn is_held(&self, id: ConnId) -> bool {
        self.slab
            .get_at(id.0)
            .is_some_and(|conn| conn.flags & HELD != 0)
    }

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) fn parts(&mut self) -> (&mut Slab<Conn<C>>, &mut Vec<u32>) {
        (self.slab, self.dirty)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn accepting(&mut self) -> (&mut Slab<Conn<C>>, Option<&'a TlsConfig>) {
        let tls = match self.dialer {
            Dialer::Epoll(_, tls) => tls,
            #[cfg(keep_tests)]
            Dialer::Record(_) => None,
        };
        (self.slab, tls)
    }

    #[inline]
    pub fn get(&mut self, id: ConnId) -> Option<Io<'_, C>> {
        let index = id.0.index();
        let conn = self.slab.get_mut_at(id.0)?;
        conn.touch(index, conn.out.len(), self.dirty);
        Some(conn.io())
    }

    #[inline]
    pub fn get_unqueued(&mut self, id: ConnId) -> Option<Io<'_, C>> {
        Some(self.slab.get_mut_at(id.0)?.io())
    }

    pub fn drain(&mut self, id: ConnId) {
        self.flag(id, DRAIN);
    }

    pub fn close(&mut self, id: ConnId) {
        self.flag(id, CLOSE);
    }

    fn flag(&mut self, id: ConnId, flag: u8) {
        if let Some(conn) = self.slab.get_mut_at(id.0) {
            conn.flags |= flag;
            conn.touch(id.0.index(), conn.out.len(), self.dirty);
        }
    }

    pub fn len(&self) -> usize {
        self.slab.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slab.is_empty()
    }

    pub fn for_each(&mut self, mut visit: impl FnMut(Io<'_, C>)) {
        let Conns { slab, dirty, .. } = self;
        for index in 0..slab.end() {
            if let Some(conn) = slab.get_mut(index) {
                let before = conn.out.len();
                visit(conn.io());
                if conn.out.len() != before {
                    conn.touch(index, before, dirty);
                }
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Clock {
    System,
    #[cfg(keep_tests)]
    Simulated {
        later: Nanos,
        suspended: u64,
    },
}

pub struct Ctx<'a, M> {
    now: Nanos,
    clock: Clock,
    mesh: &'a mut Outbox<M>,
    wake_at: Option<Nanos>,
    stop: bool,
}

impl<'a, M> Ctx<'a, M> {
    pub fn new(now: Nanos, mesh: &'a mut Outbox<M>) -> Ctx<'a, M> {
        Ctx {
            now,
            clock: Clock::System,
            mesh,
            wake_at: None,
            stop: false,
        }
    }

    #[inline]
    pub fn now(&self) -> Nanos {
        self.now
    }

    #[inline]
    pub fn read_clock(&self) -> Nanos {
        match self.clock {
            Clock::System => Nanos::now(),
            #[cfg(keep_tests)]
            Clock::Simulated { later, .. } => later,
        }
    }

    #[inline]
    pub fn read_boot(&self) -> u64 {
        match self.clock {
            Clock::System => clock::boot().unwrap_or_default(),
            #[cfg(keep_tests)]
            Clock::Simulated { later, suspended } => later.raw().saturating_add(suspended),
        }
    }

    pub fn suspended_total(&self) -> u64 {
        match self.clock {
            Clock::System => clock::suspended_total().unwrap_or_default(),
            #[cfg(keep_tests)]
            Clock::Simulated { suspended, .. } => suspended,
        }
    }

    #[inline]
    pub fn mesh(&mut self) -> &mut Outbox<M> {
        self.mesh
    }

    pub fn wake_at(&mut self, at: Nanos) {
        self.wake_at = Some(self.wake_at.map_or(at, |earlier| earlier.min(at)));
    }

    pub fn stop(&mut self) {
        self.stop = true;
    }

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) fn finish(self) -> (Option<Nanos>, bool) {
        (self.wake_at, self.stop)
    }
}

pub trait Service {
    type Conn;
    type Msg: Send;

    fn accept(&mut self, cx: &mut Ctx<'_, Self::Msg>, id: ConnId) -> Option<Self::Conn>;

    fn read(&mut self, cx: &mut Ctx<'_, Self::Msg>, io: Io<'_, Self::Conn>, data: &[u8]) -> Flow;

    fn closed(&mut self, id: ConnId, conn: Self::Conn);

    fn message(
        &mut self,
        cx: &mut Ctx<'_, Self::Msg>,
        from: usize,
        msg: Self::Msg,
        conns: &mut Conns<'_, Self::Conn>,
    );

    fn ready(
        &mut self,
        _cx: &mut Ctx<'_, Self::Msg>,
        _source: usize,
        _conns: &mut Conns<'_, Self::Conn>,
    ) {
    }

    fn tick(
        &mut self,
        cx: &mut Ctx<'_, Self::Msg>,
        conns: &mut Conns<'_, Self::Conn>,
    ) -> Option<Nanos>;

    fn end_turn(&mut self, cx: &mut Ctx<'_, Self::Msg>, conns: &mut Conns<'_, Self::Conn>)
    -> Flush;
}

#[cfg(keep_tests)]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-rt/service/test_support.rs"]
mod test_support;
