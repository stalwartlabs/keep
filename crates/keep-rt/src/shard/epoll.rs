/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::hint;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::sync::Arc;
use std::time::Duration;

use crate::clock::Nanos;
use crate::mesh::Port;
use crate::service::{
    CLOSE, Conn, ConnId, Conns, Ctx, DRAIN, Dialer, Flow, Flush, HELD, LINGER, PAUSED, QUEUED,
    Service, discard, earliest,
};
use crate::slab::{Handle, Slab};
use crate::stats::LoopStats;
use crate::tls::{Failed, Opened, Tls, TlsConfig};

use super::sys::{self, CLOSED, READABLE, Timer, WRITABLE};
use super::{COMPACT_AT, KEEP_CAPACITY, LoopConfig, SHRINK_ABOVE};

const LISTENER: u64 = u64::MAX;
const WAKE: u64 = u64::MAX - 1;
const FIRST_SOURCE: u64 = u64::MAX - 2;
pub const MAX_SOURCES: usize = 16;
const MAX_EVENTS: usize = 1024;
const ACCEPTS_PER_TURN: usize = 128;
const ACCEPT_PAUSE: Duration = Duration::from_millis(10);
const LINGER_LIMIT: Duration = Duration::from_secs(1);

#[inline]
fn token(handle: Handle) -> u64 {
    u64::from(handle.generation()) << 32 | u64::from(handle.index())
}

#[inline]
fn handle(token: u64) -> Handle {
    Handle::new(token as u32, (token >> 32) as u32)
}

pub(crate) fn dial(
    epoll: &OwnedFd,
    addr: SocketAddr,
    handle: Handle,
) -> io::Result<(OwnedFd, u32)> {
    let fd = sys::connect(addr)?;
    let interest = READABLE | WRITABLE;
    sys::ctl(
        epoll,
        libc::EPOLL_CTL_ADD,
        fd.as_raw_fd(),
        interest,
        token(handle),
    )?;
    Ok((fd, interest))
}

pub struct Shard<S: Service> {
    epoll: OwnedFd,
    listener: Option<TcpListener>,
    port: Port<S::Msg>,
    conns: Slab<Conn<S::Conn>>,
    dirty: Vec<u32>,
    service: S,
    deadline: Option<Nanos>,
    rearm: Option<Nanos>,
    lingering: Vec<(Handle, Nanos)>,
    events: Box<[libc::epoll_event]>,
    scratch: Scratch,
    sources: usize,
    config: LoopConfig,
    stats: Arc<LoopStats>,
    timer: Timer,
    tls: Option<TlsConfig>,
}

struct Scratch {
    buf: Box<[u8]>,
    plain: Vec<u8>,
}

impl<S: Service> Shard<S> {
    pub fn new(
        config: LoopConfig,
        listener: Option<TcpListener>,
        port: Port<S::Msg>,
        service: S,
        stats: Arc<LoopStats>,
    ) -> io::Result<Shard<S>> {
        let epoll = sys::epoll_create()?;
        if let Some(busy_poll) = config.busy_poll {
            sys::set_busy_poll(&epoll, busy_poll)?;
        }
        if let Some(listener) = &listener {
            listener.set_nonblocking(true)?;
            sys::ctl(
                &epoll,
                libc::EPOLL_CTL_ADD,
                listener.as_raw_fd(),
                READABLE,
                LISTENER,
            )?;
        }
        if let Some(fd) = port.wake_fd() {
            sys::ctl(&epoll, libc::EPOLL_CTL_ADD, fd.as_raw_fd(), READABLE, WAKE)?;
        }
        Ok(Shard {
            epoll,
            listener,
            port,
            conns: Slab::default(),
            dirty: Vec::with_capacity(MAX_EVENTS),
            service,
            deadline: None,
            rearm: None,
            lingering: Vec::new(),
            events: vec![libc::epoll_event { events: 0, u64: 0 }; MAX_EVENTS].into_boxed_slice(),
            scratch: Scratch {
                buf: vec![0u8; config.read_buffer.max(1)].into_boxed_slice(),
                plain: Vec::new(),
            },
            sources: 0,
            config,
            stats,
            timer: Timer::Precise,
            tls: None,
        })
    }

    pub fn tls(mut self, config: TlsConfig) -> Shard<S> {
        let len = config.read_buffer(self.scratch.buf.len());
        if self.scratch.buf.len() < len {
            self.scratch.buf = vec![0u8; len].into_boxed_slice();
        }
        self.scratch.plain = Vec::with_capacity(len);
        self.tls = Some(config);
        self
    }

    pub fn watch(&mut self, fd: impl for<'a> FnOnce(&'a S) -> BorrowedFd<'a>) -> io::Result<usize> {
        let source = self.sources;
        if source >= MAX_SOURCES {
            return Err(io::Error::other("too many watched descriptors"));
        }
        sys::ctl(
            &self.epoll,
            libc::EPOLL_CTL_ADD,
            fd(&self.service).as_raw_fd(),
            READABLE,
            FIRST_SOURCE - source as u64,
        )?;
        self.sources += 1;
        Ok(source)
    }

    pub fn run(mut self) -> io::Result<S> {
        if let Some(slack) = self.config.timer_slack
            && let Err(err) = sys::set_timer_slack(slack)
        {
            sys::keep_default_slack(&err);
        }
        let mut stop = self.start();
        self.port.publish()?;
        while !stop {
            let (ready, parked) = self.wait()?;
            stop = self.turn(ready, parked)?;
        }
        Ok(self.service)
    }

    fn start(&mut self) -> bool {
        let Shard {
            epoll,
            port,
            conns,
            dirty,
            service,
            deadline,
            tls,
            ..
        } = self;
        let (_, outbox) = port.parts();
        let mut cx = Ctx::new(Nanos::now(), outbox);
        let dialer = Dialer::Epoll(epoll, tls.as_ref());
        let next = service.tick(&mut cx, &mut Conns::new(conns, dirty, dialer));
        let (wake_at, stop) = cx.finish();
        *deadline = earliest(next, wake_at);
        stop
    }

    fn wait(&mut self) -> io::Result<(usize, bool)> {
        let linger = self.lingering.iter().map(|&(_, until)| until).min();
        let wake = earliest(earliest(self.deadline, self.rearm), linger);
        if !self.config.spin.is_zero() {
            let spun = Nanos::now().after(self.config.spin);
            let until = wake.map_or(spun, |wake| wake.min(spun));
            loop {
                let ready = sys::wait(
                    &self.epoll,
                    &mut self.events,
                    Some(Duration::ZERO),
                    &mut self.timer,
                )?;
                if ready > 0 || !self.port.is_quiet() {
                    return Ok((ready, false));
                }
                if Nanos::now() >= until {
                    break;
                }
                hint::spin_loop();
            }
        }
        let parked = self.port.park();
        let timeout = match (parked, wake) {
            (false, _) => Some(Duration::ZERO),
            (true, None) => None,
            (true, Some(at)) => Some(Nanos::now().until(at)),
        };
        let ready = sys::wait(&self.epoll, &mut self.events, timeout, &mut self.timer);
        if parked {
            self.port.resume();
        }
        Ok((ready?, parked))
    }

    fn turn(&mut self, ready: usize, parked: bool) -> io::Result<bool> {
        let Shard {
            epoll,
            listener,
            port,
            conns,
            dirty,
            service,
            deadline,
            rearm,
            lingering,
            events,
            scratch,
            sources,
            config,
            stats,
            timer: _,
            tls,
        } = self;
        let now = Nanos::now();
        if !lingering.is_empty() {
            expire(conns, dirty, lingering, now);
        }
        let mut woken = false;
        if let Some(listener) = listener
            && rearm.is_some_and(|at| at <= now)
            && sys::ctl(
                epoll,
                libc::EPOLL_CTL_MOD,
                listener.as_raw_fd(),
                READABLE,
                LISTENER,
            )
            .is_ok()
        {
            *rearm = None;
        }
        let (inbox, outbox) = port.parts();
        let mut cx = Ctx::new(now, outbox);
        let mut table = Conns::new(conns, dirty, Dialer::Epoll(epoll, tls.as_ref()));
        for event in events.iter().take(ready) {
            let (flags, token) = (event.events, event.u64);
            match token {
                LISTENER => {
                    if let Some(listener) = listener
                        && accept(listener, epoll, &mut table, service, &mut cx, config, stats)
                        && sys::ctl(
                            epoll,
                            libc::EPOLL_CTL_MOD,
                            listener.as_raw_fd(),
                            0,
                            LISTENER,
                        )
                        .is_ok()
                    {
                        *rearm = Some(now.after(ACCEPT_PAUSE));
                    }
                }
                WAKE => woken = true,
                token if token > FIRST_SOURCE - *sources as u64 => {
                    service.ready(&mut cx, (FIRST_SOURCE - token) as usize, &mut table);
                }
                token => on_event(
                    handle(token),
                    flags,
                    &mut table,
                    service,
                    &mut cx,
                    scratch,
                    stats,
                ),
            }
        }
        inbox.drain(|from, msg| service.message(&mut cx, from, msg, &mut table));
        if deadline.is_some_and(|at| at <= now) {
            *deadline = service.tick(&mut cx, &mut table);
        }
        let flush = service.end_turn(&mut cx, &mut table);
        let (wake_at, stop) = cx.finish();
        *deadline = earliest(*deadline, wake_at);
        if flush == Flush::Discard {
            discard(conns, dirty);
        }
        if woken {
            port.ack_wake();
        }
        let wakes = port.publish()?;
        write_out(epoll, conns, dirty, lingering, service, config, stats);
        stats.turn(parked, wakes);
        Ok(stop)
    }
}

fn accept<S: Service>(
    listener: &TcpListener,
    epoll: &OwnedFd,
    table: &mut Conns<'_, S::Conn>,
    service: &mut S,
    cx: &mut Ctx<'_, S::Msg>,
    config: &LoopConfig,
    stats: &LoopStats,
) -> bool {
    let (conns, tls) = table.accepting();
    for _ in 0..ACCEPTS_PER_TURN {
        let fd = match sys::accept(listener.as_fd()) {
            Ok(fd) => fd,
            Err(err) => match err.raw_os_error() {
                Some(libc::EAGAIN) => return false,
                Some(
                    libc::EINTR
                    | libc::ECONNABORTED
                    | libc::EPROTO
                    | libc::EPERM
                    | libc::ENETDOWN
                    | libc::ENETUNREACH
                    | libc::ENOPROTOOPT
                    | libc::EHOSTDOWN
                    | libc::EHOSTUNREACH
                    | libc::ENONET
                    | libc::EOPNOTSUPP,
                ) => continue,
                _ => {
                    stats.stall();
                    return true;
                }
            },
        };
        let Some(handle) = conns
            .next_handle()
            .filter(|_| conns.len() < config.max_conns)
        else {
            stats.refused();
            continue;
        };
        if sys::ctl(
            epoll,
            libc::EPOLL_CTL_ADD,
            fd.as_raw_fd(),
            READABLE,
            token(handle),
        )
        .is_err()
        {
            stats.stall();
            continue;
        }
        let session = match tls {
            Some(config) => match Tls::server(config) {
                Some(session) => Some(session),
                None => {
                    stats.refused();
                    continue;
                }
            },
            None => None,
        };
        let id = ConnId::new(handle);
        let Some(state) = service.accept(cx, id) else {
            stats.refused();
            continue;
        };
        if conns
            .insert(Conn::new(id, Some(fd), state, READABLE, session))
            .is_some()
        {
            stats.accepted();
        }
    }
    false
}

fn on_event<S: Service>(
    handle: Handle,
    flags: u32,
    table: &mut Conns<'_, S::Conn>,
    service: &mut S,
    cx: &mut Ctx<'_, S::Msg>,
    scratch: &mut Scratch,
    stats: &LoopStats,
) {
    let (conns, dirty) = table.parts();
    let Scratch { buf, plain } = scratch;
    let index = handle.index();
    let Some(conn) = conns.get_mut_at(handle) else {
        return;
    };
    if conn.flags & LINGER != 0 {
        if flags & (READABLE | CLOSED) != 0 {
            let ended = conn.fd.as_ref().is_none_or(|fd| match sys::recv(fd, buf) {
                Ok(len) => len == 0,
                Err(err) => !matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ),
            });
            if ended {
                conn.flags |= CLOSE;
                conn.touch(index, conn.out.len(), dirty);
            }
        }
        return;
    }
    let held = conn.flags & HELD != 0 && flags & CLOSED == 0;
    if flags & (READABLE | CLOSED) != 0 && conn.flags & (DRAIN | CLOSE | PAUSED) == 0 && !held {
        let Some(fd) = &conn.fd else {
            return;
        };
        let carried = conn.tls.as_deref().map_or(0, |tls| tls.carry_into(buf));
        match sys::recv(fd, buf.get_mut(carried..).unwrap_or_default()) {
            Ok(0) => conn.flags |= DRAIN,
            Ok(len) => {
                stats.read(len);
                let before = conn.out.len();
                let backlog = conn.backlog();
                let data = buf.get_mut(..carried + len).unwrap_or_default();
                let flow = match conn.tls.as_deref_mut() {
                    None => service.read(cx, conn.io(), data),
                    Some(tls) => {
                        plain.clear();
                        match tls.open(data, plain) {
                            Ok(opened) => {
                                if opened == Opened::Ready {
                                    conn.touch(index, before, dirty);
                                }
                                let flow = if plain.is_empty() {
                                    Flow::Continue
                                } else {
                                    service.read(cx, conn.io(), plain)
                                };
                                match (opened, flow) {
                                    (Opened::Closed, Flow::Continue) => Flow::Drain,
                                    (_, flow) => flow,
                                }
                            }
                            Err(Failed) => Flow::Drain,
                        }
                    }
                };
                match flow {
                    Flow::Continue => {}
                    Flow::Drain => conn.flags |= DRAIN,
                    Flow::Close => conn.flags |= CLOSE,
                    Flow::Hold => conn.flags |= HELD,
                }
                if conn.out.len() != before || conn.backlog() != backlog {
                    conn.touch(index, before, dirty);
                }
            }
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) => {}
            Err(_) => conn.flags |= CLOSE,
        }
    }
    if flags & (WRITABLE | CLOSED) != 0 || conn.flags & (DRAIN | CLOSE | HELD) != 0 {
        conn.touch(index, conn.out.len(), dirty);
    }
}

fn expire<C>(
    conns: &mut Slab<Conn<C>>,
    dirty: &mut Vec<u32>,
    lingering: &mut Vec<(Handle, Nanos)>,
    now: Nanos,
) {
    lingering.retain(|&(handle, until)| {
        if until > now {
            return true;
        }
        if let Some(conn) = conns.get_mut_at(handle) {
            conn.flags |= CLOSE;
            conn.touch(handle.index(), conn.out.len(), dirty);
        }
        false
    });
}

fn write_out<S: Service>(
    epoll: &OwnedFd,
    conns: &mut Slab<Conn<S::Conn>>,
    dirty: &mut Vec<u32>,
    lingering: &mut Vec<(Handle, Nanos)>,
    service: &mut S,
    config: &LoopConfig,
    stats: &LoopStats,
) {
    for index in dirty.drain(..) {
        let Some(conn) = conns.get_mut(index) else {
            continue;
        };
        conn.flags &= !QUEUED;
        if conn.flags & CLOSE != 0 || !send_pending(conn, stats) {
            close(conns, index, service, stats);
            continue;
        }
        let mut pending = settle(conn);
        if pending == 0 && conn.flags & (DRAIN | LINGER) == DRAIN && conn.tls.is_some() {
            let notified = conn
                .tls
                .as_deref_mut()
                .is_some_and(|tls| tls.close_notify().is_ok());
            if !notified || !send_pending(conn, stats) {
                close(conns, index, service, stats);
                continue;
            }
            pending = settle(conn);
        }
        if pending == 0 {
            if conn.flags & (DRAIN | LINGER) == DRAIN {
                let shut = conn
                    .fd
                    .as_ref()
                    .is_some_and(|fd| sys::shutdown_write(fd).is_ok());
                if !shut {
                    close(conns, index, service, stats);
                    continue;
                }
                conn.flags |= LINGER;
                lingering.push((conn.id.handle(), Nanos::now().after(LINGER_LIMIT)));
            }
            conn.flags &= !PAUSED;
        } else if pending > config.pause_output {
            conn.flags |= PAUSED;
        }
        let read = if conn.flags & LINGER != 0 || conn.flags & (DRAIN | PAUSED | HELD) == 0 {
            READABLE
        } else {
            0
        };
        let want = read | if pending > 0 { WRITABLE } else { 0 };
        if want != conn.interest {
            let changed = conn.fd.as_ref().is_some_and(|fd| {
                sys::ctl(
                    epoll,
                    libc::EPOLL_CTL_MOD,
                    fd.as_raw_fd(),
                    want,
                    token(conn.id.handle()),
                )
                .is_ok()
            });
            if changed {
                conn.interest = want;
            } else {
                close(conns, index, service, stats);
            }
        }
    }
}

fn send_pending<C>(conn: &mut Conn<C>, stats: &LoopStats) -> bool {
    let Some(fd) = &conn.fd else {
        return false;
    };
    match conn.tls.as_deref_mut() {
        None => send_from(fd, &conn.out, &mut conn.sent, stats),
        Some(tls) => {
            if tls.seal(&mut conn.out).is_err() {
                return false;
            }
            if conn.flags & DRAIN != 0 && !tls.is_ready() {
                conn.out.clear();
            }
            let (wire, sent) = tls.unsent();
            send_from(fd, wire, sent, stats)
        }
    }
}

fn send_from(fd: &OwnedFd, buf: &[u8], sent: &mut usize, stats: &LoopStats) -> bool {
    while let Some(data) = buf.get(*sent..).filter(|data| !data.is_empty()) {
        match sys::send(fd, data) {
            Ok(0) => break,
            Ok(written) => {
                stats.write(written);
                *sent += written;
            }
            Err(err) => match err.kind() {
                io::ErrorKind::Interrupted => {}
                io::ErrorKind::WouldBlock => break,
                _ => return false,
            },
        }
    }
    true
}

fn settle<C>(conn: &mut Conn<C>) -> usize {
    match conn.tls.as_deref_mut() {
        None => settle_buffer(&mut conn.out, &mut conn.sent),
        Some(tls) => {
            if conn.out.is_empty() && conn.out.capacity() > SHRINK_ABOVE {
                conn.out.shrink_to(KEEP_CAPACITY);
            }
            tls.settle()
        }
    }
}

fn settle_buffer(buf: &mut Vec<u8>, sent: &mut usize) -> usize {
    let pending = buf.len() - *sent;
    if pending == 0 {
        buf.clear();
        *sent = 0;
        if buf.capacity() > SHRINK_ABOVE {
            buf.shrink_to(KEEP_CAPACITY);
        }
    } else if *sent >= COMPACT_AT && *sent >= pending {
        buf.drain(..*sent);
        *sent = 0;
    }
    pending
}

fn close<S: Service>(
    conns: &mut Slab<Conn<S::Conn>>,
    index: u32,
    service: &mut S,
    stats: &LoopStats,
) {
    if let Some(conn) = conns.remove(index) {
        let Conn { id, fd, state, .. } = conn;
        drop(fd);
        stats.closed();
        service.closed(id, state);
    }
}
