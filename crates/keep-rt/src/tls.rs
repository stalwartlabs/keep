/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::net::SocketAddr;
use std::sync::Arc;

use rustls::client::{ClientConnectionData, UnbufferedClientConnection};
use rustls::pki_types::ServerName;
use rustls::server::{ServerConnectionData, UnbufferedServerConnection};
use rustls::unbuffered::{
    ConnectionState, EncodeError, EncryptError, InsufficientSizeError, UnbufferedStatus,
    WriteTraffic,
};
use rustls::{ClientConfig, CommonState, ServerConfig};

use crate::shard::{COMPACT_AT, KEEP_CAPACITY, SHRINK_ABOVE};

pub const MAX_RECORD: usize = 5 + (16 << 10) + 256;
pub const MAX_HANDSHAKE_CARRY: usize = 6 * MAX_RECORD;
const RECORD_OVERHEAD: usize = 5 + 1 + 16;
const MAX_FRAGMENT: usize = 16 << 10;
const CLOSE_NOTIFY_LEN: usize = RECORD_OVERHEAD + 2;
const KEY_UPDATE_RECORD: usize = RECORD_OVERHEAD + 5;
const MIN_READ_BUFFER: usize = MAX_HANDSHAKE_CARRY + MAX_RECORD;

#[derive(Clone, Debug)]
pub struct TlsConfig {
    server: Arc<ServerConfig>,
    dial: Option<Arc<ClientConfig>>,
}

impl TlsConfig {
    pub fn new(server: Arc<ServerConfig>, dial: Option<Arc<ClientConfig>>) -> TlsConfig {
        TlsConfig { server, dial }
    }

    pub fn dial(&self) -> Option<&Arc<ClientConfig>> {
        self.dial.as_ref()
    }

    pub(crate) fn read_buffer(&self, configured: usize) -> usize {
        configured.max(MIN_READ_BUFFER)
    }
}

enum Link {
    Server(UnbufferedServerConnection),
    Client(UnbufferedClientConnection),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    Handshake,
    Traffic,
    Notified,
    Failed,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Opened {
    Open,
    Ready,
    Closed,
}

#[derive(Debug)]
pub(crate) struct Failed;

pub(crate) struct Tls {
    link: Link,
    wire: Wire,
    carry: Vec<u8>,
    phase: Phase,
}

#[derive(Default)]
struct Wire {
    buf: Vec<u8>,
    end: usize,
    sent: usize,
}

impl Tls {
    pub(crate) fn server(config: &TlsConfig) -> Option<Box<Tls>> {
        let link = UnbufferedServerConnection::new(Arc::clone(&config.server)).ok()?;
        Some(Tls::boxed(Link::Server(link)))
    }

    pub(crate) fn client(config: &TlsConfig, addr: SocketAddr) -> Option<Box<Tls>> {
        let dial = config.dial.as_ref()?;
        let name = ServerName::IpAddress(addr.ip().into());
        let link = UnbufferedClientConnection::new(Arc::clone(dial), name).ok()?;
        Some(Tls::boxed(Link::Client(link)))
    }

    fn boxed(link: Link) -> Box<Tls> {
        Box::new(Tls {
            link,
            wire: Wire::default(),
            carry: Vec::new(),
            phase: Phase::Handshake,
        })
    }

    #[inline]
    pub(crate) fn backlog(&self) -> usize {
        self.wire.end - self.wire.sent
    }

    #[inline]
    pub(crate) fn unsent(&mut self) -> (&[u8], &mut usize) {
        let Wire { buf, end, sent } = &mut self.wire;
        (buf.get(..*end).unwrap_or_default(), sent)
    }

    pub(crate) fn settle(&mut self) -> usize {
        let Wire { buf, end, sent } = &mut self.wire;
        let pending = *end - *sent;
        if pending == 0 {
            *end = 0;
            *sent = 0;
            if buf.capacity() > SHRINK_ABOVE {
                buf.truncate(KEEP_CAPACITY);
                buf.shrink_to(KEEP_CAPACITY);
            }
        } else if *sent >= COMPACT_AT && *sent >= pending {
            buf.copy_within(*sent..*end, 0);
            *end = pending;
            *sent = 0;
        }
        pending
    }

    pub(crate) fn is_ready(&self) -> bool {
        matches!(self.phase, Phase::Traffic | Phase::Notified)
    }

    pub(crate) fn carry_into(&self, buf: &mut [u8]) -> usize {
        let len = self.carry.len();
        if let Some(head) = buf.get_mut(..len) {
            head.copy_from_slice(&self.carry);
        }
        len
    }

    pub(crate) fn open(&mut self, data: &mut [u8], plain: &mut Vec<u8>) -> Result<Opened, Failed> {
        if self.phase == Phase::Failed {
            return Err(Failed);
        }
        let was_ready = self.is_ready();
        let opened = match &mut self.link {
            Link::Server(link) => open(link, data, &mut self.wire, plain),
            Link::Client(link) => open(link, data, &mut self.wire, plain),
        };
        let Ok((used, end)) = opened else {
            self.phase = Phase::Failed;
            return Err(Failed);
        };
        if end == End::Traffic && self.phase == Phase::Handshake {
            self.phase = Phase::Traffic;
        }
        let rest = data.get(used..).unwrap_or_default();
        let limit = if self.is_ready() {
            MAX_RECORD
        } else {
            MAX_HANDSHAKE_CARRY
        };
        if rest.len() > limit {
            self.phase = Phase::Failed;
            return Err(Failed);
        }
        self.carry.clear();
        self.carry.extend_from_slice(rest);
        Ok(if end == End::Closed {
            Opened::Closed
        } else if !was_ready && self.is_ready() {
            self.carry.shrink_to(MAX_RECORD);
            Opened::Ready
        } else {
            Opened::Open
        })
    }

    pub(crate) fn seal(&mut self, out: &mut Vec<u8>) -> Result<(), Failed> {
        match self.phase {
            Phase::Failed => return Ok(()),
            Phase::Notified => {
                out.clear();
                return Ok(());
            }
            Phase::Handshake | Phase::Traffic => {}
        }
        let Tls {
            link,
            wire,
            carry,
            phase,
            ..
        } = self;
        let sealed = match link {
            Link::Server(link) => pump(link, carry, wire, Write::Seal(out)),
            Link::Client(link) => pump(link, carry, wire, Write::Seal(out)),
        };
        let Ok(sealed) = sealed else {
            *phase = Phase::Failed;
            return Err(Failed);
        };
        if sealed {
            out.clear();
        }
        Ok(())
    }

    pub(crate) fn close_notify(&mut self) -> Result<(), Failed> {
        if self.phase != Phase::Traffic {
            return Ok(());
        }
        let Tls {
            link,
            wire,
            carry,
            phase,
            ..
        } = self;
        let queued = match link {
            Link::Server(link) => pump(link, carry, wire, Write::CloseNotify),
            Link::Client(link) => pump(link, carry, wire, Write::CloseNotify),
        };
        match queued {
            Ok(true) => *phase = Phase::Notified,
            Ok(false) => {}
            Err(Failed) => {
                *phase = Phase::Failed;
                return Err(Failed);
            }
        }
        Ok(())
    }
}

trait Records {
    type Data;

    fn records<'c, 'i>(&'c mut self, input: &'i mut [u8]) -> UnbufferedStatus<'c, 'i, Self::Data>;

    fn wants_write(&self) -> bool;
}

impl Records for UnbufferedServerConnection {
    type Data = ServerConnectionData;

    fn records<'c, 'i>(&'c mut self, input: &'i mut [u8]) -> UnbufferedStatus<'c, 'i, Self::Data> {
        self.process_tls_records(input)
    }

    fn wants_write(&self) -> bool {
        CommonState::wants_write(self)
    }
}

impl Records for UnbufferedClientConnection {
    type Data = ClientConnectionData;

    fn records<'c, 'i>(&'c mut self, input: &'i mut [u8]) -> UnbufferedStatus<'c, 'i, Self::Data> {
        self.process_tls_records(input)
    }

    fn wants_write(&self) -> bool {
        CommonState::wants_write(self)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum End {
    Blocked,
    Traffic,
    Closed,
}

enum Step {
    Again,
    Stop(End),
    Failed,
}

fn open<L: Records>(
    link: &mut L,
    data: &mut [u8],
    wire: &mut Wire,
    plain: &mut Vec<u8>,
) -> Result<(usize, End), Failed> {
    let len = data.len();
    let mut used = 0;
    loop {
        let step = {
            let UnbufferedStatus { discard, state } =
                link.records(data.get_mut(used..).unwrap_or_default());
            used += discard;
            match state {
                Ok(ConnectionState::ReadTraffic(mut traffic)) => {
                    let mut step = Step::Again;
                    while let Some(record) = traffic.next_record() {
                        let Ok(record) = record else {
                            step = Step::Failed;
                            break;
                        };
                        used += record.discard;
                        plain.extend_from_slice(record.payload);
                    }
                    match step {
                        Step::Again if used == len => Step::Stop(End::Traffic),
                        step => step,
                    }
                }
                Ok(ConnectionState::EncodeTlsData(mut flight)) => {
                    match wire.grow(0, |spare| flight.encode(spare).map_err(Grow::from)) {
                        Ok(()) => Step::Again,
                        Err(Failed) => Step::Failed,
                    }
                }
                Ok(ConnectionState::TransmitTlsData(transmit)) => {
                    transmit.done();
                    Step::Again
                }
                Ok(ConnectionState::BlockedHandshake) => Step::Stop(End::Blocked),
                Ok(ConnectionState::WriteTraffic(_)) => Step::Stop(End::Traffic),
                Ok(ConnectionState::PeerClosed | ConnectionState::Closed) => {
                    Step::Stop(End::Closed)
                }
                Ok(_) | Err(_) => Step::Failed,
            }
        };
        match step {
            Step::Again => {}
            Step::Stop(end) => return Ok((used, end)),
            Step::Failed => {
                alert(link, wire);
                return Err(Failed);
            }
        }
    }
}

#[cold]
#[inline(never)]
fn alert<L: Records>(link: &mut L, wire: &mut Wire) {
    while link.wants_write() {
        let Ok(ConnectionState::EncodeTlsData(mut flight)) = link.records(&mut []).state else {
            return;
        };
        if wire
            .grow(0, |spare| flight.encode(spare).map_err(Grow::from))
            .is_err()
        {
            return;
        }
    }
}

#[derive(Clone, Copy)]
enum Write<'a> {
    Seal(&'a [u8]),
    CloseNotify,
}

impl Write<'_> {
    fn write<Data>(
        self,
        traffic: &mut WriteTraffic<'_, Data>,
        wire: &mut Wire,
    ) -> Result<(), Failed> {
        match self {
            Write::Seal([]) => Ok(()),
            Write::Seal(out) => {
                let records = out.len().div_ceil(MAX_FRAGMENT);
                let estimate = out.len() + records * RECORD_OVERHEAD + 2 * KEY_UPDATE_RECORD;
                wire.grow(estimate, |spare| {
                    traffic.encrypt(out, spare).map_err(Grow::from)
                })
            }
            Write::CloseNotify => wire.grow(CLOSE_NOTIFY_LEN, |spare| {
                traffic.queue_close_notify(spare).map_err(Grow::from)
            }),
        }
    }
}

fn pump<L: Records>(
    link: &mut L,
    carry: &mut Vec<u8>,
    wire: &mut Wire,
    write: Write<'_>,
) -> Result<bool, Failed> {
    loop {
        let (discard, done) = {
            let UnbufferedStatus { discard, state } = link.records(carry);
            let done = match state.map_err(|_| Failed)? {
                ConnectionState::WriteTraffic(mut traffic) => {
                    write.write(&mut traffic, wire)?;
                    Some(true)
                }
                ConnectionState::EncodeTlsData(mut flight) => {
                    wire.grow(0, |spare| flight.encode(spare).map_err(Grow::from))?;
                    None
                }
                ConnectionState::TransmitTlsData(transmit) => {
                    transmit.done();
                    None
                }
                ConnectionState::BlockedHandshake
                | ConnectionState::PeerClosed
                | ConnectionState::Closed => Some(false),
                _ => return Err(Failed),
            };
            (discard, done)
        };
        carry.drain(..discard.min(carry.len()));
        if let Some(done) = done {
            return Ok(done);
        }
    }
}

enum Grow {
    Short(usize),
    Refused,
}

impl From<EncodeError> for Grow {
    fn from(err: EncodeError) -> Grow {
        match err {
            EncodeError::InsufficientSize(InsufficientSizeError { required_size }) => {
                Grow::Short(required_size)
            }
            EncodeError::AlreadyEncoded => Grow::Refused,
        }
    }
}

impl From<EncryptError> for Grow {
    fn from(err: EncryptError) -> Grow {
        match err {
            EncryptError::InsufficientSize(InsufficientSizeError { required_size }) => {
                Grow::Short(required_size)
            }
            EncryptError::EncryptExhausted => Grow::Refused,
        }
    }
}

impl Wire {
    fn grow(
        &mut self,
        estimate: usize,
        mut write: impl FnMut(&mut [u8]) -> Result<usize, Grow>,
    ) -> Result<(), Failed> {
        let start = self.end;
        let mut room = estimate;
        for _ in 0..2 {
            if self.buf.len() < start + room {
                self.buf.resize(start + room, 0);
            }
            match write(self.buf.get_mut(start..).unwrap_or_default()) {
                Ok(len) => {
                    self.end = start + len;
                    return Ok(());
                }
                Err(Grow::Short(required)) => room = required + KEY_UPDATE_RECORD,
                Err(Grow::Refused) => break,
            }
        }
        Err(Failed)
    }
}
