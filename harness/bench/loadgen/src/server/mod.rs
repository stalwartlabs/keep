/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

pub mod framing;

use std::io::{self, Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::thread;
use std::time::{Duration, Instant};

use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token, Waker};

use self::framing::Framer;
pub use self::framing::{ServerProtocol, cluster_slots_reply};
use crate::sys;

const WAKER: Token = Token(usize::MAX);
const SERVER_OPEN_FILES: u64 = 65_536;
const READ_CHUNK: usize = 16 << 10;
const MAX_BACKLOG: usize = 64 << 20;

#[repr(align(128))]
#[derive(Default)]
struct Counter(AtomicU64);

pub struct ServerConfig {
    pub listen: String,
    pub protocol: ServerProtocol,
    pub threads: usize,
    pub cpus: Vec<usize>,
    pub spin: bool,
    pub cluster_slots: Box<[u8]>,
    pub report_every: Duration,
}

struct ServerConn {
    stream: TcpStream,
    rbuf: Vec<u8>,
    rpos: usize,
    rlen: usize,
    wbuf: Vec<u8>,
    wpos: usize,
}

enum Step {
    Open,
    Close,
}

impl ServerConn {
    fn new(stream: TcpStream) -> Self {
        ServerConn {
            stream,
            rbuf: vec![0; READ_CHUNK * 2],
            rpos: 0,
            rlen: 0,
            wbuf: Vec::with_capacity(READ_CHUNK / 4),
            wpos: 0,
        }
    }

    fn on_readable(&mut self, framer: &mut Framer, served: &mut u64) -> Step {
        loop {
            self.make_room();
            let spare = self.rbuf.get_mut(self.rlen..).unwrap_or_default();
            let spare_len = spare.len();
            match (&self.stream).read(spare) {
                Ok(0) => return Step::Close,
                Ok(n) => {
                    self.rlen += n;
                    while let Some(buf) = self
                        .rbuf
                        .get(self.rpos..self.rlen)
                        .filter(|b| !b.is_empty())
                    {
                        match framer.answer(buf, &mut self.wbuf) {
                            Ok(Some(len)) => {
                                self.rpos += len;
                                *served += 1;
                            }
                            Ok(None) => break,
                            Err(_) => return Step::Close,
                        }
                    }
                    if let Step::Close = self.flush() {
                        return Step::Close;
                    }
                    if n < spare_len {
                        return Step::Open;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Step::Open,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => return Step::Close,
            }
        }
    }

    fn flush(&mut self) -> Step {
        while let Some(pending) = self.wbuf.get(self.wpos..).filter(|b| !b.is_empty()) {
            match (&self.stream).write(pending) {
                Ok(0) => return Step::Close,
                Ok(n) => self.wpos += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    return if self.wbuf.len() > MAX_BACKLOG {
                        Step::Close
                    } else {
                        Step::Open
                    };
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => return Step::Close,
            }
        }
        self.wbuf.clear();
        self.wpos = 0;
        Step::Open
    }

    fn make_room(&mut self) {
        if self.rpos == self.rlen {
            self.rpos = 0;
            self.rlen = 0;
        }
        if self.rbuf.len() - self.rlen >= READ_CHUNK {
            return;
        }
        if self.rpos > 0 {
            self.rbuf.copy_within(self.rpos..self.rlen, 0);
            self.rlen -= self.rpos;
            self.rpos = 0;
        }
        if self.rbuf.len() - self.rlen < READ_CHUNK {
            let grown = (self.rbuf.len() * 2).max(self.rlen + READ_CHUNK);
            self.rbuf.resize(grown, 0);
        }
    }
}

struct ServerWorker {
    poll: Poll,
    inbox: Receiver<std::net::TcpStream>,
    conns: Vec<Option<ServerConn>>,
    free: Vec<usize>,
    framer: Framer,
}

impl ServerWorker {
    fn run(mut self, spin: bool, counter: &Counter) -> io::Result<()> {
        let mut events = Events::with_capacity(1024);
        let timeout = spin.then_some(Duration::ZERO);
        loop {
            if let Err(e) = self.poll.poll(&mut events, timeout)
                && e.kind() != io::ErrorKind::Interrupted
            {
                return Err(e);
            }
            let mut served = 0;
            for event in events.iter() {
                let token = event.token();
                if token == WAKER {
                    self.accept_new()?;
                    continue;
                }
                let Some(Some(conn)) = self.conns.get_mut(token.0) else {
                    continue;
                };
                let mut step = Step::Open;
                if event.is_readable() || event.is_read_closed() || event.is_error() {
                    step = conn.on_readable(&mut self.framer, &mut served);
                }
                if matches!(step, Step::Open) && event.is_writable() {
                    step = conn.flush();
                }
                if let Step::Close = step {
                    if let Some(mut conn) = self.conns.get_mut(token.0).and_then(Option::take) {
                        let _ = self.poll.registry().deregister(&mut conn.stream);
                    }
                    self.free.push(token.0);
                }
            }
            if served > 0 {
                counter.0.fetch_add(served, Ordering::Relaxed);
            }
        }
    }

    fn accept_new(&mut self) -> io::Result<()> {
        loop {
            let stream = match self.inbox.try_recv() {
                Ok(stream) => stream,
                Err(TryRecvError::Empty) => return Ok(()),
                Err(TryRecvError::Disconnected) => {
                    return Err(io::Error::other("acceptor stopped"));
                }
            };
            stream.set_nonblocking(true)?;
            let mut stream = TcpStream::from_std(stream);
            let slot = self.free.pop().unwrap_or(self.conns.len());
            self.poll.registry().register(
                &mut stream,
                Token(slot),
                Interest::READABLE | Interest::WRITABLE,
            )?;
            let conn = Some(ServerConn::new(stream));
            match self.conns.get_mut(slot) {
                Some(entry) => *entry = conn,
                None => self.conns.push(conn),
            }
        }
    }
}

pub fn serve(config: ServerConfig) -> io::Result<()> {
    if let Err(reason) = sys::ensure_open_files(SERVER_OPEN_FILES) {
        eprintln!("nullserver: {reason}");
    }
    let listener = TcpListener::bind(&config.listen)?;
    let threads = config.threads.max(1);
    let counters: Arc<[Counter]> = (0..threads).map(|_| Counter::default()).collect();
    let mut handoff: Vec<(Sender<std::net::TcpStream>, Arc<Waker>)> = Vec::with_capacity(threads);
    for id in 0..threads {
        let poll = Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), WAKER)?);
        let (tx, rx) = channel();
        handoff.push((tx, waker));
        let worker = ServerWorker {
            poll,
            inbox: rx,
            conns: Vec::new(),
            free: Vec::new(),
            framer: Framer::new(config.protocol, config.cluster_slots.clone()),
        };
        let counters = Arc::clone(&counters);
        let cpu = config.cpus.get(id).copied();
        let spin = config.spin;
        thread::Builder::new()
            .name(format!("null-{id}"))
            .spawn(move || {
                if let Some(cpu) = cpu
                    && let Err(e) = sys::pin_current_thread(cpu)
                {
                    eprintln!("null server worker {id} cannot pin to cpu {cpu}: {e}");
                }
                if let Err(e) = worker.run(spin, &counters[id]) {
                    eprintln!("null server worker {id} stopped: {e}");
                }
            })?;
    }
    eprintln!(
        "null server on {} ({:?}, {threads} threads, {})",
        config.listen,
        config.protocol,
        if config.spin { "spin" } else { "blocking" }
    );
    thread::Builder::new()
        .name(String::from("acceptor"))
        .spawn(move || {
            let mut next = 0;
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let _ = stream.set_nodelay(true);
                let Some((tx, waker)) = handoff.get(next % handoff.len()) else {
                    continue;
                };
                next += 1;
                if tx.send(stream).is_ok() {
                    let _ = waker.wake();
                }
            }
        })?;
    let mut last_total = 0u64;
    let mut last_cpu = sys::process_cpu_ns();
    let mut last = Instant::now();
    loop {
        thread::sleep(config.report_every);
        let total: u64 = counters.iter().map(|c| c.0.load(Ordering::Relaxed)).sum();
        let cpu = sys::process_cpu_ns();
        let elapsed = last.elapsed().as_secs_f64();
        let rate = (total - last_total) as f64 / elapsed;
        if rate > 0.0 {
            eprintln!(
                "null server: {rate:>12.0} req/s  cpu {:>5.0}% of {threads} threads",
                (cpu - last_cpu) as f64 / 1e9 / elapsed / threads as f64 * 100.0
            );
        }
        last_total = total;
        last_cpu = cpu;
        last = Instant::now();
    }
}
