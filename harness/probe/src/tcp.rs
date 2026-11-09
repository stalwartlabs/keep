/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    args::{CpuList, Wait},
    clock::{Clock, ns_to_secs, secs_to_ns, unix_now},
    error::{Context, Error, Result},
    hist::{LatencyHist, Summary},
    link::{read_exact, write_all},
    report::emit,
    sys,
    wire::{HEADER_LEN, Header},
};
use clap::{Args, ValueEnum};
use serde::Serialize;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::{
    hint::spin_loop,
    io::{self, Read},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    thread,
    time::Duration,
};

const ECHO_BUF: usize = 64 * 1024;
const LISTEN_BACKLOG: i32 = 4096;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EchoImpl {
    Rust,
    C,
}

#[derive(Args)]
pub struct EchoArgs {
    #[arg(long, default_value_t = 7100)]
    port: u16,
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::UNSPECIFIED))]
    bind: IpAddr,
    #[arg(long, help = "CPUs for connection threads, round robin")]
    cpus: Option<CpuList>,
    #[arg(long, value_enum, default_value_t = Wait::Block)]
    wait: Wait,
    #[arg(long, default_value_t = 50)]
    busy_poll_us: u32,
    #[arg(long = "impl", value_enum, default_value_t = EchoImpl::Rust, help = "rust: std blocking I/O, one pinned thread per connection; c: plain C read/write loop, one pthread per connection, ignores --cpus and --wait")]
    engine: EchoImpl,
}

#[derive(Serialize)]
struct Listening {
    event: &'static str,
    probe: &'static str,
    addr: SocketAddr,
    engine: EchoImpl,
    wait: Wait,
}

#[derive(Serialize)]
struct Closed {
    event: &'static str,
    peer: Option<SocketAddr>,
    cpu: Option<usize>,
    incoming_cpu: Option<usize>,
    napi_id: Option<u32>,
    bytes: u64,
    error: Option<String>,
}

pub fn listener(bind: IpAddr, port: u16) -> Result<TcpListener> {
    let addr = SocketAddr::new(bind, port);
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))
        .context("TCP socket")?;
    socket.set_reuse_address(true).context("SO_REUSEADDR")?;
    socket.bind(&SockAddr::from(addr)).context("bind")?;
    socket.listen(LISTEN_BACKLOG).context("listen")?;
    Ok(socket.into())
}

pub fn echo(args: &EchoArgs) -> Result<()> {
    let listener = listener(args.bind, args.port)?;
    emit(&Listening {
        event: "listening",
        probe: "tcp-echo",
        addr: listener.local_addr().context("local address")?,
        engine: args.engine,
        wait: args.wait,
    })?;
    match args.engine {
        EchoImpl::C => serve_c(&listener),
        EchoImpl::Rust => {
            for (index, stream) in listener.incoming().enumerate() {
                let Ok(stream) = stream else { continue };
                let cpus = args.cpus.clone();
                let (wait, busy_poll_us) = (args.wait, args.busy_poll_us);
                thread::Builder::new()
                    .name(format!("echo-{index}"))
                    .spawn(move || {
                        serve_connection(stream, cpus.as_ref(), index, wait, busy_poll_us)
                    })
                    .context("spawn connection thread")?;
            }
            Ok(())
        }
    }
}

fn serve_connection(
    stream: TcpStream,
    cpus: Option<&CpuList>,
    index: usize,
    wait: Wait,
    busy_poll_us: u32,
) {
    let peer = stream.peer_addr().ok();
    let mut bytes = 0u64;
    let outcome = sys::pin(cpus, index).and_then(|cpu| {
        stream.set_nodelay(true).context("TCP_NODELAY")?;
        match wait {
            Wait::Block => {}
            Wait::Spin => stream.set_nonblocking(true).context("O_NONBLOCK")?,
            Wait::BusyPoll => sys::set_busy_poll(&stream, busy_poll_us)?,
        }
        Ok(cpu)
    });
    let (cpu, error) = match outcome {
        Ok(cpu) => (
            cpu,
            echo_loop(&stream, &mut bytes).err().map(|e| e.to_string()),
        ),
        Err(err) => (None, Some(err.to_string())),
    };
    let _ = emit(&Closed {
        event: "closed",
        peer,
        cpu,
        incoming_cpu: sys::incoming_cpu(&stream),
        napi_id: sys::incoming_napi_id(&stream),
        bytes,
        error,
    });
}

fn echo_loop(mut stream: &TcpStream, bytes: &mut u64) -> io::Result<()> {
    let mut buf = vec![0u8; ECHO_BUF];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return Ok(()),
            Ok(n) => {
                write_all(&mut stream, buf.get(..n).unwrap_or_default())?;
                *bytes += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => spin_loop(),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) if e.kind() == io::ErrorKind::ConnectionReset => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

#[cfg(netprobe_c_echo)]
unsafe extern "C" {
    fn netprobe_c_echo_serve(listen_fd: libc::c_int) -> libc::c_int;
}

#[cfg(netprobe_c_echo)]
fn serve_c(listener: &TcpListener) -> Result<()> {
    use std::os::fd::AsRawFd;
    let errno = unsafe { netprobe_c_echo_serve(listener.as_raw_fd()) };
    Err(Error::Io {
        context: "C echo accept",
        source: io::Error::from_raw_os_error(errno),
    })
}

#[cfg(not(netprobe_c_echo))]
fn serve_c(_listener: &TcpListener) -> Result<()> {
    Err(Error::Unsupported("built with NETPROBE_SKIP_C, no C echo"))
}

#[derive(Args)]
pub struct ConnectArgs {
    #[arg(long, help = "Echo server address, ip:port")]
    target: SocketAddr,
    #[arg(long, default_value_t = 1)]
    threads: usize,
    #[arg(long, default_value_t = 10.0)]
    duration: f64,
    #[arg(long, help = "Exchange one 24-byte message before closing")]
    ping: bool,
    #[arg(
        long,
        help = "Close with SO_LINGER 0 (RST), so the client keeps no TIME_WAIT state"
    )]
    reset: bool,
    #[arg(long)]
    cpus: Option<CpuList>,
    #[arg(long)]
    label: Option<String>,
}

#[derive(Default)]
struct ErrorCounts(Vec<(io::ErrorKind, u64)>);

impl ErrorCounts {
    fn add(&mut self, kind: io::ErrorKind) {
        match self.0.iter_mut().find(|(k, _)| *k == kind) {
            Some((_, count)) => *count += 1,
            None => self.0.push((kind, 1)),
        }
    }

    fn merge(&mut self, other: &Self) {
        for &(kind, count) in &other.0 {
            match self.0.iter_mut().find(|(k, _)| *k == kind) {
                Some((_, total)) => *total += count,
                None => self.0.push((kind, count)),
            }
        }
    }

    fn total(&self) -> u64 {
        self.0.iter().map(|(_, c)| c).sum()
    }

    fn by_kind(&self) -> Vec<(String, u64)> {
        self.0.iter().map(|(k, c)| (format!("{k:?}"), *c)).collect()
    }
}

struct ConnectOutcome {
    connect: LatencyHist,
    cycle: LatencyHist,
    errors: ErrorCounts,
}

#[derive(Serialize)]
struct ConnectReport<'a> {
    probe: &'static str,
    label: Option<&'a str>,
    target: SocketAddr,
    threads: usize,
    ping: bool,
    reset: bool,
    unix_start: f64,
    elapsed_s: f64,
    connections: u64,
    connections_per_s: f64,
    errors: u64,
    errors_by_kind: Vec<(String, u64)>,
    connect_ns: Summary,
    cycle_ns: Summary,
}

pub fn connect_rate(args: &ConnectArgs) -> Result<()> {
    let clock = Clock::new();
    let unix_start = unix_now();
    let until = secs_to_ns(args.duration);
    let outcomes = thread::scope(|scope| {
        let handles: Vec<_> = (0..args.threads)
            .map(|index| {
                let clock = &clock;
                scope.spawn(move || connect_loop(args, clock, index, until))
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or(Err(Error::Protocol("connect thread panicked")))
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let elapsed = clock.now();
    let mut connect = LatencyHist::new()?;
    let mut cycle = LatencyHist::new()?;
    let mut errors = ErrorCounts::default();
    for outcome in &outcomes {
        connect.merge(&outcome.connect);
        cycle.merge(&outcome.cycle);
        errors.merge(&outcome.errors);
    }
    emit(&ConnectReport {
        probe: "tcp-connect",
        label: args.label.as_deref(),
        target: args.target,
        threads: args.threads,
        ping: args.ping,
        reset: args.reset,
        unix_start,
        elapsed_s: ns_to_secs(elapsed),
        connections: cycle.count(),
        connections_per_s: cycle.count() as f64 / ns_to_secs(elapsed).max(f64::MIN_POSITIVE),
        errors: errors.total(),
        errors_by_kind: errors.by_kind(),
        connect_ns: connect.summary(),
        cycle_ns: cycle.summary(),
    })
}

fn connect_loop(
    args: &ConnectArgs,
    clock: &Clock,
    index: usize,
    until: u64,
) -> Result<ConnectOutcome> {
    sys::pin(args.cpus.as_ref(), index)?;
    let mut outcome = ConnectOutcome {
        connect: LatencyHist::new()?,
        cycle: LatencyHist::new()?,
        errors: ErrorCounts::default(),
    };
    let mut msg = [0u8; HEADER_LEN];
    let mut reply = [0u8; HEADER_LEN];
    let mut seq = 0u64;
    while clock.now() < until {
        let t0 = clock.now();
        let stream = match TcpStream::connect_timeout(&args.target, CONNECT_TIMEOUT) {
            Ok(stream) => stream,
            Err(err) => {
                outcome.errors.add(err.kind());
                continue;
            }
        };
        outcome.connect.record(clock.now() - t0);
        if args.ping {
            Header {
                seq,
                intended: t0,
                sent: t0,
            }
            .encode(&mut msg);
            seq += 1;
            let exchanged = stream.set_nodelay(true).and_then(|()| {
                let mut s = &stream;
                write_all(&mut s, &msg)?;
                read_exact(&mut s, &mut reply)
            });
            if let Err(err) = exchanged {
                outcome.errors.add(err.kind());
                continue;
            }
        }
        if args.reset {
            let _ = socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO));
        }
        drop(stream);
        outcome.cycle.record(clock.now() - t0);
    }
    Ok(outcome)
}
