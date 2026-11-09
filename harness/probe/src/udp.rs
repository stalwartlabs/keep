/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    args::{CpuList, Wait},
    batch::{RecvBatch, SendBatch},
    clock::{Clock, ns_to_secs, secs_to_ns, unix_now},
    error::{Context, Error, Result},
    report::emit,
    sys,
};
use clap::{Args, value_parser};
use serde::Serialize;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::{
    hint::spin_loop,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::Duration,
};

const DATAGRAM_SLOT: usize = 2048;
const MAX_DATAGRAM: usize = 65_536;
const SOCKET_BUFFER: usize = 16 * 1024 * 1024;
const SINK_POLL: Duration = Duration::from_millis(100);
const NANOS_PER_MILLI: u64 = 1_000_000;

#[derive(Args)]
pub struct EchoArgs {
    #[arg(long, default_value_t = 7200)]
    port: u16,
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::UNSPECIFIED))]
    bind: IpAddr,
    #[arg(
        long,
        default_value_t = 1,
        help = "SO_REUSEPORT sockets, one thread each"
    )]
    threads: usize,
    #[arg(long)]
    cpus: Option<CpuList>,
    #[arg(long, value_enum, default_value_t = Wait::Block)]
    wait: Wait,
    #[arg(long, default_value_t = 50)]
    busy_poll_us: u32,
}

#[derive(Args)]
pub struct BlastArgs {
    #[arg(long, help = "Sink address, ip:port")]
    target: SocketAddr,
    #[arg(long, default_value_t = 18, value_parser = value_parser!(u32).range(1..=1472), help = "UDP payload bytes; 18 makes a 64-byte Ethernet frame")]
    size: u32,
    #[arg(
        long,
        default_value_t = 1,
        help = "Sender threads, one socket (flow) each"
    )]
    threads: usize,
    #[arg(long, default_value_t = 10.0)]
    duration: f64,
    #[arg(long, default_value_t = 64, value_parser = value_parser!(u32).range(1..=1024), help = "Datagrams per sendmmsg call; 1 uses plain send")]
    batch: u32,
    #[arg(long, help = "Packets per second per thread; unlimited if absent")]
    rate: Option<f64>,
    #[arg(long)]
    cpus: Option<CpuList>,
    #[arg(long)]
    label: Option<String>,
}

#[derive(Args)]
pub struct SinkArgs {
    #[arg(long, default_value_t = 7300)]
    port: u16,
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::UNSPECIFIED))]
    bind: IpAddr,
    #[arg(
        long,
        default_value_t = 1,
        help = "SO_REUSEPORT sockets, one thread each"
    )]
    threads: usize,
    #[arg(long, default_value_t = 30.0, help = "Give up after this many seconds")]
    duration: f64,
    #[arg(
        long,
        default_value_t = 1000,
        help = "Stop after this many idle milliseconds once packets arrived"
    )]
    idle_ms: u64,
    #[arg(long, default_value_t = 64, value_parser = value_parser!(u32).range(1..=1024))]
    batch: u32,
    #[arg(long)]
    cpus: Option<CpuList>,
    #[arg(long)]
    label: Option<String>,
}

fn reuseport_socket(bind: IpAddr, port: u16) -> Result<UdpSocket> {
    let addr = SocketAddr::new(bind, port);
    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))
        .context("UDP socket")?;
    socket.set_reuse_port(true).context("SO_REUSEPORT")?;
    let _ = socket.set_recv_buffer_size(SOCKET_BUFFER);
    socket.bind(&SockAddr::from(addr)).context("bind")?;
    Ok(socket.into())
}

#[derive(Serialize)]
struct Listening {
    event: &'static str,
    probe: &'static str,
    port: u16,
    threads: usize,
    wait: Wait,
}

pub fn echo(args: &EchoArgs) -> Result<()> {
    let sockets = (0..args.threads.max(1))
        .map(|_| reuseport_socket(args.bind, args.port))
        .collect::<Result<Vec<_>>>()?;
    emit(&Listening {
        event: "listening",
        probe: "udp-echo",
        port: args.port,
        threads: sockets.len(),
        wait: args.wait,
    })?;
    thread::scope(|scope| {
        let handles: Vec<_> = sockets
            .into_iter()
            .enumerate()
            .map(|(index, socket)| scope.spawn(move || echo_socket(&socket, args, index)))
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or(Err(Error::Protocol("echo thread panicked")))
            })
            .collect::<Result<Vec<_>>>()
    })?;
    Ok(())
}

fn echo_socket(socket: &UdpSocket, args: &EchoArgs, index: usize) -> Result<()> {
    sys::pin(args.cpus.as_ref(), index)?;
    match args.wait {
        Wait::Block => {}
        Wait::Spin => socket.set_nonblocking(true).context("O_NONBLOCK")?,
        Wait::BusyPoll => sys::set_busy_poll(socket, args.busy_poll_us)?,
    }
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        match socket.recv_from(&mut buf) {
            Ok((n, peer)) => {
                let reply = buf.get(..n).unwrap_or_default();
                while let Err(err) = socket.send_to(reply, peer) {
                    if err.kind() != io::ErrorKind::WouldBlock {
                        break;
                    }
                    spin_loop();
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => spin_loop(),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e).context("UDP echo receive"),
        }
    }
}

#[derive(Serialize)]
struct BlastReport<'a> {
    probe: &'static str,
    label: Option<&'a str>,
    target: SocketAddr,
    size: u32,
    threads: usize,
    batch: u32,
    rate_per_thread: Option<f64>,
    unix_start: f64,
    elapsed_s: f64,
    packets: u64,
    packets_per_s: f64,
    per_thread_pps: Vec<f64>,
    send_errors: u64,
}

struct BlastOutcome {
    packets: u64,
    errors: u64,
    elapsed: u64,
}

pub fn blast(args: &BlastArgs) -> Result<()> {
    let clock = Clock::new();
    let unix_start = unix_now();
    let until = secs_to_ns(args.duration);
    let outcomes = thread::scope(|scope| {
        let handles: Vec<_> = (0..args.threads.max(1))
            .map(|index| {
                let clock = &clock;
                scope.spawn(move || blast_thread(args, clock, index, until))
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or(Err(Error::Protocol("sender thread panicked")))
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let elapsed = outcomes.iter().map(|o| o.elapsed).max().unwrap_or_default();
    let packets = outcomes.iter().map(|o| o.packets).sum::<u64>();
    emit(&BlastReport {
        probe: "udp-blast",
        label: args.label.as_deref(),
        target: args.target,
        size: args.size,
        threads: outcomes.len(),
        batch: args.batch,
        rate_per_thread: args.rate,
        unix_start,
        elapsed_s: ns_to_secs(elapsed),
        packets,
        packets_per_s: packets as f64 / ns_to_secs(elapsed).max(f64::MIN_POSITIVE),
        per_thread_pps: outcomes
            .iter()
            .map(|o| o.packets as f64 / ns_to_secs(o.elapsed).max(f64::MIN_POSITIVE))
            .collect(),
        send_errors: outcomes.iter().map(|o| o.errors).sum(),
    })
}

fn blast_thread(args: &BlastArgs, clock: &Clock, index: usize, until: u64) -> Result<BlastOutcome> {
    sys::pin(args.cpus.as_ref(), index)?;
    let any: SocketAddr = if args.target.is_ipv4() {
        (Ipv4Addr::UNSPECIFIED, 0).into()
    } else {
        (std::net::Ipv6Addr::UNSPECIFIED, 0).into()
    };
    let socket = UdpSocket::bind(any).context("UDP bind")?;
    socket.connect(args.target).context("UDP connect")?;
    let _ = socket2::SockRef::from(&socket).set_send_buffer_size(SOCKET_BUFFER);
    let mut batch = SendBatch::new(args.size as usize, args.batch as usize);
    let ns_per_packet = args.rate.map(|rate| 1e9 / rate);
    let start = clock.now();
    let (mut packets, mut errors) = (0u64, 0u64);
    while clock.now() < until {
        if let Some(gap) = ns_per_packet {
            clock.wait_until(start + (packets as f64 * gap) as u64);
        }
        match batch.send(&socket) {
            Ok(n) => packets += n as u64,
            Err(_) => errors += 1,
        }
    }
    Ok(BlastOutcome {
        packets,
        errors,
        elapsed: clock.now() - start,
    })
}

#[derive(Serialize)]
struct SinkReport<'a> {
    probe: &'static str,
    label: Option<&'a str>,
    port: u16,
    threads: usize,
    unix_start: f64,
    active_s: f64,
    packets: u64,
    packets_per_s: f64,
    per_thread_packets: Vec<u64>,
}

struct SinkOutcome {
    packets: u64,
    first: u64,
    last: u64,
}

pub fn sink(args: &SinkArgs) -> Result<()> {
    let sockets = (0..args.threads.max(1))
        .map(|_| reuseport_socket(args.bind, args.port))
        .collect::<Result<Vec<_>>>()?;
    for socket in &sockets {
        socket
            .set_read_timeout(Some(SINK_POLL))
            .context("SO_RCVTIMEO")?;
    }
    emit(&Listening {
        event: "listening",
        probe: "udp-sink",
        port: args.port,
        threads: sockets.len(),
        wait: Wait::Block,
    })?;
    let clock = Clock::new();
    let unix_start = unix_now();
    let last_packet = AtomicU64::new(0);
    let outcomes = thread::scope(|scope| {
        let handles: Vec<_> = sockets
            .into_iter()
            .enumerate()
            .map(|(index, socket)| {
                let (clock, last_packet) = (&clock, &last_packet);
                scope.spawn(move || sink_socket(&socket, args, clock, last_packet, index))
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or(Err(Error::Protocol("sink thread panicked")))
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let packets = outcomes.iter().map(|o| o.packets).sum::<u64>();
    let first = outcomes
        .iter()
        .filter(|o| o.packets > 0)
        .map(|o| o.first)
        .min()
        .unwrap_or_default();
    let last = outcomes.iter().map(|o| o.last).max().unwrap_or_default();
    let active = ns_to_secs(last.saturating_sub(first));
    emit(&SinkReport {
        probe: "udp-sink",
        label: args.label.as_deref(),
        port: args.port,
        threads: outcomes.len(),
        unix_start,
        active_s: active,
        packets,
        packets_per_s: packets as f64 / active.max(f64::MIN_POSITIVE),
        per_thread_packets: outcomes.iter().map(|o| o.packets).collect(),
    })
}

fn sink_socket(
    socket: &UdpSocket,
    args: &SinkArgs,
    clock: &Clock,
    last_packet: &AtomicU64,
    index: usize,
) -> Result<SinkOutcome> {
    sys::pin(args.cpus.as_ref(), index)?;
    let mut batch = RecvBatch::new(DATAGRAM_SLOT, args.batch as usize);
    let give_up = secs_to_ns(args.duration);
    let idle = args.idle_ms.saturating_mul(NANOS_PER_MILLI);
    let mut outcome = SinkOutcome {
        packets: 0,
        first: 0,
        last: 0,
    };
    loop {
        match batch.recv(socket) {
            Ok(n) => {
                let now = clock.now();
                if outcome.packets == 0 {
                    outcome.first = now;
                }
                outcome.packets += n as u64;
                outcome.last = now;
                last_packet.fetch_max(now, Ordering::Relaxed);
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::Interrupted
                ) => {}
            Err(e) => return Err(e).context("UDP sink receive"),
        }
        let now = clock.now();
        let seen = last_packet.load(Ordering::Relaxed);
        if now >= give_up || (seen > 0 && now.saturating_sub(seen) >= idle) {
            return Ok(outcome);
        }
    }
}
