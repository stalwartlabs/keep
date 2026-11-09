/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    args::{Arrival, CpuList, Proto, Wait},
    clock::{Clock, Schedule, ns_to_secs, secs_to_ns},
    error::{Context, Error, Result},
    hist::{LatencyHist, Summary},
    link::{Link, Recv, RxInfo},
    report::emit,
    sys,
    wire::{Header, Message},
};
use clap::{Args, value_parser};
use serde::Serialize;
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, SystemTime},
};

const START_DELAY_NS: u64 = 20_000_000;
const NOT_DONE: u64 = u64::MAX;
const NANOS_PER_MILLI: u64 = 1_000_000;

#[derive(Args)]
pub struct PingArgs {
    #[arg(long, help = "Echo server address, ip:port")]
    target: SocketAddr,
    #[arg(long, default_value_t = 64, value_parser = value_parser!(u32).range(24..=65000), help = "Message size in bytes (the first 24 carry seq and timestamps)")]
    size: u32,
    #[arg(
        long,
        default_value_t = 100_000,
        help = "Measured messages per connection, after warmup"
    )]
    count: u64,
    #[arg(long, help = "Measured seconds per connection; overrides --count")]
    duration: Option<f64>,
    #[arg(
        long,
        default_value_t = 1000,
        help = "Unrecorded messages per connection before measuring"
    )]
    warmup: u64,
    #[arg(
        long,
        help = "Paced (open-loop) mode: messages per second per connection; latency counts from the intended send time"
    )]
    rate: Option<f64>,
    #[arg(long, value_enum, default_value_t = Arrival::Uniform, help = "Inter-send gaps in paced mode")]
    arrival: Arrival,
    #[arg(long, default_value_t = 1, value_parser = value_parser!(u16).range(1..=1024))]
    conns: u16,
    #[arg(long, value_enum, default_value_t = Wait::Block, help = "How the client waits for replies")]
    wait: Wait,
    #[arg(
        long,
        default_value_t = 50,
        help = "SO_BUSY_POLL microseconds for --wait busy-poll"
    )]
    busy_poll_us: u32,
    #[arg(
        long,
        help = "CPUs for client threads, round robin, e.g. 2-5; paced mode uses a receiver and a sender thread per connection"
    )]
    cpus: Option<CpuList>,
    #[arg(
        long,
        default_value_t = 200,
        help = "UDP reply timeout in milliseconds; later replies count as lost"
    )]
    timeout_ms: u64,
    #[arg(
        long,
        help = "Also write the latency histogram as an HdrHistogram interval log"
    )]
    hlog: Option<PathBuf>,
    #[arg(long, help = "Free-form label copied into the report")]
    label: Option<String>,
    #[arg(long, default_value_t = 1, help = "Seed for Poisson arrivals")]
    seed: u64,
}

struct Shape {
    size: usize,
    timeout_ns: u64,
}

struct ConnOutcome {
    rtt: LatencyHist,
    service: Option<LatencyHist>,
    lag: Option<LatencyHist>,
    sent: u64,
    received: u64,
    rx: RxInfo,
}

#[derive(Serialize)]
struct PingReport<'a> {
    probe: &'static str,
    label: Option<&'a str>,
    target: SocketAddr,
    size: u32,
    conns: u16,
    mode: &'static str,
    arrival: Option<Arrival>,
    rate_per_conn: Option<f64>,
    wait: Wait,
    busy_poll_us: Option<u32>,
    warmup: u64,
    sent: u64,
    received: u64,
    lost: u64,
    unix_start: f64,
    elapsed_s: f64,
    replies_per_s: f64,
    rtt_ns: Summary,
    service_ns: Option<Summary>,
    lag_ns: Option<Summary>,
    rx: Vec<RxInfo>,
}

pub fn run(args: &PingArgs, proto: Proto) -> Result<()> {
    if let (Some(_), Some(list)) = (args.rate, args.cpus.as_ref())
        && (0..usize::from(args.conns)).any(|i| list.cpu_for(2 * i) == list.cpu_for(2 * i + 1))
    {
        return Err(Error::Usage(
            "paced mode pins a receiver and a spinning sender per connection; give them different CPUs",
        ));
    }
    let clock = Clock::new();
    let shape = Shape {
        size: args.size as usize,
        timeout_ns: args.timeout_ms.saturating_mul(NANOS_PER_MILLI),
    };
    let timeout = Duration::from_millis(args.timeout_ms);
    let links = (0..args.conns)
        .map(|_| Link::connect(proto, args.target, args.wait, args.busy_poll_us, timeout))
        .collect::<Result<Vec<_>>>()?;
    let wall_start = SystemTime::now();
    let unix_start = crate::clock::unix_now();
    let started = clock.now();
    let outcomes = thread::scope(|scope| {
        let handles: Vec<_> = links
            .into_iter()
            .enumerate()
            .map(|(index, link)| {
                let (clock, shape) = (&clock, &shape);
                scope.spawn(move || match args.rate {
                    None => closed(link, args, clock, shape, index),
                    Some(rate) => paced(link, args, clock, shape, index, rate),
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or(Err(Error::Protocol("connection thread panicked")))
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let elapsed = clock.now().saturating_sub(started);

    let mut rtt = LatencyHist::new()?;
    let mut service = args.rate.map(|_| LatencyHist::new()).transpose()?;
    let mut lag = args.rate.map(|_| LatencyHist::new()).transpose()?;
    let (mut sent, mut received) = (0, 0);
    let mut rx = Vec::with_capacity(outcomes.len());
    for outcome in outcomes {
        rtt.merge(&outcome.rtt);
        if let (Some(total), Some(part)) = (service.as_mut(), outcome.service.as_ref()) {
            total.merge(part);
        }
        if let (Some(total), Some(part)) = (lag.as_mut(), outcome.lag.as_ref()) {
            total.merge(part);
        }
        sent += outcome.sent;
        received += outcome.received;
        rx.push(outcome.rx);
    }
    let report = PingReport {
        probe: proto.probe_name(),
        label: args.label.as_deref(),
        target: args.target,
        size: args.size,
        conns: args.conns,
        mode: if args.rate.is_some() {
            "paced"
        } else {
            "closed"
        },
        arrival: args.rate.map(|_| args.arrival),
        rate_per_conn: args.rate,
        wait: args.wait,
        busy_poll_us: (args.wait == Wait::BusyPoll).then_some(args.busy_poll_us),
        warmup: args.warmup,
        sent,
        received,
        lost: sent.saturating_sub(received),
        unix_start,
        elapsed_s: ns_to_secs(elapsed),
        replies_per_s: received as f64 / ns_to_secs(elapsed).max(f64::MIN_POSITIVE),
        rtt_ns: rtt.summary(),
        service_ns: service.as_ref().map(LatencyHist::summary),
        lag_ns: lag.as_ref().map(LatencyHist::summary),
        rx,
    };
    emit(&report)?;
    if let Some(path) = &args.hlog {
        rtt.write_hlog(
            path,
            wall_start,
            Duration::from_nanos(elapsed),
            proto.probe_name(),
        )?;
    }
    Ok(())
}

struct ClosedLoop<'a> {
    link: Link,
    clock: &'a Clock,
    msg: Message,
    reply: Message,
    rtt: LatencyHist,
    timeout_ns: u64,
    sent: u64,
    received: u64,
}

impl ClosedLoop<'_> {
    #[inline]
    fn round(&mut self, seq: u64, record: bool) -> Result<()> {
        let t0 = self.clock.now();
        self.msg.write_header(Header {
            seq,
            intended: t0,
            sent: t0,
        });
        self.link.send(self.msg.bytes()).context("send")?;
        self.sent += u64::from(record);
        let deadline = t0.saturating_add(self.timeout_ns);
        loop {
            match self
                .link
                .recv(self.reply.bytes_mut(), self.clock, deadline)
                .context("receive")?
            {
                Recv::Message if self.reply.header().seq != seq => continue,
                Recv::Message => {
                    if record {
                        self.rtt.record(self.clock.now().saturating_sub(t0));
                        self.received += 1;
                    }
                    return Ok(());
                }
                Recv::TimedOut => return Ok(()),
            }
        }
    }
}

fn closed(
    link: Link,
    args: &PingArgs,
    clock: &Clock,
    shape: &Shape,
    index: usize,
) -> Result<ConnOutcome> {
    let cpu = sys::pin(args.cpus.as_ref(), index)?;
    let mut lp = ClosedLoop {
        link,
        clock,
        msg: Message::new(shape.size),
        reply: Message::new(shape.size),
        rtt: LatencyHist::new()?,
        timeout_ns: shape.timeout_ns,
        sent: 0,
        received: 0,
    };
    for seq in 0..args.warmup {
        lp.round(seq, false)?;
    }
    let mut seq = args.warmup;
    match args.duration {
        Some(secs) => {
            let until = clock.now().saturating_add(secs_to_ns(secs));
            while clock.now() < until {
                lp.round(seq, true)?;
                seq += 1;
            }
        }
        None => {
            for _ in 0..args.count {
                lp.round(seq, true)?;
                seq += 1;
            }
        }
    }
    Ok(ConnOutcome {
        rx: lp.link.rx_info(cpu),
        rtt: lp.rtt,
        service: None,
        lag: None,
        sent: lp.sent,
        received: lp.received,
    })
}

struct Pace<'a> {
    clock: &'a Clock,
    shape: &'a Shape,
    warmup: u64,
    total: u64,
    done_at: AtomicU64,
}

fn paced(
    link: Link,
    args: &PingArgs,
    clock: &Clock,
    shape: &Shape,
    index: usize,
    rate: f64,
) -> Result<ConnOutcome> {
    let measured = args
        .duration
        .map_or(args.count, |secs| (secs * rate) as u64);
    let stagger = index as f64 * 1e9 / rate / f64::from(args.conns);
    let origin = clock.now() + START_DELAY_NS + stagger as u64;
    let pace = Pace {
        clock,
        shape,
        warmup: args.warmup,
        total: args.warmup + measured,
        done_at: AtomicU64::new(NOT_DONE),
    };
    let schedule = Schedule::new(
        origin,
        rate,
        args.arrival,
        args.seed.wrapping_add(index as u64),
    );
    let tx = link.try_clone()?;
    let cpus = args.cpus.as_ref();
    thread::scope(|scope| {
        let sender = scope.spawn(|| {
            let result =
                sys::pin(cpus, 2 * index + 1).and_then(|_| send_paced(tx, &pace, schedule));
            pace.done_at.store(clock.now(), Ordering::Release);
            result
        });
        let cpu = sys::pin(cpus, 2 * index)?;
        let received = receive_paced(link, &pace)?;
        let lag = sender
            .join()
            .unwrap_or(Err(Error::Protocol("sender thread panicked")))?;
        Ok(ConnOutcome {
            rx: received.link.rx_info(cpu),
            rtt: received.rtt,
            service: Some(received.service),
            lag: Some(lag),
            sent: measured,
            received: received.count,
        })
    })
}

fn send_paced(mut link: Link, pace: &Pace<'_>, mut schedule: Schedule) -> Result<LatencyHist> {
    let mut msg = Message::new(pace.shape.size);
    let mut lag = LatencyHist::new()?;
    for seq in 0..pace.total {
        let intended = schedule.next_deadline();
        pace.clock.wait_until(intended);
        let sent = pace.clock.now();
        msg.write_header(Header {
            seq,
            intended,
            sent,
        });
        if let Err(err) = link.send(msg.bytes()) {
            link.shutdown();
            return Err(err).context("send");
        }
        if seq >= pace.warmup {
            lag.record(sent - intended);
        }
    }
    Ok(lag)
}

struct Received {
    link: Link,
    rtt: LatencyHist,
    service: LatencyHist,
    count: u64,
}

fn receive_paced(mut link: Link, pace: &Pace<'_>) -> Result<Received> {
    let mut reply = Message::new(pace.shape.size);
    let mut rtt = LatencyHist::new()?;
    let mut service = LatencyHist::new()?;
    let (mut seen, mut count) = (0u64, 0u64);
    while seen < pace.total {
        let deadline = pace.clock.now().saturating_add(pace.shape.timeout_ns);
        match link
            .recv(reply.bytes_mut(), pace.clock, deadline)
            .context("receive")?
        {
            Recv::Message => {
                let now = pace.clock.now();
                let header = reply.header();
                seen += 1;
                if (pace.warmup..pace.total).contains(&header.seq) {
                    rtt.record(now.saturating_sub(header.intended));
                    service.record(now.saturating_sub(header.sent));
                    count += 1;
                }
            }
            Recv::TimedOut => {
                let done = pace.done_at.load(Ordering::Acquire);
                if done != NOT_DONE
                    && pace.clock.now() >= done.saturating_add(pace.shape.timeout_ns)
                {
                    break;
                }
            }
        }
    }
    Ok(Received {
        link,
        rtt,
        service,
        count,
    })
}
