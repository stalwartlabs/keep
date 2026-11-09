/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

mod args;
mod batch;
mod clock;
mod cpu;
mod error;
mod gap;
mod hist;
mod link;
mod ping;
mod report;
mod rng;
mod sys;
mod tcp;
mod udp;
mod wire;

use args::Proto;
use clap::{Parser, Subcommand};
use std::process::ExitCode;

#[derive(Parser)]
#[command(
    name = "netprobe",
    version,
    about = "Network and CPU probes for the Keep lab. Every report is one JSON line on stdout."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "TCP echo server (Rust or C), one thread per connection")]
    TcpEcho(tcp::EchoArgs),
    #[command(
        about = "TCP ping-pong latency histogram: closed loop, or paced from the intended send time"
    )]
    TcpPing(ping::PingArgs),
    #[command(about = "TCP connection setup rate against an echo server")]
    TcpConnect(tcp::ConnectArgs),
    #[command(about = "UDP echo server")]
    UdpEcho(udp::EchoArgs),
    #[command(about = "UDP ping-pong latency histogram with loss counting")]
    UdpPing(ping::PingArgs),
    #[command(about = "Small-packet UDP sender for packets-per-second ceilings")]
    UdpBlast(udp::BlastArgs),
    #[command(about = "UDP receiver that counts packets per second")]
    UdpSink(udp::SinkArgs),
    #[command(
        about = "Fixed CPU work (ALU chain, L2 and DRAM pointer chase) used as a session control"
    )]
    CpuLoop(cpu::CpuArgs),
    #[command(
        about = "Spin on the clock and histogram the gaps: stalls a busy-polling thread would see"
    )]
    SpinGap(gap::GapArgs),
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    sys::reduce_timer_slack();
    let result = match &cli.command {
        Command::TcpEcho(args) => tcp::echo(args),
        Command::TcpPing(args) => ping::run(args, Proto::Tcp),
        Command::TcpConnect(args) => tcp::connect_rate(args),
        Command::UdpEcho(args) => udp::echo(args),
        Command::UdpPing(args) => ping::run(args, Proto::Udp),
        Command::UdpBlast(args) => udp::blast(args),
        Command::UdpSink(args) => udp::sink(args),
        Command::CpuLoop(args) => cpu::run(args),
        Command::SpinGap(args) => gap::run(args),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("netprobe: {err}");
            ExitCode::FAILURE
        }
    }
}
