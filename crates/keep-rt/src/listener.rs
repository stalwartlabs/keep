/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fmt;
use std::io;
use std::net::{SocketAddr, TcpListener};

use socket2::{Domain, Protocol, Socket, Type};

use crate::socket::{Applied, Tuning};

pub const BACKLOG: i32 = 8192;

pub const MAX_STEERED_SHARDS: usize = 256;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Steering {
    Hash,
    Cpu,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SteeringError {
    Unpinned { shards: usize, cpus: usize },
    TooManyShards(usize),
}

impl fmt::Display for SteeringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SteeringError::Unpinned { shards, cpus } => write!(
                f,
                "cpu steering needs every shard pinned: {shards} shards, {cpus} cpus"
            ),
            SteeringError::TooManyShards(shards) => write!(
                f,
                "cpu steering maps at most {MAX_STEERED_SHARDS} shards, got {shards}"
            ),
        }
    }
}

impl std::error::Error for SteeringError {}

pub struct Listeners {
    pub listeners: Vec<TcpListener>,
    pub applied: Applied,
}

pub fn shard_listeners(
    addr: SocketAddr,
    shards: usize,
    cpus: &[usize],
    steering: Steering,
    tuning: &Tuning,
) -> io::Result<Listeners> {
    let (first, applied) = reuseport_listener(addr, tuning)?;
    let addr = first.local_addr()?;
    let mut listeners = Vec::with_capacity(shards.max(1));
    listeners.push(first);
    for _ in 1..shards {
        listeners.push(reuseport_listener(addr, tuning)?.0);
    }
    if steering == Steering::Cpu {
        let pinned = cpus.get(..shards).ok_or_else(|| {
            io::Error::other(SteeringError::Unpinned {
                shards,
                cpus: cpus.len(),
            })
        })?;
        if let Some(first) = listeners.first() {
            attach_cpu_steering(first, pinned)?;
        }
    }
    Ok(Listeners { listeners, applied })
}

fn reuseport_listener(addr: SocketAddr, tuning: &Tuning) -> io::Result<(TcpListener, Applied)> {
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.set_cloexec(true)?;
    let applied = tuning.apply(&socket)?;
    socket.bind(&addr.into())?;
    socket.listen(BACKLOG)?;
    Ok((socket.into(), applied))
}

#[cfg(target_os = "linux")]
fn attach_cpu_steering(listener: &TcpListener, cpus: &[usize]) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let program = cpu_steering_program(cpus)?;
    let len = u16::try_from(program.len())
        .map_err(|_| io::Error::other(SteeringError::TooManyShards(cpus.len())))?;
    let fprog = libc::sock_fprog {
        len,
        filter: program.as_ptr().cast_mut(),
    };
    let result = unsafe {
        libc::setsockopt(
            listener.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_ATTACH_REUSEPORT_CBPF,
            (&raw const fprog).cast(),
            size_of::<libc::sock_fprog>() as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn attach_cpu_steering(_listener: &TcpListener, _cpus: &[usize]) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "cpu steering needs Linux",
    ))
}

#[cfg(target_os = "linux")]
mod cbpf {
    pub const LD_W_ABS: u16 = 0x20;
    pub const JEQ_K: u16 = 0x15;
    pub const RET_K: u16 = 0x06;
    pub const AD_CPU: u32 = 0xffff_f000 + 36;
}

#[cfg(target_os = "linux")]
fn cpu_steering_program(cpus: &[usize]) -> io::Result<Vec<libc::sock_filter>> {
    use cbpf::{AD_CPU, JEQ_K, LD_W_ABS, RET_K};

    let too_many = || io::Error::other(SteeringError::TooManyShards(cpus.len()));
    if cpus.len() > MAX_STEERED_SHARDS {
        return Err(too_many());
    }
    let insn = |code, jt, jf, k| libc::sock_filter { code, jt, jf, k };
    let mut program = Vec::with_capacity(2 * cpus.len() + 2);
    program.push(insn(LD_W_ABS, 0, 0, AD_CPU));
    for (shard, &cpu) in (0u32..).zip(cpus) {
        let cpu = u32::try_from(cpu).map_err(|_| too_many())?;
        program.push(insn(JEQ_K, 0, 1, cpu));
        program.push(insn(RET_K, 0, 0, shard));
    }
    let fallback = u32::try_from(cpus.len()).map_err(|_| too_many())?;
    program.push(insn(RET_K, 0, 0, fallback));
    Ok(program)
}

#[cfg(all(test, keep_tests, target_os = "linux"))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-rt/listener.rs"]
mod tests;
