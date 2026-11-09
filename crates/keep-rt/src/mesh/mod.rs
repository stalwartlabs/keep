/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

mod parker;

use std::collections::VecDeque;
use std::io;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::Arc;

use rtrb::{Consumer, Producer, RingBuffer};

use parker::Parker;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct MeshConfig {
    pub capacity: usize,
    pub backlog: usize,
}

impl MeshConfig {
    pub const DEFAULT: MeshConfig = MeshConfig {
        capacity: 256,
        backlog: 4096,
    };
}

struct Peer {
    parker: Parker,
    eventfd: Option<OwnedFd>,
}

struct Link<T> {
    tx: Producer<T>,
    backlog: VecDeque<T>,
    touched: bool,
}

pub struct Inbox<T> {
    rings: Box<[(usize, Consumer<T>)]>,
}

pub struct Outbox<T> {
    links: Box<[Option<Link<T>>]>,
    touched: Vec<usize>,
    backlog: usize,
}

pub struct Port<T> {
    id: usize,
    peers: Arc<[Peer]>,
    inbox: Inbox<T>,
    outbox: Outbox<T>,
}

pub fn mesh<T: Send>(endpoints: usize, config: MeshConfig) -> io::Result<Vec<Port<T>>> {
    let peers: Arc<[Peer]> = (0..endpoints)
        .map(|_| {
            Ok(Peer {
                parker: Parker::new(),
                eventfd: eventfd()?,
            })
        })
        .collect::<io::Result<Vec<_>>>()?
        .into();
    let mut outboxes: Vec<Vec<Option<Link<T>>>> = (0..endpoints)
        .map(|_| (0..endpoints).map(|_| None).collect())
        .collect();
    let mut inboxes: Vec<Vec<(usize, Consumer<T>)>> = (0..endpoints)
        .map(|_| Vec::with_capacity(endpoints.saturating_sub(1)))
        .collect();
    for (source, links) in outboxes.iter_mut().enumerate() {
        for (target, (link, inbox)) in links.iter_mut().zip(inboxes.iter_mut()).enumerate() {
            if source != target {
                let (tx, rx) = RingBuffer::new(config.capacity.max(1));
                *link = Some(Link {
                    tx,
                    backlog: VecDeque::new(),
                    touched: false,
                });
                inbox.push((source, rx));
            }
        }
    }
    Ok(outboxes
        .into_iter()
        .zip(inboxes)
        .enumerate()
        .map(|(id, (links, rings))| Port {
            id,
            peers: peers.clone(),
            inbox: Inbox {
                rings: rings.into_boxed_slice(),
            },
            outbox: Outbox {
                links: links.into_boxed_slice(),
                touched: Vec::with_capacity(endpoints),
                backlog: config.backlog.max(1),
            },
        })
        .collect())
}

impl<T> Inbox<T> {
    #[inline]
    pub fn drain(&mut self, mut handle: impl FnMut(usize, T)) -> usize {
        let mut drained = 0;
        for (source, ring) in self.rings.iter_mut() {
            let waiting = ring.slots();
            if waiting == 0 {
                continue;
            }
            if let Ok(chunk) = ring.read_chunk(waiting) {
                drained += waiting;
                for msg in chunk {
                    handle(*source, msg);
                }
            }
        }
        drained
    }

    pub fn is_empty(&self) -> bool {
        self.rings.iter().all(|(_, ring)| ring.is_empty())
    }
}

impl<T> Outbox<T> {
    #[inline]
    pub fn send(&mut self, target: usize, msg: T) -> Result<(), T> {
        let Some(Some(link)) = self.links.get_mut(target) else {
            return Err(msg);
        };
        if link.backlog.len() >= self.backlog {
            return Err(msg);
        }
        link.backlog.push_back(msg);
        if !link.touched {
            link.touched = true;
            self.touched.push(target);
        }
        Ok(())
    }

    pub fn has_backlog(&self) -> bool {
        !self.touched.is_empty()
    }
}

impl<T> Port<T> {
    pub fn id(&self) -> usize {
        self.id
    }

    pub fn endpoints(&self) -> usize {
        self.peers.len()
    }

    pub fn parts(&mut self) -> (&mut Inbox<T>, &mut Outbox<T>) {
        (&mut self.inbox, &mut self.outbox)
    }

    pub fn wake_fd(&self) -> Option<BorrowedFd<'_>> {
        self.own()
            .and_then(|peer| peer.eventfd.as_ref())
            .map(AsFd::as_fd)
    }

    pub fn publish(&mut self) -> io::Result<u32> {
        let Port { peers, outbox, .. } = self;
        let Outbox { links, touched, .. } = outbox;
        let mut wakes = 0;
        let mut result = Ok(());
        touched.retain(|&target| {
            let Some(Some(link)) = links.get_mut(target) else {
                return false;
            };
            let fits = link.backlog.len().min(link.tx.slots());
            if fits > 0
                && let Ok(chunk) = link.tx.write_chunk_uninit(fits)
            {
                chunk.fill_from_iter(link.backlog.drain(..fits));
            }
            if let Some(peer) = peers.get(target)
                && peer.parker.claim_wake()
            {
                wakes += 1;
                if let Err(err) = wake(peer) {
                    result = Err(err);
                }
            }
            link.touched = !link.backlog.is_empty();
            link.touched
        });
        result.map(|()| wakes)
    }

    pub fn park(&mut self) -> bool {
        if !self.is_quiet() {
            return false;
        }
        let Some(peer) = self.peers.get(self.id) else {
            return false;
        };
        peer.parker.prepare();
        if self.inbox.is_empty() {
            return true;
        }
        peer.parker.resume();
        false
    }

    #[inline]
    pub fn is_quiet(&self) -> bool {
        !self.outbox.has_backlog() && self.inbox.is_empty()
    }

    pub fn resume(&self) {
        if let Some(peer) = self.own() {
            peer.parker.resume();
        }
    }

    pub fn ack_wake(&self) {
        if let Some(peer) = self.own() {
            ack(peer);
        }
    }

    fn own(&self) -> Option<&Peer> {
        self.peers.get(self.id)
    }
}

#[cfg(target_os = "linux")]
fn eventfd() -> io::Result<Option<OwnedFd>> {
    use std::os::fd::FromRawFd;

    let raw = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(Some(unsafe { OwnedFd::from_raw_fd(raw) }))
}

#[cfg(not(target_os = "linux"))]
fn eventfd() -> io::Result<Option<OwnedFd>> {
    Ok(None)
}

#[cfg(target_os = "linux")]
fn wake(peer: &Peer) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let Some(fd) = &peer.eventfd else {
        return Ok(());
    };
    let one: u64 = 1;
    let written = unsafe { libc::write(fd.as_raw_fd(), (&raw const one).cast(), size_of::<u64>()) };
    if written == size_of::<u64>() as isize {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    if err.kind() == io::ErrorKind::WouldBlock {
        Ok(())
    } else {
        Err(err)
    }
}

#[cfg(not(target_os = "linux"))]
fn wake(_peer: &Peer) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn ack(peer: &Peer) {
    use std::os::fd::AsRawFd;

    if let Some(fd) = &peer.eventfd {
        let mut count: u64 = 0;
        unsafe { libc::read(fd.as_raw_fd(), (&raw mut count).cast(), size_of::<u64>()) };
    }
}

#[cfg(not(target_os = "linux"))]
fn ack(_peer: &Peer) {}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-rt/mesh/mod.rs"]
mod tests;
