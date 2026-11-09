/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::io;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicU16, Ordering};

use io_uring::Submitter;
use io_uring::types::BufRingEntry;

pub struct BufRing {
    entries: NonNull<BufRingEntry>,
    map_len: usize,
    data: Box<[u8]>,
    buf_size: usize,
    mask: u16,
    tail: u16,
    group: u16,
}

impl BufRing {
    pub fn new(
        submitter: &Submitter<'_>,
        group: u16,
        count: u16,
        buf_size: u32,
    ) -> io::Result<Self> {
        if !count.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "buffer ring size must be a power of two",
            ));
        }
        let map_len = usize::from(count) * size_of::<BufRingEntry>();
        let addr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                map_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_PRIVATE | libc::MAP_POPULATE,
                -1,
                0,
            )
        };
        if addr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let entries =
            NonNull::new(addr.cast::<BufRingEntry>()).ok_or_else(io::Error::last_os_error)?;
        let buf_size = buf_size as usize;
        let mut ring = BufRing {
            entries,
            map_len,
            data: vec![0u8; usize::from(count) * buf_size].into_boxed_slice(),
            buf_size,
            mask: count - 1,
            tail: 0,
            group,
        };
        unsafe { submitter.register_buf_ring_with_flags(addr as u64, count, group, 0)? };
        for bid in 0..count {
            ring.push(bid);
        }
        ring.publish();
        Ok(ring)
    }

    pub fn group(&self) -> u16 {
        self.group
    }

    #[inline]
    pub fn slice(&self, bid: u16, len: usize) -> &[u8] {
        let start = usize::from(bid) * self.buf_size;
        self.data
            .get(start..start + len.min(self.buf_size))
            .unwrap_or_default()
    }

    #[inline]
    pub fn recycle(&mut self, bid: u16) {
        self.push(bid);
        self.publish();
    }

    #[inline]
    fn push(&mut self, bid: u16) {
        let start = usize::from(bid) * self.buf_size;
        let addr = self
            .data
            .get(start..)
            .map_or(0, |buffer| buffer.as_ptr() as u64);
        let slot = usize::from(self.tail & self.mask);
        let entry = unsafe { &mut *self.entries.as_ptr().add(slot) };
        entry.set_addr(addr);
        entry.set_len(self.buf_size as u32);
        entry.set_bid(bid);
        self.tail = self.tail.wrapping_add(1);
    }

    #[inline]
    fn publish(&self) {
        let tail =
            unsafe { AtomicU16::from_ptr(BufRingEntry::tail(self.entries.as_ptr()).cast_mut()) };
        tail.store(self.tail, Ordering::Release);
    }
}

impl Drop for BufRing {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.entries.as_ptr().cast(), self.map_len) };
    }
}
