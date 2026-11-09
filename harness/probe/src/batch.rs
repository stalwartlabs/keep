/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::{io, net::UdpSocket};

pub struct SendBatch {
    payload: Box<[u8]>,
    count: usize,
    #[cfg(target_os = "linux")]
    iov: libc::iovec,
    #[cfg(target_os = "linux")]
    headers: Box<[libc::mmsghdr]>,
}

pub struct RecvBatch {
    data: Box<[u8]>,
    slot: usize,
    #[cfg(target_os = "linux")]
    iovecs: Box<[libc::iovec]>,
    #[cfg(target_os = "linux")]
    headers: Box<[libc::mmsghdr]>,
}

#[cfg(target_os = "linux")]
fn empty_header() -> libc::mmsghdr {
    unsafe { std::mem::zeroed() }
}

#[cfg(target_os = "linux")]
fn os_count(rc: libc::c_int) -> io::Result<usize> {
    usize::try_from(rc).map_err(|_| io::Error::last_os_error())
}

impl SendBatch {
    pub fn new(payload_len: usize, count: usize) -> Self {
        let count = count.max(1);
        Self {
            payload: vec![0u8; payload_len].into_boxed_slice(),
            count,
            #[cfg(target_os = "linux")]
            iov: libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: payload_len,
            },
            #[cfg(target_os = "linux")]
            headers: (0..count).map(|_| empty_header()).collect(),
        }
    }

    #[cfg(target_os = "linux")]
    pub fn send(&mut self, socket: &UdpSocket) -> io::Result<usize> {
        use std::os::fd::AsRawFd;
        if self.count == 1 {
            return socket.send(&self.payload).map(|_| 1);
        }
        self.iov.iov_base = self.payload.as_mut_ptr().cast();
        let iov: *mut libc::iovec = &raw mut self.iov;
        for header in self.headers.iter_mut() {
            header.msg_hdr.msg_iov = iov;
            header.msg_hdr.msg_iovlen = 1;
        }
        let len = libc::c_uint::try_from(self.headers.len()).unwrap_or(libc::c_uint::MAX);
        os_count(unsafe { libc::sendmmsg(socket.as_raw_fd(), self.headers.as_mut_ptr(), len, 0) })
    }

    #[cfg(not(target_os = "linux"))]
    pub fn send(&mut self, socket: &UdpSocket) -> io::Result<usize> {
        for sent in 0..self.count {
            if let Err(err) = socket.send(&self.payload) {
                return if sent == 0 { Err(err) } else { Ok(sent) };
            }
        }
        Ok(self.count)
    }
}

impl RecvBatch {
    pub fn new(slot: usize, count: usize) -> Self {
        let count = count.max(1);
        Self {
            data: vec![0u8; slot * count].into_boxed_slice(),
            slot,
            #[cfg(target_os = "linux")]
            iovecs: (0..count)
                .map(|_| libc::iovec {
                    iov_base: std::ptr::null_mut(),
                    iov_len: slot,
                })
                .collect(),
            #[cfg(target_os = "linux")]
            headers: (0..count).map(|_| empty_header()).collect(),
        }
    }

    #[cfg(target_os = "linux")]
    pub fn recv(&mut self, socket: &UdpSocket) -> io::Result<usize> {
        use std::os::fd::AsRawFd;
        for (iov, chunk) in self
            .iovecs
            .iter_mut()
            .zip(self.data.chunks_exact_mut(self.slot))
        {
            iov.iov_base = chunk.as_mut_ptr().cast();
        }
        for (header, iov) in self.headers.iter_mut().zip(self.iovecs.iter_mut()) {
            header.msg_hdr.msg_iov = iov;
            header.msg_hdr.msg_iovlen = 1;
            header.msg_len = 0;
        }
        let len = libc::c_uint::try_from(self.headers.len()).unwrap_or(libc::c_uint::MAX);
        os_count(unsafe {
            libc::recvmmsg(
                socket.as_raw_fd(),
                self.headers.as_mut_ptr(),
                len,
                libc::MSG_WAITFORONE,
                std::ptr::null_mut(),
            )
        })
    }

    #[cfg(not(target_os = "linux"))]
    pub fn recv(&mut self, socket: &UdpSocket) -> io::Result<usize> {
        let first = self
            .data
            .chunks_exact_mut(self.slot)
            .next()
            .unwrap_or_default();
        socket.recv(first).map(|_| 1)
    }
}
