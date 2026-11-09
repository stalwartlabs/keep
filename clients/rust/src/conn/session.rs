/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::io::{self, Read as _, Write as _};
use std::sync::{Mutex, MutexGuard, PoisonError};

use rustls::ClientConnection;
use tokio::net::TcpStream;

use super::socket::{Raw, Sent};

pub(crate) struct Session {
    tls: Mutex<ClientConnection>,
}

fn invalid_data(err: rustls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

impl Session {
    pub fn new(tls: ClientConnection) -> Session {
        Session {
            tls: Mutex::new(tls),
        }
    }

    fn lock(&self) -> MutexGuard<'_, ClientConnection> {
        self.tls.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn send(&self, stream: &TcpStream, out: &[u8]) -> io::Result<Sent> {
        let mut tls = self.lock();
        let mut raw = Raw(stream);
        let mut accepted = 0;
        loop {
            while tls.wants_write() {
                match tls.write_tls(&mut raw) {
                    Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                    Ok(_) => {}
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(Sent::Blocked(accepted));
                    }
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                    Err(err) => return Err(err),
                }
            }
            let Some(rest) = out.get(accepted..).filter(|rest| !rest.is_empty()) else {
                return Ok(Sent::All);
            };
            match tls.writer().write(rest)? {
                0 => return Err(io::ErrorKind::WriteZero.into()),
                n => accepted += n,
            }
        }
    }

    pub fn read(&self, stream: &TcpStream, buf: &mut [u8]) -> io::Result<usize> {
        let mut tls = self.lock();
        let mut raw = Raw(stream);
        let mut filled = 0;
        loop {
            if let Some(done) = drain_plaintext(&mut tls, buf, &mut filled) {
                return done;
            }
            match tls.read_tls(&mut raw) {
                Ok(_) => {
                    tls.process_new_packets().map_err(invalid_data)?;
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock && filled > 0 => {
                    return Ok(filled);
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(err) => return Err(err),
            }
        }
    }
}

fn drain_plaintext(
    tls: &mut ClientConnection,
    buf: &mut [u8],
    filled: &mut usize,
) -> Option<io::Result<usize>> {
    while let Some(rest) = buf.get_mut(*filled..).filter(|rest| !rest.is_empty()) {
        match tls.reader().read(rest) {
            Ok(0) => return Some(Ok(*filled)),
            Ok(n) => *filled += n,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => return None,
            Err(_) if *filled > 0 => return Some(Ok(*filled)),
            Err(err) => return Some(Err(err)),
        }
    }
    Some(Ok(*filled))
}
