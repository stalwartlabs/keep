/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::sync::Arc;

use super::{Conn, Role, Sent};
use crate::error::{Cause, Error};

pub(super) async fn writer(conn: Arc<Conn>) {
    loop {
        conn.writer.notified().await;
        if conn.is_closed() {
            return;
        }
        conn.flush_async().await;
    }
}

impl Conn {
    async fn flush_async(&self) {
        loop {
            let (socket, mut out, epoch) = {
                let mut st = self.lock();
                if st.writer != Role::Background {
                    return;
                }
                let socket = match st.socket.clone() {
                    Some(socket) if !st.wbuf.is_empty() || st.backlog.is_some() => socket,
                    _ => {
                        st.writer = Role::Idle;
                        return;
                    }
                };
                let out = self.take_batch(&mut st);
                (socket, out, st.epoch)
            };
            let mut written = 0;
            let result = loop {
                match socket.send(out.get(written..).unwrap_or_default()) {
                    Ok(Sent::All) => break Ok(()),
                    Ok(Sent::Blocked(accepted)) => {
                        written += accepted;
                        if let Err(err) = socket.writable().await {
                            break Err(err);
                        }
                    }
                    Err(err) => break Err(err),
                }
            };
            let mut st = self.lock();
            if st.epoch != epoch {
                return;
            }
            match result {
                Ok(()) => {
                    st.sending = 0;
                    st.stats.writes += 1;
                    st.stats.bytes_out += out.len() as u64;
                    out.clear();
                    st.spare = out;
                }
                Err(err) => {
                    self.fail(st, epoch, Error::Unknown(Cause::Io(err.kind())));
                    return;
                }
            }
        }
    }
}
