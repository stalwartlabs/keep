/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::mem;
use std::sync::Arc;

use super::server::Server;
use crate::stats::Stats;

#[derive(Default)]
pub(crate) struct Retired {
    servers: Vec<Arc<Server>>,
    total: Stats,
}

impl Retired {
    pub fn push(&mut self, server: Arc<Server>) {
        self.servers.push(server);
    }

    pub fn fold(&mut self) -> bool {
        for server in mem::take(&mut self.servers) {
            match Arc::try_unwrap(server) {
                Ok(server) => self.total += server.stats(),
                Err(server) => self.servers.push(server),
            }
        }
        !self.servers.is_empty()
    }

    pub fn stats(&self) -> Stats {
        let mut total = self.total;
        for server in &self.servers {
            total += server.stats();
        }
        total
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-client/route/retired.rs"]
mod tests;
