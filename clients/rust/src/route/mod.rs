/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

mod cluster;
mod retired;
mod server;
mod vote;

use std::sync::Arc;
use std::time::Duration;

use keep_proto::KeyHash;

use crate::conn::{Close, Conn};
use crate::stats::Stats;

pub(crate) use cluster::{Cluster, View};
pub(crate) use retired::Retired;
pub(crate) use server::{FeedTarget, Layout, Policy, Server};

pub(crate) enum Router {
    Static(Arc<Server>),
    Cluster(Arc<Cluster>),
}

pub(crate) enum Target<'a> {
    Borrowed(&'a Conn),
    Owned(Arc<Conn>),
}

impl std::ops::Deref for Target<'_> {
    type Target = Conn;

    #[inline]
    fn deref(&self) -> &Conn {
        match self {
            Target::Borrowed(conn) => conn,
            Target::Owned(conn) => conn,
        }
    }
}

impl Router {
    #[inline]
    pub fn target(&self, hash: Option<KeyHash>) -> Option<Target<'_>> {
        match self {
            Router::Static(server) => server.pick(hash).map(|conn| Target::Borrowed(conn)),
            Router::Cluster(cluster) => cluster.target(hash).map(Target::Owned),
        }
    }

    pub fn may_move(&self, hash: Option<KeyHash>) -> bool {
        match (self, hash) {
            (Router::Cluster(cluster), Some(hash)) => cluster.may_move(hash),
            _ => false,
        }
    }

    pub fn servers(&self) -> Vec<Arc<Server>> {
        match self {
            Router::Static(server) => vec![Arc::clone(server)],
            Router::Cluster(cluster) => cluster.servers(),
        }
    }

    pub async fn moved(&self, wait: Duration) -> bool {
        match self {
            Router::Static(_) => false,
            Router::Cluster(cluster) => cluster.moved(wait).await,
        }
    }

    pub fn nudge(&self) {
        if let Router::Cluster(cluster) = self {
            cluster.nudge();
        }
    }

    pub fn stats(&self) -> Stats {
        let mut total = Stats::default();
        match self {
            Router::Static(server) => total += server.stats(),
            Router::Cluster(cluster) => total += cluster.stats(),
        }
        total
    }

    pub fn shutdown(&self) {
        match self {
            Router::Static(server) => server.shutdown(Close::Graceful),
            Router::Cluster(cluster) => cluster.shutdown(),
        }
    }
}
