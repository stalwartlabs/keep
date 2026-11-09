/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

//! Rust client for Stalwart Keep, an ephemeral key-value store and event stream server
//! for clustered applications: multiplexed connections, cluster placement, locks and leases.

#![warn(missing_docs)]

mod answer;
#[cfg(keep_bench)]
#[doc(hidden)]
pub mod bench;
mod client;
mod config;
mod conn;
mod error;
#[cfg(keep_tests)]
#[doc(hidden)]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-client/inbox_bench.rs"]
pub mod inbox_bench;
mod observe;
mod ops;
mod route;
mod stats;
mod stream;

pub use client::Client;
pub use config::{
    Backoff, ClientConfig, Discovery, FeedPlacement, Identity, Report, Routing, Selection,
    StreamMode, Tls,
};
pub use error::{Cause, Error};
pub use keep_proto::{ClusterSecret, LockToken, Topic, Ttl};
pub use stats::Stats;
pub use stream::{Delivery, Received, Subscription};
