/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

pub mod clock;
pub mod cpu;
pub mod listener;
pub mod memory;
pub mod mesh;
pub mod queues;
mod service;
pub mod shard;
#[cfg(keep_tests)]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-rt/sim.rs"]
pub mod sim;
pub mod slab;
pub mod socket;
pub mod stats;
#[cfg(feature = "tls")]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub mod tls;
#[cfg(not(feature = "tls"))]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[path = "notls.rs"]
pub mod tls;
#[cfg(all(keep_tests, feature = "tls"))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-rt/tls_bench.rs"]
pub mod tls_bench;

pub use service::{Conn, ConnId, Conns, Ctx, Flow, Flush, Io, Service};
