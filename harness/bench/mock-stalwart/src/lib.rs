/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

#![cfg_attr(
    not(all(
        feature = "redis",
        feature = "nats",
        feature = "kafka",
        feature = "zenoh"
    )),
    allow(dead_code)
)]

pub mod clock;
pub mod config;
pub mod cpu;
pub mod dispatch;
pub mod hist;
pub mod instance;
pub mod kv;
pub mod load;
pub mod progress;
pub mod report;
pub mod stats;
pub mod stream;
pub mod workload;

pub use stalwart_workload::pattern;
