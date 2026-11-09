/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

pub mod deployment;
pub mod dist;
pub mod error;
pub mod events;
pub mod mix;
pub mod pattern;
pub mod profiles;

pub use error::WorkloadError;

pub const NANOS_PER_SEC: u64 = 1_000_000_000;
pub const NANOS_PER_MILLI: u64 = 1_000_000;
