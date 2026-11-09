/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::time::Duration;

#[cfg(target_os = "linux")]
mod epoll;
#[cfg(target_os = "linux")]
mod sys;

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const KEEP_CAPACITY: usize = 16 << 10;
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const SHRINK_ABOVE: usize = 256 << 10;
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const COMPACT_AT: usize = 64 << 10;

#[cfg(target_os = "linux")]
pub use epoll::Shard;
#[cfg(target_os = "linux")]
pub(crate) use epoll::dial;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct BusyPoll {
    pub usecs: u32,
    pub budget: u16,
    pub prefer: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct LoopConfig {
    pub read_buffer: usize,
    pub pause_output: usize,
    pub max_conns: usize,
    pub busy_poll: Option<BusyPoll>,
    pub spin: Duration,
    pub timer_slack: Option<Duration>,
}

impl LoopConfig {
    pub const DEFAULT: LoopConfig = LoopConfig {
        read_buffer: 64 << 10,
        pause_output: 8 << 20,
        max_conns: 1 << 16,
        busy_poll: None,
        spin: Duration::ZERO,
        timer_slack: None,
    };
}
