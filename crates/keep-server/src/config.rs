/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::time::Duration;

use keep_engine::Cap;
use keep_proto::{ClusterSecret, NodeId};
use keep_rt::clock::Nanos;

use crate::shard::Limits;

#[derive(Clone, Debug)]
pub struct Settings {
    pub node: NodeId,
    pub secret: ClusterSecret,
    pub shards: NonZeroU16,
    pub advertise: SocketAddr,
    pub limits: Limits,
    pub cap: Option<Cap>,
    pub start: Nanos,
    pub stream_coalesce: Duration,
    pub stream_coalesce_fanout: u32,
}
