/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::fmt;
use std::net::{IpAddr, Ipv6Addr};
use std::num::NonZeroU16;

use crate::limits::MAX_MEMBERS;

const HEADER_LEN: usize = 8 + 4;
const MEMBER_LEN: usize = 8 + 16 + 2 + 2 + 1 + 1;

/// Stable identity of a Keep server.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct NodeId(pub u64);

/// A member's role in placement.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[repr(u8)]
pub enum MemberStatus {
    /// Owns slots.
    Active = 0,
    /// Joining; owns no slots yet.
    Joining = 1,
    /// Leaving; hands its slots over.
    Leaving = 2,
}

/// Whether the cluster sees a member as up.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[repr(u8)]
pub enum Liveness {
    /// Answering heartbeats.
    Alive = 0,
    /// Missed recent heartbeats.
    Suspect = 1,
    /// Declared dead.
    Dead = 2,
}

impl MemberStatus {
    /// Decodes a status byte.
    pub fn from_wire(byte: u8) -> Option<MemberStatus> {
        match byte {
            0 => Some(MemberStatus::Active),
            1 => Some(MemberStatus::Joining),
            2 => Some(MemberStatus::Leaving),
            _ => None,
        }
    }
}

impl Liveness {
    /// Decodes a liveness byte.
    pub fn from_wire(byte: u8) -> Option<Liveness> {
        match byte {
            0 => Some(Liveness::Alive),
            1 => Some(Liveness::Suspect),
            2 => Some(Liveness::Dead),
            _ => None,
        }
    }
}

/// One server of the topology.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Member {
    /// The node id.
    pub id: NodeId,
    /// The client address.
    pub ip: IpAddr,
    /// The client port.
    pub port: u16,
    /// The shard count.
    pub shards: NonZeroU16,
    /// The placement role.
    pub status: MemberStatus,
    /// The liveness.
    pub liveness: Liveness,
}

impl Member {
    /// Owns slots: active and not dead.
    #[inline]
    pub fn places(&self) -> bool {
        self.status == MemberStatus::Active && self.liveness != Liveness::Dead
    }

    fn encode(&self, out: &mut Vec<u8>) {
        let v6 = match self.ip {
            IpAddr::V4(v4) => v4.to_ipv6_mapped(),
            IpAddr::V6(v6) => v6,
        };
        out.extend_from_slice(&self.id.0.to_le_bytes());
        out.extend_from_slice(&v6.octets());
        out.extend_from_slice(&self.port.to_le_bytes());
        out.extend_from_slice(&self.shards.get().to_le_bytes());
        out.extend_from_slice(&[self.status as u8, self.liveness as u8]);
    }

    fn decode(record: &[u8; MEMBER_LEN]) -> Result<Member, TopologyError> {
        let [rest @ .., s0, s1, status, liveness] = *record;
        let [rest @ .., p0, p1] = rest;
        let [i0, i1, i2, i3, i4, i5, i6, i7, address @ ..] = rest;
        let v6 = Ipv6Addr::from(address);
        Ok(Member {
            id: NodeId(u64::from_le_bytes([i0, i1, i2, i3, i4, i5, i6, i7])),
            ip: v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
            port: u16::from_le_bytes([p0, p1]),
            shards: NonZeroU16::new(u16::from_le_bytes([s0, s1])).ok_or(TopologyError::NoShards)?,
            status: MemberStatus::from_wire(status).ok_or(TopologyError::BadStatus(status))?,
            liveness: Liveness::from_wire(liveness).ok_or(TopologyError::BadLiveness(liveness))?,
        })
    }
}

/// A server's view of the cluster, the `Topology` op's payload.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Topology {
    epoch: u64,
    members: Vec<Member>,
}

/// A topology value that cannot be decoded.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TopologyError {
    /// The value is `actual` bytes where `expected` were needed.
    Length {
        /// The length needed.
        expected: usize,
        /// The length given.
        actual: usize,
    },
    /// The member count is over the limit.
    TooManyMembers(usize),
    /// A member has no shards.
    NoShards,
    /// A member has an unknown status byte.
    BadStatus(u8),
    /// A member has an unknown liveness byte.
    BadLiveness(u8),
}

impl fmt::Display for TopologyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TopologyError::Length { expected, actual } => {
                write!(f, "topology of {actual} bytes, expected {expected}")
            }
            TopologyError::TooManyMembers(count) => {
                write!(f, "{count} topology members exceed {MAX_MEMBERS}")
            }
            TopologyError::NoShards => f.write_str("topology member with no shards"),
            TopologyError::BadStatus(byte) => write!(f, "unknown member status {byte}"),
            TopologyError::BadLiveness(byte) => write!(f, "unknown member liveness {byte}"),
        }
    }
}

impl std::error::Error for TopologyError {}

impl Topology {
    /// A topology of at most 4,096 members; IPv4-mapped IPv6 addresses
    /// become IPv4, as decoding makes them.
    pub fn new(epoch: u64, mut members: Vec<Member>) -> Result<Topology, TopologyError> {
        if members.len() > MAX_MEMBERS {
            return Err(TopologyError::TooManyMembers(members.len()));
        }
        for member in &mut members {
            member.ip = member.ip.to_canonical();
        }
        Ok(Topology { epoch, members })
    }

    /// The topology epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The members in order.
    pub fn members(&self) -> &[Member] {
        &self.members
    }

    /// Appends the encoded topology to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.reserve(HEADER_LEN + self.members.len() * MEMBER_LEN);
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&(self.members.len() as u32).to_le_bytes());
        for member in &self.members {
            member.encode(out);
        }
    }

    /// Decodes a topology value.
    pub fn decode(bytes: &[u8]) -> Result<Topology, TopologyError> {
        let length = |expected| TopologyError::Length {
            expected,
            actual: bytes.len(),
        };
        let (epoch, rest) = bytes
            .split_first_chunk::<8>()
            .ok_or_else(|| length(HEADER_LEN))?;
        let (count, rest) = rest
            .split_first_chunk::<4>()
            .ok_or_else(|| length(HEADER_LEN))?;
        let count = u32::from_le_bytes(*count) as usize;
        if count > MAX_MEMBERS {
            return Err(TopologyError::TooManyMembers(count));
        }
        let (records, tail) = rest.as_chunks::<MEMBER_LEN>();
        if records.len() != count || !tail.is_empty() {
            return Err(length(HEADER_LEN + count * MEMBER_LEN));
        }
        let members = records
            .iter()
            .map(Member::decode)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Topology {
            epoch: u64::from_le_bytes(*epoch),
            members,
        })
    }
}
