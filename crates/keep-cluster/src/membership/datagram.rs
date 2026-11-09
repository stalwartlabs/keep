/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fmt;
use std::hash::Hasher;

use keep_proto::{MAX_MEMBERS, MemberStatus, NodeId, PeerSecret};
use siphasher::sip::SipHasher24;

const MAGIC: [u8; 2] = *b"KH";
const VERSION: u8 = 1;
const KIND_HEARTBEAT: u8 = 1;
const FIXED_LEN: usize = 56;
const TAG_LEN: usize = 8;
const FLAG_ECHO: u8 = 1;
const FLAG_CONFIRM: u8 = 2;
const FLAGS_CONFIRMED: u8 = FLAG_ECHO | FLAG_CONFIRM;
const FLAG_FAR: u8 = 4;
const KEY_LABELS: [&[u8]; 2] = [b"keep membership key v1 k0", b"keep membership key v1 k1"];
const LIST_LABEL: &[u8] = b"keep member list v2";

#[derive(Clone)]
pub struct MembershipKey(SipHasher24);

impl MembershipKey {
    pub fn derive(secret: &PeerSecret) -> MembershipKey {
        let base = SipHasher24::new_with_key(&secret.to_bytes());
        let [k0, k1] = KEY_LABELS.map(|label| base.hash(label));
        MembershipKey(SipHasher24::new_with_keys(k0, k1))
    }

    #[inline]
    fn tag(&self, signed: &[u8]) -> [u8; TAG_LEN] {
        self.0.hash(signed).to_le_bytes()
    }

    pub(crate) fn member_list(&self, members: &[NodeId]) -> u64 {
        members
            .iter()
            .fold(0, |sum, node| sum.wrapping_add(self.member(*node)))
    }

    pub(crate) fn member(&self, node: NodeId) -> u64 {
        let mut hasher = self.0;
        hasher.write(LIST_LABEL);
        hasher.write(&node.0.to_le_bytes());
        hasher.finish()
    }
}

impl fmt::Debug for MembershipKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MembershipKey(..)")
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Echo {
    pub incarnation: u32,
    pub seq: u32,
    pub confirms: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Heartbeat<'a> {
    pub sender: NodeId,
    pub receiver: NodeId,
    pub incarnation: u32,
    pub seq: u32,
    pub echo: Option<Echo>,
    pub uptime_ms: u32,
    pub cluster_age_ms: u32,
    pub status: MemberStatus,
    pub far: bool,
    pub members: u16,
    pub member_list: u64,
    pub hears: &'a [u8],
    pub lock_hears: &'a [u8],
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DatagramError {
    Length(usize),
    Magic,
    Version(u8),
    Kind(u8),
    Status(u8),
    Reserved,
    Tag,
    MemberCount(u16),
    MemberList,
    UnknownSender(NodeId),
    Stale,
}

impl fmt::Display for DatagramError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatagramError::Length(len) => write!(f, "membership datagram of {len} bytes"),
            DatagramError::Magic => f.write_str("not a membership datagram"),
            DatagramError::Version(version) => write!(f, "membership version {version}"),
            DatagramError::Kind(kind) => write!(f, "unknown membership datagram kind {kind}"),
            DatagramError::Status(status) => write!(f, "unknown member status {status}"),
            DatagramError::Reserved => f.write_str("reserved membership bits set"),
            DatagramError::Tag => f.write_str("membership datagram failed authentication"),
            DatagramError::MemberCount(count) => {
                write!(f, "heartbeat for a cluster of {count} members")
            }
            DatagramError::MemberList => f.write_str("heartbeat from another member list"),
            DatagramError::UnknownSender(node) => write!(f, "heartbeat from non-member {}", node.0),
            DatagramError::Stale => f.write_str("heartbeat older than one already seen"),
        }
    }
}

impl std::error::Error for DatagramError {}

#[inline]
pub(crate) fn mask_len(members: usize) -> usize {
    members.div_ceil(8)
}

#[inline]
pub(crate) fn bit(mask: &[u8], index: usize) -> bool {
    mask.get(index / 8)
        .is_some_and(|byte| byte & (1 << (index % 8)) != 0)
}

impl<'a> Heartbeat<'a> {
    pub fn len(members: usize) -> usize {
        FIXED_LEN + 2 * mask_len(members) + TAG_LEN
    }

    pub fn encode(&self, key: &MembershipKey, out: &mut Vec<u8>) {
        let (echo_incarnation, echo_seq, echo_flags) = match self.echo {
            Some(echo) if echo.confirms => (echo.incarnation, echo.seq, FLAGS_CONFIRMED),
            Some(echo) => (echo.incarnation, echo.seq, FLAG_ECHO),
            None => (0, 0, 0),
        };
        let flags = if self.far {
            echo_flags | FLAG_FAR
        } else {
            echo_flags
        };
        out.clear();
        out.reserve(FIXED_LEN + self.hears.len() + self.lock_hears.len() + TAG_LEN);
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&[VERSION, KIND_HEARTBEAT]);
        out.extend_from_slice(&self.sender.0.to_le_bytes());
        out.extend_from_slice(&self.receiver.0.to_le_bytes());
        out.extend_from_slice(&self.incarnation.to_le_bytes());
        out.extend_from_slice(&self.seq.to_le_bytes());
        out.extend_from_slice(&echo_incarnation.to_le_bytes());
        out.extend_from_slice(&echo_seq.to_le_bytes());
        out.extend_from_slice(&self.uptime_ms.to_le_bytes());
        out.extend_from_slice(&self.cluster_age_ms.to_le_bytes());
        out.extend_from_slice(&[self.status as u8, flags]);
        out.extend_from_slice(&self.members.to_le_bytes());
        out.extend_from_slice(&self.member_list.to_le_bytes());
        out.extend_from_slice(self.hears);
        out.extend_from_slice(self.lock_hears);
        let tag = key.tag(out);
        out.extend_from_slice(&tag);
    }

    pub fn decode(bytes: &'a [u8], key: &MembershipKey) -> Result<Heartbeat<'a>, DatagramError> {
        let (signed, tag) = bytes
            .split_last_chunk::<TAG_LEN>()
            .ok_or(DatagramError::Length(bytes.len()))?;
        let (fixed, bitmaps) = signed
            .split_first_chunk::<FIXED_LEN>()
            .ok_or(DatagramError::Length(bytes.len()))?;
        let mut fields = Fields(fixed);
        if fields.take::<2>() != MAGIC {
            return Err(DatagramError::Magic);
        }
        let [version, kind] = fields.take::<2>();
        if version != VERSION {
            return Err(DatagramError::Version(version));
        }
        if key.tag(signed) != *tag {
            return Err(DatagramError::Tag);
        }
        if kind != KIND_HEARTBEAT {
            return Err(DatagramError::Kind(kind));
        }
        let sender = NodeId(u64::from_le_bytes(fields.take()));
        let receiver = NodeId(u64::from_le_bytes(fields.take()));
        let incarnation = u32::from_le_bytes(fields.take());
        let seq = u32::from_le_bytes(fields.take());
        let echo_incarnation = u32::from_le_bytes(fields.take());
        let echo_seq = u32::from_le_bytes(fields.take());
        let uptime_ms = u32::from_le_bytes(fields.take());
        let cluster_age_ms = u32::from_le_bytes(fields.take());
        let [status, flags] = fields.take::<2>();
        let members = u16::from_le_bytes(fields.take());
        let member_list = u64::from_le_bytes(fields.take());
        let count = usize::from(members);
        let (hears, lock_hears) = bitmaps
            .split_at_checked(mask_len(count))
            .filter(|(hears, lock_hears)| count <= MAX_MEMBERS && hears.len() == lock_hears.len())
            .ok_or(DatagramError::MemberCount(members))?;
        let unused =
            (count..mask_len(count) * 8).any(|index| bit(hears, index) || bit(lock_hears, index));
        let echo = match flags & !FLAG_FAR {
            0 => None,
            FLAG_ECHO => Some(false),
            FLAGS_CONFIRMED => Some(true),
            _ => return Err(DatagramError::Reserved),
        };
        if unused {
            return Err(DatagramError::Reserved);
        }
        Ok(Heartbeat {
            sender,
            receiver,
            incarnation,
            seq,
            echo: echo.map(|confirms| Echo {
                incarnation: echo_incarnation,
                seq: echo_seq,
                confirms,
            }),
            uptime_ms,
            cluster_age_ms,
            status: MemberStatus::from_wire(status).ok_or(DatagramError::Status(status))?,
            far: flags & FLAG_FAR != 0,
            members,
            member_list,
            hears,
            lock_hears,
        })
    }
}

struct Fields<'a>(&'a [u8]);

impl Fields<'_> {
    #[inline]
    fn take<const N: usize>(&mut self) -> [u8; N] {
        match self.0.split_first_chunk::<N>() {
            Some((head, rest)) => {
                self.0 = rest;
                *head
            }
            None => [0; N],
        }
    }
}
