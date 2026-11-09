/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fmt;

use keep_engine::{Record, Ttl, ValueKind};
use keep_proto::{
    Frame, HEADER_LEN, Header, MAX_VALUE_LEN, NodeId, Nonce, PEER_HELLO_LEN, PROOF_LEN,
};

use crate::slots::SlotSet;

const PEER_HELLO: u8 = 0x30;
const PEER_PROOF: u8 = 0x31;
const PULL: u8 = 0x32;
const READY: u8 = 0x33;
const ENTRY: u8 = 0x34;
const END: u8 = 0x35;
const ACK: u8 = 0x36;

const PEER_VERSION: u32 = 1;
const _: () = assert!(PEER_HELLO_LEN == 8 + 8 + 4 + 8 + Nonce::LEN);

const KIND_BYTES: u8 = 0;
const KIND_COUNTER: u8 = 1;
const KIND_TOKEN: u8 = 2;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PeerHello {
    pub sender: NodeId,
    pub receiver: NodeId,
    pub incarnation: u32,
    pub member_list: u64,
    pub nonce: Nonce,
}

impl PeerHello {
    pub fn to_bytes(&self) -> [u8; PEER_HELLO_LEN] {
        let mut bytes = [0; PEER_HELLO_LEN];
        let values = self
            .sender
            .0
            .to_le_bytes()
            .into_iter()
            .chain(self.receiver.0.to_le_bytes())
            .chain(self.incarnation.to_le_bytes())
            .chain(self.member_list.to_le_bytes())
            .chain(self.nonce.0);
        for (byte, value) in bytes.iter_mut().zip(values) {
            *byte = value;
        }
        bytes
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PeerFrame<'a> {
    Hello(PeerHello),
    Proof([u8; PROOF_LEN]),
    Pull {
        batch: u32,
        slots: SlotSet,
        since: (u32, u32),
    },
    Ready {
        batch: u32,
        quarantine_ms: u32,
        data: SlotSet,
        empty: SlotSet,
    },
    Entry {
        batch: u32,
        record: Record<'a>,
    },
    End {
        batch: u32,
        count: u32,
    },
    Ack {
        batch: u32,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PeerFrameError {
    Code(u8),
    Version(u32),
    Length { code: u8, len: usize },
    Reserved(u8),
    Kind(u8),
    Overlap,
}

impl fmt::Display for PeerFrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerFrameError::Code(code) => write!(f, "frame code {code:#04x} on the peer port"),
            PeerFrameError::Version(version) => write!(f, "peer protocol version {version}"),
            PeerFrameError::Length { code, len } => {
                write!(f, "peer frame {code:#04x} with {len} payload bytes")
            }
            PeerFrameError::Reserved(code) => write!(f, "reserved fields set in frame {code:#04x}"),
            PeerFrameError::Kind(kind) => write!(f, "unknown value kind {kind}"),
            PeerFrameError::Overlap => {
                f.write_str("Ready covers a slot both with and without entries")
            }
        }
    }
}

impl std::error::Error for PeerFrameError {}

impl<'a> PeerFrame<'a> {
    pub fn parse(frame: &Frame<'a>) -> Result<PeerFrame<'a>, PeerFrameError> {
        let code = frame.code;
        let reserved = !frame.ext.is_empty() || (code != ENTRY && !frame.key.is_empty());
        if reserved || (code != ENTRY && frame.flags != 0) {
            return Err(PeerFrameError::Reserved(code));
        }
        let length = PeerFrameError::Length {
            code,
            len: frame.value.len(),
        };
        let batch = frame.id;
        match code {
            PEER_HELLO if batch == 0 => {
                if frame.arg != PEER_VERSION {
                    return Err(PeerFrameError::Version(frame.arg));
                }
                let bytes: &[u8; PEER_HELLO_LEN] = frame.value.try_into().map_err(|_| length)?;
                let (sender, rest) = bytes.split_first_chunk::<8>().ok_or(length)?;
                let (receiver, rest) = rest.split_first_chunk::<8>().ok_or(length)?;
                let (incarnation, rest) = rest.split_first_chunk::<4>().ok_or(length)?;
                let (member_list, nonce) = rest.split_first_chunk::<8>().ok_or(length)?;
                Ok(PeerFrame::Hello(PeerHello {
                    sender: NodeId(u64::from_le_bytes(*sender)),
                    receiver: NodeId(u64::from_le_bytes(*receiver)),
                    incarnation: u32::from_le_bytes(*incarnation),
                    member_list: u64::from_le_bytes(*member_list),
                    nonce: Nonce(nonce.try_into().map_err(|_| length)?),
                }))
            }
            PEER_PROOF if frame.arg == 0 && batch == 0 => Ok(PeerFrame::Proof(
                frame.value.try_into().map_err(|_| length)?,
            )),
            PULL if frame.arg == 0 => {
                let (slots, since) = frame.value.split_at_checked(SlotSet::BYTES).ok_or(length)?;
                let (incarnation, seq) = since.split_first_chunk::<4>().ok_or(length)?;
                let seq = <[u8; 4]>::try_from(seq).map_err(|_| length)?;
                Ok(PeerFrame::Pull {
                    batch,
                    slots: SlotSet::decode(slots).ok_or(length)?,
                    since: (u32::from_le_bytes(*incarnation), u32::from_le_bytes(seq)),
                })
            }
            READY => {
                let (data, empty) = frame.value.split_at_checked(SlotSet::BYTES).ok_or(length)?;
                let data = SlotSet::decode(data).ok_or(length)?;
                let empty = SlotSet::decode(empty).ok_or(length)?;
                if data.intersects(&empty) {
                    return Err(PeerFrameError::Overlap);
                }
                Ok(PeerFrame::Ready {
                    batch,
                    quarantine_ms: frame.arg,
                    data,
                    empty,
                })
            }
            ENTRY => Ok(PeerFrame::Entry {
                batch,
                record: Record {
                    key: frame.key,
                    value: frame.value,
                    ttl: Ttl::from_secs(frame.arg),
                    kind: kind_from_wire(frame.flags)?,
                },
            }),
            END if frame.value.is_empty() => Ok(PeerFrame::End {
                batch,
                count: frame.arg,
            }),
            ACK if frame.value.is_empty() && frame.arg == 0 => Ok(PeerFrame::Ack { batch }),
            PEER_HELLO | PEER_PROOF | PULL | END | ACK => Err(PeerFrameError::Reserved(code)),
            _ => Err(PeerFrameError::Code(code)),
        }
    }

    pub fn encode(&self, out: &mut Vec<u8>) -> bool {
        match self {
            PeerFrame::Hello(hello) => put(
                out,
                0,
                PEER_HELLO,
                0,
                PEER_VERSION,
                &[],
                &[&hello.to_bytes()],
            ),
            PeerFrame::Proof(proof) => put(out, 0, PEER_PROOF, 0, 0, &[], &[proof]),
            PeerFrame::Pull {
                batch,
                slots,
                since: (incarnation, seq),
            } => put_sets(
                out,
                *batch,
                PULL,
                0,
                &[slots],
                &[&incarnation.to_le_bytes(), &seq.to_le_bytes()],
            ),
            PeerFrame::Ready {
                batch,
                quarantine_ms,
                data,
                empty,
            } => put_sets(out, *batch, READY, *quarantine_ms, &[data, empty], &[]),
            PeerFrame::Entry { batch, record } => put(
                out,
                *batch,
                ENTRY,
                kind_to_wire(record.kind),
                record.ttl.map_or(0, Ttl::secs),
                record.key,
                &[record.value],
            ),
            PeerFrame::End { batch, count } => put(out, *batch, END, 0, *count, &[], &[]),
            PeerFrame::Ack { batch } => put(out, *batch, ACK, 0, 0, &[], &[]),
        }
    }
}

fn put_sets(
    out: &mut Vec<u8>,
    batch: u32,
    code: u8,
    arg: u32,
    sets: &[&SlotSet],
    tail: &[&[u8]],
) -> bool {
    let value_len = SlotSet::BYTES * sets.len() + tail.iter().map(|part| part.len()).sum::<usize>();
    let Ok(len) = u32::try_from(value_len) else {
        return false;
    };
    out.reserve(HEADER_LEN + value_len);
    out.extend_from_slice(&header(batch, code, 0, arg, 0, len).encode());
    for set in sets {
        set.encode(out);
    }
    for part in tail {
        out.extend_from_slice(part);
    }
    true
}

fn put(
    out: &mut Vec<u8>,
    batch: u32,
    code: u8,
    flags: u8,
    arg: u32,
    key: &[u8],
    value: &[&[u8]],
) -> bool {
    let value_len: usize = value.iter().map(|part| part.len()).sum();
    let (Ok(key_len), Ok(len)) = (u8::try_from(key.len()), u32::try_from(value_len)) else {
        return false;
    };
    if len > MAX_VALUE_LEN {
        return false;
    }
    out.reserve(HEADER_LEN + key.len() + value_len);
    out.extend_from_slice(&header(batch, code, flags, arg, key_len, len).encode());
    out.extend_from_slice(key);
    for part in value {
        out.extend_from_slice(part);
    }
    true
}

fn header(id: u32, code: u8, flags: u8, arg: u32, key_len: u8, value_len: u32) -> Header {
    Header {
        id,
        code,
        flags,
        key_len,
        ext_words: 0,
        value_len,
        arg,
    }
}

fn kind_to_wire(kind: ValueKind) -> u8 {
    match kind {
        ValueKind::Bytes => KIND_BYTES,
        ValueKind::Counter => KIND_COUNTER,
        ValueKind::Token => KIND_TOKEN,
    }
}

fn kind_from_wire(byte: u8) -> Result<ValueKind, PeerFrameError> {
    match byte {
        KIND_BYTES => Ok(ValueKind::Bytes),
        KIND_COUNTER => Ok(ValueKind::Counter),
        KIND_TOKEN => Ok(ValueKind::Token),
        _ => Err(PeerFrameError::Kind(byte)),
    }
}
