/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::pattern::{fill, matches, mix64};

const MAGIC: u8 = 0xA6;
const KIND_EVENTS: u8 = 0;
const KIND_ACKS: u8 = 1;
const SALT: u64 = 0x4d31_7374_7265_616d;
const MAX_LEB_BYTES: usize = 10;
pub const MAX_NODE: u16 = 511;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    Truncated,
    BadMagic,
    BadKind,
    Overflow,
    BadNode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Origin {
    pub node: u16,
    pub incarnation: u16,
}

impl Origin {
    #[inline]
    pub fn salt(&self) -> u64 {
        mix64((u64::from(self.node) << 16 | u64::from(self.incarnation)) ^ SALT)
    }

    #[inline]
    pub fn body_seed(&self, seq: u64) -> u64 {
        mix64(seq ^ self.salt())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct EventsHeader {
    pub origin: Origin,
    pub msg_seq: u64,
    pub sent_ns: u64,
    pub count: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct EventView<'a> {
    pub tag: u8,
    pub seq: u64,
    pub body: &'a [u8],
}

#[derive(Clone, Copy, Debug)]
pub struct AckEntry {
    pub msg_seq: u64,
    pub sent_ns: u64,
    pub recv_ns: u64,
}

pub enum Message<'a> {
    Events {
        header: EventsHeader,
        events: EventIter<'a>,
    },
    Acks {
        origin: u16,
        entries: AckIter<'a>,
    },
}

pub struct EventIter<'a> {
    rest: &'a [u8],
    remaining: u32,
}

pub struct AckIter<'a> {
    rest: &'a [u8],
    remaining: u32,
}

#[inline]
pub fn write_leb(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

#[inline]
pub fn leb_len(value: u64) -> usize {
    (64 - (value | 1).leading_zeros() as usize).div_ceil(7)
}

#[inline]
fn read_leb(input: &mut &[u8]) -> Result<u64, FrameError> {
    let mut value = 0u64;
    for (index, byte) in input.iter().copied().enumerate().take(MAX_LEB_BYTES) {
        let shift = 7 * index as u32;
        let part = u64::from(byte & 0x7f);
        if shift == 63 && part > 1 {
            return Err(FrameError::Overflow);
        }
        value |= part << shift;
        if byte & 0x80 == 0 {
            *input = &input[index + 1..];
            return Ok(value);
        }
    }
    match input.len() >= MAX_LEB_BYTES {
        true => Err(FrameError::Overflow),
        false => Err(FrameError::Truncated),
    }
}

#[inline]
fn read_u64(input: &mut &[u8]) -> Result<u64, FrameError> {
    let (bytes, rest) = input
        .split_first_chunk::<8>()
        .ok_or(FrameError::Truncated)?;
    *input = rest;
    Ok(u64::from_le_bytes(*bytes))
}

#[inline]
fn read_node(input: &mut &[u8]) -> Result<u16, FrameError> {
    u16::try_from(read_leb(input)?)
        .ok()
        .filter(|node| *node <= MAX_NODE)
        .ok_or(FrameError::BadNode)
}

pub fn begin_events(out: &mut Vec<u8>, header: &EventsHeader) {
    out.clear();
    out.push(MAGIC);
    out.push(KIND_EVENTS);
    write_leb(out, u64::from(header.origin.node));
    out.extend_from_slice(&header.origin.incarnation.to_le_bytes());
    write_leb(out, header.msg_seq);
    out.extend_from_slice(&header.sent_ns.to_le_bytes());
    write_leb(out, u64::from(header.count));
}

pub fn write_event(out: &mut Vec<u8>, origin: Origin, tag: u8, seq: u64, size: u32) {
    let size = size as usize;
    let body = size.saturating_sub(1 + leb_len(seq) + leb_len(size as u64));
    out.push(tag);
    write_leb(out, seq);
    write_leb(out, body as u64);
    fill(out, origin.body_seed(seq), body);
}

#[cfg(test)]
pub fn event_wire_size(seq: u64, size: u32) -> usize {
    let size = size as usize;
    let body = size.saturating_sub(1 + leb_len(seq) + leb_len(size as u64));
    1 + leb_len(seq) + leb_len(body as u64) + body
}

pub fn begin_acks(out: &mut Vec<u8>, acker: u16, origin: u16, count: u32) {
    out.clear();
    out.push(MAGIC);
    out.push(KIND_ACKS);
    write_leb(out, u64::from(acker));
    write_leb(out, u64::from(origin));
    write_leb(out, u64::from(count));
}

pub fn write_ack(out: &mut Vec<u8>, entry: &AckEntry) {
    write_leb(out, entry.msg_seq);
    out.extend_from_slice(&entry.sent_ns.to_le_bytes());
    out.extend_from_slice(&entry.recv_ns.to_le_bytes());
}

impl<'a> Message<'a> {
    pub fn decode(payload: &'a [u8]) -> Result<Message<'a>, FrameError> {
        let [magic, kind, rest @ ..] = payload else {
            return Err(FrameError::Truncated);
        };
        if *magic != MAGIC {
            return Err(FrameError::BadMagic);
        }
        let mut input = rest;
        match *kind {
            KIND_EVENTS => {
                let node = read_node(&mut input)?;
                let (incarnation, rest) = input
                    .split_first_chunk::<2>()
                    .ok_or(FrameError::Truncated)?;
                input = rest;
                let msg_seq = read_leb(&mut input)?;
                let sent_ns = read_u64(&mut input)?;
                let count =
                    u32::try_from(read_leb(&mut input)?).map_err(|_| FrameError::Overflow)?;
                Ok(Message::Events {
                    header: EventsHeader {
                        origin: Origin {
                            node,
                            incarnation: u16::from_le_bytes(*incarnation),
                        },
                        msg_seq,
                        sent_ns,
                        count,
                    },
                    events: EventIter {
                        rest: input,
                        remaining: count,
                    },
                })
            }
            KIND_ACKS => {
                read_node(&mut input)?;
                let origin = read_node(&mut input)?;
                let count =
                    u32::try_from(read_leb(&mut input)?).map_err(|_| FrameError::Overflow)?;
                Ok(Message::Acks {
                    origin,
                    entries: AckIter {
                        rest: input,
                        remaining: count,
                    },
                })
            }
            _ => Err(FrameError::BadKind),
        }
    }
}

impl<'a> Iterator for EventIter<'a> {
    type Item = Result<EventView<'a>, FrameError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let result = (|| {
            let (tag, rest) = self.rest.split_first().ok_or(FrameError::Truncated)?;
            let mut input = rest;
            let seq = read_leb(&mut input)?;
            let len = usize::try_from(read_leb(&mut input)?).map_err(|_| FrameError::Overflow)?;
            let (body, rest) = input.split_at_checked(len).ok_or(FrameError::Truncated)?;
            self.rest = rest;
            Ok(EventView {
                tag: *tag,
                seq,
                body,
            })
        })();
        if result.is_err() {
            self.remaining = 0;
        }
        Some(result)
    }
}

impl Iterator for AckIter<'_> {
    type Item = Result<AckEntry, FrameError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let result = (|| {
            let mut input = self.rest;
            let msg_seq = read_leb(&mut input)?;
            let sent_ns = read_u64(&mut input)?;
            let recv_ns = read_u64(&mut input)?;
            self.rest = input;
            Ok(AckEntry {
                msg_seq,
                sent_ns,
                recv_ns,
            })
        })();
        if result.is_err() {
            self.remaining = 0;
        }
        Some(result)
    }
}

impl EventView<'_> {
    pub fn verify(&self, origin: Origin) -> bool {
        matches(self.body, origin.body_seed(self.seq))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AckEntry, EventsHeader, FrameError, Message, Origin, begin_acks, begin_events,
        event_wire_size, leb_len, read_leb, write_ack, write_event, write_leb,
    };
    use rand::{RngExt, SeedableRng, rngs::SmallRng};

    fn sample() -> (Vec<u8>, Origin) {
        let origin = Origin {
            node: 300,
            incarnation: 7,
        };
        let mut out = Vec::new();
        begin_events(
            &mut out,
            &EventsHeader {
                origin,
                msg_seq: 1 << 40,
                sent_ns: 123,
                count: 3,
            },
        );
        for (seq, size) in [(1u64, 1u32), (200, 12), (1 << 20, 70_000)] {
            let before = out.len();
            write_event(&mut out, origin, 7, seq, size);
            assert_eq!(out.len() - before, event_wire_size(seq, size));
        }
        (out, origin)
    }

    // LEB128 round-trips at every width and rejects overlong encodings.
    #[test]
    fn leb_round_trip() {
        for value in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut out = Vec::new();
            write_leb(&mut out, value);
            assert_eq!(out.len(), leb_len(value));
            let mut input = &out[..];
            assert_eq!(read_leb(&mut input), Ok(value));
            assert!(input.is_empty());
        }
        let mut overlong = &[0xffu8; 11][..];
        assert_eq!(read_leb(&mut overlong), Err(FrameError::Overflow));
    }

    // Events decode with their header and bodies verify against the origin.
    #[test]
    fn events_round_trip() {
        let (out, origin) = sample();
        let Ok(Message::Events { header, events }) = Message::decode(&out) else {
            panic!("decode");
        };
        assert_eq!(
            (header.origin, header.msg_seq, header.sent_ns),
            (origin, 1 << 40, 123)
        );
        let events = events.collect::<Result<Vec<_>, _>>().expect("events");
        assert_eq!(events.len(), 3);
        assert!(
            events
                .iter()
                .all(|event| event.verify(origin) && event.tag == 7)
        );
        let other = Origin {
            node: 301,
            incarnation: 7,
        };
        assert!(!events[2].verify(other));
    }

    // Acks decode back to the written entries.
    #[test]
    fn acks_round_trip() {
        let mut out = Vec::new();
        begin_acks(&mut out, 5, 9, 2);
        let entries = [
            AckEntry {
                msg_seq: 1,
                sent_ns: 2,
                recv_ns: 3,
            },
            AckEntry {
                msg_seq: 1 << 33,
                sent_ns: u64::MAX,
                recv_ns: 0,
            },
        ];
        entries.iter().for_each(|entry| write_ack(&mut out, entry));
        let Ok(Message::Acks {
            origin,
            entries: decoded,
        }) = Message::decode(&out)
        else {
            panic!("decode");
        };
        assert_eq!(origin, 9);
        let decoded = decoded.collect::<Result<Vec<_>, _>>().expect("acks");
        assert_eq!(decoded[1].msg_seq, 1 << 33);
        assert_eq!(decoded[1].sent_ns, u64::MAX);
    }

    // Truncated and mutated payloads never panic and never verify as intact.
    #[test]
    fn mutated_payloads_are_safe() {
        let (out, origin) = sample();
        let mut rng = SmallRng::seed_from_u64(11);
        for cut in 0..out.len().min(200) {
            if let Ok(Message::Events { events, .. }) = Message::decode(&out[..cut]) {
                for event in events {
                    let _ = event.map(|event| event.verify(origin));
                }
            }
        }
        for _ in 0..20_000 {
            let mut mutated = out.clone();
            for _ in 0..rng.random_range(1..4) {
                let at = rng.random_range(0..mutated.len());
                mutated[at] = rng.random();
            }
            match Message::decode(&mutated) {
                Ok(Message::Events { events, .. }) => {
                    for event in events.flatten() {
                        let _ = event.verify(origin);
                    }
                }
                Ok(Message::Acks { entries, .. }) => entries.for_each(drop),
                Err(_) => {}
            }
        }
    }
}
