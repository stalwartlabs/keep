/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use keep_proto::{Event, HEADER_LEN, Topic, Value};

const SEQ_LEN: usize = 4;
const BUCKET_WORDS: usize = 4;
const BUCKET_BITS: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub(super) struct Buckets(pub(super) [u64; BUCKET_WORDS]);

impl Buckets {
    #[inline]
    pub(super) fn bucket(topic: Topic) -> usize {
        usize::from(topic.get() & 0xFF)
    }

    #[inline]
    pub(super) fn word_and_bit(bucket: usize) -> (usize, u64) {
        (bucket / BUCKET_BITS, 1 << (bucket % BUCKET_BITS))
    }

    pub(super) fn of(topic: Topic) -> Buckets {
        let mut buckets = Buckets::default();
        buckets.insert(topic);
        buckets
    }

    #[inline]
    fn insert(&mut self, topic: Topic) {
        let (word, bit) = Buckets::word_and_bit(Buckets::bucket(topic));
        if let Some(word) = self.0.get_mut(word) {
            *word |= bit;
        }
    }
}

#[derive(Debug, Default)]
pub struct Records {
    bytes: Vec<u8>,
    buckets: Buckets,
}

impl Records {
    #[inline]
    pub(super) fn push(&mut self, topic: Topic, payload: Value<'_>) {
        Event {
            seq: 0,
            topic,
            payload,
        }
        .encode(&mut self.bytes);
        self.buckets.insert(topic);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub(super) fn buckets(&self) -> &Buckets {
        &self.buckets
    }

    pub(super) fn clear(&mut self) {
        self.bytes.clear();
        self.buckets = Buckets::default();
    }

    #[inline]
    pub(super) fn runs(&self) -> Runs<'_> {
        Runs(&self.bytes)
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Run<'a> {
    pub(super) topic: Topic,
    pub(super) frames: &'a [u8],
    pub(super) count: u32,
}

impl Run<'_> {
    #[inline]
    pub(super) fn write(&self, out: &mut Vec<u8>, mut seq: u32) -> u32 {
        out.reserve(self.frames.len());
        let mut rest = self.frames;
        while let Some((frame, tail)) = split_frame(rest) {
            if let Some((_, body)) = frame.split_first_chunk::<SEQ_LEN>() {
                seq = seq.wrapping_add(1);
                out.extend_from_slice(&seq.to_le_bytes());
                out.extend_from_slice(body);
            }
            rest = tail;
        }
        seq
    }
}

pub(super) struct Runs<'a>(&'a [u8]);

impl<'a> Iterator for Runs<'a> {
    type Item = Run<'a>;

    #[inline]
    fn next(&mut self) -> Option<Run<'a>> {
        let all = self.0;
        let (first, mut rest) = split_frame(all)?;
        let topic = topic_of(first)?;
        let mut len = first.len();
        let mut count = 1u32;
        while let Some((frame, tail)) = split_frame(rest)
            && topic_of(frame) == Some(topic)
        {
            len += frame.len();
            count += 1;
            rest = tail;
        }
        self.0 = rest;
        Some(Run {
            topic,
            frames: all.get(..len)?,
            count,
        })
    }
}

#[inline]
fn split_frame(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let (head, _) = bytes.split_first_chunk::<HEADER_LEN>()?;
    let [_, _, _, _, _, _, _, _, l0, l1, l2, l3, _, _, _, _] = *head;
    let len = HEADER_LEN + u32::from_le_bytes([l0, l1, l2, l3]) as usize;
    bytes.split_at_checked(len)
}

#[inline]
fn topic_of(frame: &[u8]) -> Option<Topic> {
    let (head, _) = frame.split_first_chunk::<HEADER_LEN>()?;
    let [_, _, _, _, _, _, _, _, _, _, _, _, a0, a1, a2, a3] = *head;
    Topic::new(u32::from_le_bytes([a0, a1, a2, a3]))
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-server/stream/records.rs"]
mod tests;
