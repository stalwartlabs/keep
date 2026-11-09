/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

pub const HEADER_LEN: usize = 24;

#[derive(Clone, Copy)]
pub struct Header {
    pub seq: u64,
    pub intended: u64,
    pub sent: u64,
}

impl Header {
    #[inline]
    pub fn encode(&self, out: &mut [u8; HEADER_LEN]) {
        let (words, _) = out.as_chunks_mut::<8>();
        for (word, value) in words.iter_mut().zip([self.seq, self.intended, self.sent]) {
            *word = value.to_le_bytes();
        }
    }

    #[inline]
    pub fn decode(bytes: &[u8; HEADER_LEN]) -> Self {
        let (words, _) = bytes.as_chunks::<8>();
        let mut values = words.iter().map(|w| u64::from_le_bytes(*w));
        let mut next = || values.next().unwrap_or_default();
        Header {
            seq: next(),
            intended: next(),
            sent: next(),
        }
    }
}

pub struct Message(Box<[u8]>);

impl Message {
    pub fn new(size: usize) -> Self {
        Self(vec![0u8; size.max(HEADER_LEN)].into_boxed_slice())
    }

    #[inline]
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }

    #[inline]
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }

    #[inline]
    pub fn write_header(&mut self, header: Header) {
        if let Some(head) = self.0.first_chunk_mut::<HEADER_LEN>() {
            header.encode(head);
        }
    }

    #[inline]
    pub fn header(&self) -> Header {
        self.0.first_chunk::<HEADER_LEN>().map_or(
            Header {
                seq: u64::MAX,
                intended: 0,
                sent: 0,
            },
            Header::decode,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trips() {
        let mut msg = Message::new(64);
        let header = Header {
            seq: 7,
            intended: u64::MAX - 1,
            sent: 1 << 40,
        };
        msg.write_header(header);
        let back = msg.header();
        assert_eq!(
            (back.seq, back.intended, back.sent),
            (7, u64::MAX - 1, 1 << 40)
        );
    }
}
