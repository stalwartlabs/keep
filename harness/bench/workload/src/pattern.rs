/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use foldhash::fast::FixedState;
use std::hash::BuildHasher;

const GOLDEN: u64 = 0x9e37_79b9_7f4a_7c15;
const FINGERPRINT_SEED: u64 = 0x6d31_6b65_6570_0001;

#[inline]
pub fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

pub fn fill(out: &mut Vec<u8>, seed: u64, len: usize) {
    let start = out.len();
    out.resize(start + len, 0);
    let mut counter = seed;
    let (chunks, rest) = out[start..].as_chunks_mut::<8>();
    for chunk in chunks {
        counter = counter.wrapping_add(GOLDEN);
        *chunk = mix64(counter).to_le_bytes();
    }
    if !rest.is_empty() {
        counter = counter.wrapping_add(GOLDEN);
        let word = mix64(counter).to_le_bytes();
        rest.copy_from_slice(&word[..rest.len()]);
    }
}

pub fn matches(bytes: &[u8], seed: u64) -> bool {
    let mut counter = seed;
    let (chunks, rest) = bytes.as_chunks::<8>();
    for chunk in chunks {
        counter = counter.wrapping_add(GOLDEN);
        if *chunk != mix64(counter).to_le_bytes() {
            return false;
        }
    }
    rest.is_empty() || {
        counter = counter.wrapping_add(GOLDEN);
        rest == &mix64(counter).to_le_bytes()[..rest.len()]
    }
}

#[inline]
pub fn fingerprint(bytes: &[u8]) -> u64 {
    FixedState::with_seed(FINGERPRINT_SEED).hash_one(bytes)
}

pub fn feistel32(value: u32, key: u64) -> u32 {
    let mut left = (value >> 16) as u16;
    let mut right = value as u16;
    for round in 0..4u32 {
        let round_key = key.rotate_left(round * 16) ^ u64::from(round);
        let next = left ^ (mix64(u64::from(right) ^ round_key) as u16);
        left = right;
        right = next;
    }
    (u32::from(left) << 16) | u32::from(right)
}

#[cfg(test)]
mod tests {
    use super::{feistel32, fill, matches};

    // Filled patterns verify, and any flipped byte is detected.
    #[test]
    fn pattern_round_trip() {
        for len in [0usize, 1, 7, 8, 9, 63, 4096 + 3] {
            let mut buf = vec![0xAA];
            fill(&mut buf, 42, len);
            assert!(matches(&buf[1..], 42));
            assert!(len == 0 || !matches(&buf[1..], 43));
            if len > 0 {
                let last = buf.len() - 1;
                buf[last] ^= 1;
                assert!(!matches(&buf[1..], 42));
            }
        }
    }

    // The 32-bit Feistel network is a bijection on a sampled domain.
    #[test]
    fn feistel_is_injective() {
        let mut seen = std::collections::HashSet::new();
        for value in 0..200_000u32 {
            assert!(seen.insert(feistel32(value.wrapping_mul(2_654_435_761), 99)));
        }
    }
}
