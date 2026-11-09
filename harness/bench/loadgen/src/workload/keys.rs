/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use rand::RngExt;
use rand::rngs::SmallRng;
use rand_distr::{Distribution, Zipf};

pub const ALPHABET: &[u8; 64] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_";
pub const WINDOW_CHARS: usize = 4;
pub const MAX_SYNTHETIC_KEY: usize = 250;
const BITS_PER_CHAR: u32 = 6;
const RANK_SCRAMBLE_PRIME: u128 = (1 << 61) - 1;

#[derive(Clone, Debug)]
pub struct KeySizes {
    bounds: Box<[(u64, u8)]>,
}

impl KeySizes {
    pub fn new(count: u64, weighted: &[(u8, f64)]) -> Self {
        let total: f64 = weighted.iter().map(|(_, w)| w).sum();
        let mut acc = 0.0;
        let mut bounds: Vec<(u64, u8)> = weighted
            .iter()
            .map(|&(size, weight)| {
                acc += weight;
                ((acc / total * count as f64).round() as u64, size)
            })
            .collect();
        if let Some(last) = bounds.last_mut() {
            last.0 = count;
        }
        KeySizes {
            bounds: bounds.into_boxed_slice(),
        }
    }

    #[inline]
    pub fn size_of(&self, index: u64) -> usize {
        self.bounds
            .iter()
            .find(|(bound, _)| index < *bound)
            .or(self.bounds.last())
            .map_or(1, |&(_, size)| size as usize)
    }

    pub fn sizes(&self) -> impl Iterator<Item = u8> + '_ {
        self.bounds.iter().map(|&(_, size)| size)
    }
}

pub fn index_chars(size: usize, windowed: bool) -> usize {
    size.saturating_sub(1 + if windowed { WINDOW_CHARS } else { 0 })
}

pub fn capacity(chars: usize) -> u64 {
    u32::try_from(chars)
        .ok()
        .and_then(|c| 1u64.checked_shl(c * BITS_PER_CHAR))
        .unwrap_or(u64::MAX)
}

pub fn write_key(out: &mut Vec<u8>, tag: u8, size: usize, window: Option<u64>, index: u64) {
    out.clear();
    out.push(tag);
    if let Some(window) = window {
        out.extend((0..WINDOW_CHARS).rev().map(|i| ALPHABET[digit(window, i)]));
    }
    let chars = size.saturating_sub(out.len());
    out.extend((0..chars).rev().map(|i| ALPHABET[digit(index, i)]));
}

#[inline]
fn digit(value: u64, position: usize) -> usize {
    let shift = u32::try_from(position)
        .unwrap_or(u32::MAX)
        .saturating_mul(BITS_PER_CHAR);
    (value.checked_shr(shift).unwrap_or(0) & 63) as usize
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Popularity {
    Uniform,
    Zipf(f64),
}

#[derive(Clone, Copy, Debug)]
pub enum KeyPicker {
    Uniform { count: u64 },
    Zipf { count: u64, dist: Zipf<f64> },
}

impl KeyPicker {
    pub fn new(popularity: Popularity, count: u64) -> Self {
        match popularity {
            Popularity::Zipf(s) => match Zipf::new(count as f64, s) {
                Ok(dist) => KeyPicker::Zipf { count, dist },
                Err(_) => KeyPicker::Uniform { count },
            },
            Popularity::Uniform => KeyPicker::Uniform { count },
        }
    }

    #[inline]
    pub fn pick(&self, rng: &mut SmallRng) -> u64 {
        match *self {
            KeyPicker::Uniform { count } => rng.random_range(0..count),
            KeyPicker::Zipf { count, dist } => {
                let rank = (dist.sample(rng) as u64).clamp(1, count) - 1;
                scramble(rank, count)
            }
        }
    }
}

#[inline]
fn scramble(rank: u64, count: u64) -> u64 {
    ((rank as u128 * RANK_SCRAMBLE_PRIME) % count as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use std::collections::HashSet;

    // Keys of one keyspace are distinct, fixed size and printable.
    #[test]
    fn keys_are_distinct_and_sized() {
        let sizes = KeySizes::new(10_000, &[(6, 0.5), (13, 0.5)]);
        let mut seen = HashSet::new();
        let mut buf = Vec::new();
        for index in 0..10_000 {
            let size = sizes.size_of(index);
            write_key(&mut buf, b'a', size, None, index);
            assert_eq!(buf.len(), size);
            assert!(buf.iter().all(|b| b.is_ascii_graphic()));
            assert!(seen.insert(buf.clone()));
        }
        assert_eq!(sizes.size_of(0), 6);
        assert_eq!(sizes.size_of(9_999), 13);
    }

    // Window chars change the key while keeping its size.
    #[test]
    fn windowed_keys() {
        let mut a = Vec::new();
        let mut b = Vec::new();
        write_key(&mut a, b'r', 13, Some(100), 7);
        write_key(&mut b, b'r', 13, Some(101), 7);
        assert_eq!(a.len(), 13);
        assert_ne!(a, b);
        assert_eq!(index_chars(13, true), 8);
    }

    // Zipf ranks map to indices inside the keyspace, and the scramble is a bijection.
    #[test]
    fn zipf_scramble_bijection() {
        let count = 1_000;
        let mapped: HashSet<u64> = (0..count).map(|r| scramble(r, count)).collect();
        assert_eq!(mapped.len(), count as usize);
        let picker = KeyPicker::new(Popularity::Zipf(0.99), count);
        let mut rng = SmallRng::seed_from_u64(1);
        assert!((0..10_000).all(|_| picker.pick(&mut rng) < count));
    }
}
