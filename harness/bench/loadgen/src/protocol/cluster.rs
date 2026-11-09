/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

pub const SLOTS: usize = 16_384;

const CRC16_TABLE: [u16; 256] = crc16_table();

const fn crc16_table() -> [u16; 256] {
    let mut table = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

pub fn crc16(bytes: &[u8]) -> u16 {
    bytes.iter().fold(0u16, |crc, &b| {
        (crc << 8) ^ CRC16_TABLE[usize::from((crc >> 8) as u8 ^ b)]
    })
}

#[inline]
pub fn slot(key: &[u8]) -> usize {
    usize::from(crc16(hash_tag(key).unwrap_or(key))) & (SLOTS - 1)
}

fn hash_tag(key: &[u8]) -> Option<&[u8]> {
    let after_open = key.splitn(2, |&b| b == b'{').nth(1)?;
    let mut parts = after_open.splitn(2, |&b| b == b'}');
    let inner = parts.next()?;
    parts.next()?;
    (!inner.is_empty()).then_some(inner)
}

#[derive(Clone, Debug)]
pub struct SlotMap {
    pub nodes: Vec<String>,
    owner: Box<[u16]>,
}

impl SlotMap {
    pub fn new(nodes: Vec<String>, ranges: &[(usize, usize, usize)]) -> Result<Self, String> {
        let mut owner = vec![u16::MAX; SLOTS].into_boxed_slice();
        for &(start, end, node) in ranges {
            let node = u16::try_from(node).map_err(|_| String::from("too many cluster nodes"))?;
            owner
                .get_mut(start..=end)
                .ok_or_else(|| format!("slot range {start}-{end} out of bounds"))?
                .fill(node);
        }
        if let Some(missing) = owner.iter().position(|&n| n == u16::MAX) {
            return Err(format!("slot {missing} has no owner"));
        }
        Ok(SlotMap { nodes, owner })
    }

    #[inline]
    pub fn node_of(&self, key: &[u8]) -> usize {
        usize::from(self.owner[slot(key)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Reference values from the Redis Cluster specification.
    #[test]
    fn crc16_reference() {
        assert_eq!(crc16(b"123456789"), 0x31c3);
        assert_eq!(slot(b"foo"), 12182);
        assert_eq!(slot(b"{user1000}.following"), slot(b"user1000"));
        assert_eq!(
            slot(b"foo{}{bar}"),
            usize::from(crc16(b"foo{}{bar}")) & (SLOTS - 1)
        );
    }

    // Every slot must have an owner.
    #[test]
    fn slot_map_coverage() {
        let nodes = vec![String::from("a:1"), String::from("b:2")];
        assert!(SlotMap::new(nodes.clone(), &[(0, 8191, 0)]).is_err());
        let map = SlotMap::new(nodes, &[(0, 8191, 0), (8192, 16383, 1)]).expect("full");
        assert_eq!(map.node_of(b"foo"), 1);
    }
}
