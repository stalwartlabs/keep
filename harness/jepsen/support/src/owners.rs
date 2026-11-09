/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::path::Path;

use keep_proto::{KeyHasher, Liveness, Member, MemberStatus, NodeId, Placement, Topology};

use crate::history::hex;
use crate::proxy;

#[derive(Clone, Debug)]
pub struct Members(Vec<Member>);

impl Members {
    pub fn parse(text: &str) -> Result<Members, String> {
        text.split(',')
            .filter(|entry| !entry.trim().is_empty())
            .map(parse_member)
            .collect::<Result<Vec<_>, _>>()
            .map(Members)
    }

    pub fn placement(&self) -> io::Result<Placement> {
        let topology = Topology::new(0, self.0.clone()).map_err(io::Error::other)?;
        Placement::new(topology).map_err(io::Error::other)
    }
}

pub fn write_proxy(path: &Path, members: &str, hasher: KeyHasher, keys: u64) -> io::Result<()> {
    let members = Members::parse(members).map_err(io::Error::other)?;
    let mut table = OwnerTable::create(&members, hasher, path)?;
    for index in 0..keys {
        table.row("kv", &proxy::kv_key(index))?;
    }
    table.finish()
}

pub struct OwnerTable {
    placement: Placement,
    hasher: KeyHasher,
    out: BufWriter<File>,
}

impl OwnerTable {
    pub fn create(members: &Members, hasher: KeyHasher, path: &Path) -> io::Result<OwnerTable> {
        let mut out = BufWriter::new(File::create(path)?);
        writeln!(out, "kind\tkey\tslot\towner")?;
        Ok(OwnerTable {
            placement: members.placement()?,
            hasher,
            out,
        })
    }

    pub fn row(&mut self, kind: &str, key: &[u8]) -> io::Result<()> {
        let slot = self.hasher.hash(key).slot();
        match self.placement.owner(slot) {
            Some(owner) => writeln!(
                self.out,
                "{kind}\t{}\t{}\t{}",
                hex(key),
                slot.get(),
                owner.member.id.0
            ),
            None => Ok(()),
        }
    }

    pub fn finish(mut self) -> io::Result<()> {
        self.out.flush()
    }
}

fn parse_member(entry: &str) -> Result<Member, String> {
    let invalid = || format!("invalid member {entry:?}: expected node@client_ip:port/peer/shards");
    let (node, rest) = entry.trim().split_once('@').ok_or_else(invalid)?;
    let (client, rest) = rest.split_once('/').ok_or_else(invalid)?;
    let (_, shards) = rest.rsplit_once('/').ok_or_else(invalid)?;
    let id = node
        .parse::<u64>()
        .ok()
        .filter(|&id| id != 0)
        .ok_or_else(invalid)?;
    let client: SocketAddr = client.parse().map_err(|_| invalid())?;
    let shards = shards.parse::<NonZeroU16>().map_err(|_| invalid())?;
    Ok(Member {
        id: NodeId(id),
        ip: client.ip(),
        port: client.port(),
        shards,
        status: MemberStatus::Active,
        liveness: Liveness::Alive,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use keep_proto::ClusterSecret;

    use super::{Members, write_proxy};

    const SECRET: &str = "00112233445566778899aabbccddeeff";

    #[test]
    fn parses_server_member_syntax() {
        // the same entries keep-server takes in --members
        let members = Members::parse("1@10.0.0.5:7000/10.0.0.5:7100/4,2@10.0.0.6:7000/7100/2")
            .expect("valid members");
        let [first, second] = members.0.as_slice() else {
            panic!("expected two members");
        };
        assert_eq!((first.id.0, first.port, first.shards.get()), (1, 7000, 4));
        assert_eq!(
            (
                second.id.0,
                second.ip.to_string().as_str(),
                second.shards.get()
            ),
            (2, "10.0.0.6", 2)
        );
        assert!(Members::parse("0@10.0.0.5:7000/7100/4").is_err());
        assert!(Members::parse("1@10.0.0.5:7000/7100/0").is_err());
        assert!(Members::parse("1@10.0.0.5/4").is_err());
    }

    #[test]
    fn proxy_owner_rows_name_every_jepsen_index() {
        // one kv row per proxy index, keyed as the proxy encodes it, owned by a member
        let dir =
            std::env::temp_dir().join(format!("keep-jepsen-proxy-owners-{}", std::process::id()));
        fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("owners.tsv");
        let hasher = ClusterSecret::from_hex(SECRET).expect("secret").hasher();
        let members = (1..=5)
            .map(|id| format!("{id}@10.77.0.1{id}:7000/7010/2"))
            .collect::<Vec<_>>()
            .join(",");
        write_proxy(&path, &members, hasher, 300).expect("owners");
        let text = fs::read_to_string(&path).expect("read owners");
        let mut owners = [0u32; 5];
        let mut rows = text.lines().skip(1);
        for index in 0..300u64 {
            let row = rows.next().expect("row per index");
            let [kind, key, _, owner] = row.split('\t').collect::<Vec<_>>()[..] else {
                panic!("four columns: {row}");
            };
            assert_eq!(kind, "kv");
            assert_eq!(key, format!("00{index:016x}"));
            let owner: usize = owner.parse().expect("owner id");
            *owners.get_mut(owner - 1).expect("owner is a member") += 1;
        }
        assert!(rows.next().is_none());
        assert!(owners.iter().all(|&count| count > 0));
        let _ = fs::remove_dir_all(dir);
    }
}
