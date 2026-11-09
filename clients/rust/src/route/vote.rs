/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::net::SocketAddr;

use keep_proto::{Member, NodeId, Topology};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Choice {
    Agreed(usize),
    Best(usize),
    Nothing,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(super) struct Placing(Vec<(NodeId, SocketAddr, u16)>);

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Listed(Vec<SocketAddr>);

impl Listed {
    fn of(topology: &Topology) -> Listed {
        let mut members: Vec<SocketAddr> = topology.members().iter().map(address).collect();
        members.sort_unstable();
        members.dedup();
        Listed(members)
    }

    fn contains(&self, addr: &SocketAddr) -> bool {
        self.0.binary_search(addr).is_ok()
    }
}

impl Placing {
    pub fn of(topology: &Topology) -> Placing {
        let mut members: Vec<(NodeId, SocketAddr, u16)> = topology
            .members()
            .iter()
            .filter(|member| member.places())
            .map(|member| (member.id, address(member), member.shards.get()))
            .collect();
        members.sort_unstable();
        Placing(members)
    }
}

pub(super) fn address(member: &Member) -> SocketAddr {
    SocketAddr::new(member.ip, member.port)
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Rank {
    agreed: bool,
    reporters: usize,
    current: bool,
    placing: usize,
}

pub(super) fn choose(reports: &[(SocketAddr, Topology)], current: Option<&Placing>) -> Choice {
    match tally(reports, current) {
        Some(Tally { top, index, .. }) if top.agreed => Choice::Agreed(index),
        Some(Tally { index, .. }) => Choice::Best(index),
        None => Choice::Nothing,
    }
}

pub(super) fn decided(
    reports: &[(SocketAddr, Topology)],
    current: Option<&Placing>,
    outstanding: usize,
) -> bool {
    tally(reports, current)
        .is_some_and(|tally| tally.top.agreed && tally.others + outstanding < tally.top.reporters)
}

struct Tally {
    top: Rank,
    index: usize,
    others: usize,
}

fn tally(reports: &[(SocketAddr, Topology)], current: Option<&Placing>) -> Option<Tally> {
    let mut voters: Vec<(SocketAddr, usize)> = reports
        .iter()
        .enumerate()
        .map(|(index, (addr, _))| (canonical(*addr), index))
        .collect();
    voters.sort_unstable();
    voters.dedup_by_key(|(addr, _)| *addr);
    let mut keyed: Vec<(Placing, Listed, SocketAddr, usize)> = voters
        .into_iter()
        .filter_map(|(addr, index)| {
            let (_, topology) = reports.get(index)?;
            Some((Placing::of(topology), Listed::of(topology), addr, index))
        })
        .collect();
    keyed.sort_unstable();
    let mut best: Option<Tally> = None;
    for group in keyed.chunk_by(|a, b| a.0 == b.0 && a.1 == b.1) {
        let Some((placing, listed, _, first)) = group.first() else {
            continue;
        };
        let reporters = group
            .iter()
            .filter(|(_, _, addr, _)| listed.contains(addr))
            .count();
        let rank = Rank {
            agreed: reporters * 2 > listed.0.len(),
            reporters,
            current: current == Some(placing),
            placing: placing.0.len(),
        };
        best = Some(match best {
            Some(tally) if rank <= tally.top => Tally {
                others: tally.others.max(reporters),
                ..tally
            },
            Some(tally) => Tally {
                others: tally.others.max(tally.top.reporters),
                top: rank,
                index: *first,
            },
            None => Tally {
                top: rank,
                index: *first,
                others: 0,
            },
        });
    }
    best
}

pub(super) fn canonical(addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(addr.ip().to_canonical(), addr.port())
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-client/route/vote.rs"]
mod tests;
