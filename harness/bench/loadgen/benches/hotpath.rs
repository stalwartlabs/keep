/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use loadgen::conn::InFlight;
use loadgen::op::{OpKind, Outcome};
use loadgen::protocol::Protocol;
use loadgen::protocol::resp::{LeaseRelease, LeaseRenew, Resp, RespMapping};
use loadgen::server::{ServerProtocol, framing::Framer};
use loadgen::stats::{Recorder, hist_template};
use loadgen::workload::keys::Popularity;
use loadgen::workload::profile::{self, Overrides};
use loadgen::workload::values::value_pool;
use loadgen::workload::{Generator, VirtualTime};

const OPS: usize = 2_000_000;
const ROUNDS: usize = 5;

fn generator(popularity: Popularity) -> Generator {
    let workload = profile::parse(
        profile::builtin("stalwart-mix").expect("builtin"),
        &Overrides {
            popularity: Some(popularity),
            keys_scale: 1.0,
        },
    )
    .expect("valid profile");
    let values: Arc<[u8]> = value_pool(workload.max_value(), 1).into();
    Generator::new(
        Arc::new(workload),
        values,
        VirtualTime {
            origin_s: 1_790_000_000,
        },
        7,
    )
}

fn best_ns_per_op(mut run: impl FnMut() -> u64) -> f64 {
    (0..ROUNDS)
        .map(|_| {
            let start = Instant::now();
            black_box(run());
            start.elapsed().as_nanos() as f64 / OPS as f64
        })
        .fold(f64::INFINITY, f64::min)
}

fn bench_generator(name: &str, popularity: Popularity) {
    let mut generator = generator(popularity);
    let ns = best_ns_per_op(|| {
        let mut acc = 0u64;
        for i in 0..OPS {
            let req = generator.next(i as u64 * 1_000);
            acc = acc.wrapping_add(req.key.len() as u64 + req.ttl as u64);
        }
        acc
    });
    println!("generate stalwart-mix {name:<12} {ns:>7.1} ns/op");
}

fn bench_protocol(name: &str, protocol: &Protocol, server: ServerProtocol) {
    let mut generator = generator(Popularity::Uniform);
    let mut wire = Vec::with_capacity(1 << 20);
    let mut framer = Framer::new(server, Box::default());
    let mut replies = Vec::with_capacity(1 << 20);
    let mut expected: Vec<(OpKind, u8)> = Vec::with_capacity(4096);
    let batch = 4096;
    let mut encode_ns = f64::INFINITY;
    let mut decode_ns = f64::INFINITY;
    for _ in 0..ROUNDS {
        let mut encode_total = 0u128;
        let mut decode_total = 0u128;
        let mut done = 0;
        while done < OPS {
            wire.clear();
            replies.clear();
            expected.clear();
            let start = Instant::now();
            for i in 0..batch {
                let req = generator.next((done + i) as u64 * 1_000);
                let parts = protocol.encode(&req, i as u32, &mut wire).expect("encodes");
                expected.push((req.kind, parts));
            }
            encode_total += start.elapsed().as_nanos();
            let mut pos = 0;
            while let Some(rest) = wire.get(pos..).filter(|r| !r.is_empty()) {
                pos += framer
                    .answer(rest, &mut replies)
                    .expect("valid")
                    .expect("complete");
            }
            let start = Instant::now();
            let mut rpos = 0;
            let mut ok = 0u64;
            for &(kind, parts) in &expected {
                for part in 0..parts {
                    let reply = protocol
                        .decode(&replies[rpos..], Some((kind, part + 1 == parts)))
                        .expect("valid")
                        .expect("complete");
                    rpos += reply.len;
                    ok += u64::from(reply.outcome == Outcome::Ok);
                }
            }
            decode_total += start.elapsed().as_nanos();
            black_box(ok);
            done += batch;
        }
        encode_ns = encode_ns.min(encode_total as f64 / done as f64);
        decode_ns = decode_ns.min(decode_total as f64 / done as f64);
    }
    println!(
        "{name:<12} generate+encode {encode_ns:>6.1} ns/op  decode {decode_ns:>6.1} ns/op  bytes/op {:>5.1} out {:>5.1} in",
        wire.len() as f64 / batch as f64,
        replies.len() as f64 / batch as f64
    );
}

fn bench_record() {
    let mut rec = Recorder::new(
        OpKind::ALL.iter().copied(),
        hist_template().expect("histogram bounds"),
        u64::MAX,
    );
    rec.open(0);
    let kinds = OpKind::ALL;
    let ns = best_ns_per_op(|| {
        for i in 0..OPS {
            let entry = InFlight {
                intended_ns: i as u64 * 100,
                lag_ns: (i % 997) as u32,
                kind: kinds[i % kinds.len()],
                parts: 1,
                phase: 0,
                flags: 0,
            };
            rec.issued(0);
            rec.complete(
                &entry,
                Outcome::Ok,
                i as u64 * 100 + 40_000 + (i % 7919) as u64,
                0,
            );
        }
        rec.outstanding(0)
    });
    println!("record latency+service (2 HDR records + counters)  {ns:>6.1} ns/op");
}

fn main() {
    bench_generator("uniform", Popularity::Uniform);
    bench_generator("zipf:0.99", Popularity::Zipf(0.99));
    let sha: Box<[u8]> = b"0000000000000000000000000000000000000000".to_vec().into();
    let protocols = [
        (
            "resp-stalwart",
            Protocol::Resp(Resp {
                mapping: RespMapping::Stalwart,
                script_sha: sha.clone(),
                lease_renew_sha: sha.clone(),
                lease_release_sha: sha.clone(),
                lease_renew: LeaseRenew::Script,
                lease_release: LeaseRelease::Script,
            }),
            ServerProtocol::Resp,
        ),
        (
            "resp-native",
            Protocol::Resp(Resp {
                mapping: RespMapping::Native,
                script_sha: sha.clone(),
                lease_renew_sha: sha.clone(),
                lease_release_sha: sha,
                lease_renew: LeaseRenew::SetIfEq,
                lease_release: LeaseRelease::DelIfEq,
            }),
            ServerProtocol::Resp,
        ),
        ("meta", Protocol::Meta, ServerProtocol::Meta),
        ("keep", Protocol::Keep, ServerProtocol::Keep),
    ];
    for (name, protocol, server) in &protocols {
        bench_protocol(name, protocol, *server);
    }
    bench_record();
}
