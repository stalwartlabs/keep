/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::env::temp_dir;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{self, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .expect("free port")
}

fn wait_listening(addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while TcpStream::connect(addr).is_err() {
        assert!(Instant::now() < deadline, "null server did not start");
        thread::sleep(Duration::from_millis(20));
    }
}

fn run_against_null(protocol: &str, io: &str) {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let mut server = Command::new(env!("CARGO_BIN_EXE_nullserver"))
        .args(["--listen", &addr, "--protocol", protocol, "--threads", "2"])
        .args(["--report-every", "600"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn nullserver");
    wait_listening(&addr);
    let out: PathBuf = temp_dir().join(format!("loadgen-{}-{protocol}-{io}", process::id()));
    let status = Command::new(env!("CARGO_BIN_EXE_loadgen"))
        .args(["run", "--target", &addr, "--protocol", protocol, "--io", io])
        .args([
            "--threads",
            "2",
            "--connections",
            "8",
            "--keys-scale",
            "0.001",
        ])
        .args([
            "--preload",
            "--rate",
            "5k,20k",
            "--warmup",
            "0.5",
            "--settle",
            "0.2",
        ])
        .args(["--duration", "1", "--drain", "1", "--max-util", "1"])
        .args([
            "--max-lag-p99-us",
            "1000000",
            "--max-lag-p999-us",
            "1000000",
        ])
        .args(["--outlier-us", "20", "--drop-unsupported", "--out"])
        .arg(&out)
        .stdout(Stdio::null())
        .status()
        .expect("run loadgen");
    let _ = server.kill();
    let _ = server.wait();
    let summary: Value =
        serde_json::from_slice(&fs::read(out.join("summary.json")).expect("summary written"))
            .expect("summary is json");
    let outliers = fs::read_to_string(out.join("outliers.jsonl")).expect("outliers written");
    let _ = fs::remove_dir_all(&out);
    assert!(status.success(), "{protocol} over {io}: {summary}");
    let points = summary["points"].as_array().expect("points");
    assert_eq!(points.len(), 2);
    for point in points {
        assert_eq!(point["issued"], point["completed"], "{point}");
        assert!(point["issued"].as_u64().is_some_and(|n| n > 0));
    }
    assert!(summary["preload"]["keys"].as_u64().is_some_and(|n| n > 0));
    let logged: u64 = points.iter().filter_map(|p| p["outliers"].as_u64()).sum();
    assert_eq!(outliers.lines().count() as u64, logged);
    for line in outliers.lines() {
        let outlier: Value = serde_json::from_str(line).expect("outlier is json");
        assert!(outlier["latency_ns"].as_u64().is_some_and(|ns| ns > 20_000));
    }
    let flows = summary["flows"].as_array().expect("flows");
    assert_eq!(flows.len(), 8);
    assert!(
        summary["clock"]["mono_base_ns"]
            .as_u64()
            .is_some_and(|ns| ns > 0)
    );
}

// Every request issued through the readiness driver is answered, for each protocol family.
#[test]
fn epoll_driver_completes_every_request() {
    for protocol in ["resp", "meta", "keep"] {
        run_against_null(protocol, "epoll");
    }
}

// Every request issued through the io_uring driver is answered, for each protocol family.
#[cfg(target_os = "linux")]
#[test]
fn uring_driver_completes_every_request() {
    for protocol in ["resp", "meta", "keep"] {
        run_against_null(protocol, "uring");
    }
}

fn preload_only(addr: &str, protocol: &str, check: bool, extra: &[&str]) -> Value {
    let out: PathBuf = temp_dir().join(format!(
        "loadgen-preload-{}-{}-{protocol}-{check}",
        process::id(),
        addr.replace([':', '.'], "-")
    ));
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_loadgen"));
    cmd.args(["run", "--target", addr, "--protocol", protocol])
        .args(extra)
        .args([
            "--threads",
            "2",
            "--connections",
            "4",
            "--keys-scale",
            "0.001",
        ])
        .args([
            "--rate",
            "1",
            "--preload-only",
            "--drop-unsupported",
            "--out",
        ])
        .arg(&out);
    if check {
        cmd.arg("--preload-check");
    }
    let status = cmd.stdout(Stdio::null()).status().expect("run loadgen");
    let summary: Value =
        serde_json::from_slice(&fs::read(out.join("summary.json")).expect("summary written"))
            .expect("summary is json");
    let _ = fs::remove_dir_all(&out);
    assert!(status.success(), "{protocol} check {check}: {summary}");
    summary
}

// --preload-check walks the same keys as the write preload and reports them under mode "check".
#[test]
fn preload_check_walks_the_preload_keys() {
    for protocol in ["resp", "meta", "keep"] {
        let port = free_port();
        let addr = format!("127.0.0.1:{port}");
        let mut server = Command::new(env!("CARGO_BIN_EXE_nullserver"))
            .args(["--listen", &addr, "--protocol", protocol, "--threads", "2"])
            .args(["--report-every", "600"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn nullserver");
        wait_listening(&addr);
        let write = preload_only(&addr, protocol, false, &[]);
        let check = preload_only(&addr, protocol, true, &[]);
        let _ = server.kill();
        let _ = server.wait();
        assert!(write["preload"].get("mode").is_none(), "{write}");
        assert_eq!(check["preload"]["mode"], "check", "{check}");
        let keys = write["preload"]["keys"].as_u64().expect("keys");
        assert!(keys > 0);
        assert_eq!(check["preload"]["keys"].as_u64(), Some(keys), "{protocol}");
        assert_eq!(check["preload"]["misses"].as_u64(), Some(0), "{check}");
        assert_eq!(check["preload"]["present"].as_u64(), Some(keys), "{check}");
        assert_eq!(error_total(&check), 0, "{check}");
        assert!(write["preload"].get("present").is_none(), "{write}");
    }
}

fn error_total(summary: &Value) -> u64 {
    ["replies", "timeouts", "failed"]
        .iter()
        .map(|kind| {
            summary["preload"]["errors"][kind]
                .as_u64()
                .expect("error count")
        })
        .sum()
}

fn resp_header(buf: &[u8], tag: u8) -> Option<(usize, usize)> {
    let (&first, rest) = buf.split_first()?;
    assert_eq!(first, tag, "RESP request framing");
    let digits = rest.split(|&b| b == b'\r').next()?;
    if rest.get(digits.len() + 1) != Some(&b'\n') {
        return None;
    }
    let value = std::str::from_utf8(digits).ok()?.parse().ok()?;
    Some((value, digits.len() + 3))
}

fn resp_request_len(buf: &[u8]) -> Option<usize> {
    let (count, mut used) = resp_header(buf, b'*')?;
    for _ in 0..count {
        let (len, head) = resp_header(buf.get(used..)?, b'$')?;
        used += head + len + 2;
    }
    (used <= buf.len()).then_some(used)
}

fn answer_zero(mut stream: TcpStream) {
    let mut pending = Vec::new();
    let mut chunk = [0u8; 16 * 1024];
    let mut replies = Vec::new();
    while let Ok(read) = stream.read(&mut chunk) {
        let Some(data) = chunk.get(..read).filter(|data| !data.is_empty()) else {
            return;
        };
        pending.extend_from_slice(data);
        let mut consumed = 0;
        while let Some(len) = pending.get(consumed..).and_then(resp_request_len) {
            consumed += len;
            replies.extend_from_slice(b":0\r\n");
        }
        pending.drain(..consumed);
        if stream.write_all(&replies).is_err() {
            return;
        }
        replies.clear();
    }
}

// Against a RESP server that answers :0 to every command, the check counts every key absent: it sends EXISTS, not the preload's SET.
#[test]
fn preload_check_sends_exists() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr").to_string();
    thread::spawn(move || {
        for stream in listener.incoming().map_while(Result::ok) {
            thread::spawn(move || answer_zero(stream));
        }
    });
    let native = ["--resp-mapping", "native"];
    let write = preload_only(&addr, "resp", false, &native);
    let check = preload_only(&addr, "resp", true, &native);
    let keys = write["preload"]["keys"].as_u64().expect("keys");
    assert!(keys > 0);
    assert_eq!(write["preload"]["misses"].as_u64(), Some(0), "{write}");
    assert_eq!(check["preload"]["keys"].as_u64(), Some(keys), "{check}");
    assert_eq!(check["preload"]["misses"].as_u64(), Some(keys), "{check}");
    assert_eq!(check["preload"]["present"].as_u64(), Some(0), "{check}");
    assert_eq!(error_total(&check), 0, "{check}");
}

// --preload-check is refused without --preload-only.
#[test]
fn preload_check_requires_preload_only() {
    let status = Command::new(env!("CARGO_BIN_EXE_loadgen"))
        .args(["run", "--target", "127.0.0.1:1", "--protocol", "resp"])
        .args(["--rate", "1", "--preload-check"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run loadgen");
    assert_eq!(status.code(), Some(2));
}
