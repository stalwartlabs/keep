/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::io;
use std::net::SocketAddr;

use keep_client::{Client, Error, LockToken, Ttl};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

const LOCK_CLASS: u8 = 14;
const KV_CLASS: u8 = 0;
const MAX_LINE: usize = 4_096;

#[derive(Deserialize)]
#[serde(tag = "f", rename_all = "kebab-case")]
enum Request {
    Read { key: u64 },
    Write { key: u64, value: u64 },
    WriteNx { key: u64, value: u64 },
    Delete { key: u64 },
    Incr { key: u64, delta: i64 },
    Lock { key: u64, token: u64, ttl: u32 },
    Renew { key: u64, token: u64, ttl: u32 },
    Unlock { key: u64, token: u64 },
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
enum Kind {
    Ok,
    Fail,
    Info,
}

#[derive(Serialize)]
struct Reply {
    #[serde(rename = "type")]
    kind: Kind,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    server: Option<SocketAddr>,
}

impl Reply {
    fn ok(value: Option<i64>) -> Reply {
        Reply {
            kind: Kind::Ok,
            value,
            error: None,
            server: None,
        }
    }

    fn fail(error: &str) -> Reply {
        Reply {
            kind: Kind::Fail,
            value: None,
            error: Some(error.to_owned()),
            server: None,
        }
    }

    fn of_error(error: &Error) -> Reply {
        Reply {
            kind: if error.not_executed() {
                Kind::Fail
            } else {
                Kind::Info
            },
            value: None,
            error: Some(error.to_string()),
            server: None,
        }
    }

    fn of_bool(result: Result<bool, Error>, refused: &str) -> Reply {
        match result {
            Ok(true) => Reply::ok(None),
            Ok(false) => Reply::fail(refused),
            Err(error) => Reply::of_error(&error),
        }
    }
}

pub fn kv_key(index: u64) -> [u8; 9] {
    key(KV_CLASS, index)
}

fn key(class: u8, index: u64) -> [u8; 9] {
    let mut out = [0u8; 9];
    let (first, rest) = out.split_at_mut(1);
    first.copy_from_slice(&[class]);
    rest.copy_from_slice(&index.to_be_bytes());
    out
}

pub async fn serve(listen: SocketAddr, url: String) -> io::Result<()> {
    let listener = TcpListener::bind(listen).await?;
    eprintln!("keep-jepsen proxy: listening on {}", listener.local_addr()?);
    loop {
        let (stream, _) = listener.accept().await?;
        let url = url.clone();
        tokio::spawn(async move {
            if let Err(error) = session(stream, &url).await {
                eprintln!("keep-jepsen proxy: session: {error}");
            }
        });
    }
}

async fn session(stream: TcpStream, url: &str) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let client = Client::connect_url(url)
        .await
        .map_err(io::Error::other)?
        .observed();
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read);
    let mut line = String::new();
    loop {
        line.clear();
        if lines.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        if line.len() > MAX_LINE {
            return Err(io::Error::other("request line too long"));
        }
        let mut reply = match serde_json::from_str::<Request>(&line) {
            Ok(request) => execute(&client, request).await,
            Err(error) => Reply::fail(&format!("bad request: {error}")),
        };
        reply.server = client.take_server();
        let mut out = serde_json::to_vec(&reply).map_err(io::Error::other)?;
        out.push(b'\n');
        write.write_all(&out).await?;
    }
}

async fn execute(client: &Client, request: Request) -> Reply {
    match request {
        Request::Read { key: index } => match client.get(&kv_key(index)).await {
            Ok(Some(value)) => match <[u8; 8]>::try_from(value.as_slice()) {
                Ok(bytes) => Reply::ok(Some(i64::from_be_bytes(bytes))),
                Err(_) => Reply::fail("value is not 8 bytes"),
            },
            Ok(None) => Reply::ok(None),
            Err(error) => Reply::of_error(&error),
        },
        Request::Write { key: index, value } => {
            match client.set(&kv_key(index), &value.to_be_bytes(), None).await {
                Ok(()) => Reply::ok(None),
                Err(error) => Reply::of_error(&error),
            }
        }
        Request::WriteNx { key: index, value } => Reply::of_bool(
            client
                .set_if_absent(&kv_key(index), &value.to_be_bytes(), None)
                .await,
            "present",
        ),
        Request::Delete { key: index } => match client.delete(&kv_key(index)).await {
            Ok(_) => Reply::ok(None),
            Err(error) => Reply::of_error(&error),
        },
        Request::Incr { key: index, delta } => {
            match client.incr(&kv_key(index), delta, None).await {
                Ok(count) => Reply::ok(Some(count)),
                Err(error) => Reply::of_error(&error),
            }
        }
        Request::Lock {
            key: index,
            token,
            ttl,
        } => match Ttl::from_secs(ttl) {
            Some(ttl) => Reply::of_bool(
                client
                    .try_lock_with(&key(LOCK_CLASS, index), ttl, LockToken::from_u64(token))
                    .await,
                "held",
            ),
            None => Reply::fail("ttl 0"),
        },
        Request::Renew {
            key: index,
            token,
            ttl,
        } => match Ttl::from_secs(ttl) {
            Some(ttl) => Reply::of_bool(
                client
                    .renew(
                        &key(LOCK_CLASS, index),
                        ttl,
                        Some(LockToken::from_u64(token)),
                    )
                    .await,
                "lost",
            ),
            None => Reply::fail("ttl 0"),
        },
        Request::Unlock { key: index, token } => Reply::of_bool(
            client
                .unlock(&key(LOCK_CLASS, index), LockToken::from_u64(token))
                .await,
            "not held",
        ),
    }
}
