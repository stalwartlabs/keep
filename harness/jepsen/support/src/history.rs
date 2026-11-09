/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fmt;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    TryLock,
    Renew,
    Unlock,
    Get,
    Exists,
    Set,
    SetNx,
    Delete,
    Incr,
    Publish,
    Subscribe,
    Unsubscribe,
    Event,
    Heartbeat,
    End,
    Gap,
    Lost,
    Resubscribed,
    Ended,
    Fault,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultKind {
    Kill,
    Pause,
    ClockStop,
    Isolate,
    Split,
    OneWay,
    PeersOnly,
    Clients,
    Flap,
    Netem,
    ClockJump,
    ClockRate,
    Leave,
    Restart,
    RollingRestart,
    MemberAdd,
    MemberRemove,
    WallRestart,
    RunOut,
    Deaf,
    LostAck,
}

impl FaultKind {
    pub fn fails_over(self) -> bool {
        matches!(
            self,
            FaultKind::Kill
                | FaultKind::Pause
                | FaultKind::ClockStop
                | FaultKind::Isolate
                | FaultKind::Split
                | FaultKind::OneWay
                | FaultKind::PeersOnly
                | FaultKind::Flap
                | FaultKind::Netem
                | FaultKind::MemberAdd
                | FaultKind::MemberRemove
                | FaultKind::RunOut
        )
    }

    pub fn changes_membership(self) -> bool {
        matches!(
            self,
            FaultKind::Leave
                | FaultKind::Restart
                | FaultKind::RollingRestart
                | FaultKind::WallRestart
        )
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Ok,
    Conflict,
    NotFound,
    Busy,
    Moved,
    Error,
    Unknown,
    WrongType,
    Overflow,
}

impl Outcome {
    pub fn not_executed(self) -> bool {
        matches!(self, Outcome::Busy | Outcome::Moved | Outcome::Error)
    }
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct Record {
    pub client: u32,
    pub op: Op,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_s: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missed: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fault: Option<FaultKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_ns: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_ns: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_ns: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<Outcome>,
}

impl Record {
    pub fn new(client: u32, op: Op) -> Record {
        Record {
            client,
            op,
            key: None,
            token: None,
            ttl_s: None,
            value: None,
            delta: None,
            count: None,
            topic: None,
            seq: None,
            missed: None,
            server: None,
            fault: None,
            node: None,
            call_ns: None,
            send_ns: None,
            reply_ns: None,
            outcome: None,
        }
    }
}

pub fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 * bytes.len());
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[derive(Debug)]
pub struct HistoryError {
    pub line: usize,
    pub error: serde_json::Error,
}

impl fmt::Display for HistoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "history line {}: {}", self.line, self.error)
    }
}

impl std::error::Error for HistoryError {}

pub fn parse(text: &str) -> Result<Vec<Record>, HistoryError> {
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            serde_json::from_str(line).map_err(|error| HistoryError {
                line: index + 1,
                error,
            })
        })
        .collect()
}

pub fn render(records: &[Record], out: &mut String) {
    for record in records {
        if let Ok(line) = serde_json::to_string(record) {
            out.push_str(&line);
            out.push('\n');
        }
    }
}
