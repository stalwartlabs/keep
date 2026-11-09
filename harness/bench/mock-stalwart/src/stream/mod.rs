/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

pub mod ack;
pub mod backend;
pub mod backlog;
pub mod frame;
pub mod publisher;
pub mod subscriber;

use crate::{
    hist::{AtomicHistogram, Percentiles},
    stats::{Tagged, counters},
    workload::trace::PublishSummary,
};
use hdrhistogram::CreationError;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt;

pub const EVENTS_TOPIC: &str = "stwt.agora";
pub const ACK_TOPIC_PREFIX: &str = "stwt.ack.";

pub const TAG_STATE_CHANGE: u8 = 0;
pub const TAG_CALENDAR_ALERT: u8 = 1;
pub const TAG_EMAIL_PUSH: u8 = 2;
pub const TAG_PUSH_SERVER_UPDATE: u8 = 3;
pub const TAG_REGISTRY_INSERT: u8 = 4;
pub const TAG_REGISTRY_DELETE: u8 = 5;
pub const TAG_REGISTRY_RELOAD: u8 = 6;
pub const TAG_CACHE_INVALIDATE: u8 = 7;
pub const TAG_CACHE_INVALIDATE_ALL: u8 = 8;
pub const TAG_CACHE_INVALIDATE_NEGATIVE: u8 = 9;
pub const TAG_QUEUE_RUNNING: u8 = 10;
pub const TAG_QUEUE_PAUSED: u8 = 11;
pub const TAG_QUEUE_REFRESH: u8 = 12;
pub const TAG_TASK_REFRESH: u8 = 13;
pub const TAG_INDEX_REFRESH: u8 = 14;
pub const TAG_VIEWER_STATE_CHANGE: u8 = 15;

#[derive(Clone, Copy, Debug)]
pub struct Event {
    pub seq: u64,
    pub created: u64,
    pub size: u32,
    pub account: u32,
    pub tag: u8,
}

const _: () = assert!(size_of::<Event>() <= 32);

#[derive(Clone, Copy, Debug)]
pub struct MessageSpec {
    pub created: u64,
    pub bytes: u32,
    pub count: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum, Serialize)]
pub enum EventReplay {
    Events,
    Messages,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Topic {
    Events,
    Ack(u16),
}

impl Topic {
    pub fn name(&self) -> String {
        match self {
            Topic::Events => EVENTS_TOPIC.to_string(),
            Topic::Ack(node) => format!("{ACK_TOPIC_PREFIX}{node}"),
        }
    }
}

#[derive(Debug)]
pub enum StreamError {
    Unsupported(&'static str),
    Connect(Box<str>),
    Publish(Box<str>),
    Subscribe(Box<str>),
    NotConnected,
}

impl StreamError {
    pub fn connect(err: impl fmt::Display) -> Self {
        StreamError::Connect(err.to_string().into_boxed_str())
    }

    pub fn publish(err: impl fmt::Display) -> Self {
        StreamError::Publish(err.to_string().into_boxed_str())
    }

    pub fn subscribe(err: impl fmt::Display) -> Self {
        StreamError::Subscribe(err.to_string().into_boxed_str())
    }
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamError::Unsupported(what) => write!(f, "unsupported: {what}"),
            StreamError::Connect(reason) => write!(f, "connect: {reason}"),
            StreamError::Publish(reason) => write!(f, "publish: {reason}"),
            StreamError::Subscribe(reason) => write!(f, "subscribe: {reason}"),
            StreamError::NotConnected => f.write_str("not connected"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum, Serialize)]
pub enum Verify {
    Full,
    Header,
}

counters!(
    StreamCounters,
    StreamCounterSnapshot {
        events_produced,
        producer_stalls,
        messages_published,
        events_published,
        bytes_published,
        publish_errors,
        oversize_dropped,
        backlog_coalesced,
        backlog_dropped,
        refreshes_deduplicated,
        messages_received,
        events_received,
        bytes_received,
        own_skipped,
        invalid_messages,
        verify_failures,
        gaps,
        gap_messages,
        duplicates,
        publisher_restarts,
        catch_up_resubscribe,
        catch_up_gap,
        invalidate_all_received,
        disconnects,
        subscribe_errors,
        acks_queued,
        acks_dropped,
        ack_messages_sent,
        ack_publish_errors,
        acks_received,
        acks_unmatched,
        deliveries_incomplete,
        deliveries_complete,
        end_markers_sent,
        end_markers_received,
        oneway_negative,
        errors_logged,
    }
);

#[derive(Default)]
pub struct StreamStats {
    pub publish: AtomicHistogram,
    pub event_delay: AtomicHistogram,
    pub batch_events: AtomicHistogram,
    pub batch_bytes: AtomicHistogram,
    pub oneway: AtomicHistogram,
    pub echo: AtomicHistogram,
    pub echo_last: AtomicHistogram,
    pub event_age_last: AtomicHistogram,
    pub lag: AtomicHistogram,
    pub counters: StreamCounters,
}

#[derive(Debug, Serialize)]
pub struct StreamReport {
    pub publish: Percentiles,
    pub event_delay: Percentiles,
    pub batch_events: Percentiles,
    pub batch_bytes: Percentiles,
    pub oneway: Percentiles,
    pub echo: Percentiles,
    pub echo_last: Percentiles,
    pub event_age_last: Percentiles,
    pub lag: Percentiles,
    pub counters: StreamCounterSnapshot,
    pub client_counters: Option<BTreeMap<&'static str, u64>>,
    pub trace_publishes: Option<PublishSummary>,
}

impl StreamStats {
    pub fn report(
        &self,
        trace_publishes: Option<PublishSummary>,
    ) -> Result<StreamReport, CreationError> {
        let of = |histogram: &AtomicHistogram| {
            histogram
                .to_histogram()
                .map(|histogram| Percentiles::of(&histogram))
        };
        Ok(StreamReport {
            publish: of(&self.publish)?,
            event_delay: of(&self.event_delay)?,
            batch_events: of(&self.batch_events)?,
            batch_bytes: of(&self.batch_bytes)?,
            oneway: of(&self.oneway)?,
            echo: of(&self.echo)?,
            echo_last: of(&self.echo_last)?,
            event_age_last: of(&self.event_age_last)?,
            lag: of(&self.lag)?,
            counters: self.counters.snapshot(),
            client_counters: None,
            trace_publishes,
        })
    }

    pub fn histograms(&self, out: &mut Vec<Tagged>) -> Result<(), CreationError> {
        for (tag, histogram) in [
            ("stream.publish", &self.publish),
            ("stream.event_delay", &self.event_delay),
            ("stream.batch_events", &self.batch_events),
            ("stream.batch_bytes", &self.batch_bytes),
            ("stream.oneway", &self.oneway),
            ("stream.echo", &self.echo),
            ("stream.echo_last", &self.echo_last),
            ("stream.event_age_last", &self.event_age_last),
            ("stream.lag", &self.lag),
        ] {
            out.push(Tagged::of(tag, histogram)?);
        }
        Ok(())
    }
}
