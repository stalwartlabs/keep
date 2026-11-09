/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::stream::{EVENTS_TOPIC, StreamError, Topic, frame::MAX_NODE};
use std::path::PathBuf;
use zenoh::{
    Config, Session,
    handlers::FifoChannelHandler,
    key_expr::KeyExpr,
    pubsub::{Publisher, Subscriber},
    sample::Sample,
};

pub type ZenohSubscription = Subscriber<FifoChannelHandler<Sample>>;

#[derive(Debug, Clone)]
pub enum ZenohTarget {
    Client(Box<[Box<str>]>),
    Peer(Box<[Box<str>]>),
    Config(PathBuf),
}

pub struct ZenohStream {
    session: Session,
}

pub struct ZenohPublisher {
    session: Session,
    publisher: Option<Publisher<'static>>,
}

pub struct ZenohAckPublisher {
    session: Session,
    keys: Vec<Option<KeyExpr<'static>>>,
}

fn endpoints(list: &[Box<str>]) -> String {
    let quoted = list
        .iter()
        .map(|endpoint| format!("\"{endpoint}\""))
        .collect::<Vec<_>>()
        .join(",");
    format!("[{quoted}]")
}

impl ZenohStream {
    pub async fn connect(target: &ZenohTarget) -> Result<ZenohStream, StreamError> {
        let config = match target {
            ZenohTarget::Config(path) => Config::from_file(path).map_err(StreamError::connect)?,
            ZenohTarget::Client(list) | ZenohTarget::Peer(list) => {
                let mode = match target {
                    ZenohTarget::Client(_) => "\"client\"",
                    _ => "\"peer\"",
                };
                let mut config = Config::default();
                for (key, value) in [
                    ("mode", mode.to_string()),
                    ("connect/endpoints", endpoints(list)),
                    ("scouting/multicast/enabled", "false".to_string()),
                ] {
                    config
                        .insert_json5(key, &value)
                        .map_err(StreamError::connect)?;
                }
                config
            }
        };
        zenoh::open(config)
            .await
            .map(|session| ZenohStream { session })
            .map_err(StreamError::connect)
    }

    pub fn publisher(&self) -> ZenohPublisher {
        ZenohPublisher {
            session: self.session.clone(),
            publisher: None,
        }
    }

    pub fn ack_publisher(&self) -> ZenohAckPublisher {
        ZenohAckPublisher {
            session: self.session.clone(),
            keys: vec![None; usize::from(MAX_NODE) + 1],
        }
    }

    pub async fn subscribe(&self, topic: Topic) -> Result<ZenohSubscription, StreamError> {
        self.session
            .declare_subscriber(topic.name())
            .await
            .map_err(StreamError::subscribe)
    }
}

impl ZenohPublisher {
    pub async fn publish(&mut self, payload: &mut Vec<u8>) -> Result<(), StreamError> {
        let publisher = match &mut self.publisher {
            Some(publisher) => publisher,
            None => self.publisher.insert(
                self.session
                    .declare_publisher(EVENTS_TOPIC)
                    .await
                    .map_err(StreamError::publish)?,
            ),
        };
        let result = publisher.put(std::mem::take(payload)).await;
        if result.is_err() {
            self.publisher = None;
        }
        result.map_err(StreamError::publish)
    }
}

impl ZenohAckPublisher {
    pub async fn publish(&mut self, origin: u16, payload: &mut Vec<u8>) -> Result<(), StreamError> {
        let slot = self
            .keys
            .get_mut(usize::from(origin))
            .ok_or_else(|| StreamError::publish("origin node out of range"))?;
        let key = match slot {
            Some(key) => key.clone(),
            None => slot
                .insert(KeyExpr::try_from(Topic::Ack(origin).name()).map_err(StreamError::publish)?)
                .clone(),
        };
        self.session
            .put(key, std::mem::take(payload))
            .await
            .map_err(StreamError::publish)
    }
}

pub async fn next<R>(
    subscription: &ZenohSubscription,
    receive: impl FnOnce(&[u8]) -> R,
) -> Option<R> {
    let sample = subscription.recv_async().await.ok()?;
    let payload = sample.payload();
    let mut slices = payload.slices();
    match (slices.next(), slices.next()) {
        (Some(slice), None) => Some(receive(slice)),
        _ => Some(receive(&payload.to_bytes())),
    }
}
