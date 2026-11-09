/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::stream::{EVENTS_TOPIC, StreamError, Topic, frame::MAX_NODE};
use futures::StreamExt;
use redis::{
    AsyncCommands, AsyncConnectionConfig, Client, IntoConnectionInfo, Msg, ProtocolVersion,
    PushInfo, PushKind,
    aio::{MultiplexedConnection, PubSubStream},
};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PubMode {
    Publish,
    Sharded,
}

#[derive(Debug, Clone)]
pub struct RedisStreamTarget {
    pub url: Box<str>,
    pub mode: PubMode,
}

pub struct RedisStream {
    client: Client,
    resp3: Client,
    mode: PubMode,
}

pub struct RedisPublisher {
    client: Client,
    mode: PubMode,
    connection: Option<MultiplexedConnection>,
}

pub struct RedisAckPublisher {
    client: Client,
    mode: PubMode,
    connection: Option<MultiplexedConnection>,
    topics: Vec<Option<String>>,
}

pub enum RedisSubscription {
    PubSub(PubSubStream),
    Push {
        _connection: MultiplexedConnection,
        rx: UnboundedReceiver<PushInfo>,
    },
}

impl RedisStream {
    pub fn connect(target: &RedisStreamTarget) -> Result<RedisStream, StreamError> {
        let client = Client::open(target.url.as_ref()).map_err(StreamError::connect)?;
        let info = target
            .url
            .as_ref()
            .into_connection_info()
            .map_err(StreamError::connect)?;
        let settings = info
            .redis_settings()
            .clone()
            .set_protocol(ProtocolVersion::RESP3);
        let resp3 =
            Client::open(info.set_redis_settings(settings)).map_err(StreamError::connect)?;
        Ok(RedisStream {
            client,
            resp3,
            mode: target.mode,
        })
    }

    pub fn publisher(&self) -> RedisPublisher {
        RedisPublisher {
            client: self.client.clone(),
            mode: self.mode,
            connection: None,
        }
    }

    pub fn ack_publisher(&self) -> RedisAckPublisher {
        RedisAckPublisher {
            client: self.client.clone(),
            mode: self.mode,
            connection: None,
            topics: vec![None; usize::from(MAX_NODE) + 1],
        }
    }

    pub async fn subscribe(&self, topic: Topic) -> Result<RedisSubscription, StreamError> {
        let name = topic.name();
        match self.mode {
            PubMode::Publish => {
                let mut pubsub = self
                    .client
                    .get_async_pubsub()
                    .await
                    .map_err(StreamError::subscribe)?;
                pubsub
                    .subscribe(&name)
                    .await
                    .map_err(StreamError::subscribe)?;
                Ok(RedisSubscription::PubSub(pubsub.into_on_message()))
            }
            PubMode::Sharded => {
                let (tx, rx) = unbounded_channel();
                let config = AsyncConnectionConfig::new().set_push_sender(tx);
                let mut connection = self
                    .resp3
                    .get_multiplexed_async_connection_with_config(&config)
                    .await
                    .map_err(StreamError::subscribe)?;
                redis::cmd("SSUBSCRIBE")
                    .arg(&name)
                    .exec_async(&mut connection)
                    .await
                    .map_err(StreamError::subscribe)?;
                Ok(RedisSubscription::Push {
                    _connection: connection,
                    rx,
                })
            }
        }
    }
}

async fn send(
    client: &Client,
    slot: &mut Option<MultiplexedConnection>,
    mode: PubMode,
    topic: &str,
    payload: &[u8],
) -> Result<(), StreamError> {
    let connection = match slot {
        Some(connection) => connection,
        None => slot.insert(
            client
                .get_multiplexed_async_connection()
                .await
                .map_err(StreamError::connect)?,
        ),
    };
    let result = match mode {
        PubMode::Publish => connection.publish::<_, _, ()>(topic, payload).await,
        PubMode::Sharded => connection.spublish::<_, _, ()>(topic, payload).await,
    };
    if result.is_err() {
        *slot = None;
    }
    result.map_err(StreamError::publish)
}

impl RedisPublisher {
    pub async fn publish(&mut self, payload: &[u8]) -> Result<(), StreamError> {
        send(
            &self.client,
            &mut self.connection,
            self.mode,
            EVENTS_TOPIC,
            payload,
        )
        .await
    }
}

impl RedisAckPublisher {
    pub async fn publish(&mut self, origin: u16, payload: &[u8]) -> Result<(), StreamError> {
        let topic = self
            .topics
            .get_mut(usize::from(origin))
            .ok_or_else(|| StreamError::publish("origin node out of range"))?
            .get_or_insert_with(|| Topic::Ack(origin).name());
        send(
            &self.client,
            &mut self.connection,
            self.mode,
            topic,
            payload,
        )
        .await
    }
}

impl RedisSubscription {
    pub async fn next<R>(&mut self, receive: impl FnOnce(&[u8]) -> R) -> Option<R> {
        match self {
            RedisSubscription::PubSub(stream) => {
                let message = stream.next().await?;
                Some(receive(message.get_payload_bytes()))
            }
            RedisSubscription::Push { rx, .. } => loop {
                let info = rx.recv().await?;
                if info.kind == PushKind::Disconnection {
                    return None;
                }
                if let Some(message) = Msg::from_push_info(info) {
                    return Some(receive(message.get_payload_bytes()));
                }
            },
        }
    }
}
