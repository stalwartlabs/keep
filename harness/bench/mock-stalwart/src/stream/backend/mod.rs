/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

#[cfg(feature = "fluvio")]
pub mod fluvio;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod keep;
pub mod loopback;
#[cfg(feature = "nats")]
pub mod nats;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "zenoh")]
pub mod zenoh;

use super::{StreamError, Topic};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum StreamTarget {
    Loopback,
    #[cfg(feature = "redis")]
    Redis(redis::RedisStreamTarget),
    #[cfg(feature = "nats")]
    Nats(nats::NatsTarget),
    #[cfg(feature = "kafka")]
    Kafka(kafka::KafkaTarget),
    #[cfg(feature = "zenoh")]
    Zenoh(zenoh::ZenohTarget),
    #[cfg(feature = "fluvio")]
    Fluvio(fluvio::FluvioTarget),
    Keep(Box<str>),
}

pub enum StreamBackend {
    Loopback(Arc<loopback::Hub>),
    #[cfg(feature = "redis")]
    Redis(redis::RedisStream),
    #[cfg(feature = "nats")]
    Nats(nats::NatsStream),
    #[cfg(feature = "kafka")]
    Kafka(kafka::KafkaStream),
    #[cfg(feature = "zenoh")]
    Zenoh(zenoh::ZenohStream),
    #[cfg(feature = "fluvio")]
    Fluvio(fluvio::FluvioStream),
    Keep(keep::KeepStream),
}

pub enum Publisher {
    Loopback(loopback::LoopbackPublisher),
    #[cfg(feature = "redis")]
    Redis(redis::RedisPublisher),
    #[cfg(feature = "nats")]
    Nats(nats::NatsPublisher),
    #[cfg(feature = "kafka")]
    Kafka(kafka::KafkaPublisher),
    #[cfg(feature = "zenoh")]
    Zenoh(zenoh::ZenohPublisher),
    #[cfg(feature = "fluvio")]
    Fluvio(fluvio::FluvioPublisher),
    Keep(keep::KeepPublisher),
}

pub enum AckPublisher {
    Loopback(loopback::LoopbackAckPublisher),
    #[cfg(feature = "redis")]
    Redis(redis::RedisAckPublisher),
    #[cfg(feature = "nats")]
    Nats(nats::NatsAckPublisher),
    #[cfg(feature = "kafka")]
    Kafka(kafka::KafkaAckPublisher),
    #[cfg(feature = "zenoh")]
    Zenoh(zenoh::ZenohAckPublisher),
    #[cfg(feature = "fluvio")]
    Fluvio(fluvio::FluvioAckPublisher),
    Keep(keep::KeepAckPublisher),
}

pub enum Subscription {
    Loopback(loopback::LoopbackSubscription),
    #[cfg(feature = "redis")]
    Redis(redis::RedisSubscription),
    #[cfg(feature = "nats")]
    Nats(async_nats::Subscriber),
    #[cfg(feature = "kafka")]
    Kafka(kafka::KafkaSubscription),
    #[cfg(feature = "zenoh")]
    Zenoh(zenoh::ZenohSubscription),
    #[cfg(feature = "fluvio")]
    Fluvio(fluvio::FluvioSubscription),
    Keep(keep::KeepSubscription),
}

#[derive(Clone, Copy, Debug)]
pub struct Identity {
    pub node: u16,
    pub incarnation: u16,
}

impl StreamBackend {
    pub async fn connect(
        target: &StreamTarget,
        identity: Identity,
        hub: &Arc<loopback::Hub>,
    ) -> Result<StreamBackend, StreamError> {
        let _ = identity;
        match target {
            StreamTarget::Loopback => Ok(StreamBackend::Loopback(hub.clone())),
            #[cfg(feature = "redis")]
            StreamTarget::Redis(target) => {
                redis::RedisStream::connect(target).map(StreamBackend::Redis)
            }
            #[cfg(feature = "nats")]
            StreamTarget::Nats(target) => nats::NatsStream::connect(target)
                .await
                .map(StreamBackend::Nats),
            #[cfg(feature = "kafka")]
            StreamTarget::Kafka(target) => kafka::KafkaStream::connect(target, identity)
                .await
                .map(StreamBackend::Kafka),
            #[cfg(feature = "zenoh")]
            StreamTarget::Zenoh(target) => zenoh::ZenohStream::connect(target)
                .await
                .map(StreamBackend::Zenoh),
            #[cfg(feature = "fluvio")]
            StreamTarget::Fluvio(target) => fluvio::FluvioStream::connect(target)
                .await
                .map(StreamBackend::Fluvio),
            StreamTarget::Keep(url) => keep::KeepStream::connect(url, identity.node)
                .await
                .map(StreamBackend::Keep),
        }
    }

    pub fn client_counters(&self, out: &mut Vec<(&'static str, u64)>) {
        #[cfg(not(feature = "keep"))]
        let _ = &out;
        match self {
            #[cfg(feature = "keep")]
            StreamBackend::Keep(keep) => keep.counters(out),
            _ => {}
        }
    }

    pub async fn publisher(&self) -> Result<Publisher, StreamError> {
        match self {
            StreamBackend::Loopback(hub) => Ok(Publisher::Loopback(hub.publisher(Topic::Events))),
            #[cfg(feature = "redis")]
            StreamBackend::Redis(redis) => Ok(Publisher::Redis(redis.publisher())),
            #[cfg(feature = "nats")]
            StreamBackend::Nats(nats) => Ok(Publisher::Nats(nats.publisher())),
            #[cfg(feature = "kafka")]
            StreamBackend::Kafka(kafka) => Ok(Publisher::Kafka(kafka.publisher())),
            #[cfg(feature = "zenoh")]
            StreamBackend::Zenoh(zenoh) => Ok(Publisher::Zenoh(zenoh.publisher())),
            #[cfg(feature = "fluvio")]
            StreamBackend::Fluvio(fluvio) => Ok(Publisher::Fluvio(fluvio.publisher())),
            StreamBackend::Keep(keep) => keep.publisher().map(Publisher::Keep),
        }
    }

    pub async fn ack_publisher(&self) -> Result<AckPublisher, StreamError> {
        match self {
            StreamBackend::Loopback(hub) => Ok(AckPublisher::Loopback(hub.ack_publisher())),
            #[cfg(feature = "redis")]
            StreamBackend::Redis(redis) => Ok(AckPublisher::Redis(redis.ack_publisher())),
            #[cfg(feature = "nats")]
            StreamBackend::Nats(nats) => Ok(AckPublisher::Nats(nats.ack_publisher())),
            #[cfg(feature = "kafka")]
            StreamBackend::Kafka(kafka) => Ok(AckPublisher::Kafka(kafka.ack_publisher())),
            #[cfg(feature = "zenoh")]
            StreamBackend::Zenoh(zenoh) => Ok(AckPublisher::Zenoh(zenoh.ack_publisher())),
            #[cfg(feature = "fluvio")]
            StreamBackend::Fluvio(fluvio) => Ok(AckPublisher::Fluvio(fluvio.ack_publisher())),
            StreamBackend::Keep(keep) => Ok(AckPublisher::Keep(keep.ack_publisher())),
        }
    }

    pub async fn subscribe(&self, topic: Topic) -> Result<Subscription, StreamError> {
        match self {
            StreamBackend::Loopback(hub) => Ok(Subscription::Loopback(hub.subscribe(topic))),
            #[cfg(feature = "redis")]
            StreamBackend::Redis(redis) => redis.subscribe(topic).await.map(Subscription::Redis),
            #[cfg(feature = "nats")]
            StreamBackend::Nats(nats) => nats.subscribe(topic).await.map(Subscription::Nats),
            #[cfg(feature = "kafka")]
            StreamBackend::Kafka(kafka) => kafka.subscribe(topic).await.map(Subscription::Kafka),
            #[cfg(feature = "zenoh")]
            StreamBackend::Zenoh(zenoh) => zenoh.subscribe(topic).await.map(Subscription::Zenoh),
            #[cfg(feature = "fluvio")]
            StreamBackend::Fluvio(fluvio) => {
                fluvio.subscribe(topic).await.map(Subscription::Fluvio)
            }
            StreamBackend::Keep(keep) => keep.subscribe(topic).await.map(Subscription::Keep),
        }
    }
}

impl Publisher {
    #[cfg_attr(
        not(any(feature = "nats", feature = "zenoh", feature = "fluvio")),
        allow(clippy::ptr_arg)
    )]
    pub async fn publish(&mut self, payload: &mut Vec<u8>) -> Result<(), StreamError> {
        match self {
            Publisher::Loopback(publisher) => publisher.publish(payload),
            #[cfg(feature = "redis")]
            Publisher::Redis(publisher) => publisher.publish(payload).await,
            #[cfg(feature = "nats")]
            Publisher::Nats(publisher) => publisher.publish(payload).await,
            #[cfg(feature = "kafka")]
            Publisher::Kafka(publisher) => publisher.publish(payload).await,
            #[cfg(feature = "zenoh")]
            Publisher::Zenoh(publisher) => publisher.publish(payload).await,
            #[cfg(feature = "fluvio")]
            Publisher::Fluvio(publisher) => publisher.publish(payload).await,
            Publisher::Keep(publisher) => publisher.publish(payload).await,
        }
    }
}

impl AckPublisher {
    #[cfg_attr(
        not(any(feature = "nats", feature = "zenoh", feature = "fluvio")),
        allow(clippy::ptr_arg)
    )]
    pub async fn publish(&mut self, origin: u16, payload: &mut Vec<u8>) -> Result<(), StreamError> {
        match self {
            AckPublisher::Loopback(publisher) => publisher.publish(origin, payload),
            #[cfg(feature = "redis")]
            AckPublisher::Redis(publisher) => publisher.publish(origin, payload).await,
            #[cfg(feature = "nats")]
            AckPublisher::Nats(publisher) => publisher.publish(origin, payload).await,
            #[cfg(feature = "kafka")]
            AckPublisher::Kafka(publisher) => publisher.publish(origin, payload).await,
            #[cfg(feature = "zenoh")]
            AckPublisher::Zenoh(publisher) => publisher.publish(origin, payload).await,
            #[cfg(feature = "fluvio")]
            AckPublisher::Fluvio(publisher) => publisher.publish(origin, payload).await,
            AckPublisher::Keep(publisher) => publisher.publish(origin, payload).await,
        }
    }
}

impl Subscription {
    pub async fn next<R>(&mut self, receive: impl FnOnce(&[u8]) -> R) -> Option<R> {
        match self {
            Subscription::Loopback(subscription) => subscription.next(receive).await,
            #[cfg(feature = "redis")]
            Subscription::Redis(subscription) => subscription.next(receive).await,
            #[cfg(feature = "nats")]
            Subscription::Nats(subscriber) => nats::next(subscriber, receive).await,
            #[cfg(feature = "kafka")]
            Subscription::Kafka(subscription) => subscription.next(receive).await,
            #[cfg(feature = "zenoh")]
            Subscription::Zenoh(subscription) => zenoh::next(subscription, receive).await,
            #[cfg(feature = "fluvio")]
            Subscription::Fluvio(subscription) => subscription.next(receive).await,
            Subscription::Keep(subscription) => subscription.next(receive).await,
        }
    }
}
