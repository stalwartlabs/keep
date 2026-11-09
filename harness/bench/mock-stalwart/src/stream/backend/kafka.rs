/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::Identity;
use crate::stream::{EVENTS_TOPIC, StreamError, Topic};
use rdkafka::{
    ClientConfig, Message, Offset, TopicPartitionList,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    error::RDKafkaErrorCode,
    producer::{FutureProducer, FutureRecord, Producer},
};
use std::time::{Duration, Instant};

pub const ACK_TOPIC: &str = "stwt.ack";
const MESSAGE_TIMEOUT_MS: &str = "5000";

const METADATA_TIMEOUT: Duration = Duration::from_secs(10);
const LEADER_WAIT: Duration = Duration::from_secs(30);
const LEADER_POLL: Duration = Duration::from_millis(100);
const STALWART_GROUP: &str = "stalwart";

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
pub enum KafkaConsumer {
    Assign,
    Group,
    Shared,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
pub enum KafkaAcks {
    One,
    All,
    Default,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
pub enum Nagle {
    On,
    Off,
}

#[derive(Debug, Clone)]
pub struct KafkaTarget {
    pub brokers: Box<str>,
    pub partitions: i32,
    pub ack_partitions: i32,
    pub consumer: KafkaConsumer,
    pub acks: KafkaAcks,
    pub nagle: Nagle,
    pub session_timeout_ms: u32,
}

pub struct KafkaStream {
    target: KafkaTarget,
    identity: Identity,
    base: ClientConfig,
    producer: FutureProducer,
    ack_producer: FutureProducer,
}

pub struct KafkaPublisher {
    producer: FutureProducer,
    key: [u8; 2],
}

pub struct KafkaAckPublisher {
    producer: FutureProducer,
    partitions: i32,
}

pub struct KafkaSubscription {
    consumer: StreamConsumer,
}

impl KafkaStream {
    pub async fn connect(
        target: &KafkaTarget,
        identity: Identity,
    ) -> Result<KafkaStream, StreamError> {
        let mut base = ClientConfig::new();
        base.set("bootstrap.servers", target.brokers.as_ref());
        if target.nagle == Nagle::Off {
            base.set("socket.nagle.disable", "true");
        }
        create_topics(&base, target).await?;
        let mut producer = base.clone();
        producer
            .set("message.timeout.ms", MESSAGE_TIMEOUT_MS)
            .set("linger.ms", "0");
        match target.acks {
            KafkaAcks::One => {
                producer.set("acks", "1");
            }
            KafkaAcks::All => {
                producer.set("acks", "all");
            }
            KafkaAcks::Default => {}
        }
        let ack_producer: FutureProducer = producer.create().map_err(StreamError::connect)?;
        let producer: FutureProducer = producer.create().map_err(StreamError::connect)?;
        await_leaders(&producer).await?;
        Ok(KafkaStream {
            ack_producer,
            producer,
            target: target.clone(),
            identity,
            base,
        })
    }

    pub fn publisher(&self) -> KafkaPublisher {
        KafkaPublisher {
            producer: self.producer.clone(),
            key: self.identity.node.to_be_bytes(),
        }
    }

    pub fn ack_publisher(&self) -> KafkaAckPublisher {
        KafkaAckPublisher {
            producer: self.ack_producer.clone(),
            partitions: self.target.ack_partitions,
        }
    }

    pub async fn subscribe(&self, topic: Topic) -> Result<KafkaSubscription, StreamError> {
        let mut config = self.base.clone();
        config
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "latest");
        let private_group = format!("ms-{}-{}", self.identity.node, self.identity.incarnation);
        let consumer = match (topic, self.target.consumer) {
            (Topic::Events, KafkaConsumer::Shared) => {
                let consumer: StreamConsumer = config
                    .set("group.id", STALWART_GROUP)
                    .set("enable.auto.commit", "true")
                    .set("auto.commit.interval.ms", "100")
                    .set(
                        "session.timeout.ms",
                        self.target.session_timeout_ms.to_string(),
                    )
                    .create()
                    .map_err(StreamError::subscribe)?;
                consumer
                    .subscribe(&[EVENTS_TOPIC])
                    .map_err(StreamError::subscribe)?;
                consumer
            }
            (Topic::Events, KafkaConsumer::Group) => {
                let consumer: StreamConsumer = config
                    .set("group.id", &private_group)
                    .set("enable.auto.commit", "false")
                    .set(
                        "session.timeout.ms",
                        self.target.session_timeout_ms.to_string(),
                    )
                    .create()
                    .map_err(StreamError::subscribe)?;
                consumer
                    .subscribe(&[EVENTS_TOPIC])
                    .map_err(StreamError::subscribe)?;
                consumer
            }
            (Topic::Events, KafkaConsumer::Assign) => {
                let consumer = private_consumer(&mut config, &private_group)?;
                let metadata = consumer
                    .fetch_metadata(Some(EVENTS_TOPIC), METADATA_TIMEOUT)
                    .map_err(StreamError::subscribe)?;
                let mut assignment = TopicPartitionList::new();
                for partition in metadata
                    .topics()
                    .iter()
                    .flat_map(|topic| topic.partitions())
                {
                    assignment
                        .add_partition_offset(EVENTS_TOPIC, partition.id(), Offset::End)
                        .map_err(StreamError::subscribe)?;
                }
                if assignment.count() == 0 {
                    return Err(StreamError::subscribe("events topic has no partitions"));
                }
                consumer
                    .assign(&assignment)
                    .map_err(StreamError::subscribe)?;
                consumer
            }
            (Topic::Ack(node), _) => {
                let consumer = private_consumer(&mut config, &private_group)?;
                let mut assignment = TopicPartitionList::new();
                assignment
                    .add_partition_offset(
                        ACK_TOPIC,
                        i32::from(node) % self.target.ack_partitions,
                        Offset::End,
                    )
                    .map_err(StreamError::subscribe)?;
                consumer
                    .assign(&assignment)
                    .map_err(StreamError::subscribe)?;
                consumer
            }
        };
        Ok(KafkaSubscription { consumer })
    }
}

fn private_consumer(config: &mut ClientConfig, group: &str) -> Result<StreamConsumer, StreamError> {
    config
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .create()
        .map_err(StreamError::subscribe)
}

async fn create_topics(base: &ClientConfig, target: &KafkaTarget) -> Result<(), StreamError> {
    let admin: AdminClient<DefaultClientContext> = base.create().map_err(StreamError::connect)?;
    let topics = [
        NewTopic::new(EVENTS_TOPIC, target.partitions, TopicReplication::Fixed(1)),
        NewTopic::new(ACK_TOPIC, target.ack_partitions, TopicReplication::Fixed(1)),
    ];
    let results = admin
        .create_topics(&topics, &AdminOptions::new())
        .await
        .map_err(StreamError::connect)?;
    for result in results {
        match result {
            Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((topic, code)) => {
                return Err(StreamError::connect(format!(
                    "create topic {topic}: {code}"
                )));
            }
        }
    }
    Ok(())
}

async fn await_leaders(producer: &FutureProducer) -> Result<(), StreamError> {
    let producer = producer.clone();
    tokio::task::spawn_blocking(move || {
        let deadline = Instant::now() + LEADER_WAIT;
        loop {
            let ready = [EVENTS_TOPIC, ACK_TOPIC].iter().all(|topic| {
                producer
                    .client()
                    .fetch_metadata(Some(topic), METADATA_TIMEOUT)
                    .is_ok_and(|metadata| {
                        metadata.topics().iter().all(|topic| {
                            topic.error().is_none()
                                && !topic.partitions().is_empty()
                                && topic
                                    .partitions()
                                    .iter()
                                    .all(|partition| partition.leader() >= 0)
                        })
                    })
            });
            if ready {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(StreamError::connect("kafka partitions have no leader"));
            }
            std::thread::sleep(LEADER_POLL);
        }
    })
    .await
    .map_err(StreamError::connect)?
}

impl KafkaPublisher {
    pub async fn publish(&mut self, payload: &[u8]) -> Result<(), StreamError> {
        self.producer
            .send(
                FutureRecord::to(EVENTS_TOPIC)
                    .key(&self.key[..])
                    .payload(payload),
                Duration::ZERO,
            )
            .await
            .map(|_| ())
            .map_err(|(err, _)| StreamError::publish(err))
    }
}

impl KafkaAckPublisher {
    pub async fn publish(&mut self, origin: u16, payload: &[u8]) -> Result<(), StreamError> {
        self.producer
            .send(
                FutureRecord::<(), [u8]>::to(ACK_TOPIC)
                    .partition(i32::from(origin) % self.partitions)
                    .payload(payload),
                Duration::ZERO,
            )
            .await
            .map(|_| ())
            .map_err(|(err, _)| StreamError::publish(err))
    }
}

impl KafkaSubscription {
    pub async fn next<R>(&mut self, receive: impl FnOnce(&[u8]) -> R) -> Option<R> {
        let message = self.consumer.recv().await.ok()?;
        Some(receive(message.payload().unwrap_or_default()))
    }
}
