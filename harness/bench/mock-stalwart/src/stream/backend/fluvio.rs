/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::stream::{StreamError, Topic};
use async_trait::async_trait;
use fluvio::{
    Fluvio, FluvioClusterConfig, Isolation, Offset, RecordKey, TopicProducerConfigBuilder,
    TopicProducerPool,
    consumer::{ConsumerConfigExtBuilder, ConsumerStream, Record},
    metadata::topic::TopicSpec,
};
use fluvio_future::net::{
    AsConnectionFd, BoxReadConnection, BoxWriteConnection, ConnectionFd, DomainConnector,
    TcpDomainConnector, TcpStream,
    tcp_stream::{SocketOpts, stream_with_opts},
};
use futures::{AsyncRead, StreamExt};
use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

pub const EVENTS: &str = "stwt-agora";
pub const ACKS: &str = "stwt-ack";
const READY_WAIT: Duration = Duration::from_secs(30);
const READY_POLL: Duration = Duration::from_millis(200);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FluvioSocket {
    Stock,
    LowLatency,
}

#[derive(Debug, Clone)]
pub struct FluvioTarget {
    pub endpoint: Box<str>,
    pub ack_partitions: u32,
    pub socket: FluvioSocket,
}

pub struct FluvioStream {
    client: Arc<Fluvio>,
    target: FluvioTarget,
    events: Arc<TopicProducerPool>,
}

pub struct FluvioPublisher {
    producer: Arc<TopicProducerPool>,
}

pub struct FluvioAckPublisher {
    client: Arc<Fluvio>,
    partitions: u32,
    producers: Vec<Option<TopicProducerPool>>,
}

type RecordStream =
    Box<dyn ConsumerStream<Item = Result<Record, fluvio::dataplane::link::ErrorCode>> + Send>;

pub struct FluvioSubscription {
    records: RecordStream,
}

#[derive(Clone)]
struct LowLatency;

struct QuickAck(TcpStream);

impl AsyncRead for QuickAck {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let read = Pin::new(&mut self.0).poll_read(cx, buf);
        #[cfg(target_os = "linux")]
        let _ = socket2::SockRef::from(&self.0).set_tcp_quickack(true);
        read
    }
}

#[async_trait]
impl TcpDomainConnector for LowLatency {
    async fn connect(
        &self,
        addr: &str,
    ) -> io::Result<(BoxWriteConnection, BoxReadConnection, ConnectionFd)> {
        let options = SocketOpts {
            nodelay: Some(true),
            ..SocketOpts::default()
        };
        let stream = stream_with_opts(addr, Some(options)).await?;
        let fd = stream.as_connection_fd();
        Ok((Box::new(stream.clone()), Box::new(QuickAck(stream)), fd))
    }

    fn new_domain(&self, _domain: String) -> DomainConnector {
        Box::new(self.clone())
    }

    fn domain(&self) -> &str {
        "localhost"
    }

    fn clone_box(&self) -> DomainConnector {
        Box::new(self.clone())
    }
}

impl FluvioStream {
    pub async fn connect(target: &FluvioTarget) -> Result<FluvioStream, StreamError> {
        let config = FluvioClusterConfig::new(target.endpoint.as_ref());
        let client = match target.socket {
            FluvioSocket::Stock => Fluvio::connect_with_config(&config).await,
            FluvioSocket::LowLatency => {
                Fluvio::connect_with_connector(Box::new(LowLatency), &config).await
            }
        }
        .map_err(StreamError::connect)?;
        create_topics(&client, target.ack_partitions).await?;
        let events = Arc::new(ready_producer(&client, EVENTS, 0).await?);
        Ok(FluvioStream {
            client: Arc::new(client),
            target: target.clone(),
            events,
        })
    }

    pub fn publisher(&self) -> FluvioPublisher {
        FluvioPublisher {
            producer: self.events.clone(),
        }
    }

    pub fn ack_publisher(&self) -> FluvioAckPublisher {
        FluvioAckPublisher {
            client: self.client.clone(),
            partitions: self.target.ack_partitions,
            producers: (0..self.target.ack_partitions).map(|_| None).collect(),
        }
    }

    pub async fn subscribe(&self, topic: Topic) -> Result<FluvioSubscription, StreamError> {
        let (name, partition) = match topic {
            Topic::Events => (EVENTS, 0),
            Topic::Ack(node) => (ACKS, u32::from(node) % self.target.ack_partitions),
        };
        let config = ConsumerConfigExtBuilder::default()
            .topic(name)
            .partition(partition)
            .offset_start(Offset::end())
            .isolation(Isolation::ReadUncommitted)
            .build()
            .map_err(StreamError::subscribe)?;
        let deadline = Instant::now() + READY_WAIT;
        loop {
            match self.client.consumer_with_config(config.clone()).await {
                Ok(records) => {
                    return Ok(FluvioSubscription {
                        records: Box::new(records),
                    });
                }
                Err(err) if Instant::now() > deadline => {
                    return Err(StreamError::subscribe(err));
                }
                Err(_) => tokio::time::sleep(READY_POLL).await,
            }
        }
    }
}

async fn create_topics(client: &Fluvio, ack_partitions: u32) -> Result<(), StreamError> {
    let admin = client.admin().await;
    for (name, partitions) in [(EVENTS, 1), (ACKS, ack_partitions)] {
        if let Err(err) = admin
            .create(
                name.to_string(),
                false,
                TopicSpec::new_computed(partitions, 1, None),
            )
            .await
        {
            let exists = admin
                .list::<TopicSpec, _>(vec![name.to_string()])
                .await
                .is_ok_and(|topics| !topics.is_empty());
            if !exists {
                return Err(StreamError::connect(format!("create topic {name}: {err}")));
            }
        }
    }
    Ok(())
}

async fn ready_producer(
    client: &Fluvio,
    topic: &str,
    partition: u32,
) -> Result<TopicProducerPool, StreamError> {
    let config = TopicProducerConfigBuilder::default()
        .linger(Duration::ZERO)
        .set_specific_partitioner(partition)
        .isolation(Isolation::ReadUncommitted)
        .build()
        .map_err(StreamError::connect)?;
    let deadline = Instant::now() + READY_WAIT;
    loop {
        match client
            .topic_producer_with_config(topic, config.clone())
            .await
        {
            Ok(producer) => return Ok(producer),
            Err(err) if Instant::now() > deadline => return Err(StreamError::connect(err)),
            Err(_) => tokio::time::sleep(READY_POLL).await,
        }
    }
}

async fn send(producer: &TopicProducerPool, payload: &mut Vec<u8>) -> Result<(), StreamError> {
    producer
        .send(RecordKey::NULL, std::mem::take(payload))
        .await
        .map_err(StreamError::publish)?
        .wait()
        .await
        .map(|_| ())
        .map_err(StreamError::publish)
}

impl FluvioPublisher {
    pub async fn publish(&mut self, payload: &mut Vec<u8>) -> Result<(), StreamError> {
        send(&self.producer, payload).await
    }
}

impl FluvioAckPublisher {
    pub async fn publish(&mut self, origin: u16, payload: &mut Vec<u8>) -> Result<(), StreamError> {
        let partition = u32::from(origin) % self.partitions;
        let slot = self
            .producers
            .get_mut(usize::try_from(partition).unwrap_or(usize::MAX))
            .ok_or_else(|| StreamError::publish("ack partition out of range"))?;
        if slot.is_none() {
            *slot = Some(ready_producer(&self.client, ACKS, partition).await?);
        }
        match slot {
            Some(producer) => send(producer, payload).await,
            None => Err(StreamError::publish("ack producer missing")),
        }
    }
}

impl FluvioSubscription {
    pub async fn next<R>(&mut self, receive: impl FnOnce(&[u8]) -> R) -> Option<R> {
        let record = self.records.next().await?.ok()?;
        Some(receive(record.value()))
    }
}
