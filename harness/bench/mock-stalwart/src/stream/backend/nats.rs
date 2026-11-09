/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::stream::{EVENTS_TOPIC, StreamError, Topic, frame::MAX_NODE};
use async_nats::{Client, ConnectOptions, Subject, Subscriber, connection::State};
use bytes::Bytes;
use futures::StreamExt;
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const PING_INTERVAL: Duration = Duration::from_secs(60);
const CLIENT_CAPACITY: usize = 2048;
const SUBSCRIPTION_CAPACITY: usize = 65536;
const READ_BUFFER_CAPACITY: u16 = 65535;

#[derive(Debug, Clone)]
pub struct NatsTarget {
    pub hosts: Box<[Box<str>]>,
}

pub struct NatsStream {
    client: Client,
}

pub struct NatsPublisher {
    client: Client,
    subject: Subject,
}

pub struct NatsAckPublisher {
    client: Client,
    subjects: Vec<Option<Subject>>,
}

impl NatsStream {
    pub async fn connect(target: &NatsTarget) -> Result<NatsStream, StreamError> {
        let options = ConnectOptions::new()
            .max_reconnects(None)
            .connection_timeout(CONNECT_TIMEOUT)
            .request_timeout(Some(REQUEST_TIMEOUT))
            .ping_interval(PING_INTERVAL)
            .client_capacity(CLIENT_CAPACITY)
            .subscription_capacity(SUBSCRIPTION_CAPACITY)
            .read_buffer_capacity(READ_BUFFER_CAPACITY)
            .no_echo();
        let hosts = target
            .hosts
            .iter()
            .map(|host| host.to_string())
            .collect::<Vec<_>>();
        async_nats::connect_with_options(hosts, options)
            .await
            .map(|client| NatsStream { client })
            .map_err(StreamError::connect)
    }

    pub fn publisher(&self) -> NatsPublisher {
        NatsPublisher {
            client: self.client.clone(),
            subject: Subject::from_static(EVENTS_TOPIC),
        }
    }

    pub fn ack_publisher(&self) -> NatsAckPublisher {
        NatsAckPublisher {
            client: self.client.clone(),
            subjects: vec![None; usize::from(MAX_NODE) + 1],
        }
    }

    pub async fn subscribe(&self, topic: Topic) -> Result<Subscriber, StreamError> {
        self.client
            .subscribe(topic.name())
            .await
            .map_err(StreamError::subscribe)
    }
}

async fn send(client: &Client, subject: Subject, payload: &mut Vec<u8>) -> Result<(), StreamError> {
    if client.connection_state() != State::Connected {
        return Err(StreamError::NotConnected);
    }
    client
        .publish(subject, Bytes::from(std::mem::take(payload)))
        .await
        .map_err(StreamError::publish)
}

impl NatsPublisher {
    pub async fn publish(&mut self, payload: &mut Vec<u8>) -> Result<(), StreamError> {
        send(&self.client, self.subject.clone(), payload).await
    }
}

impl NatsAckPublisher {
    pub async fn publish(&mut self, origin: u16, payload: &mut Vec<u8>) -> Result<(), StreamError> {
        let subject = self
            .subjects
            .get_mut(usize::from(origin))
            .ok_or_else(|| StreamError::publish("origin node out of range"))?
            .get_or_insert_with(|| Subject::from(Topic::Ack(origin).name()))
            .clone();
        send(&self.client, subject, payload).await
    }
}

pub async fn next<R>(subscriber: &mut Subscriber, receive: impl FnOnce(&[u8]) -> R) -> Option<R> {
    let message = subscriber.next().await?;
    Some(receive(&message.payload))
}
