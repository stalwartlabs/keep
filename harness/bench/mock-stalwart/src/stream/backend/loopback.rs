/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::stream::{StreamError, Topic, frame::MAX_NODE};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
};
use tokio::sync::broadcast::{self, error::RecvError};

const CAPACITY: usize = 1 << 16;

type Payload = Arc<[u8]>;

#[derive(Default)]
pub struct Hub {
    topics: Mutex<HashMap<String, broadcast::Sender<Payload>>>,
}

pub struct LoopbackPublisher {
    sender: broadcast::Sender<Payload>,
}

pub struct LoopbackAckPublisher {
    hub: Arc<Hub>,
    senders: Vec<Option<broadcast::Sender<Payload>>>,
}

pub struct LoopbackSubscription {
    receiver: broadcast::Receiver<Payload>,
}

impl Hub {
    fn sender(&self, topic: Topic) -> broadcast::Sender<Payload> {
        self.topics
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(topic.name())
            .or_insert_with(|| broadcast::channel(CAPACITY).0)
            .clone()
    }

    pub fn publisher(&self, topic: Topic) -> LoopbackPublisher {
        LoopbackPublisher {
            sender: self.sender(topic),
        }
    }

    pub fn ack_publisher(self: &Arc<Self>) -> LoopbackAckPublisher {
        LoopbackAckPublisher {
            hub: self.clone(),
            senders: vec![None; usize::from(MAX_NODE) + 1],
        }
    }

    pub fn subscribe(&self, topic: Topic) -> LoopbackSubscription {
        LoopbackSubscription {
            receiver: self.sender(topic).subscribe(),
        }
    }
}

impl LoopbackPublisher {
    pub fn publish(&mut self, payload: &[u8]) -> Result<(), StreamError> {
        let _ = self.sender.send(Arc::from(payload));
        Ok(())
    }
}

impl LoopbackAckPublisher {
    pub fn publish(&mut self, origin: u16, payload: &[u8]) -> Result<(), StreamError> {
        let slot = self
            .senders
            .get_mut(usize::from(origin))
            .ok_or_else(|| StreamError::publish("origin node out of range"))?;
        let sender = slot.get_or_insert_with(|| self.hub.sender(Topic::Ack(origin)));
        let _ = sender.send(Arc::from(payload));
        Ok(())
    }
}

impl LoopbackSubscription {
    pub async fn next<R>(&mut self, receive: impl FnOnce(&[u8]) -> R) -> Option<R> {
        loop {
            match self.receiver.recv().await {
                Ok(payload) => return Some(receive(&payload)),
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    }
}
