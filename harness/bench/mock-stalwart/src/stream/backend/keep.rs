/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::stream::{StreamError, Topic};

#[cfg(feature = "keep")]
pub use linked::{KeepAckPublisher, KeepPublisher, KeepStream, KeepSubscription};
#[cfg(not(feature = "keep"))]
pub use seam::{KeepAckPublisher, KeepPublisher, KeepStream, KeepSubscription};

#[cfg(feature = "keep")]
mod linked {
    use std::borrow::Cow;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use keep_client::{Client, Delivery, Subscription, Topic as KeepTopic};

    use super::{StreamError, Topic};

    const EVENTS_TOPIC: u16 = 1;
    const ACK_TOPIC_BASE: u16 = 2;

    fn topic_id(topic: Topic) -> Result<KeepTopic, StreamError> {
        match topic {
            Topic::Events => Ok(KeepTopic::from(EVENTS_TOPIC)),
            Topic::Ack(node) => ACK_TOPIC_BASE.checked_add(node).map(KeepTopic::from).ok_or(
                StreamError::Unsupported("ack topic out of the Keep topic range"),
            ),
        }
    }

    #[derive(Default)]
    struct Deliveries {
        gaps: AtomicU64,
        missed: AtomicU64,
        lost: AtomicU64,
        resubscribed: AtomicU64,
        ended: AtomicU64,
    }

    pub struct KeepStream {
        client: Client,
        deliveries: Arc<Deliveries>,
    }

    pub struct KeepPublisher {
        client: Client,
        topic: KeepTopic,
    }

    pub struct KeepAckPublisher {
        client: Client,
    }

    pub struct KeepSubscription {
        inner: Subscription,
        deliveries: Arc<Deliveries>,
    }

    impl KeepStream {
        pub async fn connect(url: &str, node: u16) -> Result<KeepStream, StreamError> {
            Client::connect_url(&url_with_node(url, node))
                .await
                .map(|client| KeepStream {
                    client,
                    deliveries: Arc::default(),
                })
                .map_err(StreamError::connect)
        }

        pub fn publisher(&self) -> Result<KeepPublisher, StreamError> {
            Ok(KeepPublisher {
                client: self.client.clone(),
                topic: topic_id(Topic::Events)?,
            })
        }

        pub fn ack_publisher(&self) -> KeepAckPublisher {
            KeepAckPublisher {
                client: self.client.clone(),
            }
        }

        pub async fn subscribe(&self, topic: Topic) -> Result<KeepSubscription, StreamError> {
            self.client
                .subscribe(topic_id(topic)?)
                .await
                .map(|inner| KeepSubscription {
                    inner,
                    deliveries: self.deliveries.clone(),
                })
                .map_err(StreamError::subscribe)
        }

        pub fn counters(&self, out: &mut Vec<(&'static str, u64)>) {
            out.extend(self.client.stats().fields());
            let deliveries = &self.deliveries;
            out.extend([
                ("subscription_gaps", deliveries.gaps.load(Ordering::Relaxed)),
                (
                    "subscription_missed",
                    deliveries.missed.load(Ordering::Relaxed),
                ),
                ("subscription_lost", deliveries.lost.load(Ordering::Relaxed)),
                (
                    "subscription_resubscribed",
                    deliveries.resubscribed.load(Ordering::Relaxed),
                ),
                (
                    "subscription_ended",
                    deliveries.ended.load(Ordering::Relaxed),
                ),
            ]);
        }
    }

    impl KeepPublisher {
        pub async fn publish(&mut self, payload: &[u8]) -> Result<(), StreamError> {
            self.client
                .publish(self.topic, payload)
                .await
                .map_err(StreamError::publish)
        }
    }

    impl KeepAckPublisher {
        pub async fn publish(&mut self, origin: u16, payload: &[u8]) -> Result<(), StreamError> {
            self.client
                .publish(topic_id(Topic::Ack(origin))?, payload)
                .await
                .map_err(StreamError::publish)
        }
    }

    impl KeepSubscription {
        pub async fn next<R>(&mut self, receive: impl FnOnce(&[u8]) -> R) -> Option<R> {
            let KeepSubscription { inner, deliveries } = self;
            loop {
                match inner.next().await? {
                    Delivery::Event { payload, .. } => return Some(receive(payload)),
                    Delivery::Gap { missed, .. } => {
                        deliveries.gaps.fetch_add(1, Ordering::Relaxed);
                        deliveries
                            .missed
                            .fetch_add(u64::from(missed), Ordering::Relaxed);
                    }
                    Delivery::Lost { .. } => {
                        deliveries.lost.fetch_add(1, Ordering::Relaxed);
                    }
                    Delivery::Resubscribed { .. } => {
                        deliveries.resubscribed.fetch_add(1, Ordering::Relaxed);
                    }
                    Delivery::Ended { .. } => {
                        deliveries.ended.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    fn url_with_node(url: &str, node: u16) -> Cow<'_, str> {
        let Some((_, query)) = url.split_once('?') else {
            return Cow::Borrowed(url);
        };
        let (mut feeds, mut named) = (false, false);
        for name in query
            .split('&')
            .map(|param| param.split_once('=').map_or(param, |(name, _)| name))
        {
            feeds |= name == "feeds";
            named |= name == "node";
        }
        if feeds && !named {
            Cow::Owned(format!("{url}&node={node}"))
        } else {
            Cow::Borrowed(url)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::url_with_node;

        #[test]
        fn node_is_appended_only_to_placed_feeds() {
            assert_eq!(
                url_with_node("keep://h:7000?secret=S&conns=1&feeds=spread", 7),
                "keep://h:7000?secret=S&conns=1&feeds=spread&node=7"
            );
            assert_eq!(
                url_with_node("keep://h:7000?feeds=2&node=3", 7),
                "keep://h:7000?feeds=2&node=3"
            );
            assert_eq!(
                url_with_node("keep://h:7000?secret=S&conns=1", 7),
                "keep://h:7000?secret=S&conns=1"
            );
            assert_eq!(url_with_node("keep://h:7000", 7), "keep://h:7000");
            assert_eq!(
                url_with_node("keep://h:7000?nodes=1&feedsx=2", 7),
                "keep://h:7000?nodes=1&feedsx=2"
            );
        }
    }
}

#[cfg(not(feature = "keep"))]
mod seam {
    use super::{StreamError, Topic};

    pub enum KeepStream {}

    pub enum KeepPublisher {}

    pub enum KeepAckPublisher {}

    pub enum KeepSubscription {}

    impl KeepStream {
        pub async fn connect(_url: &str, _node: u16) -> Result<KeepStream, StreamError> {
            Err(StreamError::Unsupported(
                "no Keep client in this build (build with --features keep)",
            ))
        }

        pub fn publisher(&self) -> Result<KeepPublisher, StreamError> {
            match *self {}
        }

        pub fn ack_publisher(&self) -> KeepAckPublisher {
            match *self {}
        }

        pub async fn subscribe(&self, _topic: Topic) -> Result<KeepSubscription, StreamError> {
            match *self {}
        }
    }

    impl KeepPublisher {
        pub async fn publish(&mut self, _payload: &[u8]) -> Result<(), StreamError> {
            match *self {}
        }
    }

    impl KeepAckPublisher {
        pub async fn publish(&mut self, _origin: u16, _payload: &[u8]) -> Result<(), StreamError> {
            match *self {}
        }
    }

    impl KeepSubscription {
        pub async fn next<R>(&mut self, _receive: impl FnOnce(&[u8]) -> R) -> Option<R> {
            match *self {}
        }
    }
}
