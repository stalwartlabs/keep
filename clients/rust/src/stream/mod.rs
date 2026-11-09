/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

mod inbox;
mod manager;
mod subscription;

use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use keep_proto::{Command, Topic, Value};
use tokio::time::timeout_at;

pub(crate) use inbox::{Kind, Shared};
pub(crate) use manager::{Source, Streams};
pub use subscription::{Delivery, Received, Subscription};

use crate::answer::Expect;
use crate::client::Client;
use crate::error::Error;

pub(crate) const LINGER: Duration = Duration::from_secs(1);

pub(crate) const RETRY: Duration = Duration::from_millis(250);

impl Client {
    /// Subscribes to `topic` on every server the stream mode selects. Returns once every server
    /// took it or one attempt timeout passed; later servers report `Delivery::Resubscribed`.
    pub async fn subscribe(&self, topic: Topic) -> Result<Subscription, Error> {
        let streams = &self.inner.streams;
        let shared = streams.register(topic)?;
        let subscription = Subscription::new(Arc::clone(&shared));
        let deadline = Instant::now() + self.inner.attempt_timeout;
        loop {
            let mut changed = pin!(shared.changed.notified());
            changed.as_mut().enable();
            let mut servers = streams.servers(topic);
            servers.sort_unstable();
            if shared.live_on(&servers) {
                break;
            }
            if timeout_at(deadline.into(), changed).await.is_err() {
                break;
            }
        }
        if shared.start() == 0 {
            return Err(Error::Unavailable);
        }
        Ok(subscription)
    }

    /// Publishes `payload` to `topic` without waiting for a reply: `Ok` means queued in order
    /// on the stream connection. Fails, not executed, when 16 MiB of unsent data is queued.
    pub async fn publish(&self, topic: Topic, payload: &[u8]) -> Result<(), Error> {
        let payload = Value::new(payload).ok_or(Error::ValueTooLarge(payload.len()))?;
        let deadline = Instant::now() + self.inner.connect_timeout.min(self.inner.deadline);
        loop {
            let (server, conn) = self.inner.streams.publisher(topic, deadline).await?;
            let sent = self.probe.stamp_at(server);
            let result = conn.submit_quiet(&Command::Publish { topic, payload });
            sent.finish(result.as_ref().err());
            match result {
                Err(Error::Unavailable) if !conn.is_healthy() && Instant::now() < deadline => {}
                other => return other,
            }
        }
    }

    /// Publishes `payload` to `topic` and waits for the server's `Ok`. One attempt only:
    /// after `Unknown` the caller decides, as a repeat can deliver the event twice.
    pub async fn publish_confirmed(&self, topic: Topic, payload: &[u8]) -> Result<(), Error> {
        let payload = Value::new(payload).ok_or(Error::ValueTooLarge(payload.len()))?;
        let deadline = Instant::now() + self.inner.connect_timeout.min(self.inner.deadline);
        loop {
            let (server, conn) = self.inner.streams.publisher(topic, deadline).await?;
            let sent = self.probe.stamp_at(server);
            let result = conn
                .call(&Command::Publish { topic, payload }, Expect::Status)
                .await;
            sent.finish(result.as_ref().err());
            match result {
                Err(Error::Unavailable) if !conn.is_healthy() && Instant::now() < deadline => {}
                other => return other.map(|_| ()),
            }
        }
    }
}
