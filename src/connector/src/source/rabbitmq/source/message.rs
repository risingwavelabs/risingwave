// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::fmt::Display;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, Weak};

use lapin::message::Delivery;
use lapin::options::BasicAckOptions;
use moka::future::Cache as MokaCache;

use crate::source::{SourceContextRef, SourceMessage, SourceMeta, SplitId};

pub static RABBITMQ_ACK_CACHE: LazyLock<MokaCache<String, RabbitmqAckEntry>> =
    LazyLock::new(|| moka::future::Cache::builder().build());

static RABBITMQ_ACK_CONSUMER_ID: AtomicU64 = AtomicU64::new(1);

pub fn next_ack_consumer_id() -> u64 {
    RABBITMQ_ACK_CONSUMER_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Default)]
pub struct RabbitmqAckTracker {
    tokens: Arc<Mutex<HashSet<String>>>,
}

impl RabbitmqAckTracker {
    fn track(&self, ack_token: &str) -> Weak<Mutex<HashSet<String>>> {
        lock_tracker_tokens(&self.tokens).insert(ack_token.to_owned());
        Arc::downgrade(&self.tokens)
    }
}

impl Drop for RabbitmqAckTracker {
    fn drop(&mut self) {
        let tokens = lock_tracker_tokens(&self.tokens)
            .drain()
            .collect::<Vec<_>>();
        if tokens.is_empty() {
            return;
        }

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                for ack_token in tokens {
                    RABBITMQ_ACK_CACHE.remove(&ack_token).await;
                }
            });
        } else {
            tracing::warn!(
                token_count = tokens.len(),
                "RabbitMQ ack tracker dropped outside a Tokio runtime; stale ack cache entries may remain until process exit",
            );
        }
    }
}

#[derive(Clone, Debug)]
pub struct RabbitmqAckEntry {
    acker: lapin::acker::Acker,
    tracker: Weak<Mutex<HashSet<String>>>,
}

impl RabbitmqAckEntry {
    fn new(acker: lapin::acker::Acker, tracker: Weak<Mutex<HashSet<String>>>) -> Self {
        Self { acker, tracker }
    }

    fn untrack(&self, ack_token: &str) {
        if let Some(tracker) = self.tracker.upgrade() {
            lock_tracker_tokens(&tracker).remove(ack_token);
        }
    }
}

fn lock_tracker_tokens(tokens: &Mutex<HashSet<String>>) -> MutexGuard<'_, HashSet<String>> {
    match tokens.lock() {
        Ok(tokens) => tokens,
        Err(poisoned) => {
            tracing::warn!(
                "RabbitMQ ack tracker mutex was poisoned; recovering tracked ack tokens"
            );
            poisoned.into_inner()
        }
    }
}

#[derive(Clone, Debug)]
pub struct RabbitmqMessage {
    pub split_id: SplitId,
    pub queue: String,
    pub delivery_tag: u64,
    pub ack_token: String,
    pub payload: Vec<u8>,
    pub redelivered: bool,
}

impl RabbitmqMessage {
    pub async fn new(
        split_id: SplitId,
        queue: String,
        ack_consumer_id: u64,
        ack_tracker: &RabbitmqAckTracker,
        delivery: Delivery,
        source_ctx: &SourceContextRef,
    ) -> Self {
        let Delivery {
            delivery_tag,
            redelivered,
            data,
            acker,
            ..
        } = delivery;
        let ack_token = ack_token(
            source_ctx.source_id.as_raw_id(),
            source_ctx.actor_id,
            &split_id,
            &queue,
            ack_consumer_id,
            delivery_tag,
        );
        // RabbitMQ delivery tags are only valid on the channel that delivered the message.
        // The token therefore scopes the tag to this reader-owned consumer and is used only
        // for checkpoint-time acknowledgement while the channel is alive. If the reader or
        // connection closes before the checkpoint ack, RabbitMQ requeues the unacked delivery.
        RABBITMQ_ACK_CACHE
            .insert(
                ack_token.clone(),
                RabbitmqAckEntry::new(acker, ack_tracker.track(&ack_token)),
            )
            .await;
        Self {
            split_id,
            queue,
            delivery_tag,
            ack_token,
            payload: data,
            redelivered,
        }
    }

    pub fn offset(&self) -> String {
        self.ack_token.clone()
    }
}

impl From<RabbitmqMessage> for SourceMessage {
    fn from(message: RabbitmqMessage) -> Self {
        let offset = message.offset();
        SourceMessage {
            key: None,
            payload: Some(message.payload),
            offset,
            split_id: message.split_id,
            meta: SourceMeta::Empty,
        }
    }
}

pub fn ack_token(
    source_id: impl Display,
    actor_id: impl Display,
    split_id: &SplitId,
    queue: &str,
    ack_consumer_id: u64,
    delivery_tag: u64,
) -> String {
    let queue = urlencoding::encode(queue);
    format!("{source_id}:{actor_id}:{split_id}:{queue}:{ack_consumer_id}:{delivery_tag}")
}

pub async fn ack_delivery(ack_token: String) -> crate::error::ConnectorResult<bool> {
    if let Some(entry) = RABBITMQ_ACK_CACHE.get(&ack_token).await {
        entry.acker.ack(BasicAckOptions::default()).await?;
        if let Some(entry) = RABBITMQ_ACK_CACHE.remove(&ack_token).await {
            entry.untrack(&ack_token);
        }
        Ok(true)
    } else {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::ack_token;
    use crate::source::SplitId;

    #[test]
    fn ack_token_is_scoped_beyond_delivery_tag() {
        let split_id = SplitId::from("0");
        let first = ack_token(1, 10, &split_id, "queue:orders", 100, 1);
        let different_source = ack_token(2, 10, &split_id, "queue:orders", 100, 1);
        let different_consumer = ack_token(1, 10, &split_id, "queue:orders", 101, 1);

        assert_ne!(first, different_source);
        assert_ne!(first, different_consumer);
        assert!(first.contains("queue%3Aorders"));
    }
}
