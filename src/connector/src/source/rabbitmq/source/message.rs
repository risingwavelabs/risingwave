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

use std::sync::LazyLock;

use lapin::message::Delivery;
use lapin::options::BasicAckOptions;
use moka::future::Cache as MokaCache;

use crate::source::monitor::GLOBAL_SOURCE_METRICS;
use crate::source::{SourceContextRef, SourceMessage, SourceMeta, SplitId};

pub static RABBITMQ_ACK_CACHE: LazyLock<MokaCache<String, RabbitmqAckEntry>> =
    LazyLock::new(|| moka::future::Cache::builder().build());

#[derive(Clone, Debug)]
pub struct RabbitmqAckMetricLabels {
    source_id: String,
    source_name: String,
    actor_id: String,
    fragment_id: String,
    split_id: String,
    queue: String,
}

impl RabbitmqAckMetricLabels {
    fn new(split_id: &SplitId, queue: &str, source_ctx: &SourceContextRef) -> Self {
        Self {
            source_id: source_ctx.source_id.to_string(),
            source_name: source_ctx.source_name.clone(),
            actor_id: source_ctx.actor_id.to_string(),
            fragment_id: source_ctx.fragment_id.to_string(),
            split_id: split_id.as_ref().to_owned(),
            queue: queue.to_owned(),
        }
    }

    fn label_values(&self) -> [&str; 6] {
        [
            &self.source_id,
            &self.source_name,
            &self.actor_id,
            &self.fragment_id,
            &self.split_id,
            &self.queue,
        ]
    }
}

#[derive(Clone, Debug)]
pub struct RabbitmqAckEntry {
    acker: lapin::acker::Acker,
    metric_labels: RabbitmqAckMetricLabels,
}

impl RabbitmqAckEntry {
    fn new(acker: lapin::acker::Acker, metric_labels: RabbitmqAckMetricLabels) -> Self {
        GLOBAL_SOURCE_METRICS
            .rabbitmq_unacked_message_count
            .with_label_values(&metric_labels.label_values())
            .inc();
        Self {
            acker,
            metric_labels,
        }
    }

    fn dec_unacked_count(&self) {
        GLOBAL_SOURCE_METRICS
            .rabbitmq_unacked_message_count
            .with_label_values(&self.metric_labels.label_values())
            .dec();
    }
}

#[derive(Clone, Debug)]
pub struct RabbitmqMessage {
    pub split_id: SplitId,
    pub queue: String,
    pub delivery_tag: u64,
    pub payload: Vec<u8>,
    pub redelivered: bool,
}

impl RabbitmqMessage {
    pub async fn new(
        split_id: SplitId,
        queue: String,
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
        let ack_token = ack_token(&split_id, &queue, delivery_tag);
        let ack_entry = RabbitmqAckEntry::new(
            acker,
            RabbitmqAckMetricLabels::new(&split_id, &queue, source_ctx),
        );
        if let Some(old_entry) = RABBITMQ_ACK_CACHE.get(&ack_token).await {
            RABBITMQ_ACK_CACHE.invalidate(&ack_token).await;
            old_entry.dec_unacked_count();
        }
        RABBITMQ_ACK_CACHE.insert(ack_token, ack_entry).await;
        Self {
            split_id,
            queue,
            delivery_tag,
            payload: data,
            redelivered,
        }
    }

    pub fn offset(&self) -> String {
        ack_token(&self.split_id, &self.queue, self.delivery_tag)
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

pub fn ack_token(split_id: &SplitId, queue: &str, delivery_tag: u64) -> String {
    let queue = urlencoding::encode(queue);
    format!("{split_id}:{queue}:{delivery_tag}")
}

pub async fn ack_delivery(ack_token: String) -> crate::error::ConnectorResult<bool> {
    if let Some(entry) = RABBITMQ_ACK_CACHE.get(&ack_token).await {
        let result = entry.acker.ack(BasicAckOptions::default()).await;
        RABBITMQ_ACK_CACHE.invalidate(&ack_token).await;
        entry.dec_unacked_count();
        result?;
        Ok(true)
    } else {
        Ok(false)
    }
}

pub async fn drop_delivery(ack_token: &str) -> bool {
    if let Some(entry) = RABBITMQ_ACK_CACHE.get(ack_token).await {
        RABBITMQ_ACK_CACHE.invalidate(ack_token).await;
        entry.dec_unacked_count();
        true
    } else {
        false
    }
}
