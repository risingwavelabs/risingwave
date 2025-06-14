// Copyright 2025 RisingWave Labs
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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::StreamExt;
use once_cell::sync::OnceCell;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::OwnedMessage;
use rdkafka::{ClientConfig, Message, TopicPartitionList};
use tokio::sync::{RwLock, mpsc};

use crate::connector_common::read_kafka_log_level;
use crate::error::ConnectorResult as Result;
use crate::source::SourceContextRef;
use crate::source::kafka::{
    KAFKA_ISOLATION_LEVEL, KafkaContextCommon, KafkaProperties, RwConsumerContext,
};

/// Global key == `connection_id` (already unique in metadata)
pub type ReaderKey = String;

pub struct KafkaMuxReader {
    consumer: StreamConsumer<RwConsumerContext>,
    /// (topic, partition) -> sender (each topic unique within this connection)
    senders: RwLock<HashMap<(String, i32), mpsc::Sender<OwnedMessage>>>,
}

static GLOBAL: OnceCell<RwLock<HashMap<ReaderKey, Arc<KafkaMuxReader>>>> = OnceCell::new();

impl KafkaMuxReader {
    fn registry() -> &'static RwLock<HashMap<ReaderKey, Arc<KafkaMuxReader>>> {
        GLOBAL.get_or_init(|| RwLock::new(HashMap::new()))
    }

    /// Create or reuse the reader for a given connection.
    pub async fn get_or_create(
        connection_id: ReaderKey,
        properties: KafkaProperties,
        source_ctx: SourceContextRef,
    ) -> Result<Arc<Self>> {
        // fast path – already exists
        if let Some(r) = Self::registry().read().await.get(&connection_id).cloned() {
            tracing::info!("Reusing existing KafkaMuxReader for connection_id: {connection_id}");
            return Ok(r);
        }

        let mut config = ClientConfig::new();

        let bootstrap_servers = &properties.connection.brokers;
        let broker_rewrite_map = properties.privatelink_common.broker_rewrite_map.clone();

        // disable partition eof
        config.set("enable.partition.eof", "false");
        config.set("auto.offset.reset", "smallest");
        config.set("isolation.level", KAFKA_ISOLATION_LEVEL);
        config.set("bootstrap.servers", bootstrap_servers);

        properties.connection.set_security_properties(&mut config);
        properties.set_client(&mut config);

        let prefix = properties
            .group_id_prefix
            .as_deref()
            .unwrap_or("rw-consumer");

        config.set("group.id", format!("{prefix}-mux-{connection_id}"));

        let ctx_common = KafkaContextCommon::new(
            broker_rewrite_map,
            Some(format!("shared-mux-reader-connection-{}", connection_id,)),
            // thread consumer will keep polling in the background, we don't need to call `poll`
            // explicitly
            Some(source_ctx.metrics.rdkafka_native_metric.clone()),
            properties.aws_auth_props,
            properties.connection.is_aws_msk_iam(),
        )
        .await?;

        let client_ctx = RwConsumerContext::new(ctx_common);

        if let Some(log_level) = read_kafka_log_level() {
            config.set_log_level(log_level);
        }
        let consumer: StreamConsumer<RwConsumerContext> = config
            .create_with_context(client_ctx)
            .await
            .context("failed to create kafka consumer")?;

        let reader = Arc::new(Self {
            consumer,
            senders: RwLock::new(HashMap::new()),
        });
        Self::registry()
            .write()
            .await
            .insert(connection_id, Arc::clone(&reader));
        tokio::spawn(Self::poll_loop(Arc::clone(&reader)));
        Ok(reader)
    }

    async fn poll_loop(this: Arc<Self>) {
        let mut stream = this.consumer.stream();
        while let Some(res) = stream.next().await {
            match res {
                Ok(m) => {
                    let owned = m.detach();
                    let key = (owned.topic().to_owned(), owned.partition());
                    if let Some(tx) = this.senders.read().await.get(&key) {
                        let _ = tx.send(owned).await;
                    }
                }
                Err(e) => tracing::error!("Kafka error: {e}"),
            }
        }
    }

    pub async fn register_topic_partition_list(
        &self,
        tpl: TopicPartitionList,
    ) -> anyhow::Result<mpsc::Receiver<OwnedMessage>> {
        if tpl.count() == 0 {
            anyhow::bail!("splits list is empty");
        }
        {
            let map = self.senders.read().await;
            for element in tpl.elements() {
                let key = (element.topic().to_owned(), element.partition());
                if map.contains_key(&key) {
                    anyhow::bail!("split ({:?}) already registered", key);
                }
            }
        }
        // sender / receiver
        let (tx, rx) = mpsc::channel(1024);
        {
            let mut map = self.senders.write().await;
            for element in tpl.elements() {
                map.insert(
                    (element.topic().to_owned(), element.partition()),
                    tx.clone(),
                );
            }
        }

        self.consumer
            .incremental_assign(&tpl)
            .map_err(|e| anyhow::anyhow!("assign failed: {e}"))?;
        Ok(rx)
    }

    pub async fn unregister_topic_partition_list(
        &self,
        tpl: TopicPartitionList,
    ) -> anyhow::Result<()> {
        if tpl.count() == 0 {
            return Ok(());
        }
        {
            let map = self.senders.read().await;
            for element in tpl.elements() {
                let key = (element.topic().to_owned(), element.partition());
                if !map.contains_key(&key) {
                    anyhow::bail!("split ({:?}) not registered", key);
                }
            }
        }
        {
            let mut map = self.senders.write().await;
            for element in tpl.elements() {
                map.remove(&(element.topic().to_owned(), element.partition()));
            }
        }

        self.consumer
            .incremental_unassign(&tpl)
            .map_err(|e| anyhow::anyhow!("unassign failed: {e}"))?;
        Ok(())
    }

    /// Allow `fetch_watermarks` for backfill / seek‑to‑latest use‑cases
    pub async fn fetch_watermarks(
        &self,
        topic: &str,
        partition: i32,
        timeout: Duration,
    ) -> rdkafka::error::KafkaResult<(i64, i64)> {
        self.consumer
            .fetch_watermarks(topic, partition, timeout)
            .await
    }

    pub async fn seek(
        &self,
        topic_partition_list: TopicPartitionList,
        sync_call_timeout: Duration,
    ) -> KafkaResult<TopicPartitionList> {
        self.consumer
            .seek_partitions(topic_partition_list.clone(), sync_call_timeout)
            .await
    }
}
