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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, ensure};
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::for_await;
use once_cell::sync::OnceCell;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::OwnedMessage;
use rdkafka::{ClientConfig, Message, TopicPartitionList};
use tokio::sync::{OnceCell as TokioOnceCell, RwLock, mpsc};

use crate::connector_common::read_kafka_log_level;
use crate::error::ConnectorResult as Result;
use crate::source::SourceContextRef;
use crate::source::kafka::{
    KAFKA_ISOLATION_LEVEL, KafkaContextCommon, KafkaProperties, RwConsumerContext,
};
/// Global key == `connection_id` (already unique in metadata)
pub type ReaderKey = String;

type PartitionRoute = (String, i32);

pub struct KafkaMuxReader {
    key: ReaderKey,

    consumer: StreamConsumer<RwConsumerContext>,
    /// (topic, partition) -> sender (each topic unique within this connection)
    senders: RwLock<HashMap<PartitionRoute, mpsc::Sender<Vec<OwnedMessage>>>>,
}

type Registry = HashMap<ReaderKey, Arc<TokioOnceCell<Arc<KafkaMuxReader>>>>;

static GLOBAL: OnceCell<RwLock<Registry>> = OnceCell::new();
static ACTIVE_MUX_READERS: AtomicUsize = AtomicUsize::new(0);

impl KafkaMuxReader {
    fn registry() -> &'static RwLock<Registry> {
        GLOBAL.get_or_init(|| RwLock::new(HashMap::new()))
    }

    /// Create or reuse the reader for a given connection.
    pub async fn get_or_create(
        connection_id: ReaderKey,
        properties: KafkaProperties,
        source_ctx: SourceContextRef,
    ) -> Result<Arc<Self>> {
        let cell = {
            let mut registry = Self::registry().write().await;
            registry
                .entry(connection_id.clone())
                .or_insert_with(|| Arc::new(TokioOnceCell::new()))
                .clone()
        };

        if let Some(reader) = cell.get() {
            tracing::info!("Reusing existing KafkaMuxReader for connection_id: {connection_id}");
            return Ok(reader.clone());
        }

        let reader_ref = cell
            .get_or_try_init(|| {
                let properties = properties.clone();
                let source_ctx = source_ctx.clone();
                let key = connection_id.clone();
                async move {
                    tracing::info!("Creating new KafkaMuxReader for connection_id: {key}");
                    Self::build_reader(key, properties, source_ctx).await
                }
            })
            .await?;

        Ok(reader_ref.clone())
    }

    async fn build_reader(
        connection_id: ReaderKey,
        properties: KafkaProperties,
        source_ctx: SourceContextRef,
    ) -> Result<Arc<Self>> {
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
            key: connection_id.clone(),
            consumer,
            senders: RwLock::new(HashMap::new()),
        });

        let active = ACTIVE_MUX_READERS.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::info!(
            reader = %reader.key,
            active_mux_readers = active,
            "Kafka mux reader created"
        );

        let max_chunk_size = source_ctx.source_ctrl_opts.chunk_size;

        tokio::spawn(Self::poll_loop(Arc::clone(&reader), max_chunk_size));
        Ok(reader)
    }

    async fn poll_loop(this: Arc<Self>, max_chunk_size: usize) {
        let stream = this.consumer.stream();

        #[for_await]
        for messages_result in stream.ready_chunks(max_chunk_size) {
            let mut grouped_messages: HashMap<String, HashMap<i32, Vec<OwnedMessage>>> =
                HashMap::new();

            for msg in messages_result {
                match msg {
                    Ok(msg) => {
                        let msg = msg.detach();
                        grouped_messages
                            .entry(msg.topic().to_owned())
                            .or_default()
                            .entry(msg.partition())
                            .or_default()
                            .push(msg);
                    }
                    Err(err) => {
                        tracing::error!(
                            error = %err,
                            reader = %this.key,
                            "Kafka mux poll error"
                        );
                    }
                }
            }

            if grouped_messages.is_empty() {
                continue;
            }

            for (topic, partition_map) in grouped_messages {
                for (partition, messages) in partition_map {
                    let key = (topic.clone(), partition);
                    let sender_opt = {
                        let guard = this.senders.read().await;
                        guard.get(&key).cloned()
                    };

                    let Some(sender) = sender_opt else {
                        tracing::warn!(
                            reader = %this.key,
                            topic = %topic,
                            partition,
                            "No receiver registered for mux reader; dropping messages"
                        );
                        continue;
                    };

                    let batch_size = messages.len();
                    tracing::debug!(
                        reader = %this.key,
                        topic = %topic,
                        partition,
                        batch_size,
                        "Dispatching mux reader batch"
                    );

                    match sender.send(messages).await {
                        Ok(()) => {}
                        Err(tokio::sync::mpsc::error::SendError(_messages)) => {
                            tracing::warn!(
                                reader = %this.key,
                                topic = %topic,
                                partition,
                                "Mux reader failed to deliver messages; receiver dropped"
                            );
                            let mut guard = this.senders.write().await;
                            guard.remove(&key);
                        }
                    }
                }
            }
        }

        tracing::info!(
            reader = %this.key,
            "Kafka mux poll loop exited"
        );
    }

    pub async fn register_topic_partition_list(
        &self,
        tpl: TopicPartitionList,
    ) -> anyhow::Result<mpsc::Receiver<Vec<OwnedMessage>>> {
        if tpl.count() == 0 {
            anyhow::bail!("splits list is empty");
        }

        tracing::info!(
            "Registering topic partition list: {:?} in mux reader {}",
            tpl,
            self.key
        );
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
        let (tx, rx) = mpsc::channel(128);
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
            .context("assign failed")?;
        Ok(rx)
    }

    pub async fn unregister_topic_partition_list(
        self: Arc<Self>,
        tpl: TopicPartitionList,
    ) -> anyhow::Result<()> {
        if tpl.count() == 0 {
            return Ok(());
        }

        tracing::info!(
            "Unregistering topic partition list: {:?} in mux reader {}",
            tpl,
            self.key
        );

        const MAX_RETRIES: usize = 3;
        let mut attempt = 0;
        loop {
            match self
                .consumer
                .incremental_unassign(&tpl)
                .context("unassign failed")
            {
                Ok(_) => break,
                Err(err) => {
                    attempt += 1;
                    if attempt >= MAX_RETRIES {
                        return Err(err);
                    }
                    tracing::warn!(
                        reader = %self.key,
                        attempt,
                        "Failed to unassign mux reader partitions, retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }

        let is_empty_after_removal = {
            let mut map = self.senders.write().await;
            for element in tpl.elements() {
                map.remove(&(element.topic().to_owned(), element.partition()));
            }
            map.is_empty()
        };

        if is_empty_after_removal {
            tracing::info!(
                "All partitions unregistered for mux reader {}. Removing from global registry.",
                self.key
            );

            // Now we can safely access the key and remove from the global registry.
            Self::registry().write().await.remove(&self.key);

            tracing::info!(
                "Successfully removed mux reader {} from the global registry.",
                self.key
            );
        }

        Ok(())
    }

    pub async fn reassign_partitions(
        &self,
        tpl_with_offset: TopicPartitionList,
    ) -> anyhow::Result<()> {
        let count = tpl_with_offset.count();
        if count == 0 {
            return Ok(());
        }

        let elements = tpl_with_offset.elements();
        let mut unassign_tpl = TopicPartitionList::with_capacity(count);

        {
            let map = self.senders.read().await;
            for element in elements {
                let key = (element.topic().to_owned(), element.partition());
                ensure!(
                    map.contains_key(&key),
                    "split ({:?}) not registered in mux reader {}",
                    key,
                    self.key
                );
                unassign_tpl.add_partition(&key.0, key.1);
            }
        }

        self.consumer
            .incremental_unassign(&unassign_tpl)
            .context("unassign failed during reassign")?;

        self.consumer
            .incremental_assign(&tpl_with_offset)
            .context("assign failed during reassign")?;

        Ok(())
    }

    // pub async fn seek(
    //     &self,
    //     topic_partition_list: TopicPartitionList,
    //     _sync_call_timeout: Duration,
    // ) -> KafkaResult<TopicPartitionList> {
    //         // self.consumer
    //     //     .seek_partitions(topic_partition_list.clone(), sync_call_timeout)
    //     //     .await
    // }
}

impl Drop for KafkaMuxReader {
    fn drop(&mut self) {
        let prev = ACTIVE_MUX_READERS.fetch_sub(1, Ordering::Relaxed);
        let active = prev.saturating_sub(1);
        tracing::info!(
            reader = %self.key,
            active_mux_readers = active,
            "Kafka mux reader dropped"
        );
    }
}

use crate::source::kafka::source::reader::KafkaMetaFetcher;

#[async_trait]
impl KafkaMetaFetcher for KafkaMuxReader {
    async fn proxy_fetch_watermarks(
        &self,
        topic: &str,
        partition: i32,
        timeout: Duration,
    ) -> KafkaResult<(i64, i64)> {
        self.consumer
            .fetch_watermarks(topic, partition, timeout)
            .await
    }
}
