// Copyright 2023 RisingWave Labs
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

use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use futures::future::try_join_all;
use futures::{Future, FutureExt};
use futures_async_stream::for_await;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::ToBytes;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::ClientConfig;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_rpc_client::ConnectorClient;
use serde_derive::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

use super::encoder::{JsonEncoder, TimestampHandlingMode};
use super::formatter::{AppendOnlyFormatter, UpsertFormatter};
use super::{
    FormattedSink, Sink, SinkError, SinkParam, SINK_TYPE_APPEND_ONLY, SINK_TYPE_DEBEZIUM,
    SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};
use crate::common::KafkaCommon;
use crate::sink::utils::{gen_debezium_message_stream, DebeziumAdapterOpts};
use crate::sink::{
    DummySinkCommitCoordinator, Result, SinkWriterParam, SinkWriterV1, SinkWriterV1Adapter,
};
use crate::source::kafka::{KafkaProperties, KafkaSplitEnumerator, PrivateLinkProducerContext};
use crate::source::{SourceEnumeratorContext, SplitEnumerator};
use crate::{
    deserialize_bool_from_string, deserialize_duration_from_string, deserialize_u32_from_string,
};

pub const KAFKA_SINK: &str = "kafka";

const fn _default_timeout() -> Duration {
    Duration::from_secs(5)
}

const fn _default_max_retries() -> u32 {
    3
}

const fn _default_retry_backoff() -> Duration {
    Duration::from_millis(100)
}

const fn _default_use_transaction() -> bool {
    false
}

const fn _default_force_append_only() -> bool {
    false
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdKafkaPropertiesProducer {
    /// Maximum number of messages allowed on the producer queue. This queue is shared by all
    /// topics and partitions. A value of 0 disables this limit.
    #[serde(rename = "properties.queue.buffering.max.messages")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub queue_buffering_max_messages: Option<usize>,

    /// Maximum total message size sum allowed on the producer queue. This queue is shared by all
    /// topics and partitions. This property has higher priority than queue.buffering.max.messages.
    #[serde(rename = "properties.queue.buffering.max.kbytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    queue_buffering_max_kbytes: Option<usize>,

    /// Delay in milliseconds to wait for messages in the producer queue to accumulate before
    /// constructing message batches (MessageSets) to transmit to brokers. A higher value allows
    /// larger and more effective (less overhead, improved compression) batches of messages to
    /// accumulate at the expense of increased message delivery latency.
    #[serde(rename = "properties.queue.buffering.max.ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    queue_buffering_max_ms: Option<f64>,

    /// When set to true, the producer will ensure that messages are successfully produced exactly
    /// once and in the original produce order. The following configuration properties are adjusted
    /// automatically (if not modified by the user) when idempotence is enabled:
    /// max.in.flight.requests.per.connection=5 (must be less than or equal to 5),
    /// retries=INT32_MAX (must be greater than 0), acks=all, queuing.strategy=fifo. Producer
    /// will fail if user-supplied configuration is incompatible.
    #[serde(rename = "properties.enable.idempotence")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    enable_idempotence: Option<bool>,

    /// How many times to retry sending a failing Message.
    #[serde(rename = "properties.message.send.max.retries")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    message_send_max_retries: Option<usize>,

    /// The backoff time in milliseconds before retrying a protocol request.
    #[serde(rename = "properties.retry.backoff.ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    retry_backoff_ms: Option<usize>,

    /// Maximum number of messages batched in one MessageSet
    #[serde(rename = "properties.batch.num.messages")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    batch_num_messages: Option<usize>,

    /// Maximum size (in bytes) of all messages batched in one MessageSet, including protocol
    /// framing overhead. This limit is applied after the first message has been added to the
    /// batch, regardless of the first message's size, this is to ensure that messages that exceed
    /// batch.size are produced.
    #[serde(rename = "properties.batch.size")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    batch_size: Option<usize>,
}

impl RdKafkaPropertiesProducer {
    pub(crate) fn set_client(&self, c: &mut rdkafka::ClientConfig) {
        if let Some(v) = self.queue_buffering_max_messages {
            c.set("queue.buffering.max.messages", v.to_string());
        }
        if let Some(v) = self.queue_buffering_max_kbytes {
            c.set("queue.buffering.max.kbytes", v.to_string());
        }
        if let Some(v) = self.queue_buffering_max_ms {
            c.set("queue.buffering.max.ms", v.to_string());
        }
        if let Some(v) = self.enable_idempotence {
            c.set("enable.idempotence", v.to_string());
        }
        if let Some(v) = self.message_send_max_retries {
            c.set("message.send.max.retries", v.to_string());
        }
        if let Some(v) = self.retry_backoff_ms {
            c.set("retry.backoff.ms", v.to_string());
        }
        if let Some(v) = self.batch_num_messages {
            c.set("batch.num.messages", v.to_string());
        }
        if let Some(v) = self.batch_size {
            c.set("batch.size", v.to_string());
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    #[serde(skip_serializing)]
    pub connector: String, // Must be "kafka" here.

    // #[serde(rename = "connection.name")]
    // pub connection: String,
    #[serde(flatten)]
    pub common: KafkaCommon,

    pub r#type: String, // accept "append-only", "debezium", or "upsert"

    #[serde(
        default = "_default_force_append_only",
        deserialize_with = "deserialize_bool_from_string"
    )]
    pub force_append_only: bool,

    #[serde(
        rename = "properties.timeout",
        default = "_default_timeout",
        deserialize_with = "deserialize_duration_from_string"
    )]
    pub timeout: Duration,

    #[serde(
        rename = "properties.retry.max",
        default = "_default_max_retries",
        deserialize_with = "deserialize_u32_from_string"
    )]
    pub max_retry_num: u32,

    #[serde(
        rename = "properties.retry.interval",
        default = "_default_retry_backoff",
        deserialize_with = "deserialize_duration_from_string"
    )]
    pub retry_interval: Duration,

    #[serde(
        default = "_default_use_transaction",
        deserialize_with = "deserialize_bool_from_string"
    )]
    pub use_transaction: bool,

    /// We have parsed the primary key for an upsert kafka sink into a `usize` vector representing
    /// the indices of the pk columns in the frontend, so we simply store the primary key here
    /// as a string.
    pub primary_key: Option<String>,

    #[serde(flatten)]
    pub rdkafka_properties: RdKafkaPropertiesProducer,
}

impl KafkaConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<KafkaConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;

        if config.r#type != SINK_TYPE_APPEND_ONLY
            && config.r#type != SINK_TYPE_DEBEZIUM
            && config.r#type != SINK_TYPE_UPSERT
        {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_DEBEZIUM,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }

    pub(crate) fn set_client(&self, c: &mut rdkafka::ClientConfig) {
        self.common.set_client(c);
        self.rdkafka_properties.set_client(c);

        tracing::info!("kafka client starts with: {:?}", c);
    }
}

impl From<KafkaConfig> for KafkaProperties {
    fn from(val: KafkaConfig) -> Self {
        KafkaProperties {
            bytes_per_second: None,
            max_num_messages: None,
            scan_startup_mode: None,
            time_offset: None,
            consumer_group: None,
            upsert: None,
            common: val.common,
            rdkafka_properties: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct KafkaSink {
    pub config: KafkaConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    db_name: String,
    sink_from_name: String,
}

impl KafkaSink {
    pub fn new(config: KafkaConfig, param: SinkParam) -> Self {
        Self {
            config,
            schema: param.schema(),
            pk_indices: param.downstream_pk,
            is_append_only: param.sink_type.is_append_only(),
            db_name: param.db_name,
            sink_from_name: param.sink_from_name,
        }
    }
}

#[async_trait::async_trait]
impl Sink for KafkaSink {
    type Coordinator = DummySinkCommitCoordinator;
    type Writer = SinkWriterV1Adapter<KafkaSinkWriter>;

    async fn new_writer(&self, writer_param: SinkWriterParam) -> Result<Self::Writer> {
        Ok(SinkWriterV1Adapter::new(
            KafkaSinkWriter::new(
                self.config.clone(),
                self.schema.clone(),
                self.pk_indices.clone(),
                self.is_append_only,
                self.db_name.clone(),
                self.sink_from_name.clone(),
                format!("sink-{:?}", writer_param.executor_id),
            )
            .await?,
        ))
    }

    async fn validate(&self, _client: Option<ConnectorClient>) -> Result<()> {
        // For upsert Kafka sink, the primary key must be defined.
        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "primary key not defined for {} kafka sink (please define in `primary_key` field)",
                self.config.r#type
            )));
        }

        // Try Kafka connection.
        // There is no such interface for kafka producer to validate a connection
        // use enumerator to validate broker reachability and existence of topic
        let mut ticker = KafkaSplitEnumerator::new(
            KafkaProperties::from(self.config.clone()),
            Arc::new(SourceEnumeratorContext::default()),
        )
        .await?;
        _ = ticker.list_splits().await?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, enum_as_inner::EnumAsInner)]
enum KafkaSinkState {
    Init,
    // State running with epoch.
    Running(u64),
}

/// The delivery buffer queue size
/// When the `DeliveryFuture` the current `future_delivery_buffer`
/// is buffering is greater than this size, then enforcing commit once
const KAFKA_WRITER_MAX_QUEUE_SIZE: usize = 65536;

pub struct KafkaSinkWriter {
    pub config: KafkaConfig,
    pub inner: FutureProducer<PrivateLinkProducerContext>,
    identifier: String,
    state: KafkaSinkState,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    future_delivery_buffer: VecDeque<DeliveryFuture>,
    db_name: String,
    sink_from_name: String,
}

impl KafkaSinkWriter {
    pub async fn new(
        mut config: KafkaConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
        db_name: String,
        sink_from_name: String,
        identifier: String,
    ) -> Result<Self> {
        let inner: FutureProducer<PrivateLinkProducerContext> = {
            let mut c = ClientConfig::new();

            // KafkaConfig configuration
            config.common.set_security_properties(&mut c);
            config.set_client(&mut c);

            // ClientConfig configuration
            c.set("bootstrap.servers", &config.common.brokers)
                .set("message.timeout.ms", "5000");
            // Note that we will not use transaction during sinking, thus set it to false
            config.use_transaction = false;

            // Create the producer context, will be used to create the producer
            let producer_ctx = PrivateLinkProducerContext::new(
                config.common.broker_rewrite_map.clone(),
                // fixme: enable kafka native metrics for sink
                None,
                None,
            )?;

            // Generate the producer
            c.create_with_context(producer_ctx).await?
        };

        Ok(KafkaSinkWriter {
            config: config.clone(),
            inner,
            identifier,
            state: KafkaSinkState::Init,
            schema,
            pk_indices,
            is_append_only,
            future_delivery_buffer: VecDeque::new(),
            db_name,
            sink_from_name,
        })
    }

    /// The actual `send_result` function, will be called when the `KafkaSinkWriter` needs to sink
    /// messages
    async fn send_result<'a, K, P>(
        &'a mut self,
        mut record: FutureRecord<'a, K, P>,
    ) -> KafkaResult<()>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let mut success_flag = false;

        let mut ret = Ok(());

        for _ in 0..self.config.max_retry_num {
            match self.inner.send_result(record) {
                Ok(delivery_future) => {
                    // First check if the current length is
                    // greater than the preset limit
                    while self.future_delivery_buffer.len() >= KAFKA_WRITER_MAX_QUEUE_SIZE {
                        Self::map_future_result(
                            self.future_delivery_buffer
                                .pop_front()
                                .expect("Expect the future not to be None")
                                .await,
                        )?;
                    }

                    self.future_delivery_buffer.push_back(delivery_future);
                    success_flag = true;
                    break;
                }
                // The enqueue buffer is full, `send_result` will immediately return
                // We can retry for another round after sleeping for sometime
                Err((e, rec)) => {
                    record = rec;
                    match e {
                        err @ KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
                        | err @ KafkaError::MessageProduction(RDKafkaErrorCode::MessageTimedOut) => {
                            tracing::warn!(
                                "producing message (key {:?}) to topic {} failed, err {:?}, retrying",
                                record.key.map(|k| k.to_bytes()),
                                record.topic,
                                err
                            );
                            tokio::time::sleep(self.config.retry_interval).await;
                            continue;
                        }
                        _ => return Err(e),
                    }
                }
            }
        }

        if !success_flag {
            // In this case, after trying `max_retry_num`
            // The enqueue buffer is still full
            ret = Err(KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull));
        }

        ret
    }

    async fn write_inner(
        &mut self,
        event_key_object: Option<Vec<u8>>,
        event_object: Option<Vec<u8>>,
    ) -> Result<()> {
        let topic = self.config.common.topic.clone();
        // here we assume the key part always exists and value part is optional.
        // if value is None, we will skip the payload part.
        let key_str = event_key_object.unwrap();
        let mut record = FutureRecord::<[u8], [u8]>::to(topic.as_str()).key(&key_str);
        let payload;
        if let Some(value) = event_object {
            payload = value;
            record = record.payload(&payload);
        }
        // Send the data but not wait it to finish sinking
        // Will join all `DeliveryFuture` during commit
        self.send_result(record).await?;
        Ok(())
    }

    fn map_future_result(
        delivery_future_result: <DeliveryFuture as Future>::Output,
    ) -> KafkaResult<()> {
        match delivery_future_result {
            // Successfully sent the record
            // Will return the partition and offset of the message (i32, i64)
            // Note that `Vec<()>` won't cause memory allocation
            Ok(Ok(_)) => Ok(()),
            // If the message failed to be delivered. (i.e., flush)
            // The error & the copy of the original message will be returned
            // i.e., (KafkaError, OwnedMessage)
            // We will just stop the loop, and return the error
            // The sink executor will back to the latest checkpoint
            Ok(Err((k_err, _msg))) => Err(k_err),
            // This represents the producer is dropped
            // before the delivery status is received
            // Return `KafkaError::Canceled`
            Err(_) => Err(KafkaError::Canceled),
        }
    }

    async fn commit_inner(&mut self) -> Result<()> {
        let _v = try_join_all(
            self.future_delivery_buffer
                .drain(..)
                .map(|delivery_future| {
                    delivery_future.map(|delivery_future_result| {
                        Self::map_future_result(delivery_future_result).map_err(SinkError::Kafka)
                    })
                }),
        )
        .await?;

        // Sanity check
        debug_assert!(
            self.future_delivery_buffer.is_empty(),
            "The buffer after `commit_inner` must be empty"
        );

        Ok(())
    }

    async fn debezium_update(&mut self, chunk: StreamChunk, ts_ms: u64) -> Result<()> {
        // TODO: Remove the clones here, only to satisfy borrow checker at present
        let schema = self.schema.clone();
        let pk_indices = self.pk_indices.clone();
        let db_name = self.db_name.clone();
        let sink_from_name = self.sink_from_name.clone();

        // Initialize the dbz_stream
        let dbz_stream = gen_debezium_message_stream(
            &schema,
            &pk_indices,
            chunk,
            ts_ms,
            DebeziumAdapterOpts::default(),
            &db_name,
            &sink_from_name,
        );

        #[for_await]
        for msg in dbz_stream {
            let (event_key_object, event_object) = msg?;
            self.write_inner(
                event_key_object.map(|j| j.to_string().into_bytes()),
                event_object.map(|j| j.to_string().into_bytes()),
            )
            .await?;
        }
        Ok(())
    }

    async fn upsert(&mut self, chunk: StreamChunk) -> Result<()> {
        // TODO: Remove the clones here, only to satisfy borrow checker at present
        let schema = self.schema.clone();
        let pk_indices = self.pk_indices.clone();
        let key_encoder =
            JsonEncoder::new(&schema, Some(&pk_indices), TimestampHandlingMode::Milli);
        let val_encoder = JsonEncoder::new(&schema, None, TimestampHandlingMode::Milli);

        // Initialize the upsert_stream
        let f = UpsertFormatter::new(key_encoder, val_encoder);

        self.write_chunk(chunk, f).await
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        // TODO: Remove the clones here, only to satisfy borrow checker at present
        let schema = self.schema.clone();
        let pk_indices = self.pk_indices.clone();
        let key_encoder =
            JsonEncoder::new(&schema, Some(&pk_indices), TimestampHandlingMode::Milli);
        let val_encoder = JsonEncoder::new(&schema, None, TimestampHandlingMode::Milli);

        // Initialize the append_only_stream
        let f = AppendOnlyFormatter::new(key_encoder, val_encoder);

        self.write_chunk(chunk, f).await
    }
}

impl FormattedSink for KafkaSinkWriter {
    type K = Vec<u8>;
    type V = Vec<u8>;

    async fn write_one(&mut self, k: Option<Self::K>, v: Option<Self::V>) -> Result<()> {
        self.write_inner(k, v).await
    }
}

#[async_trait::async_trait]
impl SinkWriterV1 for KafkaSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.is_append_only {
            // Append-only
            self.append_only(chunk).await
        } else {
            // Debezium
            if self.config.r#type == SINK_TYPE_DEBEZIUM {
                self.debezium_update(
                    chunk,
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                )
                .await
            } else {
                // Upsert
                self.upsert(chunk).await
            }
        }
    }

    /// ---------------------------------------------------------------------------------------
    /// Note: The following functions are just to satisfy `SinkWriterV1` trait                |
    /// We do not need transaction-related functionality for sink executor, return Ok(())     |
    /// ---------------------------------------------------------------------------------------
    // Note that epoch 0 is reserved for initializing, so we should not use epoch 0 for
    // transaction.
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        // Group delivery (await the `FutureRecord`) here
        self.commit_inner().await?;
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use maplit::hashmap;
    use risingwave_common::catalog::Field;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;
    use serde_json::Value;

    use super::*;
    use crate::sink::utils::*;

    #[test]
    fn parse_rdkafka_props() {
        let props: HashMap<String, String> = hashmap! {
            // basic
            "connector".to_string() => "kafka".to_string(),
            "properties.bootstrap.server".to_string() => "localhost:9092".to_string(),
            "topic".to_string() => "test".to_string(),
            "type".to_string() => "append-only".to_string(),
            // RdKafkaPropertiesCommon
            "properties.message.max.bytes".to_string() => "12345".to_string(),
            "properties.receive.message.max.bytes".to_string() => "54321".to_string(),
            // RdKafkaPropertiesProducer
            "properties.queue.buffering.max.messages".to_string() => "114514".to_string(),
            "properties.queue.buffering.max.kbytes".to_string() => "114514".to_string(),
            "properties.queue.buffering.max.ms".to_string() => "114.514".to_string(),
            "properties.enable.idempotence".to_string() => "false".to_string(),
            "properties.message.send.max.retries".to_string() => "114514".to_string(),
            "properties.retry.backoff.ms".to_string() => "114514".to_string(),
            "properties.batch.num.messages".to_string() => "114514".to_string(),
            "properties.batch.size".to_string() => "114514".to_string(),
        };
        let c = KafkaConfig::from_hashmap(props).unwrap();
        assert_eq!(
            c.rdkafka_properties.queue_buffering_max_ms,
            Some(114.514f64)
        );

        let props: HashMap<String, String> = hashmap! {
            // basic
            "connector".to_string() => "kafka".to_string(),
            "properties.bootstrap.server".to_string() => "localhost:9092".to_string(),
            "topic".to_string() => "test".to_string(),
            "type".to_string() => "append-only".to_string(),

            "properties.enable.idempotence".to_string() => "True".to_string(), // can only be 'true' or 'false'
        };
        assert!(KafkaConfig::from_hashmap(props).is_err());

        let props: HashMap<String, String> = hashmap! {
            // basic
            "connector".to_string() => "kafka".to_string(),
            "properties.bootstrap.server".to_string() => "localhost:9092".to_string(),
            "topic".to_string() => "test".to_string(),
            "type".to_string() => "append-only".to_string(),
            "properties.queue.buffering.max.kbytes".to_string() => "-114514".to_string(), // usize cannot be negative
        };
        assert!(KafkaConfig::from_hashmap(props).is_err());
    }

    #[test]
    fn parse_kafka_config() {
        let properties: HashMap<String, String> = hashmap! {
            "connector".to_string() => "kafka".to_string(),
            "properties.bootstrap.server".to_string() => "localhost:9092".to_string(),
            "topic".to_string() => "test".to_string(),
            "type".to_string() => "append-only".to_string(),
            "force_append_only".to_string() => "true".to_string(),
            "use_transaction".to_string() => "False".to_string(),
            "properties.security.protocol".to_string() => "SASL".to_string(),
            "properties.sasl.mechanism".to_string() => "SASL".to_string(),
            "properties.sasl.username".to_string() => "test".to_string(),
            "properties.sasl.password".to_string() => "test".to_string(),
            "properties.timeout".to_string() => "10s".to_string(),
            "properties.retry.max".to_string() => "20".to_string(),
            "properties.retry.interval".to_string() => "500ms".to_string(),
        };
        let config = KafkaConfig::from_hashmap(properties).unwrap();
        assert_eq!(config.common.brokers, "localhost:9092");
        assert_eq!(config.common.topic, "test");
        assert_eq!(config.r#type, "append-only");
        assert!(config.force_append_only);
        assert!(!config.use_transaction);
        assert_eq!(config.timeout, Duration::from_secs(10));
        assert_eq!(config.max_retry_num, 20);
        assert_eq!(config.retry_interval, Duration::from_millis(500));

        // Optional fields eliminated.
        let properties: HashMap<String, String> = hashmap! {
            "connector".to_string() => "kafka".to_string(),
            "properties.bootstrap.server".to_string() => "localhost:9092".to_string(),
            "topic".to_string() => "test".to_string(),
            "type".to_string() => "upsert".to_string(),
        };
        let config = KafkaConfig::from_hashmap(properties).unwrap();
        assert!(!config.force_append_only);
        assert!(!config.use_transaction);
        assert_eq!(config.timeout, Duration::from_secs(5));
        assert_eq!(config.max_retry_num, 3);
        assert_eq!(config.retry_interval, Duration::from_millis(100));

        // Invalid u32 input.
        let properties: HashMap<String, String> = hashmap! {
            "connector".to_string() => "kafka".to_string(),
            "properties.bootstrap.server".to_string() => "localhost:9092".to_string(),
            "topic".to_string() => "test".to_string(),
            "type".to_string() => "upsert".to_string(),
            "properties.retry.max".to_string() => "-20".to_string(),  // error!
        };
        assert!(KafkaConfig::from_hashmap(properties).is_err());

        // Invalid bool input.
        let properties: HashMap<String, String> = hashmap! {
            "connector".to_string() => "kafka".to_string(),
            "properties.bootstrap.server".to_string() => "localhost:9092".to_string(),
            "topic".to_string() => "test".to_string(),
            "type".to_string() => "upsert".to_string(),
            "force_append_only".to_string() => "yes".to_string(),  // error!
        };
        assert!(KafkaConfig::from_hashmap(properties).is_err());

        // Invalid duration input.
        let properties: HashMap<String, String> = hashmap! {
            "connector".to_string() => "kafka".to_string(),
            "properties.bootstrap.server".to_string() => "localhost:9092".to_string(),
            "topic".to_string() => "test".to_string(),
            "type".to_string() => "upsert".to_string(),
            "properties.retry.interval".to_string() => "500minutes".to_string(),  // error!
        };
        assert!(KafkaConfig::from_hashmap(properties).is_err());
    }

    /// Note: Please enable the kafka by running `./risedev configure` before commenting #[ignore]
    /// to run the test, also remember to modify `risedev.yml`
    #[ignore]
    #[tokio::test]
    async fn test_kafka_producer() -> Result<()> {
        // Create a dummy kafka properties
        let properties = hashmap! {
            "connector".to_string() => "kafka".to_string(),
            "properties.bootstrap.server".to_string() => "localhost:29092".to_string(),
            "type".to_string() => "append-only".to_string(),
            "topic".to_string() => "test_topic".to_string(),
        };

        // Create a table with two columns (| id : INT32 | v2 : VARCHAR |) here
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".into(),
                sub_fields: vec![],
                type_name: "".into(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "v2".into(),
                sub_fields: vec![],
                type_name: "".into(),
            },
        ]);

        // We do not specify primary key for this schema
        let pk_indices = vec![];
        let kafka_config = KafkaConfig::from_hashmap(properties)?;

        // Create the actual sink writer to Kafka
        let mut sink = KafkaSinkWriter::new(
            kafka_config.clone(),
            schema,
            pk_indices,
            true,
            "test_sink_1".to_string(),
            "test_db".into(),
            "test_table".into(),
        )
        .await
        .unwrap();

        for i in 0..10 {
            let mut fail_flag = false;
            sink.begin_epoch(i).await?;
            println!("epoch: {}", i);
            for j in 0..100 {
                match sink
                    .send_result(
                        FutureRecord::to(kafka_config.common.topic.as_str())
                            .payload(format!("value-{}", j).as_bytes())
                            .key(format!("dummy_key_for_epoch-{}", i).as_bytes()),
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        fail_flag = true;
                        println!("{:?}", e);
                        sink.abort().await?;
                    }
                };
            }
            if !fail_flag {
                sink.commit().await?;
                println!("commit success");
            }
        }

        Ok(())
    }

    #[test]
    fn test_chunk_to_json() -> Result<()> {
        let chunk = StreamChunk::from_pretty(
            " i   f   {i,f}
            + 0 0.0 {0,0.0}
            + 1 1.0 {1,1.0}
            + 2 2.0 {2,2.0}
            + 3 3.0 {3,3.0}
            + 4 4.0 {4,4.0}
            + 5 5.0 {5,5.0}
            + 6 6.0 {6,6.0}
            + 7 7.0 {7,7.0}
            + 8 8.0 {8,8.0}
            + 9 9.0 {9,9.0}",
        );

        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "v1".into(),
                sub_fields: vec![],
                type_name: "".into(),
            },
            Field {
                data_type: DataType::Float32,
                name: "v2".into(),
                sub_fields: vec![],
                type_name: "".into(),
            },
            Field {
                data_type: DataType::new_struct(
                    vec![DataType::Int32, DataType::Float32],
                    vec!["v4".to_string(), "v5".to_string()],
                ),
                name: "v3".into(),
                sub_fields: vec![
                    Field {
                        data_type: DataType::Int32,
                        name: "v4".into(),
                        sub_fields: vec![],
                        type_name: "".into(),
                    },
                    Field {
                        data_type: DataType::Float32,
                        name: "v5".into(),
                        sub_fields: vec![],
                        type_name: "".into(),
                    },
                ],
                type_name: "".into(),
            },
        ]);

        let json_chunk = chunk_to_json(chunk, &schema).unwrap();
        let schema_json = schema_to_json(&schema, "test_db", "test_table");
        assert_eq!(schema_json, serde_json::from_str::<Value>("{\"fields\":[{\"field\":\"before\",\"fields\":[{\"field\":\"v1\",\"optional\":true,\"type\":\"int32\"},{\"field\":\"v2\",\"optional\":true,\"type\":\"float\"},{\"field\":\"v3\",\"optional\":true,\"type\":\"string\"}],\"name\":\"RisingWave.test_db.test_table.Key\",\"optional\":true,\"type\":\"struct\"},{\"field\":\"after\",\"fields\":[{\"field\":\"v1\",\"optional\":true,\"type\":\"int32\"},{\"field\":\"v2\",\"optional\":true,\"type\":\"float\"},{\"field\":\"v3\",\"optional\":true,\"type\":\"string\"}],\"name\":\"RisingWave.test_db.test_table.Key\",\"optional\":true,\"type\":\"struct\"},{\"field\":\"source\",\"fields\":[{\"field\":\"db\",\"optional\":false,\"type\":\"string\"},{\"field\":\"table\",\"optional\":true,\"type\":\"string\"}],\"name\":\"RisingWave.test_db.test_table.Source\",\"optional\":false,\"type\":\"struct\"},{\"field\":\"op\",\"optional\":false,\"type\":\"string\"},{\"field\":\"ts_ms\",\"optional\":false,\"type\":\"int64\"}],\"name\":\"RisingWave.test_db.test_table.Envelope\",\"optional\":false,\"type\":\"struct\"}").unwrap());
        assert_eq!(
            serde_json::from_str::<Value>(&json_chunk[0]).unwrap(),
            serde_json::from_str::<Value>("{\"v1\":0,\"v2\":0.0,\"v3\":{\"v4\":0,\"v5\":0.0}}")
                .unwrap()
        );

        Ok(())
    }
}
