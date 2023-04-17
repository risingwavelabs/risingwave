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

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::ToBytes;
use rdkafka::producer::{BaseRecord, Producer, ThreadedProducer};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::ClientConfig;
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::Row;
use serde_derive::Deserialize;
use serde_json::{json, Map, Value};
use tracing::warn;

use super::{
    Sink, SinkError, SINK_TYPE_APPEND_ONLY, SINK_TYPE_DEBEZIUM, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};
use crate::common::KafkaCommon;
use crate::sink::{datum_to_json_object, record_to_json, Result};
use crate::source::kafka::PrivateLinkProducerContext;
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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

    pub identifier: String,

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
}

#[derive(Debug, Clone, PartialEq, enum_as_inner::EnumAsInner)]
enum KafkaSinkState {
    Init,
    // State running with epoch.
    Running(u64),
}

pub struct KafkaSink<const APPEND_ONLY: bool> {
    pub config: KafkaConfig,
    pub conductor: KafkaTransactionConductor,
    state: KafkaSinkState,
    schema: Schema,
    pk_indices: Vec<usize>,
    in_transaction_epoch: Option<u64>,
}

impl<const APPEND_ONLY: bool> KafkaSink<APPEND_ONLY> {
    pub async fn new(config: KafkaConfig, schema: Schema, pk_indices: Vec<usize>) -> Result<Self> {
        Ok(KafkaSink {
            config: config.clone(),
            conductor: KafkaTransactionConductor::new(config).await?,
            in_transaction_epoch: None,
            state: KafkaSinkState::Init,
            schema,
            pk_indices,
        })
    }

    pub async fn validate(config: KafkaConfig, pk_indices: Vec<usize>) -> Result<()> {
        // For upsert Kafka sink, the primary key must be defined.
        if !APPEND_ONLY && pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "primary key not defined for upsert kafka sink (please define in `primary_key` field)"
            )));
        }

        // Try Kafka connection.
        // TODO: Reuse the conductor instance we create during validation.
        KafkaTransactionConductor::new(config).await?;

        Ok(())
    }

    // any error should report to upper level and requires revert to previous epoch.
    pub async fn do_with_retry<'a, F, FutKR, T>(&'a self, f: F) -> KafkaResult<T>
    where
        F: Fn(&'a KafkaTransactionConductor) -> FutKR,
        FutKR: Future<Output = KafkaResult<T>> + 'a,
    {
        let mut err = KafkaError::Canceled;
        for _ in 0..self.config.max_retry_num {
            match f(&self.conductor).await {
                Ok(res) => return Ok(res),
                Err(e) => err = e,
            }
            // a back off policy
            tokio::time::sleep(self.config.retry_interval).await;
        }
        Err(err)
    }

    async fn send<'a, K, P>(&'a self, mut record: BaseRecord<'a, K, P>) -> KafkaResult<()>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let mut err = KafkaError::Canceled;

        for _ in 0..self.config.max_retry_num {
            match self.conductor.send(record).await {
                Ok(()) => return Ok(()),
                Err((e, rec)) => {
                    err = e;
                    record = rec;
                }
            }
            if let KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull) = err {
                // if the queue is full, we need to wait for some time and retry.
                tokio::time::sleep(self.config.retry_interval).await;
                continue;
            } else {
                return Err(err);
            }
        }
        Err(err)
    }

    fn gen_message_key(&self) -> String {
        format!(
            "{}-{}",
            self.config.identifier,
            self.in_transaction_epoch.unwrap()
        )
    }

    async fn debezium_update(&self, chunk: StreamChunk, ts_ms: u64) -> Result<()> {
        let mut update_cache: Option<Map<String, Value>> = None;
        let schema = &self.schema;
        for (op, row) in chunk.rows() {
            let event_object = match op {
                Op::Insert => Some(json!({
                    "schema": schema_to_json(schema),
                    "payload": {
                        "before": null,
                        "after": record_to_json(row, &schema.fields)?,
                        "op": "c",
                        "ts_ms": ts_ms,
                    }
                })),
                Op::Delete => Some(json!({
                    "schema": schema_to_json(schema),
                    "payload": {
                        "before": record_to_json(row, &schema.fields)?,
                        "after": null,
                        "op": "d",
                        "ts_ms": ts_ms,
                    }
                })),
                Op::UpdateDelete => {
                    update_cache = Some(record_to_json(row, &schema.fields)?);
                    continue;
                }
                Op::UpdateInsert => {
                    if let Some(before) = update_cache.take() {
                        Some(json!({
                            "schema": schema_to_json(schema),
                            "payload": {
                                "before": before,
                                "after": record_to_json(row, &schema.fields)?,
                                "op": "u",
                                "ts_ms": ts_ms,
                            }
                        }))
                    } else {
                        warn!(
                            "not found UpdateDelete in prev row, skipping, row index {:?}",
                            row.index()
                        );
                        continue;
                    }
                }
            };
            if let Some(obj) = event_object {
                self.send(
                    BaseRecord::to(self.config.common.topic.as_str())
                        .key(self.gen_message_key().as_bytes())
                        .payload(obj.to_string().as_bytes()),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn upsert(&self, chunk: StreamChunk) -> Result<()> {
        let mut update_cache: Option<Map<String, Value>> = None;
        let schema = &self.schema;
        for (op, row) in chunk.rows() {
            let event_object = match op {
                Op::Insert => Some(Value::Object(record_to_json(row, &schema.fields)?)),
                Op::Delete => Some(Value::Null),
                Op::UpdateDelete => {
                    update_cache = Some(record_to_json(row, &schema.fields)?);
                    continue;
                }
                Op::UpdateInsert => {
                    if update_cache.take().is_some() {
                        Some(Value::Object(record_to_json(row, &schema.fields)?))
                    } else {
                        warn!(
                            "not found UpdateDelete in prev row, skipping, row index {:?}",
                            row.index()
                        );
                        continue;
                    }
                }
            };
            if let Some(obj) = event_object {
                let event_key = Value::Object(pk_to_json(row, &schema.fields, &self.pk_indices)?);
                self.send(
                    BaseRecord::to(self.config.common.topic.as_str())
                        .key(event_key.to_string().as_bytes())
                        .payload(obj.to_string().as_bytes()),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn append_only(&self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op == Op::Insert {
                let record = Value::Object(record_to_json(row, &self.schema.fields)?).to_string();
                self.send(
                    BaseRecord::to(self.config.common.topic.as_str())
                        .key(self.gen_message_key().as_bytes())
                        .payload(record.as_bytes()),
                )
                .await?;
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<const APPEND_ONLY: bool> Sink for KafkaSink<APPEND_ONLY> {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if APPEND_ONLY {
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

    // Note that epoch 0 is reserved for initializing, so we should not use epoch 0 for
    // transaction.
    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.in_transaction_epoch = Some(epoch);
        self.do_with_retry(|conductor| conductor.start_transaction())
            .await?;
        tracing::debug!("begin epoch {:?}", epoch);
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        self.do_with_retry(|conductor| conductor.flush()) // flush before commit
            .await?;

        self.do_with_retry(|conductor| conductor.commit_transaction())
            .await?;
        if let Some(epoch) = self.in_transaction_epoch.take() {
            self.state = KafkaSinkState::Running(epoch);
        } else {
            tracing::error!(
                "commit without begin_epoch, last success epoch {:?}",
                self.state
            );
            return Err(SinkError::Kafka(KafkaError::Canceled));
        }
        tracing::debug!("commit epoch {:?}", self.state);
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        self.do_with_retry(|conductor| conductor.abort_transaction())
            .await?;
        tracing::debug!("abort epoch {:?}", self.in_transaction_epoch);
        self.in_transaction_epoch = None;
        Ok(())
    }
}

impl<const APPEND_ONLY: bool> Debug for KafkaSink<APPEND_ONLY> {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!();
    }
}

fn pk_to_json(
    row: RowRef<'_>,
    schema: &[Field],
    pk_indices: &[usize],
) -> Result<Map<String, Value>> {
    let mut mappings = Map::with_capacity(schema.len());
    for idx in pk_indices {
        let field = &schema[*idx];
        let key = field.name.clone();
        let value = datum_to_json_object(field, row.datum_at(*idx))
            .map_err(|e| SinkError::JsonParse(e.to_string()))?;
        mappings.insert(key, value);
    }
    Ok(mappings)
}

pub fn chunk_to_json(chunk: StreamChunk, schema: &Schema) -> Result<Vec<String>> {
    let mut records: Vec<String> = Vec::with_capacity(chunk.capacity());
    for (_, row) in chunk.rows() {
        let record = Value::Object(record_to_json(row, &schema.fields)?);
        records.push(record.to_string());
    }

    Ok(records)
}

fn fields_to_json(fields: &[Field]) -> Value {
    let mut res = Vec::new();

    fields.iter().for_each(|field| {
        // mapping from 'https://debezium.io/documentation/reference/2.1/connectors/postgresql.html#postgresql-data-types'
        let r#type = match field.data_type() {
            risingwave_common::types::DataType::Boolean => "boolean",
            risingwave_common::types::DataType::Int16 => "int16",
            risingwave_common::types::DataType::Int32 => "int32",
            risingwave_common::types::DataType::Int64 => "int64",
            risingwave_common::types::DataType::Int256 => "string",
            risingwave_common::types::DataType::Float32 => "float32",
            risingwave_common::types::DataType::Float64 => "float64",
            // currently, we only support handling decimal as string.
            // https://debezium.io/documentation/reference/2.1/connectors/postgresql.html#postgresql-decimal-types
            risingwave_common::types::DataType::Decimal => "string",

            risingwave_common::types::DataType::Varchar => "string",

            risingwave_common::types::DataType::Date => "int32",
            risingwave_common::types::DataType::Time => "int64",
            risingwave_common::types::DataType::Timestamp => "int64",
            risingwave_common::types::DataType::Timestamptz => "string",
            risingwave_common::types::DataType::Interval => "string",

            risingwave_common::types::DataType::Bytea => "bytes",
            risingwave_common::types::DataType::Jsonb => "string",
            risingwave_common::types::DataType::Serial => "int32",
            // since the original debezium pg support HSTORE via encoded as json string by default,
            // we do the same here
            risingwave_common::types::DataType::Struct(_) => "string",
            risingwave_common::types::DataType::List { .. } => "string",
        };
        res.push(json!({
            "field": field.name,
            "optional": true,
            "type": r#type,
        }))
    });

    json!(res)
}

fn schema_to_json(schema: &Schema) -> Value {
    let mut schema_fields = Vec::new();
    schema_fields.push(json!({
        "type": "struct",
        "fields": fields_to_json(&schema.fields),
        "optional": true,
        "field": "before",
    }));
    schema_fields.push(json!({
        "type": "struct",
        "fields": fields_to_json(&schema.fields),
        "optional": true,
        "field": "after",
    }));
    json!({
        "type": "struct",
        "fields": schema_fields,
        "optional": false,
    })
}

/// the struct conducts all transactions with Kafka
pub struct KafkaTransactionConductor {
    properties: KafkaConfig,
    inner: ThreadedProducer<PrivateLinkProducerContext>,
}

impl KafkaTransactionConductor {
    async fn new(mut config: KafkaConfig) -> Result<Self> {
        let inner: ThreadedProducer<PrivateLinkProducerContext> = {
            let mut c = ClientConfig::new();
            config.common.set_security_properties(&mut c);
            c.set("bootstrap.servers", &config.common.brokers)
                .set("message.timeout.ms", "5000");
            config.use_transaction = false;
            if config.use_transaction {
                c.set("transactional.id", &config.identifier); // required by kafka transaction
            }
            let client_ctx =
                PrivateLinkProducerContext::new(config.common.broker_rewrite_map.clone())?;
            c.create_with_context(client_ctx).await?
        };

        if config.use_transaction {
            inner.init_transactions(config.timeout).await?;
        }

        Ok(KafkaTransactionConductor {
            properties: config,
            inner,
        })
    }

    #[expect(clippy::unused_async)]
    async fn start_transaction(&self) -> KafkaResult<()> {
        if self.properties.use_transaction {
            self.inner.begin_transaction()
        } else {
            Ok(())
        }
    }

    async fn commit_transaction(&self) -> KafkaResult<()> {
        if self.properties.use_transaction {
            self.inner.commit_transaction(self.properties.timeout).await
        } else {
            Ok(())
        }
    }

    async fn abort_transaction(&self) -> KafkaResult<()> {
        if self.properties.use_transaction {
            self.inner.abort_transaction(self.properties.timeout).await
        } else {
            Ok(())
        }
    }

    async fn flush(&self) -> KafkaResult<()> {
        self.inner.flush(self.properties.timeout).await
    }

    #[expect(clippy::unused_async)]
    async fn send<'a, K, P>(
        &'a self,
        record: BaseRecord<'a, K, P>,
    ) -> core::result::Result<(), (KafkaError, BaseRecord<'a, K, P>)>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        self.inner.send(record)
    }
}

#[cfg(test)]
mod test {
    use maplit::hashmap;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;

    use super::*;

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
            "identifier".to_string() => "test_sink_1".to_string(),
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
            "identifier".to_string() => "test_sink_2".to_string(),
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
            "identifier".to_string() => "test_sink_3".to_string(),
            "properties.retry.max".to_string() => "-20".to_string(),  // error!
        };
        assert!(KafkaConfig::from_hashmap(properties).is_err());

        // Invalid bool input.
        let properties: HashMap<String, String> = hashmap! {
            "connector".to_string() => "kafka".to_string(),
            "properties.bootstrap.server".to_string() => "localhost:9092".to_string(),
            "topic".to_string() => "test".to_string(),
            "type".to_string() => "upsert".to_string(),
            "identifier".to_string() => "test_sink_4".to_string(),
            "force_append_only".to_string() => "yes".to_string(),  // error!
        };
        assert!(KafkaConfig::from_hashmap(properties).is_err());

        // Invalid duration input.
        let properties: HashMap<String, String> = hashmap! {
            "connector".to_string() => "kafka".to_string(),
            "properties.bootstrap.server".to_string() => "localhost:9092".to_string(),
            "topic".to_string() => "test".to_string(),
            "type".to_string() => "upsert".to_string(),
            "identifier".to_string() => "test_sink_5".to_string(),
            "properties.retry.interval".to_string() => "500minutes".to_string(),  // error!
        };
        assert!(KafkaConfig::from_hashmap(properties).is_err());
    }

    #[ignore]
    #[tokio::test]
    async fn test_kafka_producer() -> Result<()> {
        let properties = hashmap! {
            "properties.bootstrap.server".to_string() => "localhost:29092".to_string(),
            "identifier".to_string() => "test_sink_1".to_string(),
            "type".to_string() => "append-only".to_string(),
            "topic".to_string() => "test_topic".to_string(),
        };
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
        let pk_indices = vec![];
        let kafka_config = KafkaConfig::from_hashmap(properties)?;
        let mut sink = KafkaSink::<true>::new(kafka_config.clone(), schema, pk_indices)
            .await
            .unwrap();

        for i in 0..10 {
            let mut fail_flag = false;
            sink.begin_epoch(i).await?;
            for i in 0..100 {
                match sink
                    .send(
                        BaseRecord::to(kafka_config.common.topic.as_str())
                            .payload(format!("value-{}", i).as_bytes())
                            .key(sink.gen_message_key().as_bytes()),
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
        let schema_json = schema_to_json(&schema);
        assert_eq!(schema_json.to_string(), "{\"fields\":[{\"field\":\"before\",\"fields\":[{\"field\":\"v1\",\"optional\":true,\"type\":\"int32\"},{\"field\":\"v2\",\"optional\":true,\"type\":\"float32\"},{\"field\":\"v3\",\"optional\":true,\"type\":\"string\"}],\"optional\":true,\"type\":\"struct\"},{\"field\":\"after\",\"fields\":[{\"field\":\"v1\",\"optional\":true,\"type\":\"int32\"},{\"field\":\"v2\",\"optional\":true,\"type\":\"float32\"},{\"field\":\"v3\",\"optional\":true,\"type\":\"string\"}],\"optional\":true,\"type\":\"struct\"}],\"optional\":false,\"type\":\"struct\"}");
        assert_eq!(
            json_chunk[0].as_str(),
            "{\"v1\":0,\"v2\":0.0,\"v3\":{\"v4\":0,\"v5\":0.0}}"
        );

        Ok(())
    }
}
