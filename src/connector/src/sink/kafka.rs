// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

use itertools::Itertools;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::ToBytes;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::ClientConfig;
use risingwave_common::array::{ArrayResult, Op, RowRef, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, DatumRef, ScalarRefImpl};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use tracing::warn;

use super::{Sink, SinkError};
use crate::sink::Result;

pub const KAFKA_SINK: &str = "kafka";

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    #[serde(rename = "kafka.brokers")]
    pub brokers: String,

    #[serde(rename = "kafka.topic")]
    pub topic: String,

    // Optional. If not specified, the default value is None and messages are sent to random
    // partition. if we want to guarantee exactly once delivery, we need to specify the
    // partition number. The partition number should set by meta.
    pub partition: Option<i32>,

    pub format: String, // accept "append_only" or "debezium"

    pub identifier: String,

    pub timeout: Duration,
    pub max_retry_num: u32,
    pub retry_interval: Duration,
}

impl KafkaConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        let brokers = values
            .get("kafka.brokers")
            .expect("kafka.brokers must be set");
        let identifier = values
            .get("identifier")
            .expect("kafka.identifier must be set");
        let format = values.get("format").expect("format must be set");
        if format != "append_only" && format != "debezium" {
            return Err(SinkError::Config(
                "format must be set to \"append_only\" or \"debezium\"".to_string(),
            ));
        }

        let topic = values.get("kafka.topic").expect("kafka.topic must be set");

        Ok(KafkaConfig {
            brokers: brokers.to_string(),
            topic: topic.to_string(),
            identifier: identifier.to_owned(),
            partition: None,
            timeout: Duration::from_secs(5), // default timeout is 5 seconds
            max_retry_num: 3,                // default max retry num is 3
            retry_interval: Duration::from_millis(100), // default retry interval is 100ms
            format: format.to_string(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, enum_as_inner::EnumAsInner)]
enum KafkaSinkState {
    Init,
    // State running with epoch.
    Running(u64),
}

pub struct KafkaSink {
    pub config: KafkaConfig,
    pub conductor: KafkaTransactionConductor,
    state: KafkaSinkState,
    in_transaction_epoch: Option<u64>,
}

impl KafkaSink {
    pub async fn new(config: KafkaConfig) -> Result<Self> {
        Ok(KafkaSink {
            config: config.clone(),
            conductor: KafkaTransactionConductor::new(config).await?,
            in_transaction_epoch: None,
            state: KafkaSinkState::Init,
        })
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

    async fn debezium_update(&self, chunk: StreamChunk, schema: &Schema, ts_ms: u64) -> Result<()> {
        let mut update_cache: Option<Map<String, Value>> = None;
        for (op, row) in chunk.rows() {
            let event_object = match op {
                Op::Insert => Some(json!({
                    "schema": schema_to_json(schema),
                    "payload": {
                        "before": null,
                        "after": record_to_json(row.clone(), schema.fields.clone())?,
                        "op": "c",
                        "ts_ms": ts_ms,
                    }
                })),
                Op::Delete => Some(json!({
                    "schema": schema_to_json(schema),
                    "payload": {
                        "before": record_to_json(row.clone(), schema.fields.clone())?,
                        "after": null,
                        "op": "d",
                        "ts_ms": ts_ms,
                    }
                })),
                Op::UpdateDelete => {
                    update_cache = Some(record_to_json(row.clone(), schema.fields.clone())?);
                    continue;
                }
                Op::UpdateInsert => {
                    if let Some(before) = update_cache.take() {
                        Some(json!({
                            "schema": schema_to_json(schema),
                            "payload": {
                                "before": before,
                                "after": record_to_json(row.clone(), schema.fields.clone())?,
                                "op": "u",
                                "ts_ms": ts_ms,
                            }
                        }))
                    } else {
                        warn!(
                            "not found UpdateDelete in prev row, skipping, row_id {:?}",
                            row.index()
                        );
                        continue;
                    }
                }
            };
            if let Some(obj) = event_object {
                self.send(
                    BaseRecord::to(self.config.topic.as_str())
                        .key(self.gen_message_key().as_bytes())
                        .payload(obj.to_string().as_bytes()),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn append_only(&self, chunk: StreamChunk, schema: &Schema) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op == Op::Insert {
                let record = Value::Object(record_to_json(row, schema.fields.clone())?).to_string();
                self.send(
                    BaseRecord::to(self.config.topic.as_str())
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
impl Sink for KafkaSink {
    async fn write_batch(&mut self, chunk: StreamChunk, schema: &Schema) -> Result<()> {
        // when sinking the snapshot, it is required to begin epoch 0 for transaction
        // if let (KafkaSinkState::Running(epoch), in_txn_epoch) = (&self.state,
        // &self.in_transaction_epoch.unwrap()) && in_txn_epoch <= epoch {     return Ok(())
        // }

        println!("sink chunk {:?}", chunk);

        match self.config.format.as_str() {
            "append_only" => self.append_only(chunk, schema).await,
            "debezium" => {
                self.debezium_update(
                    chunk,
                    schema,
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                )
                .await
            }
            _ => unreachable!(),
        }
    }

    //  Note that epoch 0 is reserved for initializing, so we should not use epoch 0 for
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

impl Debug for KafkaSink {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!();
    }
}

fn datum_to_json_object(field: &Field, datum: DatumRef<'_>) -> ArrayResult<Value> {
    let scalar_ref = match datum {
        None => return Ok(Value::Null),
        Some(datum) => datum,
    };

    let data_type = field.data_type();

    let value = match (data_type, scalar_ref) {
        (DataType::Boolean, ScalarRefImpl::Bool(v)) => {
            json!(v)
        }
        (DataType::Int16, ScalarRefImpl::Int16(v)) => {
            json!(v)
        }
        (DataType::Int32, ScalarRefImpl::Int32(v)) => {
            json!(v)
        }
        (DataType::Int64, ScalarRefImpl::Int64(v)) => {
            json!(v)
        }
        (DataType::Float32, ScalarRefImpl::Float32(v)) => {
            json!(f32::from(v))
        }
        (DataType::Float64, ScalarRefImpl::Float64(v)) => {
            json!(f64::from(v))
        }
        (DataType::Varchar, ScalarRefImpl::Utf8(v)) => {
            json!(v)
        }
        (DataType::Decimal, ScalarRefImpl::Decimal(v)) => {
            // fixme
            json!(v.to_string())
        }
        (DataType::Time, ScalarRefImpl::NaiveTime(_v)) => {
            unimplemented!()
        }
        (DataType::List { .. }, ScalarRefImpl::List(list_ref)) => {
            let mut vec = Vec::with_capacity(field.sub_fields.len());
            for (sub_datum_ref, sub_field) in list_ref
                .values_ref()
                .into_iter()
                .zip_eq(field.sub_fields.iter())
            {
                let value = datum_to_json_object(sub_field, sub_datum_ref)?;
                vec.push(value);
            }
            json!(vec)
        }
        (DataType::Struct { .. }, ScalarRefImpl::Struct(struct_ref)) => {
            let mut map = Map::with_capacity(field.sub_fields.len());
            for (sub_datum_ref, sub_field) in struct_ref
                .fields_ref()
                .into_iter()
                .zip_eq(field.sub_fields.iter())
            {
                let value = datum_to_json_object(sub_field, sub_datum_ref)?;
                map.insert(sub_field.name.clone(), value);
            }
            json!(map)
        }
        _ => unimplemented!(),
    };

    Ok(value)
}

fn record_to_json(row: RowRef<'_>, schema: Vec<Field>) -> Result<Map<String, Value>> {
    let mut mappings = Map::with_capacity(schema.len());
    for (field, datum_ref) in schema.iter().zip_eq(row.values()) {
        let key = field.name.clone();
        let value = datum_to_json_object(field, datum_ref)
            .map_err(|e| SinkError::JsonParse(e.to_string()))?;
        mappings.insert(key, value);
    }
    Ok(mappings)
}

pub fn chunk_to_json(chunk: StreamChunk, schema: &Schema) -> Result<Vec<String>> {
    let mut records: Vec<String> = Vec::with_capacity(chunk.capacity());
    for (_, row) in chunk.rows() {
        let record = Value::Object(record_to_json(row, schema.fields.clone())?);
        records.push(record.to_string());
    }

    Ok(records)
}

fn fields_to_json(fields: &[Field]) -> Value {
    let mut res = Vec::new();
    fields.iter().for_each(|field| {
        res.push(json!({
            "field": field.name,
            "optional": true,
            "type": field.type_name,
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
    inner: ThreadedProducer<DefaultProducerContext>,
}

impl KafkaTransactionConductor {
    async fn new(config: KafkaConfig) -> Result<Self> {
        let inner: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", "5000")
            .set("transactional.id", &config.identifier) // required by kafka transaction
            .create()
            .await?;

        inner.init_transactions(config.timeout).await?;

        Ok(KafkaTransactionConductor {
            properties: config,
            inner,
        })
    }

    #[expect(clippy::unused_async)]
    async fn start_transaction(&self) -> KafkaResult<()> {
        self.inner.begin_transaction()
    }

    async fn commit_transaction(&self) -> KafkaResult<()> {
        self.inner.commit_transaction(self.properties.timeout).await
    }

    async fn abort_transaction(&self) -> KafkaResult<()> {
        self.inner.abort_transaction(self.properties.timeout).await
    }

    async fn flush(&self) -> KafkaResult<()> {
        self.inner.flush(self.properties.timeout).await;
        Ok(())
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
    use std::sync::Arc;

    use maplit::hashmap;
    use risingwave_common::array;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{
        ArrayBuilder, ArrayImpl, F32Array, F32ArrayBuilder, I32Array, I32ArrayBuilder, StructArray,
    };
    use risingwave_common::types::OrderedF32;

    use super::*;

    #[ignore]
    #[tokio::test]
    async fn test_kafka_producer() -> Result<()> {
        let properties = hashmap! {
            "kafka.brokers".to_string() => "localhost:29092".to_string(),
            "identifier".to_string() => "test_sink_1".to_string(),
            "sink.type".to_string() => "append_only".to_string(),
            "kafka.topic".to_string() => "test_topic".to_string(),
        };
        let kafka_config = KafkaConfig::from_hashmap(properties)?;
        let mut sink = KafkaSink::new(kafka_config.clone()).await.unwrap();

        for i in 0..10 {
            let mut fail_flag = false;
            sink.begin_epoch(i).await?;
            for i in 0..100 {
                match sink
                    .send(
                        BaseRecord::to(kafka_config.topic.as_str())
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
        let mut column_i32_builder = I32ArrayBuilder::new(10);
        for i in 0..10 {
            column_i32_builder.append(Some(i));
        }
        let column_i32 = Column::new(Arc::new(ArrayImpl::from(column_i32_builder.finish())));
        let mut column_f32_builder = F32ArrayBuilder::new(10);
        for i in 0..10 {
            column_f32_builder.append(Some(OrderedF32::from(i as f32)));
        }
        let column_f32 = Column::new(Arc::new(ArrayImpl::from(column_f32_builder.finish())));

        let column_struct = Column::new(Arc::new(ArrayImpl::from(
            StructArray::from_slices(
                &[true, true, true, true, true, true, true, true, true, true],
                vec![
                    array! { I32Array, [Some(1), Some(2), Some(3), Some(4), Some(5), Some(6), Some(7), Some(8), Some(9), Some(10)] }.into(),
                    array! { F32Array, [Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0), Some(6.0), Some(7.0), Some(8.0), Some(9.0),Some(10.0)] }.into(),
                ],
                vec![DataType::Int32, DataType::Float32],
            ),
        )));
        let ops = vec![
            Op::Insert,
            Op::Insert,
            Op::Insert,
            Op::Insert,
            Op::Insert,
            Op::Insert,
            Op::Insert,
            Op::Insert,
            Op::Insert,
            Op::Insert,
        ];

        let chunk = StreamChunk::new(ops, vec![column_i32, column_f32, column_struct], None);

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
        assert_eq!(schema_json.to_string(), "{\"fields\":[{\"field\":\"before\",\"fields\":[{\"field\":\"v1\",\"optional\":true,\"type\":\"\"},{\"field\":\"v2\",\"optional\":true,\"type\":\"\"},{\"field\":\"v3\",\"optional\":true,\"type\":\"\"}],\"optional\":true,\"type\":\"struct\"},{\"field\":\"after\",\"fields\":[{\"field\":\"v1\",\"optional\":true,\"type\":\"\"},{\"field\":\"v2\",\"optional\":true,\"type\":\"\"},{\"field\":\"v3\",\"optional\":true,\"type\":\"\"}],\"optional\":true,\"type\":\"struct\"}],\"optional\":false,\"type\":\"struct\"}");
        assert_eq!(
            json_chunk[0].as_str(),
            "{\"v1\":0,\"v2\":0.0,\"v3\":{\"v4\":1,\"v5\":1.0}}"
        );

        Ok(())
    }
}
