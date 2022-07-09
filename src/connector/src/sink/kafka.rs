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
use std::sync::Arc;
use std::time::Duration;

use futures::{Future, TryFutureExt};
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
use tokio::task;

use super::{Sink, SinkError};
use crate::sink::{Result, SinkState};

pub const KAFKA_SINK: &str = "kafka";

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    #[serde(rename = "kafka.brokers")]
    pub brokers: String,

    #[serde(rename = "kafka.topic")]
    pub topic: String,

    // Optional. If not specified, the default value is None and messages are sent to random
    // partition. if we want to guarantee exactly once delivery, we need to specify the
    // partition number.
    // (the partition number should set by meta)
    pub partition: Option<i32>,

    #[serde(rename = "sink.type")]
    pub sink_type: String, // accept "append_only" or "debezium"

    pub identifier: String,

    pub timeout: Duration,
    pub max_retry_num: i32,
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
        let sink_type = values.get("sink.type").expect("sink.type must be set");
        if sink_type != "append_only" && sink_type != "debezium" {
            return Err(SinkError::Config(
                "sink.type must be set to \"append_only\" or \"debezium\"".to_string(),
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
            sink_type: sink_type.to_string(),
        })
    }
}

pub struct KafkaSink {
    pub config: KafkaConfig,
    pub conductor: KafkaTransactionConductor,
    latest_success_epoch: u64,
    in_transaction_epoch: Option<u64>,
}

impl KafkaSink {
    pub async fn new(config: KafkaConfig) -> Result<Self> {
        Ok(KafkaSink {
            config: config.clone(),
            conductor: KafkaTransactionConductor::new(config).await?,
            in_transaction_epoch: None,
            latest_success_epoch: 0,
        })
    }

    // any error should report to upper level and requires revert to previous epoch.
    pub async fn do_with_retry<F, FutKR, T>(&self, f: F) -> KafkaResult<T>
    where
        F: Fn(KafkaTransactionConductor) -> FutKR,
        FutKR: Future<Output = KafkaResult<T>>,
    {
        let conductor = self.conductor.clone();
        let mut err_placeholder = KafkaError::Canceled;
        for _ in 0..self.config.max_retry_num {
            match f(conductor.clone()).await {
                Ok(res) => {
                    return Ok(res);
                }
                Err(e) => {
                    err_placeholder = e;
                }
            }
            // a back off policy
            tokio::time::sleep(self.config.retry_interval).await;
        }

        Err(err_placeholder)
    }

    async fn send<'a, K, P>(&self, mut record: BaseRecord<'a, K, P>) -> KafkaResult<()>
    where
        K: ToBytes + ?Sized,
        P: ToBytes + ?Sized,
    {
        let mut err_placeholder = KafkaError::Canceled;

        for _ in 0..self.config.max_retry_num {
            match self.conductor.send(record).await {
                Ok(()) => {
                    return Ok(());
                }
                Err((e, rec)) => {
                    err_placeholder = e;
                    record = rec;
                    if let KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull) =
                        err_placeholder
                    {
                        // if the queue is full, we need to wait for some time and retry.
                        tokio::time::sleep(self.config.retry_interval).await;
                        continue;
                    } else {
                        return Err(err_placeholder);
                    }
                }
            }
        }
        Err(err_placeholder)
    }

    async fn append_only(&self, chunk: StreamChunk, schema: &Schema) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op == Op::Insert {
                let record = Value::Object(record_to_json(row, schema.fields.clone())?).to_string();
                let msg_key = format!(
                    "{}-{}",
                    self.config.identifier,
                    self.in_transaction_epoch.unwrap()
                );
                self.send(
                    BaseRecord::to(self.config.topic.as_str())
                        .key(msg_key.as_bytes())
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
        if self.in_transaction_epoch.unwrap() <= self.latest_success_epoch {
            return Ok(());
        }
        if self.config.sink_type.as_str() == "append_only" {
            self.append_only(chunk, schema).await
        } else if self.config.sink_type.as_str() == "debezium" {
            todo!()
        } else {
            unreachable!()
        }
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.in_transaction_epoch = Some(epoch);
        if self.latest_success_epoch == 0 {
            self.do_with_retry(|conductor| conductor.init_transaction())
                .await
                .map_err(SinkError::Kafka)?;
        }

        self.do_with_retry(|conductor| conductor.start_transaction())
            .await
            .map_err(SinkError::Kafka)?;
        tracing::debug!("begin epoch {:?}", epoch);
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        self.do_with_retry(|conductor| conductor.commit_transaction())
            .await
            .map_err(SinkError::Kafka)?;
        if let Some(epoch) = self.in_transaction_epoch.take() {
            self.latest_success_epoch = epoch;
        } else {
            tracing::error!(
                "commit without begin_epoch, last success epoch {:?}",
                self.latest_success_epoch
            );
            return Err(SinkError::Kafka(KafkaError::Canceled));
        }
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        self.do_with_retry(|conductor| conductor.abort_transaction())
            .await
            .map_err(SinkError::Kafka)
    }

    async fn take_snapshot(&self) -> Result<SinkState> {
        todo!()
    }
}

fn datum_to_json_object(field: &Field, datum: DatumRef) -> ArrayResult<Value> {
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

fn record_to_json(row: RowRef, schema: Vec<Field>) -> Result<Map<String, Value>> {
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

/// the struct conducts all transactions with Kafka
#[derive(Clone)]
pub struct KafkaTransactionConductor {
    pub properties: KafkaConfig,
    pub inner: Arc<ThreadedProducer<DefaultProducerContext>>,
    in_transaction: bool,
}

impl KafkaTransactionConductor {
    async fn new(config: KafkaConfig) -> Result<Self> {
        let inner = ClientConfig::new()
            .set("bootstrap.servers", config.brokers.as_str())
            .set("message.timeout.ms", "5000")
            .set("transactional.id", config.identifier.as_str()) // required by kafka transaction
            .create_with_context(DefaultProducerContext)
            .expect("Producer creation error");

        Ok(KafkaTransactionConductor {
            properties: config,
            inner: Arc::new(inner),
            in_transaction: false,
        })
    }

    fn init_transaction(&self) -> impl Future<Output = KafkaResult<()>> {
        let inner = self.inner.clone();
        let timeout = self.properties.timeout;
        task::spawn_blocking(move || inner.init_transactions(timeout))
            .unwrap_or_else(|_| Err(KafkaError::Canceled))
    }

    fn start_transaction(&self) -> impl Future<Output = KafkaResult<()>> {
        let inner = Arc::clone(&self.inner);
        task::spawn_blocking(move || inner.begin_transaction())
            .unwrap_or_else(|_| Err(KafkaError::Canceled))
    }

    fn commit_transaction(&self) -> impl Future<Output = KafkaResult<()>> {
        let inner = Arc::clone(&self.inner);
        let timeout = self.properties.timeout;
        task::spawn_blocking(move || inner.commit_transaction(timeout))
            .unwrap_or_else(|_| Err(KafkaError::Canceled))
    }

    fn abort_transaction(&self) -> impl Future<Output = KafkaResult<()>> {
        let inner = Arc::clone(&self.inner);
        let timeout = self.properties.timeout;
        task::spawn_blocking(move || inner.abort_transaction(timeout))
            .unwrap_or_else(|_| Err(KafkaError::Canceled))
    }

    async fn flush(&self) -> impl Future<Output = KafkaResult<()>> {
        let inner = Arc::clone(&self.inner);
        let timeout = self.properties.timeout;
        task::spawn_blocking(move || inner.flush(timeout))
            .map_ok(|_| KafkaResult::Ok(()))
            .unwrap_or_else(|_| Err(KafkaError::Canceled))
    }

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

mod test {

    // #[tokio::test]
    // async fn test_kafka_producer() -> Result<()> {
    //     let kafka_config = KafkaConfig {
    //         brokers: "127.0.0.1:29092".to_string(),
    //         topic: "test_producer".to_string(),
    //     };
    //     let sink = KafkaSink::new(kafka_config.clone()).await.unwrap();
    //     let mut content = FutureRecord::to(kafka_config.topic.as_str());

    //     for i in 0..10 {
    //         content = content.payload(format!("{:?}", i).as_str());
    //     }

    //     Ok(())
    // }

    // #[test]
    // fn test_chunk_to_json() -> Result<()> {
    //     let mut column_i32_builder = I32ArrayBuilder::new(10);
    //     for i in 0..10 {
    //         column_i32_builder.append(Some(i)).unwrap();
    //     }
    //     let column_i32 = Column::new(Arc::new(ArrayImpl::from(
    //         column_i32_builder.finish().unwrap(),
    //     )));
    //     let mut column_f32_builder = F32ArrayBuilder::new(10);
    //     for i in 0..10 {
    //         column_f32_builder
    //             .append(Some(OrderedF32::from(i as f32)))
    //             .unwrap();
    //     }
    //     let column_f32 = Column::new(Arc::new(ArrayImpl::from(
    //         column_f32_builder.finish().unwrap(),
    //     )));

    //     let column_struct = Column::new(Arc::new(ArrayImpl::from(StructArray::from_slices(
    //         &[true, true, true, true, true, true, true, true, true, true],
    //         vec![
    //             array! { I32Array, [Some(1), Some(2), Some(3), Some(4), Some(5), Some(6),
    // Some(7), Some(8), Some(9), Some(10)] }.into(),             array! { F32Array, [Some(1.0),
    // Some(2.0), Some(3.0), Some(4.0), Some(5.0), Some(6.0), Some(7.0), Some(8.0), Some(9.0),
    // Some(10.0)] }.into(),         ],
    //         vec![DataType::Int32, DataType::Float32],
    //     )
    //         .unwrap())));

    //     let chunk = DataChunk::new(vec![column_i32, column_f32], Vis::Compact(10));

    //     // let chunk = StreamChunk {};

    //     // let x =
    // chunk.row_at(0).unwrap().0.value_at(2).unwrap().into_scalar_impl().into_struct();     //
    // println! ("{:?}", x);

    //     let schema = Schema::new(vec![
    //         Field {
    //             data_type: DataType::Int32,
    //             name: "v1".into(),
    //             sub_fields: vec![],
    //             type_name: "".into(),
    //         },
    //         Field {
    //             data_type: DataType::Float32,
    //             name: "v2".into(),
    //             sub_fields: vec![],
    //             type_name: "".into(),
    //         },
    //     ]);

    //     println!("{:?}", chunk_to_json(chunk, &schema));

    //     Ok(())
    // }
}
