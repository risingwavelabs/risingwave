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

use aws_sdk_kinesis::model::record;
use aws_sdk_s3::model::FilterRuleName;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::producer::{DefaultProducerContext, FutureProducer};
use rdkafka::ClientConfig;
use risingwave_common::array::{ArrayBuilder, StreamChunk, RowRef};
use risingwave_common::catalog::{Schema, Field};
use risingwave_common::error::{ErrorCode, RwError};
use risingwave_common::types::DataType;
use serde::Deserialize;
use serde_json::{Value, Map, json};

use risingwave_common::types::{OrderedF32, OrderedF64};

use super::{Sink, SinkError};
use crate::sink::Result;

pub const KAFKA_SINK: &str = "kafka";

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    #[serde(rename = "kafka.brokers")]
    pub brokers: String,

    #[serde(rename = "kafka.topic")]
    pub topic: String,
}

impl KafkaConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        let brokers = values
            .get("kafka.brokers")
            .expect("kafka.brokers must be set");
        let topic = values.get("kafka.topic").expect("kafka.topic must be set");

        Ok(KafkaConfig {
            brokers: brokers.to_string(),
            topic: topic.to_string(),
        })
    }
}

pub struct KafkaSink {
    pub properties: KafkaConfig,
    pub producer: FutureProducer,
}

macro_rules! json_type_cast_impl {
    ( [], $({ $original_type:ident, $scalar:ty })* ) => {
        $( $original_type => { json!( $scalar::try_from(row.value_at(idx).unwrap()).map_err(|e| SinkError::JsonParse(e.to_string()))?) }, )*
    };
}

fn record_to_json(row: RowRef, schema: Vec<Field>) -> Result<Map<String, Value>> {
    let mut mappings = Map::with_capacity(schema.len());
    for (idx, field) in schema.iter().enumerate() {
        let key = field.name.clone();

        let value: Value = match field.data_type {
            DataType::Boolean => { json!(bool::try_from(row.value_at(idx).unwrap()).map_err(|e| SinkError::JsonParse(e.to_string()))?) },
            DataType::Int16 => { json!(i16::try_from(row.value_at(idx).unwrap()).map_err(|e| SinkError::JsonParse(e.to_string()))?) },
            DataType::Int32 => { json!(i32::try_from(row.value_at(idx).unwrap()).map_err(|e| SinkError::JsonParse(e.to_string()))?) },
            DataType::Int64 => { json!(i64::try_from(row.value_at(idx).unwrap()).map_err(|e| SinkError::JsonParse(e.to_string()))?) },
            DataType::Float32 => { json!(f32::from(OrderedF32::try_from(row.value_at(idx).unwrap()).map_err(|e| SinkError::JsonParse(e.to_string()))?)) },
            DataType::Float64 => { json!(f64::from(OrderedF64::try_from(row.value_at(idx).unwrap()).map_err(|e| SinkError::JsonParse(e.to_string()))?)) },
            DataType::Varchar => { json!(row.value_at(idx).unwrap().into_scalar_impl().to_string()) }
            // DataType::List { datatype } => {}
            // DataType::Struct { fields: _ } => {
            //     let x = row.value_at(idx).unwrap().into_scalar_impl().to_string();
            //     let obj_value = record_to_json(, field.sub_fields.clone())?;
            //     Value::Object(obj_value)
            // },
            _ => unimplemented!(),
        };
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

impl KafkaSink {
    async fn new(config: KafkaConfig) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", config.brokers.as_str())
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");

        Ok(KafkaSink {
            properties: config,
            producer,
        })
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl Sink for KafkaSink {
    async fn write_batch(&mut self, chunk: StreamChunk, _schema: &Schema) -> Result<()> {
        Ok(())
    }
}

mod test {
    use rdkafka::producer::FutureRecord;
    use risingwave_common::array;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{
        ArrayImpl, ArrayMeta, DataChunk, F32Array, F32ArrayBuilder, I32Array, I32ArrayBuilder,
        PrimitiveArrayBuilder, StructArray, StructArrayBuilder, Vis,
    };
    use risingwave_common::types::{DataType, OrderedF32};

    use super::*;

    #[tokio::test]
    async fn test_kafka_producer() -> Result<()> {
        let kafka_config = KafkaConfig {
            brokers: "127.0.0.1:29092".to_string(),
            topic: "test_producer".to_string(),
        };
        let sink = KafkaSink::new(kafka_config).await.unwrap();

        // let content = FutureRecord::to("test_producer").payload("1");

        Ok(())
    }

    #[test]
    fn test_chunk_to_json() -> Result<()> {
        let mut column_i32_builder = I32ArrayBuilder::new(10);
        for i in 0..10 {
            column_i32_builder.append(Some(i)).unwrap();
        }
        let column_i32 = Column::new(Arc::new(ArrayImpl::from(
            column_i32_builder.finish().unwrap(),
        )));
        let mut column_f32_builder = F32ArrayBuilder::new(10);
        for i in 0..10 {
            column_f32_builder
                .append(Some(OrderedF32::from(i as f32)))
                .unwrap();
        }
        let column_f32 = Column::new(Arc::new(ArrayImpl::from(
            column_f32_builder.finish().unwrap(),
        )));

        let column_struct = Column::new(Arc::new(ArrayImpl::from(StructArray::from_slices(
            &[true, true, true, true, true, true, true, true, true, true],
            vec![
                array! { I32Array, [Some(1), Some(2), Some(3), Some(4), Some(5), Some(6), Some(7), Some(8), Some(9), Some(10)] }.into(),
                array! { F32Array, [Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0), Some(6.0), Some(7.0), Some(8.0), Some(9.0), Some(10.0)] }.into(),
            ],
            vec![DataType::Int32, DataType::Float32],
        )
        .unwrap())));

        let chunk = DataChunk::new(
            vec![column_i32, column_f32],
            Vis::Compact(10),
        );

        let chunk = StreamChunk {
            
        };

        // let x = chunk.row_at(0).unwrap().0.value_at(2).unwrap().into_scalar_impl().into_struct();
        // println!("{:?}", x);

        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32, 
                name: "v1".into(),
                sub_fields: vec![],
                type_name: "".into(),
            }, Field {
                data_type: DataType::Float32, 
                name: "v2".into(),
                sub_fields: vec![],
                type_name: "".into(),
            }
        ]);

        println!("{:?}", chunk_to_json(chunk, &schema));

        Ok(())
    }
}
