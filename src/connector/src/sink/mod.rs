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

pub mod catalog;
pub mod kafka;
pub mod redis;
pub mod remote;

use std::collections::HashMap;

use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{Datelike, NaiveDateTime, Timelike};
use enum_as_inner::EnumAsInner;
use risingwave_common::array::{ArrayError, ArrayResult, RowRef, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, RwError};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, DatumRef, ScalarRefImpl, ToText};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_rpc_client::error::RpcError;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use thiserror::Error;
pub use tracing;

use self::catalog::{SinkCatalog, SinkType};
use crate::sink::kafka::{KafkaConfig, KafkaSink, KAFKA_SINK};
use crate::sink::redis::{RedisConfig, RedisSink};
use crate::sink::remote::{RemoteConfig, RemoteSink};
use crate::ConnectorParams;

pub const DOWNSTREAM_SINK_KEY: &str = "connector";
pub const SINK_TYPE_OPTION: &str = "type";
pub const SINK_TYPE_APPEND_ONLY: &str = "append-only";
pub const SINK_TYPE_DEBEZIUM: &str = "debezium";
pub const SINK_TYPE_UPSERT: &str = "upsert";
pub const SINK_USER_FORCE_APPEND_ONLY_OPTION: &str = "force_append_only";

#[async_trait]
pub trait Sink {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()>;

    // the following interface is for transactions, if not supported, return Ok(())
    // start a transaction with epoch number. Note that epoch number should be increasing.
    async fn begin_epoch(&mut self, epoch: u64) -> Result<()>;

    // commits the current transaction and marks all messages in the transaction success.
    async fn commit(&mut self) -> Result<()>;

    // aborts the current transaction because some error happens. we should rollback to the last
    // commit point.
    async fn abort(&mut self) -> Result<()>;
}

#[derive(Clone, Debug, EnumAsInner)]
pub enum SinkConfig {
    Redis(RedisConfig),
    Kafka(Box<KafkaConfig>),
    Remote(RemoteConfig),
    BlackHole,
}

#[derive(Clone, Debug, EnumAsInner, Serialize, Deserialize)]
pub enum SinkState {
    Kafka,
    Redis,
    Remote,
    Blackhole,
}

pub const BLACKHOLE_SINK: &str = "blackhole";

#[derive(Debug)]
pub struct BlockHoleSink;

#[async_trait]
impl Sink for BlockHoleSink {
    async fn write_batch(&mut self, _chunk: StreamChunk) -> Result<()> {
        Ok(())
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

impl SinkConfig {
    pub fn from_hashmap(mut properties: HashMap<String, String>) -> Result<Self> {
        const CONNECTOR_TYPE_KEY: &str = "connector";
        const CONNECTION_NAME_KEY: &str = "connection.name";
        const PRIVATE_LINK_TARGET_KEY: &str = "privatelink.targets";

        // remove privatelink related properties if any
        properties.remove(PRIVATE_LINK_TARGET_KEY);
        properties.remove(CONNECTION_NAME_KEY);

        let sink_type = properties
            .get(CONNECTOR_TYPE_KEY)
            .ok_or_else(|| SinkError::Config(anyhow!("missing config: {}", CONNECTOR_TYPE_KEY)))?;
        match sink_type.to_lowercase().as_str() {
            KAFKA_SINK => Ok(SinkConfig::Kafka(Box::new(KafkaConfig::from_hashmap(
                properties,
            )?))),
            BLACKHOLE_SINK => Ok(SinkConfig::BlackHole),
            _ => Ok(SinkConfig::Remote(RemoteConfig::from_hashmap(properties)?)),
        }
    }

    pub fn get_connector(&self) -> &'static str {
        match self {
            SinkConfig::Kafka(_) => "kafka",
            SinkConfig::Redis(_) => "redis",
            SinkConfig::Remote(_) => "remote",
            SinkConfig::BlackHole => "blackhole",
        }
    }
}

#[derive(Debug)]
pub enum SinkImpl {
    Redis(RedisSink),
    Kafka(KafkaSink<true>),
    UpsertKafka(KafkaSink<false>),
    Remote(RemoteSink<true>),
    UpsertRemote(RemoteSink<false>),
    BlackHole(BlockHoleSink),
}

#[macro_export]
macro_rules! dispatch_sink {
    ($impl:expr, $sink:ident, $body:tt) => {{
        use $crate::sink::SinkImpl;

        match $impl {
            SinkImpl::Redis($sink) => $body,
            SinkImpl::Kafka($sink) => $body,
            SinkImpl::UpsertKafka($sink) => $body,
            SinkImpl::Remote($sink) => $body,
            SinkImpl::UpsertRemote($sink) => $body,
            SinkImpl::BlackHole($sink) => $body,
        }
    }};
}

impl SinkImpl {
    pub async fn new(
        cfg: SinkConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        connector_params: ConnectorParams,
        sink_type: SinkType,
        sink_id: u64,
    ) -> Result<Self> {
        Ok(match cfg {
            SinkConfig::Redis(cfg) => SinkImpl::Redis(RedisSink::new(cfg, schema)?),
            SinkConfig::Kafka(cfg) => {
                if sink_type.is_append_only() {
                    // Append-only Kafka sink
                    SinkImpl::Kafka(KafkaSink::<true>::new(*cfg, schema, pk_indices).await?)
                } else {
                    // Upsert Kafka sink
                    SinkImpl::UpsertKafka(KafkaSink::<false>::new(*cfg, schema, pk_indices).await?)
                }
            }
            SinkConfig::Remote(cfg) => {
                if sink_type.is_append_only() {
                    // Append-only remote sink
                    SinkImpl::Remote(
                        RemoteSink::<true>::new(cfg, schema, pk_indices, connector_params, sink_id)
                            .await?,
                    )
                } else {
                    // Upsert remote sink
                    SinkImpl::UpsertRemote(
                        RemoteSink::<false>::new(
                            cfg,
                            schema,
                            pk_indices,
                            connector_params,
                            sink_id,
                        )
                        .await?,
                    )
                }
            }
            SinkConfig::BlackHole => SinkImpl::BlackHole(BlockHoleSink),
        })
    }

    pub async fn validate(
        cfg: SinkConfig,
        sink_catalog: SinkCatalog,
        connector_rpc_endpoint: Option<String>,
    ) -> Result<()> {
        match cfg {
            SinkConfig::Redis(cfg) => RedisSink::new(cfg, sink_catalog.schema()).map(|_| ()),
            SinkConfig::Kafka(cfg) => {
                if sink_catalog.sink_type.is_append_only() {
                    KafkaSink::<true>::validate(*cfg, sink_catalog.downstream_pk_indices()).await
                } else {
                    KafkaSink::<false>::validate(*cfg, sink_catalog.downstream_pk_indices()).await
                }
            }
            SinkConfig::Remote(cfg) => {
                if sink_catalog.sink_type.is_append_only() {
                    RemoteSink::<true>::validate(cfg, sink_catalog, connector_rpc_endpoint).await
                } else {
                    RemoteSink::<false>::validate(cfg, sink_catalog, connector_rpc_endpoint).await
                }
            }
            SinkConfig::BlackHole => Ok(()),
        }
    }
}

pub type Result<T> = std::result::Result<T, SinkError>;

#[derive(Error, Debug)]
pub enum SinkError {
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("Remote sink error: {0}")]
    Remote(String),
    #[error("Json parse error: {0}")]
    JsonParse(String),
    #[error("config error: {0}")]
    Config(#[from] anyhow::Error),
}

impl From<RpcError> for SinkError {
    fn from(value: RpcError) -> Self {
        SinkError::Remote(format!("{}", value))
    }
}

impl From<SinkError> for RwError {
    fn from(e: SinkError) -> Self {
        ErrorCode::SinkError(Box::new(e)).into()
    }
}

pub fn record_to_json(row: RowRef<'_>, schema: &[Field]) -> Result<Map<String, Value>> {
    let mut mappings = Map::with_capacity(schema.len());
    for (field, datum_ref) in schema.iter().zip_eq_fast(row.iter()) {
        let key = field.name.clone();
        let value = datum_to_json_object(field, datum_ref)
            .map_err(|e| SinkError::JsonParse(e.to_string()))?;
        mappings.insert(key, value);
    }
    Ok(mappings)
}

fn datum_to_json_object(field: &Field, datum: DatumRef<'_>) -> ArrayResult<Value> {
    let scalar_ref = match datum {
        None => return Ok(Value::Null),
        Some(datum) => datum,
    };

    let data_type = field.data_type();

    tracing::debug!("datum_to_json_object: {:?}, {:?}", data_type, scalar_ref);

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
            json!(v.to_text())
        }
        (DataType::Timestamptz, ScalarRefImpl::Int64(v)) => {
            // risingwave's timestamp with timezone is stored in UTC and does not maintain the
            // timezone info and the time is in microsecond.
            let secs = v.div_euclid(1_000_000);
            let nsecs = v.rem_euclid(1_000_000) * 1000;
            let parsed = NaiveDateTime::from_timestamp_opt(secs, nsecs as u32).unwrap();
            let v = parsed.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
            json!(v)
        }
        (DataType::Time, ScalarRefImpl::Time(v)) => {
            // todo: just ignore the nanos part to avoid leap second complex
            json!(v.0.num_seconds_from_midnight() as i64 * 1000)
        }
        (DataType::Date, ScalarRefImpl::Date(v)) => {
            json!(v.0.num_days_from_ce())
        }
        (DataType::Timestamp, ScalarRefImpl::Timestamp(v)) => {
            json!(v.0.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
        }
        (DataType::Bytea, ScalarRefImpl::Bytea(v)) => {
            json!(hex::encode(v))
        }
        // P<years>Y<months>M<days>DT<hours>H<minutes>M<seconds>S
        (DataType::Interval, ScalarRefImpl::Interval(v)) => {
            json!(v.as_iso_8601())
        }
        (DataType::Jsonb, ScalarRefImpl::Jsonb(jsonb_ref)) => {
            json!(jsonb_ref.to_string())
        }
        (DataType::List(datatype), ScalarRefImpl::List(list_ref)) => {
            let elems = list_ref.iter();
            let mut vec = Vec::with_capacity(elems.len());
            let inner_field = Field::unnamed(Box::<DataType>::into_inner(datatype));
            for sub_datum_ref in elems {
                let value = datum_to_json_object(&inner_field, sub_datum_ref)?;
                vec.push(value);
            }
            json!(vec)
        }
        (DataType::Struct(st), ScalarRefImpl::Struct(struct_ref)) => {
            let mut map = Map::with_capacity(st.fields.len());
            for (sub_datum_ref, sub_field) in struct_ref.iter_fields_ref().zip_eq_fast(
                st.fields
                    .iter()
                    .zip_eq_fast(st.field_names.iter())
                    .map(|(dt, name)| Field::with_name(dt.clone(), name)),
            ) {
                let value = datum_to_json_object(&sub_field, sub_datum_ref)?;
                map.insert(sub_field.name.clone(), value);
            }
            json!(map)
        }
        (data_type, scalar_ref) => {
            return Err(ArrayError::internal(
                format!("datum_to_json_object: unsupported data type: field name: {:?}, logical type: {:?}, physical type: {:?}", field.name, data_type, scalar_ref),
            ));
        }
    };

    Ok(value)
}

#[cfg(test)]
mod tests {

    use risingwave_common::cast::str_with_time_zone_to_timestamptz;
    use risingwave_common::types::{Interval, ScalarImpl, Time, Timestamp};

    use super::*;
    #[test]
    fn test_to_json_basic_type() {
        let mock_field = Field {
            data_type: DataType::Boolean,
            name: Default::default(),
            sub_fields: Default::default(),
            type_name: Default::default(),
        };
        let boolean_value = datum_to_json_object(
            &Field {
                data_type: DataType::Boolean,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Bool(false).as_scalar_ref_impl()),
        )
        .unwrap();
        assert_eq!(boolean_value, json!(false));

        let int16_value = datum_to_json_object(
            &Field {
                data_type: DataType::Int16,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Int16(16).as_scalar_ref_impl()),
        )
        .unwrap();
        assert_eq!(int16_value, json!(16));

        let int64_value = datum_to_json_object(
            &Field {
                data_type: DataType::Int64,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Int64(std::i64::MAX).as_scalar_ref_impl()),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_string(&int64_value).unwrap(),
            std::i64::MAX.to_string()
        );

        // https://github.com/debezium/debezium/blob/main/debezium-core/src/main/java/io/debezium/time/ZonedTimestamp.java
        let tstz_str = "2018-01-26T18:30:09.453Z";
        let tstz_inner = str_with_time_zone_to_timestamptz(tstz_str).unwrap();
        let tstz_value = datum_to_json_object(
            &Field {
                data_type: DataType::Timestamptz,
                ..mock_field.clone()
            },
            Some(ScalarImpl::Int64(tstz_inner).as_scalar_ref_impl()),
        )
        .unwrap();
        assert_eq!(tstz_value, "2018-01-26 18:30:09.453000");

        let ts_value = datum_to_json_object(
            &Field {
                data_type: DataType::Timestamp,
                ..mock_field.clone()
            },
            Some(
                ScalarImpl::Timestamp(Timestamp::from_timestamp_uncheck(1000, 0))
                    .as_scalar_ref_impl(),
            ),
        )
        .unwrap();
        assert_eq!(ts_value, json!("1970-01-01 00:16:40.000000".to_string()));

        // Represents the number of microseconds past midnigh, io.debezium.time.Time
        let time_value = datum_to_json_object(
            &Field {
                data_type: DataType::Time,
                ..mock_field.clone()
            },
            Some(
                ScalarImpl::Time(Time::from_num_seconds_from_midnight_uncheck(1000, 0))
                    .as_scalar_ref_impl(),
            ),
        )
        .unwrap();
        assert_eq!(time_value, json!(1000 * 1000));

        let interval_value = datum_to_json_object(
            &Field {
                data_type: DataType::Interval,
                ..mock_field
            },
            Some(
                ScalarImpl::Interval(Interval::from_month_day_usec(13, 2, 1000000))
                    .as_scalar_ref_impl(),
            ),
        )
        .unwrap();
        assert_eq!(interval_value, json!("P1Y1M2DT0H0M1S"));
    }
}
