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

pub mod blackhole;
pub mod boxed;
pub mod catalog;
pub mod clickhouse;
pub mod coordinate;
pub mod encoder;
pub mod formatter;
pub mod iceberg;
pub mod kafka;
pub mod kinesis;
pub mod log_store;
pub mod nats;
pub mod pulsar;
pub mod redis;
pub mod remote;
#[cfg(any(test, madsim))]
pub mod test_sink;
pub mod utils;
pub mod writer;

use std::collections::HashMap;
use std::future::Future;

use ::clickhouse::error::Error as ClickHouseError;
use anyhow::anyhow;
use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use prometheus::{Histogram, HistogramOpts};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::error::{anyhow_error, ErrorCode, RwError};
use risingwave_pb::catalog::PbSinkType;
use risingwave_pb::connector_service::{PbSinkParam, SinkMetadata, TableSchema};
use risingwave_rpc_client::error::RpcError;
use risingwave_rpc_client::{ConnectorClient, MetaClient};
use thiserror::Error;
pub use tracing;

use self::catalog::SinkType;
use self::clickhouse::{ClickHouseConfig, ClickHouseSink};
use self::iceberg::{IcebergSink, ICEBERG_SINK, REMOTE_ICEBERG_SINK};
use self::pulsar::{PulsarConfig, PulsarSink};
use crate::sink::blackhole::{BlackHoleSink, BLACKHOLE_SINK};
use crate::sink::boxed::BoxSink;
use crate::sink::catalog::{SinkCatalog, SinkId};
use crate::sink::clickhouse::CLICKHOUSE_SINK;
use crate::sink::iceberg::{IcebergConfig, RemoteIcebergConfig, RemoteIcebergSink};
use crate::sink::kafka::{KafkaConfig, KafkaSink, KAFKA_SINK};
use crate::sink::kinesis::{KinesisSink, KinesisSinkConfig, KINESIS_SINK};
use crate::sink::log_store::LogReader;
use crate::sink::nats::{NatsConfig, NatsSink, NATS_SINK};
use crate::sink::pulsar::PULSAR_SINK;
use crate::sink::redis::{RedisConfig, RedisSink};
use crate::sink::remote::{CoordinatedRemoteSink, RemoteConfig, RemoteSink};
#[cfg(any(test, madsim))]
use crate::sink::test_sink::{build_test_sink, TEST_SINK_NAME};
use crate::sink::writer::SinkWriter;
use crate::ConnectorParams;

pub const DOWNSTREAM_SINK_KEY: &str = "connector";
pub const SINK_TYPE_OPTION: &str = "type";
pub const SINK_TYPE_APPEND_ONLY: &str = "append-only";
pub const SINK_TYPE_DEBEZIUM: &str = "debezium";
pub const SINK_TYPE_UPSERT: &str = "upsert";
pub const SINK_USER_FORCE_APPEND_ONLY_OPTION: &str = "force_append_only";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkParam {
    pub sink_id: SinkId,
    pub properties: HashMap<String, String>,
    pub columns: Vec<ColumnDesc>,
    pub downstream_pk: Vec<usize>,
    pub sink_type: SinkType,
    pub db_name: String,
    pub sink_from_name: String,
}

impl SinkParam {
    pub fn from_proto(pb_param: PbSinkParam) -> Self {
        let table_schema = pb_param.table_schema.expect("should contain table schema");
        Self {
            sink_id: SinkId::from(pb_param.sink_id),
            properties: pb_param.properties,
            columns: table_schema.columns.iter().map(ColumnDesc::from).collect(),
            downstream_pk: table_schema
                .pk_indices
                .iter()
                .map(|i| *i as usize)
                .collect(),
            sink_type: SinkType::from_proto(
                PbSinkType::from_i32(pb_param.sink_type).expect("should be able to convert"),
            ),
            db_name: pb_param.db_name,
            sink_from_name: pb_param.sink_from_name,
        }
    }

    pub fn to_proto(&self) -> PbSinkParam {
        PbSinkParam {
            sink_id: self.sink_id.sink_id,
            properties: self.properties.clone(),
            table_schema: Some(TableSchema {
                columns: self.columns.iter().map(|col| col.to_protobuf()).collect(),
                pk_indices: self.downstream_pk.iter().map(|i| *i as u32).collect(),
            }),
            sink_type: self.sink_type.to_proto().into(),
            db_name: self.db_name.clone(),
            sink_from_name: self.sink_from_name.clone(),
        }
    }

    pub fn schema(&self) -> Schema {
        Schema {
            fields: self.columns.iter().map(Field::from).collect(),
        }
    }
}

impl From<SinkCatalog> for SinkParam {
    fn from(sink_catalog: SinkCatalog) -> Self {
        let columns = sink_catalog
            .visible_columns()
            .map(|col| col.column_desc.clone())
            .collect();
        Self {
            sink_id: sink_catalog.id,
            properties: sink_catalog.properties,
            columns,
            downstream_pk: sink_catalog.downstream_pk,
            sink_type: sink_catalog.sink_type,
            db_name: sink_catalog.db_name,
            sink_from_name: sink_catalog.sink_from_name,
        }
    }
}

#[derive(Clone)]
pub struct SinkMetrics {
    pub sink_commit_duration_metrics: Histogram,
}

impl Default for SinkMetrics {
    fn default() -> Self {
        SinkMetrics {
            sink_commit_duration_metrics: Histogram::with_opts(HistogramOpts::new(
                "unused", "unused",
            ))
            .unwrap(),
        }
    }
}

#[derive(Clone, Default)]
pub struct SinkWriterParam {
    pub connector_params: ConnectorParams,
    pub executor_id: u64,
    pub vnode_bitmap: Option<Bitmap>,
    pub meta_client: Option<MetaClient>,
    pub sink_metrics: SinkMetrics,
}

#[async_trait]
pub trait Sink {
    type LogSinker: LogSinker;
    type Coordinator: SinkCommitCoordinator;

    async fn validate(&self, client: Option<ConnectorClient>) -> Result<()>;
    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker>;
    async fn new_coordinator(
        &self,
        _connector_client: Option<ConnectorClient>,
    ) -> Result<Self::Coordinator> {
        Err(SinkError::Coordinator(anyhow!("no coordinator")))
    }
}

pub trait LogSinker: Send + 'static {
    fn consume_log_and_sink(
        self,
        log_reader: impl LogReader,
    ) -> impl Future<Output = Result<()>> + Send + 'static;
}

#[async_trait]
pub trait SinkCommitCoordinator {
    /// Initialize the sink committer coordinator
    async fn init(&mut self) -> Result<()>;
    /// After collecting the metadata from each sink writer, a coordinator will call `commit` with
    /// the set of metadata. The metadata is serialized into bytes, because the metadata is expected
    /// to be passed between different gRPC node, so in this general trait, the metadata is
    /// serialized bytes.
    async fn commit(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> Result<()>;
}

pub struct DummySinkCommitCoordinator;

#[async_trait]
impl SinkCommitCoordinator for DummySinkCommitCoordinator {
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn commit(&mut self, _epoch: u64, _metadata: Vec<SinkMetadata>) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug, EnumAsInner)]
pub enum SinkConfig {
    Redis(RedisConfig),
    Kafka(Box<KafkaConfig>),
    Remote(RemoteConfig),
    Kinesis(Box<KinesisSinkConfig>),
    Iceberg(IcebergConfig),
    RemoteIceberg(RemoteIcebergConfig),
    Pulsar(PulsarConfig),
    BlackHole,
    ClickHouse(Box<ClickHouseConfig>),
    Nats(NatsConfig),
    #[cfg(any(test, madsim))]
    Test,
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
            KINESIS_SINK => Ok(SinkConfig::Kinesis(Box::new(
                KinesisSinkConfig::from_hashmap(properties)?,
            ))),
            CLICKHOUSE_SINK => Ok(SinkConfig::ClickHouse(Box::new(
                ClickHouseConfig::from_hashmap(properties)?,
            ))),
            BLACKHOLE_SINK => Ok(SinkConfig::BlackHole),
            PULSAR_SINK => Ok(SinkConfig::Pulsar(PulsarConfig::from_hashmap(properties)?)),
            REMOTE_ICEBERG_SINK => Ok(SinkConfig::RemoteIceberg(
                RemoteIcebergConfig::from_hashmap(properties)?,
            )),
            ICEBERG_SINK => Ok(SinkConfig::Iceberg(IcebergConfig::from_hashmap(
                properties,
            )?)),
            NATS_SINK => Ok(SinkConfig::Nats(NatsConfig::from_hashmap(properties)?)),
            // Only in test or deterministic test, test sink is enabled.
            #[cfg(any(test, madsim))]
            TEST_SINK_NAME => Ok(SinkConfig::Test),
            _ => Ok(SinkConfig::Remote(RemoteConfig::from_hashmap(properties)?)),
        }
    }
}

pub fn build_sink(param: SinkParam) -> Result<SinkImpl> {
    let config = SinkConfig::from_hashmap(param.properties.clone())?;
    SinkImpl::new(config, param)
}

#[derive(Debug)]
pub enum SinkImpl {
    Redis(RedisSink),
    Kafka(KafkaSink),
    Remote(RemoteSink),
    Pulsar(PulsarSink),
    BlackHole(BlackHoleSink),
    Kinesis(KinesisSink),
    ClickHouse(ClickHouseSink),
    Iceberg(IcebergSink),
    Nats(NatsSink),
    RemoteIceberg(RemoteIcebergSink),
    TestSink(BoxSink),
}

impl SinkConfig {
    pub fn get_connector(&self) -> &'static str {
        match self {
            SinkConfig::Kafka(_) => "kafka",
            SinkConfig::Redis(_) => "redis",
            SinkConfig::Remote(_) => "remote",
            SinkConfig::Pulsar(_) => "pulsar",
            SinkConfig::BlackHole => "blackhole",
            SinkConfig::Kinesis(_) => "kinesis",
            SinkConfig::ClickHouse(_) => "clickhouse",
            SinkConfig::Iceberg(_) => "iceberg",
            SinkConfig::Nats(_) => "nats",
            SinkConfig::RemoteIceberg(_) => "iceberg",
            #[cfg(any(test, madsim))]
            SinkConfig::Test => "test",
        }
    }
}

#[macro_export]
macro_rules! dispatch_sink {
    ($impl:expr, $sink:ident, $body:tt) => {{
        use $crate::sink::SinkImpl;

        match $impl {
            SinkImpl::Redis($sink) => $body,
            SinkImpl::Kafka($sink) => $body,
            SinkImpl::Remote($sink) => $body,
            SinkImpl::Pulsar($sink) => $body,
            SinkImpl::BlackHole($sink) => $body,
            SinkImpl::Kinesis($sink) => $body,
            SinkImpl::ClickHouse($sink) => $body,
            SinkImpl::Iceberg($sink) => $body,
            SinkImpl::Nats($sink) => $body,
            SinkImpl::RemoteIceberg($sink) => $body,
            SinkImpl::TestSink($sink) => $body,
        }
    }};
}

impl SinkImpl {
    pub fn new(cfg: SinkConfig, param: SinkParam) -> Result<Self> {
        Ok(match cfg {
            SinkConfig::Redis(cfg) => SinkImpl::Redis(RedisSink::new(cfg, param.schema())?),
            SinkConfig::Kafka(cfg) => SinkImpl::Kafka(KafkaSink::new(*cfg, param)),
            SinkConfig::Kinesis(cfg) => SinkImpl::Kinesis(KinesisSink::new(*cfg, param)),
            SinkConfig::Remote(cfg) => SinkImpl::Remote(RemoteSink::new(cfg, param)),
            SinkConfig::Pulsar(cfg) => SinkImpl::Pulsar(PulsarSink::new(cfg, param)),
            SinkConfig::BlackHole => SinkImpl::BlackHole(BlackHoleSink),
            SinkConfig::ClickHouse(cfg) => SinkImpl::ClickHouse(ClickHouseSink::new(
                *cfg,
                param.schema(),
                param.downstream_pk,
                param.sink_type.is_append_only(),
            )?),
            SinkConfig::Iceberg(cfg) => SinkImpl::Iceberg(IcebergSink::new(cfg, param)?),
            SinkConfig::Nats(cfg) => SinkImpl::Nats(NatsSink::new(
                cfg,
                param.schema(),
                param.sink_type.is_append_only(),
            )),
            SinkConfig::RemoteIceberg(cfg) => {
                SinkImpl::RemoteIceberg(CoordinatedRemoteSink(RemoteSink::new(cfg, param)))
            }
            #[cfg(any(test, madsim))]
            SinkConfig::Test => SinkImpl::TestSink(build_test_sink(param)?),
        })
    }
}

pub type Result<T> = std::result::Result<T, SinkError>;

#[derive(Error, Debug)]
pub enum SinkError {
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("Kinesis error: {0}")]
    Kinesis(anyhow::Error),
    #[error("Remote sink error: {0}")]
    Remote(anyhow::Error),
    #[error("Json parse error: {0}")]
    JsonParse(String),
    #[error("Iceberg error: {0}")]
    Iceberg(anyhow::Error),
    #[error("config error: {0}")]
    Config(#[from] anyhow::Error),
    #[error("coordinator error: {0}")]
    Coordinator(anyhow::Error),
    #[error("ClickHouse error: {0}")]
    ClickHouse(String),
    #[error("Nats error: {0}")]
    Nats(anyhow::Error),
    #[error("Pulsar error: {0}")]
    Pulsar(anyhow::Error),
}

impl From<RpcError> for SinkError {
    fn from(value: RpcError) -> Self {
        SinkError::Remote(anyhow_error!("{}", value))
    }
}

impl From<ClickHouseError> for SinkError {
    fn from(value: ClickHouseError) -> Self {
        SinkError::ClickHouse(format!("{}", value))
    }
}

impl From<SinkError> for RwError {
    fn from(e: SinkError) -> Self {
        ErrorCode::SinkError(Box::new(e)).into()
    }
}
