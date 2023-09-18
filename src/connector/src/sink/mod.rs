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

pub mod boxed;
pub mod catalog;
pub mod clickhouse;
pub mod coordinate;
pub mod encoder;
pub mod formatter;
pub mod iceberg;
pub mod kafka;
pub mod kinesis;
pub mod nats;
pub mod redis;
pub mod remote;
#[cfg(any(test, madsim))]
pub mod test_sink;
pub mod utils;

use std::collections::HashMap;
use std::sync::Arc;

use ::clickhouse::error::Error as ClickHouseError;
use anyhow::anyhow;
use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use risingwave_common::array::StreamChunk;
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
use self::encoder::SerTo;
use self::formatter::SinkFormatter;
use self::iceberg::{IcebergSink, ICEBERG_SINK, REMOTE_ICEBERG_SINK};
use crate::sink::boxed::BoxSink;
use crate::sink::catalog::{SinkCatalog, SinkId};
use crate::sink::clickhouse::CLICKHOUSE_SINK;
use crate::sink::iceberg::{IcebergConfig, RemoteIcebergConfig, RemoteIcebergSink};
use crate::sink::kafka::{KafkaConfig, KafkaSink, KAFKA_SINK};
use crate::sink::kinesis::{KinesisSink, KinesisSinkConfig, KINESIS_SINK};
use crate::sink::nats::{NatsConfig, NatsSink, NATS_SINK};
use crate::sink::redis::{RedisConfig, RedisSink};
use crate::sink::remote::{CoordinatedRemoteSink, RemoteConfig, RemoteSink};
#[cfg(any(test, madsim))]
use crate::sink::test_sink::{build_test_sink, TEST_SINK_NAME};
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

#[derive(Clone, Default)]
pub struct SinkWriterParam {
    pub connector_params: ConnectorParams,
    pub executor_id: u64,
    pub vnode_bitmap: Option<Bitmap>,
    pub meta_client: Option<MetaClient>,
}

#[async_trait]
pub trait Sink {
    type Writer: SinkWriter<CommitMetadata = ()>;
    type Coordinator: SinkCommitCoordinator;

    async fn validate(&self, client: Option<ConnectorClient>) -> Result<()>;
    async fn new_writer(&self, writer_param: SinkWriterParam) -> Result<Self::Writer>;
    async fn new_coordinator(
        &self,
        _connector_client: Option<ConnectorClient>,
    ) -> Result<Self::Coordinator> {
        Err(SinkError::Coordinator(anyhow!("no coordinator")))
    }
}

#[async_trait]
pub trait SinkWriter: Send + 'static {
    type CommitMetadata: Send = ();
    /// Begin a new epoch
    async fn begin_epoch(&mut self, epoch: u64) -> Result<()>;

    /// Write a stream chunk to sink
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()>;

    /// Receive a barrier and mark the end of current epoch. When `is_checkpoint` is true, the sink
    /// writer should commit the current epoch.
    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Self::CommitMetadata>;

    /// Clean up
    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    /// Update the vnode bitmap of current sink writer
    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
// An old version of SinkWriter for backward compatibility
pub trait SinkWriterV1: Send + 'static {
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

/// A free-form sink that may output in multiple formats and encodings. Examples include kafka,
/// kinesis, nats and redis.
///
/// The implementor specifies required key & value type (likely string or bytes), as well as how to
/// write a single pair. The provided `write_chunk` method would handle the interaction with a
/// `SinkFormatter`.
///
/// Currently kafka takes `&mut self` while kinesis takes `&self`. So we use `&mut self` in trait
/// but implement it for `&Kinesis`. This allows us to hold `&mut &Kinesis` and `&Kinesis`
/// simultaneously, preventing the schema clone issue propagating from kafka to kinesis.
pub trait FormattedSink {
    type K;
    type V;
    async fn write_one(&mut self, k: Option<Self::K>, v: Option<Self::V>) -> Result<()>;

    async fn write_chunk<F: SinkFormatter>(
        &mut self,
        chunk: StreamChunk,
        formatter: F,
    ) -> Result<()>
    where
        F::K: SerTo<Self::K>,
        F::V: SerTo<Self::V>,
    {
        for r in formatter.format_chunk(&chunk) {
            let (event_key_object, event_object) = r?;

            self.write_one(
                event_key_object.map(SerTo::ser_to).transpose()?,
                event_object.map(SerTo::ser_to).transpose()?,
            )
            .await?;
        }

        Ok(())
    }
}

pub struct SinkWriterV1Adapter<W: SinkWriterV1> {
    is_empty: bool,
    epoch: u64,
    inner: W,
}

impl<W: SinkWriterV1> SinkWriterV1Adapter<W> {
    pub(crate) fn new(inner: W) -> Self {
        Self {
            inner,
            is_empty: true,
            epoch: u64::MIN,
        }
    }
}

#[async_trait]
impl<W: SinkWriterV1> SinkWriter for SinkWriterV1Adapter<W> {
    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.epoch = epoch;
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.is_empty {
            self.is_empty = false;
            self.inner.begin_epoch(self.epoch).await?;
        }
        self.inner.write_batch(chunk).await
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<()> {
        if is_checkpoint {
            if !self.is_empty {
                self.inner.commit().await?
            }
            self.is_empty = true;
        }
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
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
    BlackHole,
    ClickHouse(Box<ClickHouseConfig>),
    Nats(NatsConfig),
    #[cfg(any(test, madsim))]
    Test,
}

pub const BLACKHOLE_SINK: &str = "blackhole";

#[derive(Debug)]
pub struct BlackHoleSink;

#[async_trait]
impl Sink for BlackHoleSink {
    type Coordinator = DummySinkCommitCoordinator;
    type Writer = Self;

    async fn new_writer(&self, _writer_env: SinkWriterParam) -> Result<Self::Writer> {
        Ok(Self)
    }

    async fn validate(&self, _client: Option<ConnectorClient>) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl SinkWriter for BlackHoleSink {
    async fn write_batch(&mut self, _chunk: StreamChunk) -> Result<()> {
        Ok(())
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, _is_checkpoint: bool) -> Result<()> {
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
            KINESIS_SINK => Ok(SinkConfig::Kinesis(Box::new(
                KinesisSinkConfig::from_hashmap(properties)?,
            ))),
            CLICKHOUSE_SINK => Ok(SinkConfig::ClickHouse(Box::new(
                ClickHouseConfig::from_hashmap(properties)?,
            ))),
            BLACKHOLE_SINK => Ok(SinkConfig::BlackHole),
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
    BlackHole(BlackHoleSink),
    Kinesis(KinesisSink),
    ClickHouse(ClickHouseSink),
    Iceberg(IcebergSink),
    Nats(NatsSink),
    RemoteIceberg(RemoteIcebergSink),
    TestSink(BoxSink),
}

impl SinkImpl {
    pub fn get_connector(&self) -> &'static str {
        match self {
            SinkImpl::Kafka(_) => "kafka",
            SinkImpl::Redis(_) => "redis",
            SinkImpl::Remote(_) => "remote",
            SinkImpl::BlackHole(_) => "blackhole",
            SinkImpl::Kinesis(_) => "kinesis",
            SinkImpl::ClickHouse(_) => "clickhouse",
            SinkImpl::Iceberg(_) => "iceberg",
            SinkImpl::Nats(_) => "nats",
            SinkImpl::RemoteIceberg(_) => "iceberg",
            SinkImpl::TestSink(_) => "test",
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
