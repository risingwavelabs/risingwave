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
pub mod kinesis;
pub mod redis;
pub mod remote;
pub mod utils;

use std::collections::HashMap;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use enum_as_inner::EnumAsInner;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnCatalog, Schema};
use risingwave_common::error::{ErrorCode, RwError};
use risingwave_rpc_client::error::RpcError;
use risingwave_rpc_client::ConnectorClient;
use thiserror::Error;
pub use tracing;

use self::catalog::SinkType;
use crate::sink::catalog::SinkId;
use crate::sink::kafka::{KafkaConfig, KafkaSink, KAFKA_SINK};
use crate::sink::kinesis::{KinesisSink, KinesisSinkConfig, KINESIS_SINK};
use crate::sink::redis::{RedisConfig, RedisSink};
use crate::sink::remote::{RemoteConfig, RemoteSink};
use crate::ConnectorParams;

pub const DOWNSTREAM_SINK_KEY: &str = "connector";
pub const SINK_TYPE_OPTION: &str = "type";
pub const SINK_TYPE_APPEND_ONLY: &str = "append-only";
pub const SINK_TYPE_DEBEZIUM: &str = "debezium";
pub const SINK_TYPE_UPSERT: &str = "upsert";
pub const SINK_USER_FORCE_APPEND_ONLY_OPTION: &str = "force_append_only";

pub struct SinkWriterParam {
    pub connector_params: ConnectorParams,
    pub executor_id: u64,
    pub vnode_bitmap: Option<Bitmap>,
}

#[async_trait]
pub trait Sink {
    type Writer: SinkWriter;
    type Coordinator: SinkCommitCoordinator;

    async fn validate(&self, client: Option<ConnectorClient>) -> Result<()>;
    async fn new_writer(&self, writer_param: SinkWriterParam) -> Result<Self::Writer>;
    async fn new_coordinator(
        &self,
        _connector_rpc_endpoint: Option<String>,
    ) -> Result<Self::Coordinator> {
        Err(SinkError::Coordinator(anyhow!("no coordinator")))
    }
}

#[async_trait]
pub trait SinkWriter: Send {
    /// Begin a new epoch
    async fn begin_epoch(&mut self, epoch: u64) -> Result<()>;

    /// Write a stream chunk to sink
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()>;

    /// Receive a barrier and mark the end of current epoch. When `is_checkpoint` is true, the sink
    /// writer should commit the current epoch.
    async fn barrier(&mut self, is_checkpoint: bool) -> Result<()>;

    /// Clean up
    async fn abort(&mut self) -> Result<()>;

    /// Update the vnode bitmap of current sink writer
    async fn update_vnode_bitmap(&mut self, vnode_bitmap: Bitmap) -> Result<()>;
}

#[async_trait]
// An old version of SinkWriter for backward compatibility
pub trait SinkWriterV1: Send {
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

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Bitmap) -> Result<()> {
        Ok(())
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
    async fn commit(&mut self, epoch: u64, metadata: Vec<Bytes>) -> Result<()>;
}

pub struct DummySinkCommitCoordinator;

#[async_trait]
impl SinkCommitCoordinator for DummySinkCommitCoordinator {
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn commit(&mut self, _epoch: u64, _metadata: Vec<Bytes>) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug, EnumAsInner)]
pub enum SinkConfig {
    Redis(RedisConfig),
    Kafka(Box<KafkaConfig>),
    Remote(RemoteConfig),
    Kinesis(Box<KinesisSinkConfig>),
    BlackHole,
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

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, _is_checkpoint: bool) -> Result<()> {
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Bitmap) -> Result<()> {
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
            SinkConfig::Kinesis(_) => "kinesis",
        }
    }
}

pub fn build_sink(
    config: SinkConfig,
    columns: &[ColumnCatalog],
    pk_indices: Vec<usize>,
    sink_type: SinkType,
    sink_id: SinkId,
) -> Result<SinkImpl> {
    // The downstream sink can only see the visible columns.
    let schema: Schema = columns
        .iter()
        .filter_map(|column| (!column.is_hidden).then(|| column.column_desc.clone().into()))
        .collect();
    SinkImpl::new(config, schema, pk_indices, sink_type, sink_id)
}

#[derive(Debug)]
pub enum SinkImpl {
    Redis(RedisSink),
    Kafka(KafkaSink),
    Remote(RemoteSink),
    BlackHole(BlackHoleSink),
    Kinesis(KinesisSink),
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
        }
    }};
}

impl SinkImpl {
    pub fn new(
        cfg: SinkConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        sink_type: SinkType,
        sink_id: SinkId,
    ) -> Result<Self> {
        Ok(match cfg {
            SinkConfig::Redis(cfg) => SinkImpl::Redis(RedisSink::new(cfg, schema)?),
            SinkConfig::Kafka(cfg) => SinkImpl::Kafka(KafkaSink::new(
                *cfg,
                schema,
                pk_indices,
                sink_type.is_append_only(),
            )),
            SinkConfig::Kinesis(cfg) => SinkImpl::Kinesis(KinesisSink::new(
                *cfg,
                schema,
                pk_indices,
                sink_type.is_append_only(),
            )),
            SinkConfig::Remote(cfg) => {
                SinkImpl::Remote(RemoteSink::new(cfg, schema, pk_indices, sink_id, sink_type))
            }
            SinkConfig::BlackHole => SinkImpl::BlackHole(BlackHoleSink),
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
    Remote(String),
    #[error("Json parse error: {0}")]
    JsonParse(String),
    #[error("config error: {0}")]
    Config(#[from] anyhow::Error),
    #[error("coordinator error: {0}")]
    Coordinator(anyhow::Error),
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
