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

pub mod big_query;
pub mod blackhole;
pub mod boxed;
pub mod catalog;
pub mod clickhouse;
pub mod coordinate;
pub mod doris;
pub mod doris_connector;
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
pub mod test_sink;
pub mod utils;
pub mod writer;

use std::collections::HashMap;

use ::clickhouse::error::Error as ClickHouseError;
use ::redis::RedisError;
use anyhow::anyhow;
use async_trait::async_trait;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::error::{anyhow_error, ErrorCode, RwError};
use risingwave_common::metrics::{
    LabelGuardedHistogram, LabelGuardedIntCounter, LabelGuardedIntGauge,
};
use risingwave_pb::catalog::PbSinkType;
use risingwave_pb::connector_service::{PbSinkParam, SinkMetadata, TableSchema};
use risingwave_rpc_client::error::RpcError;
use risingwave_rpc_client::MetaClient;
use thiserror::Error;
pub use tracing;

use self::catalog::{SinkFormatDesc, SinkType};
use crate::sink::catalog::desc::SinkDesc;
use crate::sink::catalog::{SinkCatalog, SinkId};
use crate::sink::log_store::LogReader;
use crate::sink::writer::SinkWriter;
use crate::ConnectorParams;

#[macro_export]
macro_rules! for_all_sinks {
    ($macro:path $(, $arg:tt)*) => {
        $macro! {
            {
                { Redis, $crate::sink::redis::RedisSink },
                { Kafka, $crate::sink::kafka::KafkaSink },
                { Pulsar, $crate::sink::pulsar::PulsarSink },
                { BlackHole, $crate::sink::blackhole::BlackHoleSink },
                { Kinesis, $crate::sink::kinesis::KinesisSink },
                { ClickHouse, $crate::sink::clickhouse::ClickHouseSink },
                { Iceberg, $crate::sink::iceberg::IcebergSink },
                { Nats, $crate::sink::nats::NatsSink },
                { RemoteIceberg, $crate::sink::iceberg::RemoteIcebergSink },
                { Jdbc, $crate::sink::remote::JdbcSink },
                { DeltaLake, $crate::sink::remote::DeltaLakeSink },
                { ElasticSearch, $crate::sink::remote::ElasticSearchSink },
                { Cassandra, $crate::sink::remote::CassandraSink },
                { Doris, $crate::sink::doris::DorisSink },
                { BigQuery, $crate::sink::big_query::BigQuerySink },
                { Test, $crate::sink::test_sink::TestSink }
            }
            $(,$arg)*
        }
    };
}

#[macro_export]
macro_rules! dispatch_sink {
    ({$({$variant_name:ident, $sink_type:ty}),*}, $impl:tt, $sink:tt, $body:tt) => {{
        use $crate::sink::SinkImpl;

        match $impl {
            $(
                SinkImpl::$variant_name($sink) => $body,
            )*
        }
    }};
    ($impl:expr, $sink:ident, $body:expr) => {{
        $crate::for_all_sinks! {$crate::dispatch_sink, {$impl}, $sink, {$body}}
    }};
}

#[macro_export]
macro_rules! match_sink_name_str {
    ({$({$variant_name:ident, $sink_type:ty}),*}, $name_str:tt, $type_name:ident, $body:tt, $on_other_closure:tt) => {{
        use $crate::sink::Sink;
        match $name_str {
            $(
                <$sink_type>::SINK_NAME => {
                    type $type_name = $sink_type;
                    {
                        $body
                    }
                },
            )*
            other => ($on_other_closure)(other),
        }
    }};
    ($name_str:expr, $type_name:ident, $body:expr, $on_other_closure:expr) => {{
        $crate::for_all_sinks! {$crate::match_sink_name_str, {$name_str}, $type_name, {$body}, {$on_other_closure}}
    }};
}

pub const CONNECTOR_TYPE_KEY: &str = "connector";
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
    pub format_desc: Option<SinkFormatDesc>,
    pub db_name: String,
    pub sink_from_name: String,
}

impl SinkParam {
    pub fn from_proto(pb_param: PbSinkParam) -> Self {
        let table_schema = pb_param.table_schema.expect("should contain table schema");
        let format_desc = match pb_param.format_desc {
            Some(f) => f.try_into().ok(),
            None => {
                let connector = pb_param.properties.get(CONNECTOR_TYPE_KEY);
                let r#type = pb_param.properties.get(SINK_TYPE_OPTION);
                match (connector, r#type) {
                    (Some(c), Some(t)) => SinkFormatDesc::from_legacy_type(c, t).ok().flatten(),
                    _ => None,
                }
            }
        };
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
                PbSinkType::try_from(pb_param.sink_type).expect("should be able to convert"),
            ),
            format_desc,
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
            format_desc: self.format_desc.as_ref().map(|f| f.to_proto()),
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
            format_desc: sink_catalog.format_desc,
            db_name: sink_catalog.db_name,
            sink_from_name: sink_catalog.sink_from_name,
        }
    }
}

#[derive(Clone)]
pub struct SinkMetrics {
    pub sink_commit_duration_metrics: LabelGuardedHistogram<3>,
    pub connector_sink_rows_received: LabelGuardedIntCounter<2>,
    pub log_store_first_write_epoch: LabelGuardedIntGauge<3>,
    pub log_store_latest_write_epoch: LabelGuardedIntGauge<3>,
    pub log_store_write_rows: LabelGuardedIntCounter<3>,
    pub log_store_latest_read_epoch: LabelGuardedIntGauge<3>,
    pub log_store_read_rows: LabelGuardedIntCounter<3>,
}

impl SinkMetrics {
    fn for_test() -> Self {
        SinkMetrics {
            sink_commit_duration_metrics: LabelGuardedHistogram::test_histogram(),
            connector_sink_rows_received: LabelGuardedIntCounter::test_int_counter(),
            log_store_first_write_epoch: LabelGuardedIntGauge::test_int_gauge(),
            log_store_latest_write_epoch: LabelGuardedIntGauge::test_int_gauge(),
            log_store_latest_read_epoch: LabelGuardedIntGauge::test_int_gauge(),
            log_store_write_rows: LabelGuardedIntCounter::test_int_counter(),
            log_store_read_rows: LabelGuardedIntCounter::test_int_counter(),
        }
    }
}

#[derive(Clone)]
pub struct SinkWriterParam {
    pub connector_params: ConnectorParams,
    pub executor_id: u64,
    pub vnode_bitmap: Option<Bitmap>,
    pub meta_client: Option<MetaClient>,
    pub sink_metrics: SinkMetrics,
}

impl SinkWriterParam {
    pub fn for_test() -> Self {
        SinkWriterParam {
            connector_params: Default::default(),
            executor_id: Default::default(),
            vnode_bitmap: Default::default(),
            meta_client: Default::default(),
            sink_metrics: SinkMetrics::for_test(),
        }
    }
}

pub trait Sink: TryFrom<SinkParam, Error = SinkError> {
    const SINK_NAME: &'static str;
    type LogSinker: LogSinker;
    type Coordinator: SinkCommitCoordinator;

    fn default_sink_decouple(_desc: &SinkDesc) -> bool {
        false
    }

    async fn validate(&self) -> Result<()>;
    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker>;
    #[expect(clippy::unused_async)]
    async fn new_coordinator(&self) -> Result<Self::Coordinator> {
        Err(SinkError::Coordinator(anyhow!("no coordinator")))
    }
}

#[async_trait]
pub trait LogSinker: 'static {
    async fn consume_log_and_sink(self, log_reader: impl LogReader) -> Result<()>;
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

impl SinkImpl {
    pub fn new(mut param: SinkParam) -> Result<Self> {
        const CONNECTION_NAME_KEY: &str = "connection.name";
        const PRIVATE_LINK_TARGET_KEY: &str = "privatelink.targets";

        // remove privatelink related properties if any
        param.properties.remove(PRIVATE_LINK_TARGET_KEY);
        param.properties.remove(CONNECTION_NAME_KEY);

        let sink_type = param
            .properties
            .get(CONNECTOR_TYPE_KEY)
            .ok_or_else(|| SinkError::Config(anyhow!("missing config: {}", CONNECTOR_TYPE_KEY)))?;
        match_sink_name_str!(
            sink_type.to_lowercase().as_str(),
            SinkType,
            Ok(SinkType::try_from(param)?.into()),
            |other| {
                Err(SinkError::Config(anyhow!(
                    "unsupported sink connector {}",
                    other
                )))
            }
        )
    }
}

pub fn build_sink(param: SinkParam) -> Result<SinkImpl> {
    SinkImpl::new(param)
}

macro_rules! def_sink_impl {
    () => {
        $crate::for_all_sinks! { def_sink_impl }
    };
    ({ $({ $variant_name:ident, $sink_type:ty }),* }) => {
        #[derive(Debug)]
        pub enum SinkImpl {
            $(
                $variant_name($sink_type),
            )*
        }

        $(
            impl From<$sink_type> for SinkImpl {
                fn from(sink: $sink_type) -> SinkImpl {
                    SinkImpl::$variant_name(sink)
                }
            }
        )*
    };
}

def_sink_impl!();

pub type Result<T> = std::result::Result<T, SinkError>;

#[derive(Error, Debug)]
pub enum SinkError {
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("Kinesis error: {0}")]
    Kinesis(anyhow::Error),
    #[error("Remote sink error: {0}")]
    Remote(anyhow::Error),
    #[error("Encode error: {0}")]
    Encode(String),
    #[error("Iceberg error: {0}")]
    Iceberg(anyhow::Error),
    #[error("config error: {0}")]
    Config(#[from] anyhow::Error),
    #[error("coordinator error: {0}")]
    Coordinator(anyhow::Error),
    #[error("ClickHouse error: {0}")]
    ClickHouse(String),
    #[error("Redis error: {0}")]
    Redis(String),
    #[error("Nats error: {0}")]
    Nats(anyhow::Error),
    #[error("Doris http error: {0}")]
    Http(anyhow::Error),
    #[error("Doris error: {0}")]
    Doris(String),
    #[error("Pulsar error: {0}")]
    Pulsar(anyhow::Error),
    #[error("Internal error: {0}")]
    Internal(anyhow::Error),
    #[error("BigQuery error: {0}")]
    BigQuery(anyhow::Error),
}

impl From<icelake::Error> for SinkError {
    fn from(value: icelake::Error) -> Self {
        SinkError::Iceberg(anyhow_error!("{}", value))
    }
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

impl From<RedisError> for SinkError {
    fn from(value: RedisError) -> Self {
        SinkError::Redis(format!("{}", value))
    }
}

impl From<SinkError> for RwError {
    fn from(e: SinkError) -> Self {
        ErrorCode::SinkError(Box::new(e)).into()
    }
}
