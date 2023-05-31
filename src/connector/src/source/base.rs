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
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use enum_as_inner::EnumAsInner;
use futures::stream::BoxStream;
use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::TableId;
use risingwave_common::error::{ErrorCode, ErrorSuppressor, Result as RwResult, RwError};
use risingwave_common::transaction::transaction_message::TxnMsg;
use risingwave_common::types::{JsonbVal, Scalar};
use risingwave_pb::connector_service::TableSchema;
use risingwave_pb::source::ConnectorSplit;
use serde::{Deserialize, Serialize};

use super::datagen::DatagenMeta;
use super::filesystem::{FsSplit, S3FileReader, S3Properties, S3SplitEnumerator, S3_CONNECTOR};
use super::google_pubsub::GooglePubsubMeta;
use super::kafka::KafkaMeta;
use super::monitor::SourceMetrics;
use super::nexmark::source::message::NexmarkMeta;
use crate::parser::ParserConfig;
use crate::source::cdc::{
    CdcProperties, CdcSplit, CdcSplitReader, DebeziumSplitEnumerator, CITUS_CDC_CONNECTOR,
    MYSQL_CDC_CONNECTOR, POSTGRES_CDC_CONNECTOR,
};
use crate::source::datagen::{
    DatagenProperties, DatagenSplit, DatagenSplitEnumerator, DatagenSplitReader, DATAGEN_CONNECTOR,
};
use crate::source::dummy_connector::DummySplitReader;
use crate::source::google_pubsub::{
    PubsubProperties, PubsubSplit, PubsubSplitEnumerator, PubsubSplitReader,
    GOOGLE_PUBSUB_CONNECTOR,
};
use crate::source::kafka::enumerator::KafkaSplitEnumerator;
use crate::source::kafka::source::KafkaSplitReader;
use crate::source::kafka::{KafkaProperties, KafkaSplit, KAFKA_CONNECTOR};
use crate::source::kinesis::enumerator::client::KinesisSplitEnumerator;
use crate::source::kinesis::source::reader::KinesisSplitReader;
use crate::source::kinesis::split::KinesisSplit;
use crate::source::kinesis::{KinesisProperties, KINESIS_CONNECTOR};
use crate::source::nexmark::source::reader::NexmarkSplitReader;
use crate::source::nexmark::{
    NexmarkProperties, NexmarkSplit, NexmarkSplitEnumerator, NEXMARK_CONNECTOR,
};
use crate::source::pulsar::source::reader::PulsarSplitReader;
use crate::source::pulsar::{
    PulsarProperties, PulsarSplit, PulsarSplitEnumerator, PULSAR_CONNECTOR,
};
use crate::{impl_connector_properties, impl_split, impl_split_enumerator, impl_split_reader};

const SPLIT_TYPE_FIELD: &str = "split_type";
const SPLIT_INFO_FIELD: &str = "split_info";

/// [`SplitEnumerator`] fetches the split metadata from the external source service.
/// NOTE: It runs in the meta server, so probably it should be moved to the `meta` crate.
#[async_trait]
pub trait SplitEnumerator: Sized {
    type Split: SplitMetaData + Send + Sync;
    type Properties;

    async fn new(properties: Self::Properties) -> Result<Self>;
    async fn list_splits(&mut self) -> Result<Vec<Self::Split>>;
}

pub type SourceContextRef = Arc<SourceContext>;

#[derive(Debug, Default)]
pub struct SourceContext {
    pub source_info: SourceInfo,
    pub metrics: Arc<SourceMetrics>,
    error_suppressor: Option<Arc<Mutex<ErrorSuppressor>>>,
}
impl SourceContext {
    pub fn new(
        actor_id: u32,
        table_id: TableId,
        fragment_id: u32,
        metrics: Arc<SourceMetrics>,
    ) -> Self {
        Self {
            source_info: SourceInfo {
                actor_id,
                source_id: table_id,
                fragment_id,
            },
            metrics,
            error_suppressor: None,
        }
    }

    pub fn add_suppressor(&mut self, error_suppressor: Arc<Mutex<ErrorSuppressor>>) {
        self.error_suppressor = Some(error_suppressor)
    }

    pub(crate) fn report_user_source_error(&self, e: RwError) -> RwResult<()> {
        // Repropagate the error if batch
        if self.source_info.fragment_id == u32::MAX {
            return Err(e);
        }
        let mut err_str = e.inner().to_string();
        if let Some(suppressor) = &self.error_suppressor
            && suppressor.lock().suppress_error(&err_str)
        {
            err_str = format!(
                "error msg suppressed (due to per-actor error limit: {})",
                suppressor.lock().max()
            );
        }
        self.metrics
            .user_source_error_count
            .with_label_values(&[
                "SourceError",
                // TODO(jon-chuang): add the error msg truncator to truncate these
                &err_str,
                // Let's be a bit more specific for SourceExecutor
                "SourceExecutor",
                &self.source_info.fragment_id.to_string(),
                &self.source_info.source_id.table_id.to_string(),
            ])
            .inc();
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SourceInfo {
    pub actor_id: u32,
    pub source_id: TableId,
    // There should be a 1-1 mapping between `source_id` & `fragment_id`
    pub fragment_id: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceFormat {
    Invalid,
    Json,
    UpsertJson,
    Protobuf,
    DebeziumJson,
    Avro,
    UpsertAvro,
    Maxwell,
    CanalJson,
    Csv,
    Native,
    DebeziumAvro,
    DebeziumMongoJson,
}

pub type BoxSourceStream = BoxStream<'static, Result<Vec<SourceMessage>>>;
pub type BoxSourceWithStateStream = BoxStream<'static, Result<StreamChunkWithState, RwError>>;
pub type BoxTxnMsgStream = BoxStream<'static, Result<TxnMsg, RwError>>;

/// [`StreamChunkWithState`] returns stream chunk together with offset for each split. In the
/// current design, one connector source can have multiple split reader. The keys are unique
/// `split_id` and values are the latest offset for each split.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamChunkWithState {
    pub chunk: StreamChunk,
    pub split_offset_mapping: Option<HashMap<SplitId, String>>,
}

/// The `split_offset_mapping` field is unused for the table source, so we implement `From` for it.
impl From<StreamChunk> for StreamChunkWithState {
    fn from(chunk: StreamChunk) -> Self {
        Self {
            chunk,
            split_offset_mapping: None,
        }
    }
}

/// [`SplitReader`] is a new abstraction of the external connector read interface which is
/// responsible for parsing, it is used to read messages from the outside and transform them into a
/// stream of parsed [`StreamChunk`]
#[async_trait]
pub trait SplitReader: Sized {
    type Properties;

    async fn new(
        properties: Self::Properties,
        state: Vec<SplitImpl>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        columns: Option<Vec<Column>>,
    ) -> Result<Self>;

    fn into_stream(self) -> BoxSourceWithStateStream;
}

/// The max size of a chunk yielded by source stream.
pub const MAX_CHUNK_SIZE: usize = 1024;

#[derive(Clone, Debug, Deserialize)]
pub enum ConnectorProperties {
    Kafka(Box<KafkaProperties>),
    Pulsar(Box<PulsarProperties>),
    Kinesis(Box<KinesisProperties>),
    Nexmark(Box<NexmarkProperties>),
    Datagen(Box<DatagenProperties>),
    S3(Box<S3Properties>),
    MySqlCdc(Box<CdcProperties>),
    PostgresCdc(Box<CdcProperties>),
    CitusCdc(Box<CdcProperties>),
    GooglePubsub(Box<PubsubProperties>),
    Dummy(Box<()>),
}

impl ConnectorProperties {
    fn new_cdc_properties(
        connector_name: &str,
        properties: HashMap<String, String>,
    ) -> Result<Self> {
        match connector_name {
            MYSQL_CDC_CONNECTOR => Ok(Self::MySqlCdc(Box::new(CdcProperties {
                props: properties,
                source_type: "mysql".to_string(),
                ..Default::default()
            }))),
            POSTGRES_CDC_CONNECTOR => Ok(Self::PostgresCdc(Box::new(CdcProperties {
                props: properties,
                source_type: "postgres".to_string(),
                ..Default::default()
            }))),
            CITUS_CDC_CONNECTOR => Ok(Self::CitusCdc(Box::new(CdcProperties {
                props: properties,
                source_type: "citus".to_string(),
                ..Default::default()
            }))),
            _ => Err(anyhow!("unexpected cdc connector '{}'", connector_name,)),
        }
    }

    pub fn init_properties_for_cdc(
        &mut self,
        source_id: u32,
        rpc_addr: String,
        table_schema: Option<TableSchema>,
    ) {
        match self {
            ConnectorProperties::MySqlCdc(c)
            | ConnectorProperties::PostgresCdc(c)
            | ConnectorProperties::CitusCdc(c) => {
                c.source_id = source_id;
                c.connector_node_addr = rpc_addr;
                c.table_schema = table_schema;
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, EnumAsInner, PartialEq, Hash)]
pub enum SplitImpl {
    Kafka(KafkaSplit),
    Pulsar(PulsarSplit),
    Kinesis(KinesisSplit),
    Nexmark(NexmarkSplit),
    Datagen(DatagenSplit),
    GooglePubsub(PubsubSplit),
    MySqlCdc(CdcSplit),
    PostgresCdc(CdcSplit),
    CitusCdc(CdcSplit),
    S3(FsSplit),
}

// for the `FsSourceExecutor`
impl SplitImpl {
    #[allow(clippy::result_unit_err)]
    pub fn into_fs(self) -> Result<FsSplit, ()> {
        match self {
            Self::S3(split) => Ok(split),
            _ => Err(()),
        }
    }

    pub fn as_fs(&self) -> Option<&FsSplit> {
        match self {
            Self::S3(split) => Some(split),
            _ => None,
        }
    }
}

pub enum SplitReaderImpl {
    S3(Box<S3FileReader>),
    Dummy(Box<DummySplitReader>),
    Kinesis(Box<KinesisSplitReader>),
    Kafka(Box<KafkaSplitReader>),
    Nexmark(Box<NexmarkSplitReader>),
    Pulsar(Box<PulsarSplitReader>),
    Datagen(Box<DatagenSplitReader>),
    MySqlCdc(Box<CdcSplitReader>),
    PostgresCdc(Box<CdcSplitReader>),
    CitusCdc(Box<CdcSplitReader>),
    GooglePubsub(Box<PubsubSplitReader>),
}

pub enum SplitEnumeratorImpl {
    Kafka(KafkaSplitEnumerator),
    Pulsar(PulsarSplitEnumerator),
    Kinesis(KinesisSplitEnumerator),
    Nexmark(NexmarkSplitEnumerator),
    Datagen(DatagenSplitEnumerator),
    MySqlCdc(DebeziumSplitEnumerator),
    PostgresCdc(DebeziumSplitEnumerator),
    CitusCdc(DebeziumSplitEnumerator),
    GooglePubsub(PubsubSplitEnumerator),
    S3(S3SplitEnumerator),
}

impl_connector_properties! {
    { Kafka, KAFKA_CONNECTOR },
    { Pulsar, PULSAR_CONNECTOR },
    { Kinesis, KINESIS_CONNECTOR },
    { Nexmark, NEXMARK_CONNECTOR },
    { Datagen, DATAGEN_CONNECTOR },
    { S3, S3_CONNECTOR },
    { MySqlCdc, MYSQL_CDC_CONNECTOR },
    { PostgresCdc, POSTGRES_CDC_CONNECTOR },
    { CitusCdc, CITUS_CDC_CONNECTOR },
    { GooglePubsub, GOOGLE_PUBSUB_CONNECTOR}
}

impl_split_enumerator! {
    { Kafka, KafkaSplitEnumerator },
    { Pulsar, PulsarSplitEnumerator },
    { Kinesis, KinesisSplitEnumerator },
    { Nexmark, NexmarkSplitEnumerator },
    { Datagen, DatagenSplitEnumerator },
    { MySqlCdc, DebeziumSplitEnumerator },
    { PostgresCdc, DebeziumSplitEnumerator },
    { CitusCdc, DebeziumSplitEnumerator },
    { GooglePubsub, PubsubSplitEnumerator},
    { S3, S3SplitEnumerator }
}

impl_split! {
    { Kafka, KAFKA_CONNECTOR, KafkaSplit },
    { Pulsar, PULSAR_CONNECTOR, PulsarSplit },
    { Kinesis, KINESIS_CONNECTOR, KinesisSplit },
    { Nexmark, NEXMARK_CONNECTOR, NexmarkSplit },
    { Datagen, DATAGEN_CONNECTOR, DatagenSplit },
    { GooglePubsub, GOOGLE_PUBSUB_CONNECTOR, PubsubSplit },
    { MySqlCdc, MYSQL_CDC_CONNECTOR, CdcSplit },
    { PostgresCdc, POSTGRES_CDC_CONNECTOR, CdcSplit },
    { CitusCdc, CITUS_CDC_CONNECTOR, CdcSplit },
    { S3, S3_CONNECTOR, FsSplit }
}

impl_split_reader! {
    { S3, S3FileReader },
    { Kafka, KafkaSplitReader },
    { Pulsar, PulsarSplitReader },
    { Kinesis, KinesisSplitReader },
    { Nexmark, NexmarkSplitReader },
    { Datagen, DatagenSplitReader },
    { MySqlCdc, CdcSplitReader},
    { PostgresCdc, CdcSplitReader},
    { CitusCdc, CdcSplitReader },
    { GooglePubsub, PubsubSplitReader },
    { Dummy, DummySplitReader }
}

pub type DataType = risingwave_common::types::DataType;

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub is_visible: bool,
}

/// Split id resides in every source message, use `Arc` to avoid copying.
pub type SplitId = Arc<str>;

/// The message pumped from the external source service.
/// The third-party message structs will eventually be transformed into this struct.
#[derive(Debug, Clone)]
pub struct SourceMessage {
    pub payload: Option<Vec<u8>>,
    pub offset: String,
    pub split_id: SplitId,

    pub meta: SourceMeta,
}

#[derive(Debug, Clone)]
pub enum SourceMeta {
    Kafka(KafkaMeta),
    Nexmark(NexmarkMeta),
    GooglePubsub(GooglePubsubMeta),
    Datagen(DatagenMeta),
    // For the source that doesn't have meta data.
    Empty,
}

/// Implement Eq manually to ignore the `meta` field.
impl PartialEq for SourceMessage {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
            && self.split_id == other.split_id
            && self.payload == other.payload
    }
}
impl Eq for SourceMessage {}

/// The metadata of a split.
pub trait SplitMetaData: Sized {
    fn id(&self) -> SplitId;
    fn encode_to_bytes(&self) -> Bytes {
        self.encode_to_json()
            .as_scalar_ref()
            .value_serialize()
            .into()
    }
    fn restore_from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::restore_from_json(JsonbVal::value_deserialize(bytes).unwrap())
    }

    fn encode_to_json(&self) -> JsonbVal;
    fn restore_from_json(value: JsonbVal) -> Result<Self>;
}

/// [`ConnectorState`] maintains the consuming splits' info. In specific split readers,
/// `ConnectorState` cannot be [`None`] and contains one(for mq split readers) or many(for fs
/// split readers) [`SplitImpl`]. If no split is assigned to source executor, `ConnectorState` is
/// [`None`] and [`DummySplitReader`] is up instead of other split readers.
pub type ConnectorState = Option<Vec<SplitImpl>>;

#[cfg(test)]
mod tests {
    use maplit::*;
    use nexmark::event::EventType;

    use super::*;

    #[test]
    fn test_split_impl_get_fn() -> Result<()> {
        let split = KafkaSplit::new(0, Some(0), Some(0), "demo".to_string());
        let split_impl = SplitImpl::Kafka(split.clone());
        let get_value = split_impl.into_kafka().unwrap();
        println!("{:?}", get_value);
        assert_eq!(split.encode_to_bytes(), get_value.encode_to_bytes());
        assert_eq!(split.encode_to_json(), get_value.encode_to_json());

        Ok(())
    }

    #[test]
    fn test_cdc_split_state() -> Result<()> {
        let offset_str = "{\"sourcePartition\":{\"server\":\"RW_CDC_mydb.products\"},\"sourceOffset\":{\"transaction_id\":null,\"ts_sec\":1670407377,\"file\":\"binlog.000001\",\"pos\":98587,\"row\":2,\"server_id\":1,\"event\":2}}";
        let split_impl = SplitImpl::MySqlCdc(CdcSplit::new(1001, offset_str.to_string()));
        let encoded_split = split_impl.encode_to_bytes();
        let restored_split_impl = SplitImpl::restore_from_bytes(encoded_split.as_ref())?;
        assert_eq!(
            split_impl.encode_to_bytes(),
            restored_split_impl.encode_to_bytes()
        );
        assert_eq!(
            split_impl.encode_to_json(),
            restored_split_impl.encode_to_json()
        );

        let encoded_split = split_impl.encode_to_json();
        let restored_split_impl = SplitImpl::restore_from_json(encoded_split)?;
        assert_eq!(
            split_impl.encode_to_bytes(),
            restored_split_impl.encode_to_bytes()
        );
        assert_eq!(
            split_impl.encode_to_json(),
            restored_split_impl.encode_to_json()
        );
        Ok(())
    }

    #[test]
    fn test_extract_nexmark_config() {
        let props: HashMap<String, String> = convert_args!(hashmap!(
            "connector" => "nexmark",
            "nexmark.table.type" => "Person",
            "nexmark.split.num" => "1",
        ));

        let props = ConnectorProperties::extract(props).unwrap();

        if let ConnectorProperties::Nexmark(props) = props {
            assert_eq!(props.table_type, Some(EventType::Person));
            assert_eq!(props.split_num, 1);
        } else {
            panic!("extract nexmark config failed");
        }
    }

    #[test]
    fn test_extract_kafka_config() {
        let props: HashMap<String, String> = convert_args!(hashmap!(
            "connector" => "kafka",
            "properties.bootstrap.server" => "b1,b2",
            "topic" => "test",
            "scan.startup.mode" => "earliest",
            "broker.rewrite.endpoints" => r#"{"b-1:9092":"dns-1", "b-2:9092":"dns-2"}"#,
        ));

        let props = ConnectorProperties::extract(props).unwrap();
        if let ConnectorProperties::Kafka(k) = props {
            assert!(k.common.broker_rewrite_map.is_some());
            println!("{:?}", k.common.broker_rewrite_map);
        } else {
            panic!("extract kafka config failed");
        }
    }

    #[test]
    fn test_extract_cdc_properties() {
        let user_props_mysql: HashMap<String, String> = convert_args!(hashmap!(
            "connector_node_addr" => "localhost",
            "connector" => "mysql-cdc",
            "database.hostname" => "127.0.0.1",
            "database.port" => "3306",
            "database.user" => "root",
            "database.password" => "123456",
            "database.name" => "mydb",
            "table.name" => "products",
        ));

        let user_props_postgres: HashMap<String, String> = convert_args!(hashmap!(
            "connector_node_addr" => "localhost",
            "connector" => "postgres-cdc",
            "database.hostname" => "127.0.0.1",
            "database.port" => "5432",
            "database.user" => "root",
            "database.password" => "654321",
            "schema.name" => "public",
            "database.name" => "mypgdb",
            "table.name" => "orders",
        ));

        let conn_props = ConnectorProperties::extract(user_props_mysql).unwrap();
        if let ConnectorProperties::MySqlCdc(c) = conn_props {
            assert_eq!(c.source_id, 0);
            assert_eq!(c.source_type, "mysql");
            assert_eq!(c.props.get("connector_node_addr").unwrap(), "localhost");
            assert_eq!(c.props.get("database.hostname").unwrap(), "127.0.0.1");
            assert_eq!(c.props.get("database.port").unwrap(), "3306");
            assert_eq!(c.props.get("database.user").unwrap(), "root");
            assert_eq!(c.props.get("database.password").unwrap(), "123456");
            assert_eq!(c.props.get("database.name").unwrap(), "mydb");
            assert_eq!(c.props.get("table.name").unwrap(), "products");
        } else {
            panic!("extract cdc config failed");
        }

        let conn_props = ConnectorProperties::extract(user_props_postgres).unwrap();
        if let ConnectorProperties::PostgresCdc(c) = conn_props {
            assert_eq!(c.source_id, 0);
            assert_eq!(c.source_type, "postgres");
            assert_eq!(c.props.get("connector_node_addr").unwrap(), "localhost");
            assert_eq!(c.props.get("database.hostname").unwrap(), "127.0.0.1");
            assert_eq!(c.props.get("database.port").unwrap(), "5432");
            assert_eq!(c.props.get("database.user").unwrap(), "root");
            assert_eq!(c.props.get("database.password").unwrap(), "654321");
            assert_eq!(c.props.get("schema.name").unwrap(), "public");
            assert_eq!(c.props.get("database.name").unwrap(), "mypgdb");
            assert_eq!(c.props.get("table.name").unwrap(), "orders");
        } else {
            panic!("extract cdc config failed");
        }
    }
}
