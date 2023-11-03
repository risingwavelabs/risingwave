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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_s3::types::Object;
use bytes::Bytes;
use enum_as_inner::EnumAsInner;
use futures::stream::BoxStream;
use futures::Stream;
use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::TableId;
use risingwave_common::error::{ErrorSuppressor, RwError};
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_common::types::{JsonbVal, Scalar};
use risingwave_pb::catalog::{PbSource, PbStreamSourceInfo};
use risingwave_pb::source::ConnectorSplit;
use risingwave_rpc_client::ConnectorClient;
use serde::de::DeserializeOwned;

use super::cdc::DebeziumCdcMeta;
use super::datagen::DatagenMeta;
use super::filesystem::FsSplit;
use super::google_pubsub::GooglePubsubMeta;
use super::kafka::KafkaMeta;
use super::monitor::SourceMetrics;
use super::nexmark::source::message::NexmarkMeta;
use crate::parser::ParserConfig;
pub(crate) use crate::source::common::CommonSplitReader;
use crate::source::filesystem::{FsPageItem, S3Properties, S3_V2_CONNECTOR};
use crate::source::monitor::EnumeratorMetrics;
use crate::source::S3_CONNECTOR;
use crate::{
    dispatch_source_prop, dispatch_split_impl, for_all_sources, impl_connector_properties,
    impl_split, match_source_name_str,
};

const SPLIT_TYPE_FIELD: &str = "split_type";
const SPLIT_INFO_FIELD: &str = "split_info";
pub const UPSTREAM_SOURCE_KEY: &str = "connector";

pub trait TryFromHashmap: Sized {
    fn try_from_hashmap(props: HashMap<String, String>) -> Result<Self>;
}

pub trait SourceProperties: TryFromHashmap + Clone {
    const SOURCE_NAME: &'static str;
    type Split: SplitMetaData + TryFrom<SplitImpl, Error = anyhow::Error> + Into<SplitImpl>;
    type SplitEnumerator: SplitEnumerator<Properties = Self, Split = Self::Split>;
    type SplitReader: SplitReader<Split = Self::Split, Properties = Self>;

    fn init_from_pb_source(&mut self, _source: &PbSource) {}
}

impl<P: DeserializeOwned> TryFromHashmap for P {
    fn try_from_hashmap(props: HashMap<String, String>) -> Result<Self> {
        let json_value = serde_json::to_value(props).map_err(|e| anyhow!(e))?;
        serde_json::from_value::<P>(json_value).map_err(|e| anyhow!(e.to_string()))
    }
}

pub async fn create_split_reader<P: SourceProperties>(
    prop: P,
    splits: Vec<SplitImpl>,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
    columns: Option<Vec<Column>>,
) -> Result<P::SplitReader> {
    let splits = splits.into_iter().map(P::Split::try_from).try_collect()?;
    P::SplitReader::new(prop, splits, parser_config, source_ctx, columns).await
}

/// [`SplitEnumerator`] fetches the split metadata from the external source service.
/// NOTE: It runs in the meta server, so probably it should be moved to the `meta` crate.
#[async_trait]
pub trait SplitEnumerator: Sized {
    type Split: SplitMetaData + Send;
    type Properties;

    async fn new(properties: Self::Properties, context: SourceEnumeratorContextRef)
        -> Result<Self>;
    async fn list_splits(&mut self) -> Result<Vec<Self::Split>>;
}

pub type SourceContextRef = Arc<SourceContext>;
pub type SourceEnumeratorContextRef = Arc<SourceEnumeratorContext>;

/// The max size of a chunk yielded by source stream.
pub const MAX_CHUNK_SIZE: usize = 1024;

#[derive(Debug, Clone)]
pub struct SourceCtrlOpts {
    // comes from developer::stream_chunk_size in stream scenario and developer::batch_chunk_size
    // in batch scenario
    pub chunk_size: usize,
}

impl Default for SourceCtrlOpts {
    fn default() -> Self {
        Self {
            chunk_size: MAX_CHUNK_SIZE,
        }
    }
}

#[derive(Debug, Default)]
pub struct SourceEnumeratorContext {
    pub info: SourceEnumeratorInfo,
    pub metrics: Arc<EnumeratorMetrics>,
    pub connector_client: Option<ConnectorClient>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SourceEnumeratorInfo {
    pub source_id: u32,
}

#[derive(Debug, Default)]
pub struct SourceContext {
    pub connector_client: Option<ConnectorClient>,
    pub source_info: SourceInfo,
    pub metrics: Arc<SourceMetrics>,
    pub source_ctrl_opts: SourceCtrlOpts,
    error_suppressor: Option<Arc<Mutex<ErrorSuppressor>>>,
}
impl SourceContext {
    pub fn new(
        actor_id: u32,
        table_id: TableId,
        fragment_id: u32,
        metrics: Arc<SourceMetrics>,
        source_ctrl_opts: SourceCtrlOpts,
        connector_client: Option<ConnectorClient>,
    ) -> Self {
        Self {
            connector_client,
            source_info: SourceInfo {
                actor_id,
                source_id: table_id,
                fragment_id,
            },
            metrics,
            source_ctrl_opts,
            error_suppressor: None,
        }
    }

    pub fn new_with_suppressor(
        actor_id: u32,
        table_id: TableId,
        fragment_id: u32,
        metrics: Arc<SourceMetrics>,
        source_ctrl_opts: SourceCtrlOpts,
        connector_client: Option<ConnectorClient>,
        error_suppressor: Arc<Mutex<ErrorSuppressor>>,
    ) -> Self {
        let mut ctx = Self::new(
            actor_id,
            table_id,
            fragment_id,
            metrics,
            source_ctrl_opts,
            connector_client,
        );
        ctx.error_suppressor = Some(error_suppressor);
        ctx
    }

    pub(crate) fn report_user_source_error(&self, e: RwError) {
        if self.source_info.fragment_id == u32::MAX {
            return;
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
        GLOBAL_ERROR_METRICS.user_source_error.report([
            "SourceError".to_owned(),
            // TODO(jon-chuang): add the error msg truncator to truncate these
            err_str,
            // Let's be a bit more specific for SourceExecutor
            "SourceExecutor".to_owned(),
            self.source_info.fragment_id.to_string(),
            self.source_info.source_id.table_id.to_string(),
        ]);
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SourceInfo {
    pub actor_id: u32,
    pub source_id: TableId,
    // There should be a 1-1 mapping between `source_id` & `fragment_id`
    pub fragment_id: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SourceFormat {
    #[default]
    Invalid,
    Native,
    Debezium,
    DebeziumMongo,
    Maxwell,
    Canal,
    Upsert,
    Plain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SourceEncode {
    #[default]
    Invalid,
    Native,
    Avro,
    Csv,
    Protobuf,
    Json,
    Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct SourceStruct {
    pub format: SourceFormat,
    pub encode: SourceEncode,
}

impl SourceStruct {
    pub fn new(format: SourceFormat, encode: SourceEncode) -> Self {
        Self { format, encode }
    }
}

// Only return valid (format, encode)
pub fn extract_source_struct(info: &PbStreamSourceInfo) -> Result<SourceStruct> {
    use risingwave_pb::plan_common::{PbEncodeType, PbFormatType, RowFormatType};

    // old version meta.
    if let Ok(format) = info.get_row_format() {
        let (format, encode) = match format {
            RowFormatType::Json => (SourceFormat::Plain, SourceEncode::Json),
            RowFormatType::Protobuf => (SourceFormat::Plain, SourceEncode::Protobuf),
            RowFormatType::DebeziumJson => (SourceFormat::Debezium, SourceEncode::Json),
            RowFormatType::Avro => (SourceFormat::Plain, SourceEncode::Avro),
            RowFormatType::Maxwell => (SourceFormat::Maxwell, SourceEncode::Json),
            RowFormatType::CanalJson => (SourceFormat::Canal, SourceEncode::Json),
            RowFormatType::Csv => (SourceFormat::Plain, SourceEncode::Csv),
            RowFormatType::Native => (SourceFormat::Native, SourceEncode::Native),
            RowFormatType::DebeziumAvro => (SourceFormat::Debezium, SourceEncode::Avro),
            RowFormatType::UpsertJson => (SourceFormat::Upsert, SourceEncode::Json),
            RowFormatType::UpsertAvro => (SourceFormat::Upsert, SourceEncode::Avro),
            RowFormatType::DebeziumMongoJson => (SourceFormat::DebeziumMongo, SourceEncode::Json),
            RowFormatType::Bytes => (SourceFormat::Plain, SourceEncode::Bytes),
            RowFormatType::RowUnspecified => unreachable!(),
        };
        return Ok(SourceStruct::new(format, encode));
    }
    let source_format = info.get_format().map_err(|e| anyhow!("{e:?}"))?;
    let source_encode = info.get_row_encode().map_err(|e| anyhow!("{e:?}"))?;
    let (format, encode) = match (source_format, source_encode) {
        (PbFormatType::Plain, PbEncodeType::Json) => (SourceFormat::Plain, SourceEncode::Json),
        (PbFormatType::Plain, PbEncodeType::Protobuf) => {
            (SourceFormat::Plain, SourceEncode::Protobuf)
        }
        (PbFormatType::Debezium, PbEncodeType::Json) => {
            (SourceFormat::Debezium, SourceEncode::Json)
        }
        (PbFormatType::Plain, PbEncodeType::Avro) => (SourceFormat::Plain, SourceEncode::Avro),
        (PbFormatType::Maxwell, PbEncodeType::Json) => (SourceFormat::Maxwell, SourceEncode::Json),
        (PbFormatType::Canal, PbEncodeType::Json) => (SourceFormat::Canal, SourceEncode::Json),
        (PbFormatType::Plain, PbEncodeType::Csv) => (SourceFormat::Plain, SourceEncode::Csv),
        (PbFormatType::Native, PbEncodeType::Native) => {
            (SourceFormat::Native, SourceEncode::Native)
        }
        (PbFormatType::Debezium, PbEncodeType::Avro) => {
            (SourceFormat::Debezium, SourceEncode::Avro)
        }
        (PbFormatType::Upsert, PbEncodeType::Json) => (SourceFormat::Upsert, SourceEncode::Json),
        (PbFormatType::Upsert, PbEncodeType::Avro) => (SourceFormat::Upsert, SourceEncode::Avro),
        (PbFormatType::DebeziumMongo, PbEncodeType::Json) => {
            (SourceFormat::DebeziumMongo, SourceEncode::Json)
        }
        (PbFormatType::Plain, PbEncodeType::Bytes) => (SourceFormat::Plain, SourceEncode::Bytes),
        (format, encode) => {
            return Err(anyhow!(
                "Unsupported combination of format {:?} and encode {:?}",
                format,
                encode
            ));
        }
    };
    Ok(SourceStruct::new(format, encode))
}

pub type BoxSourceStream = BoxStream<'static, Result<Vec<SourceMessage>>>;

pub trait SourceWithStateStream =
    Stream<Item = Result<StreamChunkWithState, RwError>> + Send + 'static;
pub type BoxSourceWithStateStream = BoxStream<'static, Result<StreamChunkWithState, RwError>>;
pub type BoxTryStream<M> = BoxStream<'static, Result<M, RwError>>;

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
pub trait SplitReader: Sized + Send {
    type Properties;
    type Split: SplitMetaData;

    async fn new(
        properties: Self::Properties,
        state: Vec<Self::Split>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        columns: Option<Vec<Column>>,
    ) -> Result<Self>;

    fn into_stream(self) -> BoxSourceWithStateStream;
}

for_all_sources!(impl_connector_properties);

impl ConnectorProperties {
    pub fn is_new_fs_connector_b_tree_map(props: &BTreeMap<String, String>) -> bool {
        props
            .get(UPSTREAM_SOURCE_KEY)
            .map(|s| s.eq_ignore_ascii_case(S3_V2_CONNECTOR))
            .unwrap_or(false)
    }

    pub fn is_new_fs_connector_hash_map(props: &HashMap<String, String>) -> bool {
        props
            .get(UPSTREAM_SOURCE_KEY)
            .map(|s| s.eq_ignore_ascii_case(S3_V2_CONNECTOR))
            .unwrap_or(false)
    }

    pub fn rewrite_upstream_source_key_hash_map(props: &mut HashMap<String, String>) {
        let connector = props.remove(UPSTREAM_SOURCE_KEY).unwrap();
        match connector.as_str() {
            S3_V2_CONNECTOR => {
                tracing::info!(
                    "using new fs source, rewrite connector from '{}' to '{}'",
                    S3_V2_CONNECTOR,
                    S3_CONNECTOR
                );
                props.insert(UPSTREAM_SOURCE_KEY.to_string(), S3_CONNECTOR.to_string());
            }
            _ => {
                props.insert(UPSTREAM_SOURCE_KEY.to_string(), connector);
            }
        }
    }
}

impl ConnectorProperties {
    pub fn extract(mut props: HashMap<String, String>) -> Result<Self> {
        if Self::is_new_fs_connector_hash_map(&props) {
            _ = props
                .remove(UPSTREAM_SOURCE_KEY)
                .ok_or_else(|| anyhow!("Must specify 'connector' in WITH clause"))?;
            return Ok(ConnectorProperties::S3(Box::new(
                S3Properties::try_from_hashmap(props)?,
            )));
        }

        let connector = props
            .remove(UPSTREAM_SOURCE_KEY)
            .ok_or_else(|| anyhow!("Must specify 'connector' in WITH clause"))?;
        match_source_name_str!(
            connector.to_lowercase().as_str(),
            PropType,
            PropType::try_from_hashmap(props).map(ConnectorProperties::from),
            |other| Err(anyhow!("connector '{}' is not supported", other))
        )
    }

    pub fn enable_split_scale_in(&self) -> bool {
        // enable split scale in just for Kinesis
        matches!(self, ConnectorProperties::Kinesis(_))
    }

    pub fn init_from_pb_source(&mut self, source: &PbSource) {
        dispatch_source_prop!(self, prop, prop.init_from_pb_source(source))
    }

    pub fn support_multiple_splits(&self) -> bool {
        matches!(self, ConnectorProperties::Kafka(_))
    }
}

for_all_sources!(impl_split);

impl From<&SplitImpl> for ConnectorSplit {
    fn from(split: &SplitImpl) -> Self {
        dispatch_split_impl!(split, inner, SourcePropType, {
            ConnectorSplit {
                split_type: String::from(SourcePropType::SOURCE_NAME),
                encoded_split: inner.encode_to_bytes().to_vec(),
            }
        })
    }
}

impl TryFrom<&ConnectorSplit> for SplitImpl {
    type Error = anyhow::Error;

    fn try_from(split: &ConnectorSplit) -> std::result::Result<Self, Self::Error> {
        match_source_name_str!(
            split.split_type.to_lowercase().as_str(),
            PropType,
            {
                <PropType as SourceProperties>::Split::restore_from_bytes(
                    split.encoded_split.as_ref(),
                )
                .map(Into::into)
            },
            |other| Err(anyhow!("connector '{}' is not supported", other))
        )
    }
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

impl SplitImpl {
    fn restore_from_json_inner(split_type: &str, value: JsonbVal) -> Result<Self> {
        match_source_name_str!(
            split_type.to_lowercase().as_str(),
            PropType,
            <PropType as SourceProperties>::Split::restore_from_json(value).map(Into::into),
            |other| Err(anyhow!("connector '{}' is not supported", other))
        )
    }
}

impl SplitMetaData for SplitImpl {
    fn id(&self) -> SplitId {
        dispatch_split_impl!(self, inner, IgnoreType, inner.id())
    }

    fn encode_to_json(&self) -> JsonbVal {
        use serde_json::json;
        let inner = self.encode_to_json_inner().take();
        json!({ SPLIT_TYPE_FIELD: self.get_type(), SPLIT_INFO_FIELD: inner}).into()
    }

    fn restore_from_json(value: JsonbVal) -> Result<Self> {
        let mut value = value.take();
        let json_obj = value.as_object_mut().unwrap();
        let split_type = json_obj
            .remove(SPLIT_TYPE_FIELD)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let inner_value = json_obj.remove(SPLIT_INFO_FIELD).unwrap();
        Self::restore_from_json_inner(&split_type, inner_value.into())
    }

    fn update_with_offset(&mut self, start_offset: String) -> Result<()> {
        dispatch_split_impl!(
            self,
            inner,
            IgnoreType,
            inner.update_with_offset(start_offset)
        )
    }
}

impl SplitImpl {
    pub fn get_type(&self) -> String {
        dispatch_split_impl!(self, _ignored, PropType, {
            PropType::SOURCE_NAME.to_string()
        })
    }

    pub fn update_in_place(&mut self, start_offset: String) -> Result<()> {
        dispatch_split_impl!(self, inner, IgnoreType, {
            inner.update_with_offset(start_offset)?
        });
        Ok(())
    }

    pub fn encode_to_json_inner(&self) -> JsonbVal {
        dispatch_split_impl!(self, inner, IgnoreType, inner.encode_to_json())
    }
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
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    pub offset: String, // TODO: use `Arc<str>`
    pub split_id: SplitId,
    pub meta: SourceMeta,
}

#[derive(Debug, Clone)]
pub enum SourceMeta {
    Kafka(KafkaMeta),
    Nexmark(NexmarkMeta),
    GooglePubsub(GooglePubsubMeta),
    Datagen(DatagenMeta),
    DebeziumCdc(DebeziumCdcMeta),
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
    fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()>;
}

/// [`ConnectorState`] maintains the consuming splits' info. In specific split readers,
/// `ConnectorState` cannot be [`None`] and contains one(for mq split readers) or many(for fs
/// split readers) [`SplitImpl`]. If no split is assigned to source executor, `ConnectorState` is
/// [`None`] and the created source stream will be a pending stream.
pub type ConnectorState = Option<Vec<SplitImpl>>;

#[derive(Debug, Clone, Default)]
pub struct FsFilterCtrlCtx;
pub type FsFilterCtrlCtxRef = Arc<FsFilterCtrlCtx>;

#[async_trait]
pub trait FsListInner: Sized {
    // fixme: better to implement as an Iterator, but the last page still have some contents
    async fn get_next_page<T: for<'a> From<&'a Object>>(&mut self) -> Result<(Vec<T>, bool)>;
    fn filter_policy(&self, ctx: &FsFilterCtrlCtx, page_num: usize, item: &FsPageItem) -> bool;
}

#[cfg(test)]
mod tests {
    use maplit::*;
    use nexmark::event::EventType;

    use super::*;
    use crate::source::cdc::{DebeziumCdcSplit, MySqlCdcSplit};
    use crate::source::kafka::KafkaSplit;

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
        let mysql_split = MySqlCdcSplit::new(1001, offset_str.to_string());
        let split = DebeziumCdcSplit::new(Some(mysql_split), None);
        let split_impl = SplitImpl::MysqlCdc(split);
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
        if let ConnectorProperties::MysqlCdc(c) = conn_props {
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
