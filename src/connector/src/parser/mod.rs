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
use std::fmt::Debug;

use auto_enums::auto_enum;
pub use avro::AvroParserConfig;
pub use canal::*;
use csv_parser::CsvParser;
pub use debezium::*;
use futures::Future;
use futures_async_stream::try_stream;
use itertools::Itertools;
pub use json_parser::*;
pub use protobuf::*;
use risingwave_common::array::{ArrayBuilderImpl, Op, StreamChunk};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::Datum;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::catalog::StreamSourceInfo;

use self::avro::AvroAccessBuilder;
use self::bytes_parser::BytesAccessBuilder;
pub use self::csv_parser::CsvParserConfig;
use self::plain_parser::PlainParser;
use self::simd_json_parser::DebeziumJsonAccessBuilder;
use self::unified::AccessImpl;
use self::upsert_parser::UpsertParser;
use self::util::get_kafka_topic;
use crate::aws_auth::AwsAuthProps;
use crate::parser::maxwell::MaxwellParser;
use crate::source::{
    BoxSourceStream, SourceColumnDesc, SourceContext, SourceContextRef, SourceFormat, SourceMeta,
    SourceWithStateStream, SplitId, StreamChunkWithState,
};

mod avro;
mod bytes_parser;
mod canal;
mod common;
mod csv_parser;
mod debezium;
mod json_parser;
mod maxwell;
mod plain_parser;
mod protobuf;
mod schema_registry;
mod unified;
mod upsert_parser;
mod util;
/// A builder for building a [`StreamChunk`] from [`SourceColumnDesc`].
pub struct SourceStreamChunkBuilder {
    descs: Vec<SourceColumnDesc>,
    builders: Vec<ArrayBuilderImpl>,
    op_builder: Vec<Op>,
}

impl SourceStreamChunkBuilder {
    pub fn with_capacity(descs: Vec<SourceColumnDesc>, cap: usize) -> Self {
        let builders = descs
            .iter()
            .map(|desc| desc.data_type.create_array_builder(cap))
            .collect();
        Self {
            descs,
            builders,
            op_builder: Vec::with_capacity(cap),
        }
    }

    pub fn row_writer(&mut self) -> SourceStreamChunkRowWriter<'_> {
        SourceStreamChunkRowWriter {
            descs: &self.descs,
            builders: &mut self.builders,
            op_builder: &mut self.op_builder,
        }
    }

    pub fn finish(self) -> StreamChunk {
        StreamChunk::new(
            self.op_builder,
            self.builders
                .into_iter()
                .map(|builder| builder.finish().into())
                .collect(),
            None,
        )
    }

    pub fn op_num(&self) -> usize {
        self.op_builder.len()
    }
}

/// `SourceStreamChunkRowWriter` is responsible to write one row (Insert/Delete) or two rows
/// (Update) to the [`StreamChunk`].
pub struct SourceStreamChunkRowWriter<'a> {
    descs: &'a [SourceColumnDesc],
    builders: &'a mut [ArrayBuilderImpl],
    op_builder: &'a mut Vec<Op>,
}

/// `WriteGuard` can't be constructed directly in other mods due to a private field, so it can be
/// used to ensure that all methods on [`SourceStreamChunkRowWriter`] are called at least once in
/// the [`SourceParser::parse`] implementation.
#[derive(Debug)]
pub struct WriteGuard(());

trait OpAction {
    type Output;

    const DEFAULT_OUTPUT: Self::Output;

    fn apply(builder: &mut ArrayBuilderImpl, output: Self::Output);

    fn rollback(builder: &mut ArrayBuilderImpl);

    fn finish(writer: &mut SourceStreamChunkRowWriter<'_>);
}

struct OpActionInsert;

impl OpAction for OpActionInsert {
    type Output = Datum;

    const DEFAULT_OUTPUT: Self::Output = None;

    #[inline(always)]
    fn apply(builder: &mut ArrayBuilderImpl, output: Datum) {
        builder.append(&output)
    }

    #[inline(always)]
    fn rollback(builder: &mut ArrayBuilderImpl) {
        builder.pop().unwrap()
    }

    #[inline(always)]
    fn finish(writer: &mut SourceStreamChunkRowWriter<'_>) {
        writer.op_builder.push(Op::Insert)
    }
}

struct OpActionDelete;

impl OpAction for OpActionDelete {
    type Output = Datum;

    const DEFAULT_OUTPUT: Self::Output = None;

    #[inline(always)]
    fn apply(builder: &mut ArrayBuilderImpl, output: Datum) {
        builder.append(&output)
    }

    #[inline(always)]
    fn rollback(builder: &mut ArrayBuilderImpl) {
        builder.pop().unwrap()
    }

    #[inline(always)]
    fn finish(writer: &mut SourceStreamChunkRowWriter<'_>) {
        writer.op_builder.push(Op::Delete)
    }
}

struct OpActionUpdate;

impl OpAction for OpActionUpdate {
    type Output = (Datum, Datum);

    const DEFAULT_OUTPUT: Self::Output = (None, None);

    #[inline(always)]
    fn apply(builder: &mut ArrayBuilderImpl, output: (Datum, Datum)) {
        builder.append(&output.0);
        builder.append(&output.1);
    }

    #[inline(always)]
    fn rollback(builder: &mut ArrayBuilderImpl) {
        builder.pop().unwrap();
        builder.pop().unwrap();
    }

    #[inline(always)]
    fn finish(writer: &mut SourceStreamChunkRowWriter<'_>) {
        writer.op_builder.push(Op::UpdateDelete);
        writer.op_builder.push(Op::UpdateInsert);
    }
}

impl SourceStreamChunkRowWriter<'_> {
    #[expect(
        clippy::disallowed_methods,
        reason = "FIXME: why zip_eq_fast leads to compile error?"
    )]
    fn do_action<A: OpAction>(
        &mut self,
        mut f: impl FnMut(&SourceColumnDesc) -> Result<A::Output>,
    ) -> Result<WriteGuard> {
        let mut modify_col = Vec::with_capacity(self.descs.len());
        self.descs
            .iter()
            .zip_eq(self.builders.iter_mut())
            .enumerate()
            .try_for_each(|(idx, (desc, builder))| -> Result<()> {
                if desc.is_meta {
                    return Ok(());
                }
                let output = if desc.is_row_id {
                    A::DEFAULT_OUTPUT
                } else {
                    f(desc)?
                };
                A::apply(builder, output);
                modify_col.push(idx);

                Ok(())
            })
            .inspect_err(|e| {
                tracing::warn!("failed to parse source data: {}", e);
                modify_col.iter().for_each(|idx| {
                    A::rollback(&mut self.builders[*idx]);
                });
            })?;

        A::finish(self);

        Ok(WriteGuard(()))
    }

    /// Write an `Insert` record to the [`StreamChunk`].
    ///
    /// # Arguments
    ///
    /// * `self`: Ownership is consumed so only one record can be written.
    /// * `f`: A failable closure that produced one [`Datum`] by corresponding [`SourceColumnDesc`].
    pub fn insert(
        &mut self,
        f: impl FnMut(&SourceColumnDesc) -> Result<Datum>,
    ) -> Result<WriteGuard> {
        self.do_action::<OpActionInsert>(f)
    }

    /// For other op like 'insert', 'update', 'delete', we will leave the hollow for the meta column
    /// builder. e.g after insert
    /// `data_builder` = [1], `meta_column_builder` = [], `op` = [insert]
    ///
    /// This function is used to fulfill this hollow in `meta_column_builder`.
    /// e.g after fulfill
    /// `data_builder` = [1], `meta_column_builder` = [1], `op` = [insert]
    pub fn fulfill_meta_column(
        &mut self,
        mut f: impl FnMut(&SourceColumnDesc) -> Option<Datum>,
    ) -> Result<WriteGuard> {
        self.descs
            .iter()
            .zip_eq_fast(self.builders.iter_mut())
            .for_each(|(desc, builder)| {
                if let Some(output) = f(desc) {
                    builder.append(output);
                }
            });

        Ok(WriteGuard(()))
    }

    /// Write a `Delete` record to the [`StreamChunk`].
    ///
    /// # Arguments
    ///
    /// * `self`: Ownership is consumed so only one record can be written.
    /// * `f`: A failable closure that produced one [`Datum`] by corresponding [`SourceColumnDesc`].
    pub fn delete(
        &mut self,
        f: impl FnMut(&SourceColumnDesc) -> Result<Datum>,
    ) -> Result<WriteGuard> {
        self.do_action::<OpActionDelete>(f)
    }

    /// Write a `Update` record to the [`StreamChunk`].
    ///
    /// # Arguments
    ///
    /// * `self`: Ownership is consumed so only one record can be written.
    /// * `f`: A failable closure that produced two [`Datum`]s as old and new value by corresponding
    ///   [`SourceColumnDesc`].
    pub fn update(
        &mut self,
        f: impl FnMut(&SourceColumnDesc) -> Result<(Datum, Datum)>,
    ) -> Result<WriteGuard> {
        self.do_action::<OpActionUpdate>(f)
    }
}

/// `ByteStreamSourceParser` is a new message parser, the parser should consume
/// the input data stream and return a stream of parsed msgs.
pub trait ByteStreamSourceParser: Send + Debug + Sized + 'static {
    /// The column descriptors of the output chunk.
    fn columns(&self) -> &[SourceColumnDesc];

    /// The source context, used to report parsing error.
    fn source_ctx(&self) -> &SourceContext;

    /// Parse one record from the given `payload` and write it to the `writer`.
    fn parse_one<'a>(
        &'a mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> impl Future<Output = Result<WriteGuard>> + Send + 'a;

    /// Parse a data stream of one source split into a stream of [`StreamChunk`].
    ///
    /// # Arguments
    /// - `data_stream`: A data stream of one source split.
    ///  To be able to split multiple messages from mq, so it is not a pure byte stream
    ///
    /// # Returns
    ///
    /// A [`BoxSourceWithStateStream`] which is a stream of parsed msgs.
    fn into_stream(self, data_stream: BoxSourceStream) -> impl SourceWithStateStream {
        into_chunk_stream(self, data_stream)
    }
}

// TODO: when upsert is disabled, how to filter those empty payload
// Currently, an err is returned for non upsert with empty payload
#[try_stream(ok = StreamChunkWithState, error = RwError)]
async fn into_chunk_stream<P: ByteStreamSourceParser>(mut parser: P, data_stream: BoxSourceStream) {
    #[for_await]
    for batch in data_stream {
        let batch = batch?;
        let mut builder =
            SourceStreamChunkBuilder::with_capacity(parser.columns().to_vec(), batch.len());
        let mut split_offset_mapping: HashMap<SplitId, String> = HashMap::new();

        for msg in batch {
            if msg.key.is_none() && msg.payload.is_none() {
                continue;
            }

            split_offset_mapping.insert(msg.split_id, msg.offset);

            let old_op_num = builder.op_num();

            if let Err(e) = parser
                .parse_one(msg.key, msg.payload, builder.row_writer())
                .await
            {
                tracing::warn!("message parsing failed {}, skipping", e.to_string());
                // This will throw an error for batch
                parser.source_ctx().report_user_source_error(e)?;
                continue;
            }

            let new_op_num = builder.op_num();

            // new_op_num - old_op_num is the number of rows added to the builder
            for _ in old_op_num..new_op_num {
                // TODO: support more kinds of SourceMeta
                if let SourceMeta::Kafka(kafka_meta) = &msg.meta {
                    let f = |desc: &SourceColumnDesc| -> Option<risingwave_common::types::Datum> {
                        if !desc.is_meta {
                            return None;
                        }
                        match desc.name.as_str() {
                            "_rw_kafka_timestamp" => Some(kafka_meta.timestamp.map(|ts| {
                                risingwave_common::cast::i64_to_timestamptz(ts)
                                    .unwrap()
                                    .into()
                            })),
                            _ => {
                                unreachable!("kafka will not have this meta column: {}", desc.name)
                            }
                        }
                    };
                    builder.row_writer().fulfill_meta_column(f)?;
                }
            }
        }

        yield StreamChunkWithState {
            chunk: builder.finish(),
            split_offset_mapping: Some(split_offset_mapping),
        };
    }
}

pub trait AccessBuilder {
    async fn generate_accessor(&mut self, payload: Vec<u8>) -> Result<AccessImpl<'_, '_>>;
}

#[derive(Debug)]
pub enum EncodingType {
    Key,
    Value,
}

#[derive(Debug)]
pub enum AccessBuilderImpl {
    Avro(AvroAccessBuilder),
    Protobuf(ProtobufAccessBuilder),
    Json(JsonAccessBuilder),
    Bytes(BytesAccessBuilder),
    DebeziumAvro(DebeziumAvroAccessBuilder),
    DebeziumJson(DebeziumJsonAccessBuilder),
}

impl AccessBuilderImpl {
    pub async fn new_default(config: EncodingProperties, kv: EncodingType) -> Result<Self> {
        let accessor = match config {
            EncodingProperties::Avro(_) => {
                let config = AvroParserConfig::new(config).await?;
                AccessBuilderImpl::Avro(AvroAccessBuilder::new(config, kv)?)
            }
            EncodingProperties::Protobuf(_) => {
                let config = ProtobufParserConfig::new(config).await?;
                AccessBuilderImpl::Protobuf(ProtobufAccessBuilder::new(config)?)
            }
            EncodingProperties::Bytes => AccessBuilderImpl::Bytes(BytesAccessBuilder::new()?),
            EncodingProperties::Json(_) => AccessBuilderImpl::Json(JsonAccessBuilder::new()?),
            EncodingProperties::Csv(_) => unreachable!(),
            EncodingProperties::None => unreachable!(),
        };
        Ok(accessor)
    }

    pub async fn generate_accessor(&mut self, payload: Vec<u8>) -> Result<AccessImpl<'_, '_>> {
        let accessor = match self {
            Self::Avro(builder) => builder.generate_accessor(payload).await?,
            Self::Protobuf(builder) => builder.generate_accessor(payload).await?,
            Self::Json(builder) => builder.generate_accessor(payload).await?,
            Self::Bytes(builder) => builder.generate_accessor(payload).await?,
            Self::DebeziumAvro(builder) => builder.generate_accessor(payload).await?,
            Self::DebeziumJson(builder) => builder.generate_accessor(payload).await?,
        };
        Ok(accessor)
    }
}

#[derive(Debug)]
pub enum ByteStreamSourceParserImpl {
    Csv(CsvParser),
    Json(JsonParser),
    Debezium(DebeziumParser),
    Plain(PlainParser),
    Upsert(UpsertParser),
    DebeziumMongoJson(DebeziumMongoJsonParser),
    Maxwell(MaxwellParser),
    CanalJson(CanalJsonParser),
}

pub type ParserStream = impl SourceWithStateStream + Unpin;

impl ByteStreamSourceParserImpl {
    /// Converts this parser into a stream of [`StreamChunk`].
    pub fn into_stream(self, msg_stream: BoxSourceStream) -> ParserStream {
        #[auto_enum(futures03::Stream)]
        let stream = match self {
            Self::Csv(parser) => parser.into_stream(msg_stream),
            Self::Json(parser) => parser.into_stream(msg_stream),
            Self::Debezium(parser) => parser.into_stream(msg_stream),
            Self::DebeziumMongoJson(parser) => parser.into_stream(msg_stream),
            Self::Maxwell(parser) => parser.into_stream(msg_stream),
            Self::CanalJson(parser) => parser.into_stream(msg_stream),
            Self::Plain(parser) => parser.into_stream(msg_stream),
            Self::Upsert(parser) => parser.into_stream(msg_stream),
        };
        Box::pin(stream)
    }
}

impl ByteStreamSourceParserImpl {
    pub async fn create(parser_config: ParserConfig, source_ctx: SourceContextRef) -> Result<Self> {
        let CommonParserConfig { rw_columns } = parser_config.common;
        match parser_config.specific {
            SpecificParserConfig::Json => JsonParser::new(rw_columns, source_ctx).map(Self::Json),
            SpecificParserConfig::Csv(config) => {
                CsvParser::new(rw_columns, config, source_ctx).map(Self::Csv)
            }
            SpecificParserConfig::CanalJson => {
                CanalJsonParser::new(rw_columns, source_ctx).map(Self::CanalJson)
            }
            SpecificParserConfig::DebeziumMongoJson => {
                DebeziumMongoJsonParser::new(rw_columns, source_ctx).map(Self::DebeziumMongoJson)
            }
            SpecificParserConfig::Native => {
                unreachable!("Native parser should not be created")
            }
            SpecificParserConfig::Upsert(config) => {
                let parser = UpsertParser::new(config, rw_columns, source_ctx).await?;
                Ok(Self::Upsert(parser))
            }
            SpecificParserConfig::Plain(config) => {
                let parser = PlainParser::new(config, rw_columns, source_ctx).await?;
                Ok(Self::Plain(parser))
            }
            SpecificParserConfig::Debezium(config) => {
                let parser = DebeziumParser::new(config, rw_columns, source_ctx).await?;
                Ok(Self::Debezium(parser))
            }
            SpecificParserConfig::Maxwell(config) => {
                let parser = MaxwellParser::new(config, rw_columns, source_ctx).await?;
                Ok(Self::Maxwell(parser))
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ParserConfig {
    pub common: CommonParserConfig,
    pub specific: SpecificParserConfig,
}

#[derive(Debug, Clone, Default)]
pub struct CommonParserConfig {
    pub rw_columns: Vec<SourceColumnDesc>,
}

#[derive(Debug, Clone, Default)]
pub enum SpecificParserConfig {
    Upsert(ParserProperties),
    Plain(ParserProperties),
    Debezium(ParserProperties),
    Maxwell(ParserProperties),
    DebeziumMongoJson,
    Csv(CsvParserConfig),
    // Json and CanalJson are special since each of their payload generates
    // multiple accessors.
    Json,
    CanalJson,
    #[default]
    Native,
}

#[derive(Debug, Clone, Default)]
pub struct SchemaRegistryAuth {
    username: Option<String>,
    password: Option<String>,
}

impl From<&HashMap<String, String>> for SchemaRegistryAuth {
    fn from(props: &HashMap<String, String>) -> Self {
        const SCHEMA_REGISTRY_USERNAME: &str = "schema.registry.username";
        const SCHEMA_REGISTRY_PASSWORD: &str = "schema.registry.password";

        SchemaRegistryAuth {
            username: props.get(SCHEMA_REGISTRY_USERNAME).cloned(),
            password: props.get(SCHEMA_REGISTRY_PASSWORD).cloned(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct AvroProperties {
    pub use_schema_registry: bool,
    pub row_schema_location: String,
    pub upsert_primary_key: String,
    pub client_config: SchemaRegistryAuth,
    pub aws_auth_props: Option<AwsAuthProps>,
    pub topic: String,
    pub enable_upsert: bool,
}

#[derive(Debug, Default, Clone)]
pub struct ProtobufProperties {
    pub message_name: String,
    pub use_schema_registry: bool,
    pub row_schema_location: String,
    pub aws_auth_props: Option<AwsAuthProps>,
    pub client_config: SchemaRegistryAuth,
    pub topic: String,
}

#[derive(Debug, Default, Clone)]
pub struct CsvProperties {
    pub delimiter: u8,
    pub has_header: bool,
}

#[derive(Debug, Default, Clone)]
pub struct JsonProperties {}

#[derive(Debug, Default, Clone)]
pub enum EncodingProperties {
    Avro(AvroProperties),
    Protobuf(ProtobufProperties),
    Csv(CsvProperties),
    Json(JsonProperties),
    Bytes,
    #[default]
    None,
}

#[derive(Debug, Default, Clone)]
pub enum ProtocolProperties {
    Debezium,
    Maxwell,
    Canal,
    #[default]
    Plain,
    Upsert,
}

#[derive(Debug, Default, Clone)]
pub struct ParserProperties {
    pub key_encoding_config: Option<EncodingProperties>,
    pub encoding_config: EncodingProperties,
    pub protocol_config: ProtocolProperties,
}

/// Requirements of each parser currently
/// 1. CSV: delimiter, has header
/// 2. AVRO/AVRO UPSERT: row schema location, use schema registry
///    client info, topic, aws config, optional primary key
/// 3. PROTOBUF: message name, row schema location, use schema registry
///    client info, topic, aws config
/// 4. DEBEZIUM AVRO: row schema location, topic, client info
/// 5. Other: none

impl ParserProperties {
    pub fn new(
        format: SourceFormat,
        props: &HashMap<String, String>,
        info: &StreamSourceInfo,
    ) -> Result<Self> {
        let (encoding_config, protocol_config) = match format {
            SourceFormat::Csv => (
                EncodingProperties::Csv(CsvProperties {
                    delimiter: info.csv_delimiter as u8,
                    has_header: info.csv_has_header,
                }),
                ProtocolProperties::Plain,
            ),
            SourceFormat::Avro | SourceFormat::UpsertAvro => {
                let mut config = AvroProperties {
                    use_schema_registry: info.use_schema_registry,
                    row_schema_location: info.row_schema_location.clone(),
                    upsert_primary_key: info.upsert_avro_primary_key.clone(),
                    ..Default::default()
                };
                // TODO:
                if let SourceFormat::UpsertAvro = format {
                    config.enable_upsert = true;
                }
                if info.use_schema_registry {
                    config.topic = get_kafka_topic(props)?.clone();
                    config.client_config = SchemaRegistryAuth::from(props);
                } else {
                    config.aws_auth_props = Some(AwsAuthProps::from_pairs(
                        props.iter().map(|(k, v)| (k.as_str(), v.as_str())),
                    ));
                }
                (
                    EncodingProperties::Avro(config),
                    match format {
                        SourceFormat::Avro => ProtocolProperties::Plain,
                        SourceFormat::UpsertAvro => ProtocolProperties::Upsert,
                        _ => unreachable!(),
                    },
                )
            }
            SourceFormat::Protobuf => {
                let mut config = ProtobufProperties {
                    message_name: info.proto_message_name.clone(),
                    use_schema_registry: info.use_schema_registry,
                    row_schema_location: info.row_schema_location.clone(),
                    ..Default::default()
                };
                if info.use_schema_registry {
                    config.topic = get_kafka_topic(props)?.clone();
                    config.client_config = SchemaRegistryAuth::from(props);
                } else {
                    config.aws_auth_props = Some(AwsAuthProps::from_pairs(
                        props.iter().map(|(k, v)| (k.as_str(), v.as_str())),
                    ));
                }
                (
                    EncodingProperties::Protobuf(config),
                    ProtocolProperties::Plain,
                )
            }
            SourceFormat::DebeziumAvro => (
                EncodingProperties::Avro(AvroProperties {
                    row_schema_location: info.row_schema_location.clone(),
                    topic: get_kafka_topic(props).unwrap().clone(),
                    client_config: SchemaRegistryAuth::from(props),
                    ..Default::default()
                }),
                ProtocolProperties::Debezium,
            ),
            SourceFormat::DebeziumJson => (
                EncodingProperties::Json(JsonProperties {}),
                ProtocolProperties::Debezium,
            ),
            SourceFormat::DebeziumMongoJson => (
                EncodingProperties::Json(JsonProperties {}),
                ProtocolProperties::Debezium,
            ),
            SourceFormat::Maxwell => (
                EncodingProperties::Json(JsonProperties {}),
                ProtocolProperties::Maxwell,
            ),
            SourceFormat::CanalJson => (
                EncodingProperties::Json(JsonProperties {}),
                ProtocolProperties::Canal,
            ),
            SourceFormat::Json => (
                EncodingProperties::Json(JsonProperties {}),
                ProtocolProperties::Plain,
            ),
            SourceFormat::UpsertJson => (
                EncodingProperties::Json(JsonProperties {}),
                ProtocolProperties::Upsert,
            ),
            SourceFormat::Bytes => (EncodingProperties::Bytes, ProtocolProperties::Plain),
            SourceFormat::Native | SourceFormat::Invalid => {
                (EncodingProperties::None, ProtocolProperties::Plain)
            }
        };
        // TODO: need to build correct key encoding config
        Ok(ParserProperties {
            key_encoding_config: None,
            encoding_config,
            protocol_config,
        })
    }
}

impl SpecificParserConfig {
    // Need to be fixed when message key is introduced.
    pub fn get_source_format(&self) -> SourceFormat {
        match self {
            SpecificParserConfig::Csv(_) => SourceFormat::Csv,
            SpecificParserConfig::Json => SourceFormat::Json,
            SpecificParserConfig::CanalJson => SourceFormat::CanalJson,
            SpecificParserConfig::Native => SourceFormat::Native,
            SpecificParserConfig::DebeziumMongoJson => SourceFormat::DebeziumMongoJson,

            SpecificParserConfig::Upsert(config) => match config.encoding_config {
                EncodingProperties::Avro(_) => SourceFormat::UpsertAvro,
                EncodingProperties::Json(_) => SourceFormat::UpsertJson,
                _ => unreachable!(),
            },
            SpecificParserConfig::Plain(config) => match config.encoding_config {
                EncodingProperties::Avro(_) => SourceFormat::Avro,
                EncodingProperties::Protobuf(_) => SourceFormat::Protobuf,
                EncodingProperties::Csv(_) => SourceFormat::Csv,
                EncodingProperties::Bytes => SourceFormat::Bytes,
                _ => unreachable!(),
            },
            SpecificParserConfig::Maxwell(_) => SourceFormat::Maxwell,
            SpecificParserConfig::Debezium(config) => match config.encoding_config {
                EncodingProperties::Avro(_) => SourceFormat::DebeziumAvro,
                EncodingProperties::Json(_) => SourceFormat::DebeziumJson,
                _ => unreachable!(),
            },
        }
    }

    pub fn new(
        format: SourceFormat,
        info: &StreamSourceInfo,
        props: &HashMap<String, String>,
    ) -> Result<Self> {
        let parser_properties = ParserProperties::new(format, props, info)?;
        let conf = match format {
            SourceFormat::Csv => {
                SpecificParserConfig::Csv(CsvParserConfig::new(parser_properties.encoding_config)?)
            }
            SourceFormat::Json => SpecificParserConfig::Json,
            SourceFormat::DebeziumMongoJson => SpecificParserConfig::DebeziumMongoJson,
            SourceFormat::CanalJson => SpecificParserConfig::CanalJson,
            SourceFormat::Native => SpecificParserConfig::Native,

            SourceFormat::DebeziumJson => SpecificParserConfig::Debezium(parser_properties),
            SourceFormat::DebeziumAvro => SpecificParserConfig::Debezium(parser_properties),

            SourceFormat::Maxwell => SpecificParserConfig::Maxwell(parser_properties),

            SourceFormat::Avro => SpecificParserConfig::Plain(parser_properties),
            SourceFormat::Bytes => SpecificParserConfig::Plain(parser_properties),
            SourceFormat::Protobuf => SpecificParserConfig::Plain(parser_properties),

            SourceFormat::UpsertAvro => SpecificParserConfig::Upsert(parser_properties),
            SourceFormat::UpsertJson => SpecificParserConfig::Upsert(parser_properties),

            SourceFormat::Invalid => {
                return Err(RwError::from(ProtocolError(
                    "invalid source format".to_string(),
                )));
            }
        };
        Ok(conf)
    }
}

impl ParserConfig {
    pub fn new(
        format: SourceFormat,
        info: &StreamSourceInfo,
        props: &HashMap<String, String>,
        rw_columns: &Vec<SourceColumnDesc>,
    ) -> Result<Self> {
        let common = CommonParserConfig {
            rw_columns: rw_columns.to_owned(),
        };
        let specific = SpecificParserConfig::new(format, info, props)?;

        Ok(Self { common, specific })
    }
}
