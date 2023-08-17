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
use futures::{Future, TryFutureExt};
use futures_async_stream::try_stream;
use itertools::{Either, Itertools};
pub use json_parser::*;
pub use protobuf::*;
use risingwave_common::array::stream_chunk::{Offset, StreamChunkMeta};
use risingwave_common::array::{ArrayBuilderImpl, Op, StreamChunk};
use risingwave_common::catalog::KAFKA_TIMESTAMP_COLUMN_NAME;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::Datum;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::catalog::{
    SchemaRegistryNameStrategy as PbSchemaRegistryNameStrategy, StreamSourceInfo,
};
pub use schema_registry::name_strategy_from_str;

use self::avro::AvroAccessBuilder;
use self::bytes_parser::BytesAccessBuilder;
pub use self::common::mysql_row_to_datums;
use self::plain_parser::PlainParser;
use self::simd_json_parser::DebeziumJsonAccessBuilder;
use self::unified::AccessImpl;
use self::upsert_parser::UpsertParser;
use self::util::get_kafka_topic;
use crate::aws_auth::AwsAuthProps;
use crate::parser::maxwell::MaxwellParser;
use crate::source::{
    BoxSourceStream, SourceColumnDesc, SourceContext, SourceContextRef, SourceEncode, SourceFormat,
    SourceMeta, SourceStruct, SourceWithStateStream, SplitId, StreamChunkWithState,
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
    offset_builder: Vec<Offset>,
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
            offset_builder: Vec::with_capacity(cap),
        }
    }

    pub fn row_writer(&mut self) -> SourceStreamChunkRowWriter<'_> {
        SourceStreamChunkRowWriter {
            descs: &self.descs,
            builders: &mut self.builders,
            op_builder: &mut self.op_builder,
            offset_builder: &mut self.offset_builder,
            row_offset: None,
        }
    }

    pub fn row_writer_with_offset(&mut self, offset: Offset) -> SourceStreamChunkRowWriter<'_> {
        SourceStreamChunkRowWriter {
            descs: &self.descs,
            builders: &mut self.builders,
            op_builder: &mut self.op_builder,
            offset_builder: &mut self.offset_builder,
            row_offset: Some(offset),
        }
    }

    /// Consumes the builder and returns a [`StreamChunk`].
    pub fn finish(self) -> StreamChunk {
        let mut chunk = StreamChunk::new(
            self.op_builder,
            self.builders
                .into_iter()
                .map(|builder| builder.finish().into())
                .collect(),
            None,
        );
        if !self.offset_builder.is_empty() {
            chunk.set_meta(StreamChunkMeta::new(self.offset_builder));
        }
        chunk
    }

    /// Resets the builder and returns a [`StreamChunk`], while reserving `next_cap` capacity for
    /// the builders of the next [`StreamChunk`].
    #[must_use]
    pub fn take(&mut self, next_cap: usize) -> StreamChunk {
        let descs = std::mem::take(&mut self.descs);
        let builder = std::mem::replace(self, Self::with_capacity(descs, next_cap));
        builder.finish()
    }

    pub fn op_num(&self) -> usize {
        self.op_builder.len()
    }

    pub fn is_empty(&self) -> bool {
        self.op_builder.is_empty()
    }
}

/// `SourceStreamChunkRowWriter` is responsible to write one row (Insert/Delete) or two rows
/// (Update) to the [`StreamChunk`].
pub struct SourceStreamChunkRowWriter<'a> {
    descs: &'a [SourceColumnDesc],
    builders: &'a mut [ArrayBuilderImpl],
    op_builder: &'a mut Vec<Op>,
    offset_builder: &'a mut Vec<Offset>,
    row_offset: Option<Offset>,
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
        writer.op_builder.push(Op::Insert);
        if let Some(offset) = writer.row_offset.clone() {
            writer.offset_builder.push(offset)
        }
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
        writer.op_builder.push(Op::Delete);
        if let Some(offset) = writer.row_offset.clone() {
            writer.offset_builder.push(offset)
        }
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
        // use same offsets for update delete and update insert event
        if let Some(offset) = &writer.row_offset {
            writer.offset_builder.push(offset.clone());
            writer.offset_builder.push(offset.clone());
        }
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

/// Transaction control message. Currently only used by Debezium messages.
pub enum TransactionControl {
    Begin { id: Box<str> },
    Commit { id: Box<str> },
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

    /// Parse one record from the given `payload`, either write it to the `writer` or interpret it
    /// as a transaction control message.
    ///
    /// The default implementation forwards to [`ByteStreamSourceParser::parse_one`] for
    /// non-transactional sources.
    fn parse_one_with_txn<'a>(
        &'a mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> impl Future<Output = Result<Either<WriteGuard, TransactionControl>>> + Send + 'a {
        self.parse_one(key, payload, writer).map_ok(Either::Left)
    }

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

/// Maximum number of rows in a transaction. If a transaction is larger than this, it will be force
/// committed to avoid potential OOM.
const MAX_ROWS_FOR_TRANSACTION: usize = 4096;

// TODO: when upsert is disabled, how to filter those empty payload
// Currently, an err is returned for non upsert with empty payload
#[try_stream(ok = StreamChunkWithState, error = RwError)]
async fn into_chunk_stream<P: ByteStreamSourceParser>(mut parser: P, data_stream: BoxSourceStream) {
    let columns = parser.columns().to_vec();

    let mut builder = SourceStreamChunkBuilder::with_capacity(columns, 0);
    let mut split_offset_mapping = HashMap::<SplitId, String>::new();

    struct Transaction {
        id: Box<str>,
        len: usize,
    }
    let mut current_transaction = None;
    let mut yield_asap = false; // whether we should yield the chunk as soon as possible (txn commits)

    #[for_await]
    for batch in data_stream {
        let batch = batch?;
        let batch_len = batch.len();

        if let Some(Transaction { len, id }) = &mut current_transaction {
            // Dirty state. The last batch is not yielded due to uncommitted transaction.
            if *len > MAX_ROWS_FOR_TRANSACTION {
                // Large transaction. Force commit.
                tracing::warn!(
                    id,
                    len,
                    "transaction is larger than {MAX_ROWS_FOR_TRANSACTION} rows, force commit"
                );
                *len = 0; // reset `len` while keeping `id`
                yield_asap = false;
                yield StreamChunkWithState {
                    chunk: builder.take(batch_len),
                    split_offset_mapping: Some(std::mem::take(&mut split_offset_mapping)),
                };
            } else {
                // Normal transaction. After the transaction is committed, we should yield the last
                // batch immediately, so set `yield_asap` to true.
                yield_asap = true;
            }
        } else {
            // Clean state. Reserve capacity for the builder.
            assert!(builder.is_empty());
            assert!(!yield_asap);
            assert!(split_offset_mapping.is_empty());
            let _ = builder.take(batch_len);
        }

        for (i, msg) in batch.into_iter().enumerate() {
            if msg.key.is_none() && msg.payload.is_none() {
                continue;
            }

            let msg_offset = msg.offset;
            split_offset_mapping.insert(msg.split_id, msg_offset.clone());

            let old_op_num = builder.op_num();

            match parser
                .parse_one_with_txn(
                    msg.key,
                    msg.payload,
                    builder.row_writer_with_offset(Offset::new(msg_offset)),
                )
                .await
            {
                Ok(Either::Left(WriteGuard(_))) => {
                    // new_op_num - old_op_num is the number of rows added to the builder
                    let new_op_num = builder.op_num();

                    // Aggregate the number of rows in the current transaction.
                    if let Some(Transaction { len, .. }) = &mut current_transaction {
                        *len += new_op_num - old_op_num;
                    }

                    for _ in old_op_num..new_op_num {
                        // TODO: support more kinds of SourceMeta
                        if let SourceMeta::Kafka(kafka_meta) = &msg.meta {
                            let f = |desc: &SourceColumnDesc| -> Option<risingwave_common::types::Datum> {
                        if !desc.is_meta {
                            return None;
                        }
                        match desc.name.as_str() {
                            KAFKA_TIMESTAMP_COLUMN_NAME => Some(kafka_meta.timestamp.map(|ts| {
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

                Ok(Either::Right(txn_ctl)) => {
                    match txn_ctl {
                        TransactionControl::Begin { id } => {
                            if let Some(Transaction { id: current_id, .. }) = &current_transaction {
                                tracing::warn!(current_id, id, "already in transaction");
                            }
                            current_transaction = Some(Transaction { id, len: 0 });
                        }
                        TransactionControl::Commit { id } => {
                            let current_id = current_transaction.as_ref().map(|t| &t.id);
                            if current_id != Some(&id) {
                                tracing::warn!(?current_id, id, "transaction id mismatch");
                            }
                            current_transaction = None;
                        }
                    }

                    // Not in a transaction anymore and `yield_asap` is set, so we should yield the
                    // chunk now.
                    if current_transaction.is_none() && yield_asap {
                        yield_asap = false;
                        yield StreamChunkWithState {
                            chunk: builder.take(batch_len - (i + 1)),
                            split_offset_mapping: Some(std::mem::take(&mut split_offset_mapping)),
                        };
                    }
                }

                Err(error) => {
                    tracing::warn!(%error, "message parsing failed, skipping");
                    // This will throw an error for batch
                    parser.source_ctx().report_user_source_error(error)?;
                    continue;
                }
            }
        }

        // If we are not in a transaction, we should yield the chunk now.
        if current_transaction.is_none() {
            yield_asap = false;
            yield StreamChunkWithState {
                chunk: builder.take(0),
                split_offset_mapping: Some(std::mem::take(&mut split_offset_mapping)),
            };
        }
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
            EncodingProperties::Bytes(_) => {
                AccessBuilderImpl::Bytes(BytesAccessBuilder::new(config)?)
            }
            EncodingProperties::Json(_) => AccessBuilderImpl::Json(JsonAccessBuilder::new()?),
            _ => unreachable!(),
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
        let protocol = &parser_config.specific.protocol_config;
        let encode = &parser_config.specific.encoding_config;
        match (protocol, encode) {
            (ProtocolProperties::Plain, EncodingProperties::Json(_)) => {
                JsonParser::new(rw_columns, source_ctx).map(Self::Json)
            }
            (ProtocolProperties::Plain, EncodingProperties::Csv(config)) => {
                CsvParser::new(rw_columns, *config, source_ctx).map(Self::Csv)
            }
            (ProtocolProperties::DebeziumMongo, EncodingProperties::Json(_)) => {
                DebeziumMongoJsonParser::new(rw_columns, source_ctx).map(Self::DebeziumMongoJson)
            }
            (ProtocolProperties::Canal, EncodingProperties::Json(_)) => {
                CanalJsonParser::new(rw_columns, source_ctx).map(Self::CanalJson)
            }
            (ProtocolProperties::Native, _) => unreachable!("Native parser should not be created"),
            (ProtocolProperties::Upsert, _) => {
                let parser =
                    UpsertParser::new(parser_config.specific, rw_columns, source_ctx).await?;
                Ok(Self::Upsert(parser))
            }
            (ProtocolProperties::Plain, _) => {
                let parser =
                    PlainParser::new(parser_config.specific, rw_columns, source_ctx).await?;
                Ok(Self::Plain(parser))
            }
            (ProtocolProperties::Debezium, _) => {
                let parser =
                    DebeziumParser::new(parser_config.specific, rw_columns, source_ctx).await?;
                Ok(Self::Debezium(parser))
            }
            (ProtocolProperties::Maxwell, _) => {
                let parser =
                    MaxwellParser::new(parser_config.specific, rw_columns, source_ctx).await?;
                Ok(Self::Maxwell(parser))
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ParserConfig {
    pub common: CommonParserConfig,
    pub specific: SpecificParserConfig,
}

impl ParserConfig {
    pub fn get_config(self) -> (Vec<SourceColumnDesc>, SpecificParserConfig) {
        (self.common.rw_columns, self.specific)
    }
}

#[derive(Debug, Clone, Default)]
pub struct CommonParserConfig {
    pub rw_columns: Vec<SourceColumnDesc>,
}

#[derive(Debug, Clone, Default)]
pub struct SpecificParserConfig {
    pub key_encoding_config: Option<EncodingProperties>,
    pub encoding_config: EncodingProperties,
    pub protocol_config: ProtocolProperties,
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
    pub record_name: Option<String>,
    pub key_record_name: Option<String>,
    pub name_strategy: PbSchemaRegistryNameStrategy,
}

#[derive(Debug, Default, Clone)]
pub struct ProtobufProperties {
    pub message_name: String,
    pub use_schema_registry: bool,
    pub row_schema_location: String,
    pub aws_auth_props: Option<AwsAuthProps>,
    pub client_config: SchemaRegistryAuth,
    pub enable_upsert: bool,
    pub topic: String,
    pub key_message_name: Option<String>,
    pub name_strategy: PbSchemaRegistryNameStrategy,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CsvProperties {
    pub delimiter: u8,
    pub has_header: bool,
}

#[derive(Debug, Default, Clone)]
pub struct JsonProperties {}

#[derive(Debug, Default, Clone)]
pub struct BytesProperties {
    pub column_name: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub enum EncodingProperties {
    Avro(AvroProperties),
    Protobuf(ProtobufProperties),
    Csv(CsvProperties),
    Json(JsonProperties),
    Bytes(BytesProperties),
    Native,
    #[default]
    Unspecified,
}

#[derive(Debug, Default, Clone)]
pub enum ProtocolProperties {
    Debezium,
    DebeziumMongo,
    Maxwell,
    Canal,
    Plain,
    Upsert,
    Native,
    #[default]
    Unspecified,
}

impl SpecificParserConfig {
    pub fn get_source_struct(&self) -> SourceStruct {
        let format = match self.protocol_config {
            ProtocolProperties::Debezium => SourceFormat::Debezium,
            ProtocolProperties::DebeziumMongo => SourceFormat::DebeziumMongo,
            ProtocolProperties::Maxwell => SourceFormat::Maxwell,
            ProtocolProperties::Canal => SourceFormat::Canal,
            ProtocolProperties::Plain => SourceFormat::Plain,
            ProtocolProperties::Upsert => SourceFormat::Upsert,
            ProtocolProperties::Native => SourceFormat::Native,
            ProtocolProperties::Unspecified => unreachable!(),
        };
        let encode = match self.encoding_config {
            EncodingProperties::Avro(_) => SourceEncode::Avro,
            EncodingProperties::Protobuf(_) => SourceEncode::Protobuf,
            EncodingProperties::Csv(_) => SourceEncode::Csv,
            EncodingProperties::Json(_) => SourceEncode::Json,
            EncodingProperties::Bytes(_) => SourceEncode::Bytes,
            EncodingProperties::Native => SourceEncode::Native,
            EncodingProperties::Unspecified => unreachable!(),
        };
        SourceStruct { format, encode }
    }

    // The validity of (format, encode) is ensured by `extract_format_encode`
    pub fn new(
        source_struct: SourceStruct,
        info: &StreamSourceInfo,
        props: &HashMap<String, String>,
    ) -> Result<Self> {
        let format = source_struct.format;
        let encode = source_struct.encode;
        // this transformation is needed since there may be config for the protocol
        // in the future
        let protocol_config = match format {
            SourceFormat::Native => ProtocolProperties::Native,
            SourceFormat::Debezium => ProtocolProperties::Debezium,
            SourceFormat::DebeziumMongo => ProtocolProperties::DebeziumMongo,
            SourceFormat::Maxwell => ProtocolProperties::Maxwell,
            SourceFormat::Canal => ProtocolProperties::Canal,
            SourceFormat::Upsert => ProtocolProperties::Upsert,
            SourceFormat::Plain => ProtocolProperties::Plain,
            _ => unreachable!(),
        };

        let encoding_config = match (format, encode) {
            (SourceFormat::Plain, SourceEncode::Csv) => EncodingProperties::Csv(CsvProperties {
                delimiter: info.csv_delimiter as u8,
                has_header: info.csv_has_header,
            }),
            (SourceFormat::Plain, SourceEncode::Avro)
            | (SourceFormat::Upsert, SourceEncode::Avro) => {
                let mut config = AvroProperties {
                    record_name: if info.proto_message_name.is_empty() {
                        None
                    } else {
                        Some(info.proto_message_name.clone())
                    },
                    key_record_name: info.key_message_name.clone(),
                    name_strategy: PbSchemaRegistryNameStrategy::from_i32(info.name_strategy)
                        .unwrap(),
                    use_schema_registry: info.use_schema_registry,
                    row_schema_location: info.row_schema_location.clone(),
                    upsert_primary_key: info.upsert_avro_primary_key.clone(),
                    ..Default::default()
                };
                if format == SourceFormat::Upsert {
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
                EncodingProperties::Avro(config)
            }
            (SourceFormat::Plain, SourceEncode::Protobuf)
            | (SourceFormat::Upsert, SourceEncode::Protobuf) => {
                if info.row_schema_location.is_empty() {
                    return Err(
                        ProtocolError("protobuf file location not provided".to_string()).into(),
                    );
                }
                let mut config = ProtobufProperties {
                    message_name: info.proto_message_name.clone(),
                    use_schema_registry: info.use_schema_registry,
                    row_schema_location: info.row_schema_location.clone(),
                    name_strategy: PbSchemaRegistryNameStrategy::from_i32(info.name_strategy)
                        .unwrap(),
                    key_message_name: info.key_message_name.clone(),
                    ..Default::default()
                };
                if format == SourceFormat::Upsert {
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
                EncodingProperties::Protobuf(config)
            }
            (SourceFormat::Debezium, SourceEncode::Avro) => {
                EncodingProperties::Avro(AvroProperties {
                    record_name: if info.proto_message_name.is_empty() {
                        None
                    } else {
                        Some(info.proto_message_name.clone())
                    },
                    name_strategy: PbSchemaRegistryNameStrategy::from_i32(info.name_strategy)
                        .unwrap(),
                    key_record_name: info.key_message_name.clone(),
                    row_schema_location: info.row_schema_location.clone(),
                    topic: get_kafka_topic(props).unwrap().clone(),
                    client_config: SchemaRegistryAuth::from(props),
                    ..Default::default()
                })
            }
            (
                SourceFormat::Debezium
                | SourceFormat::DebeziumMongo
                | SourceFormat::Maxwell
                | SourceFormat::Canal
                | SourceFormat::Plain
                | SourceFormat::Upsert,
                SourceEncode::Json,
            ) => EncodingProperties::Json(JsonProperties {}),
            (SourceFormat::Plain, SourceEncode::Bytes) => {
                EncodingProperties::Bytes(BytesProperties { column_name: None })
            }
            (SourceFormat::Native, SourceEncode::Native) => EncodingProperties::Native,
            (format, encode) => {
                return Err(RwError::from(ProtocolError(format!(
                    "Unsupported format {:?} encode {:?}",
                    format, encode
                ))));
            }
        };
        Ok(Self {
            key_encoding_config: None,
            encoding_config,
            protocol_config,
        })
    }
}

impl ParserConfig {
    pub fn new(
        source_struct: SourceStruct,
        info: &StreamSourceInfo,
        props: &HashMap<String, String>,
        rw_columns: &Vec<SourceColumnDesc>,
    ) -> Result<Self> {
        let common = CommonParserConfig {
            rw_columns: rw_columns.to_owned(),
        };
        let specific = SpecificParserConfig::new(source_struct, info, props)?;

        Ok(Self { common, specific })
    }
}
