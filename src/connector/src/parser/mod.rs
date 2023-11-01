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
pub use json_parser::*;
pub use protobuf::*;
use risingwave_common::array::{ArrayBuilderImpl, Op, StreamChunk};
use risingwave_common::catalog::{KAFKA_TIMESTAMP_COLUMN_NAME, TABLE_NAME_COLUMN_NAME};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{Datum, Scalar};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::catalog::{
    SchemaRegistryNameStrategy as PbSchemaRegistryNameStrategy, StreamSourceInfo,
};
use tracing_futures::Instrument;

use self::avro::AvroAccessBuilder;
use self::bytes_parser::BytesAccessBuilder;
pub use self::mysql::mysql_row_to_datums;
use self::plain_parser::PlainParser;
use self::simd_json_parser::DebeziumJsonAccessBuilder;
use self::unified::{AccessImpl, AccessResult};
use self::upsert_parser::UpsertParser;
use self::util::get_kafka_topic;
use crate::aws_auth::AwsAuthProps;
use crate::parser::maxwell::MaxwellParser;
use crate::schema::schema_registry::SchemaRegistryAuth;
use crate::source::{
    extract_source_struct, BoxSourceStream, SourceColumnDesc, SourceColumnType, SourceContext,
    SourceContextRef, SourceEncode, SourceFormat, SourceMeta, SourceWithStateStream, SplitId,
    StreamChunkWithState,
};

mod avro;
mod bytes_parser;
mod canal;
mod common;
mod csv_parser;
mod debezium;
mod json_parser;
mod maxwell;
mod mysql;
mod plain_parser;
mod protobuf;
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
            row_meta: None,
        }
    }

    /// Consumes the builder and returns a [`StreamChunk`].
    pub fn finish(self) -> StreamChunk {
        StreamChunk::new(
            self.op_builder,
            self.builders
                .into_iter()
                .map(|builder| builder.finish().into())
                .collect(),
        )
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

/// `SourceStreamChunkRowWriter` is responsible to write one or more records to the [`StreamChunk`],
/// where each contains either one row (Insert/Delete) or two rows (Update) that can be written atomically.
///
/// Callers are supposed to call one of the `insert`, `delete` or `update` methods to write a record,
/// providing a closure that produces one or two [`Datum`]s by corresponding [`SourceColumnDesc`].
/// Specifically,
/// - only columns with [`SourceColumnType::Normal`] need to be handled;
/// - errors for non-primary key columns will be ignored and filled with default value instead;
/// - other errors will be propagated.
pub struct SourceStreamChunkRowWriter<'a> {
    descs: &'a [SourceColumnDesc],
    builders: &'a mut [ArrayBuilderImpl],
    op_builder: &'a mut Vec<Op>,

    /// An optional meta data of the original message.
    ///
    /// When this is set by `with_meta`, it'll be used to fill the columns of types other than [`SourceColumnType::Normal`].
    row_meta: Option<MessageMeta<'a>>,
}

/// The meta data of the original message for a row writer.
///
/// Extracted from the `SourceMessage`.
#[derive(Clone, Copy)]
pub struct MessageMeta<'a> {
    meta: &'a SourceMeta,
    split_id: &'a str,
    offset: &'a str,
}

impl MessageMeta<'_> {
    /// Extract the value for the given column.
    ///
    /// Returns `None` if the column is not a meta column.
    fn value_for_column(self, desc: &SourceColumnDesc) -> Option<Datum> {
        match desc.column_type {
            // Row id columns are filled with `NULL` here and will be filled with the real
            // row id generated by `RowIdGenExecutor` later.
            SourceColumnType::RowId => Datum::None.into(),
            // Extract the offset from the meta data.
            SourceColumnType::Offset => Datum::Some(self.offset.into()).into(),
            // Extract custom meta data per connector.
            SourceColumnType::Meta if let SourceMeta::Kafka(kafka_meta) = self.meta => {
                assert_eq!(
                    desc.name.as_str(),
                    KAFKA_TIMESTAMP_COLUMN_NAME,
                    "unexpected kafka meta column name"
                );
                kafka_meta
                    .timestamp
                    .map(|ts| {
                        risingwave_common::cast::i64_to_timestamptz(ts)
                            .unwrap()
                            .to_scalar_value()
                    })
                    .into()
            },
            SourceColumnType::Meta if let SourceMeta::DebeziumCdc(cdc_meta) = self.meta => {
                assert_eq!(desc.name.as_str(), TABLE_NAME_COLUMN_NAME, "unexpected cdc meta column name");
                Datum::Some(cdc_meta.full_table_name.as_str().into()).into()
            }

            // For other cases, return `None`.
            SourceColumnType::Meta | SourceColumnType::Normal => None,
        }
    }
}

trait OpAction {
    type Output;

    fn output_for(datum: Datum) -> Self::Output;

    fn apply(builder: &mut ArrayBuilderImpl, output: Self::Output);

    fn rollback(builder: &mut ArrayBuilderImpl);

    fn finish(writer: &mut SourceStreamChunkRowWriter<'_>);
}

struct OpActionInsert;

impl OpAction for OpActionInsert {
    type Output = Datum;

    #[inline(always)]
    fn output_for(datum: Datum) -> Self::Output {
        datum
    }

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
    }
}

struct OpActionDelete;

impl OpAction for OpActionDelete {
    type Output = Datum;

    #[inline(always)]
    fn output_for(datum: Datum) -> Self::Output {
        datum
    }

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
    }
}

struct OpActionUpdate;

impl OpAction for OpActionUpdate {
    type Output = (Datum, Datum);

    #[inline(always)]
    fn output_for(datum: Datum) -> Self::Output {
        (datum.clone(), datum)
    }

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

impl<'a> SourceStreamChunkRowWriter<'a> {
    /// Set the meta data of the original message for this row writer.
    ///
    /// This should always be called except for tests.
    fn with_meta(self, row_meta: MessageMeta<'a>) -> Self {
        Self {
            row_meta: Some(row_meta),
            ..self
        }
    }
}

impl SourceStreamChunkRowWriter<'_> {
    fn do_action<A: OpAction>(
        &mut self,
        mut f: impl FnMut(&SourceColumnDesc) -> AccessResult<A::Output>,
    ) -> AccessResult<()> {
        let mut wrapped_f = |desc: &SourceColumnDesc| {
            if let Some(meta_value) =
                (self.row_meta.as_ref()).and_then(|row_meta| row_meta.value_for_column(desc))
            {
                // For meta columns, fill in the meta data.
                Ok(A::output_for(meta_value))
            } else {
                // For normal columns, call the user provided closure.
                match f(desc) {
                    Ok(output) => Ok(output),

                    // Throw error for failed access to primary key columns.
                    Err(e) if desc.is_pk => Err(e),
                    // Ignore error for other columns and fill in `NULL` instead.
                    Err(error) => {
                        // TODO: figure out a way to fill in not-null default value if user specifies one
                        // TODO: decide whether the error should not be ignored (e.g., even not a valid Debezium message)
                        // TODO: not using tracing span to provide `split_id` and `offset` due to performance concern,
                        //       see #13105
                        tracing::warn!(
                            %error,
                            split_id = self.row_meta.as_ref().map(|m| m.split_id),
                            offset = self.row_meta.as_ref().map(|m| m.offset),
                            column = desc.name,
                            "failed to parse non-pk column, padding with `NULL`"
                        );
                        Ok(A::output_for(Datum::None))
                    }
                }
            }
        };

        // Columns that changes have been applied to. Used to rollback when an error occurs.
        let mut applied_columns = Vec::with_capacity(self.descs.len());

        let result = (self.descs.iter())
            .zip_eq_fast(self.builders.iter_mut())
            .try_for_each(|(desc, builder)| {
                wrapped_f(desc).map(|output| {
                    A::apply(builder, output);
                    applied_columns.push(builder);
                })
            });

        match result {
            Ok(_) => {
                A::finish(self);
                Ok(())
            }
            Err(e) => {
                for builder in applied_columns {
                    A::rollback(builder);
                }
                Err(e)
            }
        }
    }

    /// Write an `Insert` record to the [`StreamChunk`], with the given fallible closure that
    /// produces one [`Datum`] by corresponding [`SourceColumnDesc`].
    ///
    /// See the [struct-level documentation](SourceStreamChunkRowWriter) for more details.
    pub fn insert(
        &mut self,
        f: impl FnMut(&SourceColumnDesc) -> AccessResult<Datum>,
    ) -> AccessResult<()> {
        self.do_action::<OpActionInsert>(f)
    }

    /// Write a `Delete` record to the [`StreamChunk`], with the given fallible closure that
    /// produces one [`Datum`] by corresponding [`SourceColumnDesc`].
    ///
    /// See the [struct-level documentation](SourceStreamChunkRowWriter) for more details.
    pub fn delete(
        &mut self,
        f: impl FnMut(&SourceColumnDesc) -> AccessResult<Datum>,
    ) -> AccessResult<()> {
        self.do_action::<OpActionDelete>(f)
    }

    /// Write a `Update` record to the [`StreamChunk`], with the given fallible closure that
    /// produces two [`Datum`]s as old and new value by corresponding [`SourceColumnDesc`].
    ///
    /// See the [struct-level documentation](SourceStreamChunkRowWriter) for more details.
    pub fn update(
        &mut self,
        f: impl FnMut(&SourceColumnDesc) -> AccessResult<(Datum, Datum)>,
    ) -> AccessResult<()> {
        self.do_action::<OpActionUpdate>(f)
    }
}

/// Transaction control message. Currently only used by Debezium messages.
#[derive(Debug)]
pub enum TransactionControl {
    Begin { id: Box<str> },
    Commit { id: Box<str> },
}

/// The result of parsing a message.
#[derive(Debug)]
pub enum ParseResult {
    /// Some rows are parsed and written to the [`SourceStreamChunkRowWriter`].
    Rows,
    /// A transaction control message is parsed.
    TransactionControl(TransactionControl),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ParserFormat {
    CanalJson,
    Csv,
    Json,
    Maxwell,
    Debezium,
    DebeziumMongo,
    Upsert,
    Plain,
}

/// `ByteStreamSourceParser` is a new message parser, the parser should consume
/// the input data stream and return a stream of parsed msgs.
pub trait ByteStreamSourceParser: Send + Debug + Sized + 'static {
    /// The column descriptors of the output chunk.
    fn columns(&self) -> &[SourceColumnDesc];

    /// The source context, used to report parsing error.
    fn source_ctx(&self) -> &SourceContext;

    /// The format of the specific parser.
    fn parser_format(&self) -> ParserFormat;

    /// Parse one record from the given `payload` and write rows to the `writer`.
    ///
    /// Returns error if **any** of the rows in the message failed to parse.
    fn parse_one<'a>(
        &'a mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    /// Parse one record from the given `payload`, either write rows to the `writer` or interpret it
    /// as a transaction control message.
    ///
    /// The default implementation forwards to [`ByteStreamSourceParser::parse_one`] for
    /// non-transactional sources.
    ///
    /// Returns error if **any** of the rows in the message failed to parse.
    fn parse_one_with_txn<'a>(
        &'a mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> impl Future<Output = Result<ParseResult>> + Send + 'a {
        self.parse_one(key, payload, writer)
            .map_ok(|_| ParseResult::Rows)
    }
}

#[easy_ext::ext(SourceParserIntoStreamExt)]
impl<P: ByteStreamSourceParser> P {
    /// Parse a data stream of one source split into a stream of [`StreamChunk`].
    ///
    /// # Arguments
    /// - `data_stream`: A data stream of one source split.
    ///  To be able to split multiple messages from mq, so it is not a pure byte stream
    ///
    /// # Returns
    ///
    /// A [`SourceWithStateStream`] which is a stream of parsed messages.
    pub fn into_stream(self, data_stream: BoxSourceStream) -> impl SourceWithStateStream {
        // Enable tracing to provide more information for parsing failures.
        let source_info = &self.source_ctx().source_info;
        let span = tracing::info_span!(
            "source_parser",
            actor_id = source_info.actor_id,
            source_id = source_info.source_id.table_id()
        );

        into_chunk_stream(self, data_stream).instrument(span)
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
                if parser.parser_format() == ParserFormat::Debezium {
                    tracing::debug!(offset = msg.offset, "skip parsing of heartbeat message");
                    // empty payload means a heartbeat in cdc source
                    // heartbeat message offset should not overwrite data messages offset
                    split_offset_mapping
                        .entry(msg.split_id)
                        .or_insert(msg.offset.clone());
                }

                continue;
            }

            split_offset_mapping.insert(msg.split_id.clone(), msg.offset.clone());

            let old_op_num = builder.op_num();
            match parser
                .parse_one_with_txn(
                    msg.key,
                    msg.payload,
                    builder.row_writer().with_meta(MessageMeta {
                        meta: &msg.meta,
                        split_id: &msg.split_id,
                        offset: &msg.offset,
                    }),
                )
                .await
            {
                // It's possible that parsing multiple rows in a single message PARTIALLY failed.
                // We still have to maintain the row number in this case.
                res @ (Ok(ParseResult::Rows) | Err(_)) => {
                    // The number of rows added to the builder.
                    let num = builder.op_num() - old_op_num;

                    // Aggregate the number of rows in the current transaction.
                    if let Some(Transaction { len, .. }) = &mut current_transaction {
                        *len += num;
                    }

                    if let Err(error) = res {
                        // TODO: not using tracing span to provide `split_id` and `offset` due to performance concern,
                        //       see #13105
                        tracing::error!(
                            %error,
                            split_id = &*msg.split_id,
                            offset = msg.offset,
                            "failed to parse message, skipping"
                        );
                        parser.source_ctx().report_user_source_error(error);
                    }
                }

                Ok(ParseResult::TransactionControl(txn_ctl)) => {
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
            EncodingProperties::Json(config) => {
                AccessBuilderImpl::Json(JsonAccessBuilder::new(config.use_schema_registry)?)
            }
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

pub type ParsedStreamImpl = impl SourceWithStateStream + Unpin;

impl ByteStreamSourceParserImpl {
    /// Converts this parser into a stream of [`StreamChunk`].
    pub fn into_stream(self, msg_stream: BoxSourceStream) -> ParsedStreamImpl {
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
                JsonParser::new(parser_config.specific, rw_columns, source_ctx).map(Self::Json)
            }
            (ProtocolProperties::Plain, EncodingProperties::Csv(config)) => {
                CsvParser::new(rw_columns, *config, source_ctx).map(Self::Csv)
            }
            (ProtocolProperties::DebeziumMongo, EncodingProperties::Json(_)) => {
                DebeziumMongoJsonParser::new(rw_columns, source_ctx).map(Self::DebeziumMongoJson)
            }
            (ProtocolProperties::Canal, EncodingProperties::Json(config)) => {
                CanalJsonParser::new(rw_columns, source_ctx, config).map(Self::CanalJson)
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

impl SpecificParserConfig {
    // for test only
    pub const DEFAULT_PLAIN_JSON: SpecificParserConfig = SpecificParserConfig {
        key_encoding_config: None,
        encoding_config: EncodingProperties::Json(JsonProperties {
            use_schema_registry: false,
        }),
        protocol_config: ProtocolProperties::Plain,
    };
}

#[derive(Debug, Default, Clone)]
pub struct AvroProperties {
    pub use_schema_registry: bool,
    pub row_schema_location: String,
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
pub struct JsonProperties {
    pub use_schema_registry: bool,
}

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
    // The validity of (format, encode) is ensured by `extract_format_encode`
    pub fn new(info: &StreamSourceInfo, props: &HashMap<String, String>) -> Result<Self> {
        let source_struct = extract_source_struct(info)?;
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
                    name_strategy: PbSchemaRegistryNameStrategy::try_from(info.name_strategy)
                        .unwrap(),
                    use_schema_registry: info.use_schema_registry,
                    row_schema_location: info.row_schema_location.clone(),
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
                    name_strategy: PbSchemaRegistryNameStrategy::try_from(info.name_strategy)
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
                    name_strategy: PbSchemaRegistryNameStrategy::try_from(info.name_strategy)
                        .unwrap(),
                    key_record_name: info.key_message_name.clone(),
                    row_schema_location: info.row_schema_location.clone(),
                    topic: get_kafka_topic(props).unwrap().clone(),
                    client_config: SchemaRegistryAuth::from(props),
                    ..Default::default()
                })
            }
            (
                SourceFormat::Plain
                | SourceFormat::Debezium
                | SourceFormat::Maxwell
                | SourceFormat::Canal
                | SourceFormat::Upsert,
                SourceEncode::Json,
            ) => EncodingProperties::Json(JsonProperties {
                use_schema_registry: info.use_schema_registry,
            }),
            (SourceFormat::DebeziumMongo, SourceEncode::Json) => {
                EncodingProperties::Json(JsonProperties {
                    use_schema_registry: false,
                })
            }
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
