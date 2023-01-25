// Copyright 2023 Singularity Data
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
use std::sync::Arc;

pub use avro::*;
pub use canal::*;
use csv_parser::CsvParser;
pub use debezium::*;
use enum_as_inner::EnumAsInner;
use futures::Future;
use itertools::Itertools;
pub use json_parser::*;
pub use protobuf::*;
use risingwave_common::array::{ArrayBuilderImpl, Op, StreamChunk};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::Datum;
use risingwave_pb::catalog::StreamSourceInfo;

pub use self::csv_parser::CsvParserConfig;
use crate::parser::maxwell::MaxwellParser;
use crate::source::{
    BoxSourceStream, BoxSourceWithStateStream, SourceColumnDesc, SourceFormat, StreamChunkWithState,
};

mod avro;
mod canal;
mod common;
mod csv_parser;
mod debezium;
mod json_parser;
mod macros;
mod maxwell;
mod protobuf;
mod schema_registry;
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
        builder.append_datum(&output)
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
        builder.append_datum(&output)
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
        builder.append_datum(&output.0);
        builder.append_datum(&output.1);
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
    fn do_action<A: OpAction>(
        &mut self,
        mut f: impl FnMut(&SourceColumnDesc) -> Result<A::Output>,
    ) -> Result<WriteGuard> {
        let mut modify_col = vec![];

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
    /// `data_budiler` = [1], `meta_column_builder` = [], `op` = [insert]
    ///
    /// This function is used to fulfill this hollow in `meta_column_builder`.
    /// e.g after fulfill
    /// `data_budiler` = [1], `meta_column_builder` = [1], `op` = [insert]
    pub fn fulfill_meta_column(
        &mut self,
        mut f: impl FnMut(&SourceColumnDesc) -> Option<Datum>,
    ) -> Result<WriteGuard> {
        self.descs
            .iter()
            .zip_eq(self.builders.iter_mut())
            .for_each(|(desc, builder)| {
                if let Some(output) = f(desc) {
                    builder.append_datum(output);
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

pub trait ParseFuture<'a, Out> = Future<Output = Out> + Send + 'a;

// TODO: use `async_fn_in_traits` to implement it
/// `SourceParser` is the message parser, `ChunkReader` will parse the messages in `SourceReader`
/// one by one through `SourceParser` and assemble them into `DataChunk`
/// Note that the `skip_parse` parameter in `SourceColumnDesc`, when it is true, should skip the
/// parse and return `Datum` of `None`
pub trait SourceParser: Send + Debug + 'static {
    type ParseResult<'a>: ParseFuture<'a, Result<WriteGuard>>;
    /// Parse the payload and append the result to the [`StreamChunk`] directly.
    ///
    /// # Arguments
    ///
    /// - `self`: A needs to be a member method because some format like Protobuf needs to be
    ///   pre-compiled.
    /// - writer: Write exactly one record during a `parse` call.
    ///
    /// # Returns
    ///
    /// A [`WriteGuard`] to ensure that at least one record was appended or error occurred.
    fn parse<'a, 'b, 'c>(
        &'a self,
        payload: &'b [u8],
        writer: SourceStreamChunkRowWriter<'c>,
    ) -> Self::ParseResult<'a>
    where
        'b: 'a,
        'c: 'a;
}

#[derive(Debug)]
pub enum SourceParserImpl {
    Json(JsonParser),
    Protobuf(ProtobufParser),
    DebeziumJson(DebeziumJsonParser),
    Avro(AvroParser),
    Maxwell(MaxwellParser),
    CanalJson(CanalJsonParser),
}

impl SourceParserImpl {
    pub async fn parse(
        &self,
        payload: &[u8],
        writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        match self {
            Self::Json(parser) => parser.parse(payload, writer).await,
            Self::Protobuf(parser) => parser.parse(payload, writer).await,
            Self::DebeziumJson(parser) => parser.parse(payload, writer).await,
            Self::Avro(avro_parser) => avro_parser.parse(payload, writer).await,
            Self::Maxwell(maxwell_parser) => maxwell_parser.parse(payload, writer).await,
            Self::CanalJson(parser) => parser.parse(payload, writer).await,
        }
    }

    pub async fn create(
        format: &SourceFormat,
        properties: &HashMap<String, String>,
        schema_location: &str,
        use_schema_registry: bool,
        proto_message_name: String,
    ) -> Result<Arc<Self>> {
        const PROTOBUF_MESSAGE_KEY: &str = "proto.message";
        const USE_SCHEMA_REGISTRY: &str = "use_schema_registry";
        let parser = match format {
            SourceFormat::Json => SourceParserImpl::Json(JsonParser),
            SourceFormat::Protobuf => SourceParserImpl::Protobuf(
                ProtobufParser::new(
                    schema_location,
                    &proto_message_name,
                    use_schema_registry,
                    properties.clone(),
                )
                .await?,
            ),
            SourceFormat::DebeziumJson => SourceParserImpl::DebeziumJson(DebeziumJsonParser),
            SourceFormat::Avro => SourceParserImpl::Avro(
                AvroParser::new(schema_location, use_schema_registry, properties.clone()).await?,
            ),
            SourceFormat::Maxwell => SourceParserImpl::Maxwell(MaxwellParser),
            SourceFormat::CanalJson => SourceParserImpl::CanalJson(CanalJsonParser),
            _ => {
                return Err(RwError::from(ProtocolError(
                    "format not support".to_string(),
                )));
            }
        };
        Ok(Arc::new(parser))
    }
}

/// `ByteStreamSourceParser` is a new message parser, the parser should consume
/// the input data stream and return a stream of parsed msgs.
pub trait ByteStreamSourceParser: Send + Debug + 'static {
    /// Parse a data stream of one source split into a stream of [`StreamChunk`].

    /// # Arguments
    /// - `data_stream`: A data stream of one source split.
    ///  To be able to split multiple messages from mq, so it is not a pure byte stream
    ///
    /// # Returns
    ///
    /// A [`BoxSourceWithStateStream`] which is a stream of parsed msgs.
    fn into_stream(self, data_stream: BoxSourceStream) -> BoxSourceWithStateStream;
}

#[derive(Debug)]
pub enum ByteStreamSourceParserImpl {
    Csv(CsvParser),
}

#[derive(Debug, Clone)]
pub struct ParserConfig {
    pub common: CommonParserConfig,
    pub specific: SpecificParserConfig,
}

#[derive(Debug, Clone)]
pub struct CommonParserConfig {
    pub props: HashMap<String, String>,
    pub rw_columns: Vec<SourceColumnDesc>,
}

#[derive(Debug, Clone, EnumAsInner)]
pub enum SpecificParserConfig {
    Csv(CsvParserConfig),
}

impl SpecificParserConfig {
    pub fn new(format: &SourceFormat, info: &StreamSourceInfo) -> Self {
        match format {
            SourceFormat::Csv => SpecificParserConfig::Csv(CsvParserConfig {
                delimiter: info.csv_delimiter as u8,
                has_header: info.csv_has_header,
            }),
            _ => unreachable!(),
        }
    }
}

impl ParserConfig {
    pub fn new(
        format: &SourceFormat,
        info: &StreamSourceInfo,
        props: &HashMap<String, String>,
        rw_columns: &Vec<SourceColumnDesc>,
    ) -> Self {
        let common = CommonParserConfig {
            props: props.clone(),
            rw_columns: rw_columns.to_owned(),
        };
        let specific = SpecificParserConfig::new(format, info);

        Self { common, specific }
    }
}

impl ByteStreamSourceParserImpl {
    pub fn into_stream(self, msg_stream: BoxSourceStream) -> BoxSourceWithStateStream {
        match self {
            Self::Csv(parser) => parser.into_stream(msg_stream),
        }
    }

    // Keep this `async` in consideration of other parsers in the future.
    #[allow(clippy::unused_async)]
    pub async fn create(parser_config: ParserConfig) -> Result<Self> {
        let CommonParserConfig { rw_columns, .. } = parser_config.common;
        match parser_config.specific {
            SpecificParserConfig::Csv(csv_parser_config) => {
                CsvParser::new(rw_columns, csv_parser_config).map(Self::Csv)
            }
        }
    }
}
