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

pub use avro::*;
pub use canal::*;
use csv_parser::CsvParser;
pub use debezium::*;
use itertools::Itertools;
pub use json_parser::*;
pub use protobuf::*;
use risingwave_common::array::{ArrayBuilderImpl, Op, StreamChunk};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::Datum;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::catalog::StreamSourceInfo;

pub use self::csv_parser::CsvParserConfig;
use crate::parser::maxwell::MaxwellParser;
use crate::source::{
    BoxSourceStream, BoxSourceWithStateStream, SourceColumnDesc, SourceContextRef, SourceFormat,
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
mod unified;
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
    Json(JsonParser),
    Protobuf(ProtobufParser),
    DebeziumJson(DebeziumJsonParser),
    DebeziumMongoJson(DebeziumMongoJsonParser),
    Avro(AvroParser),
    Maxwell(MaxwellParser),
    CanalJson(CanalJsonParser),
    DebeziumAvro(DebeziumAvroParser),
}
impl ByteStreamSourceParser for ByteStreamSourceParserImpl {
    fn into_stream(
        self,
        msg_stream: crate::source::BoxSourceStream,
    ) -> crate::source::BoxSourceWithStateStream {
        match self {
            Self::Csv(parser) => parser.into_stream(msg_stream),
            Self::Json(parser) => parser.into_stream(msg_stream),
            Self::Protobuf(parser) => parser.into_stream(msg_stream),
            Self::DebeziumJson(parser) => parser.into_stream(msg_stream),
            Self::DebeziumMongoJson(parser) => parser.into_stream(msg_stream),
            Self::Avro(parser) => parser.into_stream(msg_stream),
            Self::Maxwell(parser) => parser.into_stream(msg_stream),
            Self::CanalJson(parser) => parser.into_stream(msg_stream),
            Self::DebeziumAvro(parser) => parser.into_stream(msg_stream),
        }
    }
}

impl ByteStreamSourceParserImpl {
    pub fn create(parser_config: ParserConfig, source_ctx: SourceContextRef) -> Result<Self> {
        let CommonParserConfig { rw_columns } = parser_config.common;
        match parser_config.specific {
            SpecificParserConfig::Csv(config) => {
                CsvParser::new(rw_columns, config, source_ctx).map(Self::Csv)
            }
            SpecificParserConfig::Avro(config) | SpecificParserConfig::UpsertAvro(config) => {
                AvroParser::new(rw_columns, config, source_ctx).map(Self::Avro)
            }
            SpecificParserConfig::Protobuf(config) => {
                ProtobufParser::new(rw_columns, config, source_ctx).map(Self::Protobuf)
            }
            SpecificParserConfig::Json => JsonParser::new(rw_columns, source_ctx).map(Self::Json),
            SpecificParserConfig::UpsertJson => {
                JsonParser::new_with_upsert(rw_columns, source_ctx).map(Self::Json)
            }
            SpecificParserConfig::CanalJson => {
                CanalJsonParser::new(rw_columns, source_ctx).map(Self::CanalJson)
            }
            SpecificParserConfig::DebeziumJson => {
                DebeziumJsonParser::new(rw_columns, source_ctx).map(Self::DebeziumJson)
            }
            SpecificParserConfig::DebeziumMongoJson => {
                DebeziumMongoJsonParser::new(rw_columns, source_ctx).map(Self::DebeziumMongoJson)
            }
            SpecificParserConfig::Maxwell => {
                MaxwellParser::new(rw_columns, source_ctx).map(Self::Maxwell)
            }
            SpecificParserConfig::DebeziumAvro(config) => {
                DebeziumAvroParser::new(rw_columns, config, source_ctx).map(Self::DebeziumAvro)
            }
            SpecificParserConfig::Native => {
                unreachable!("Native parser should not be created")
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
    Csv(CsvParserConfig),
    Avro(AvroParserConfig),
    UpsertAvro(AvroParserConfig),
    Protobuf(ProtobufParserConfig),
    Json,
    UpsertJson,
    DebeziumJson,
    DebeziumMongoJson,
    Maxwell,
    CanalJson,
    #[default]
    Native,
    DebeziumAvro(DebeziumAvroParserConfig),
}

impl SpecificParserConfig {
    pub fn get_source_format(&self) -> SourceFormat {
        match self {
            SpecificParserConfig::Avro(_) => SourceFormat::Avro,
            SpecificParserConfig::UpsertAvro(_) => SourceFormat::UpsertAvro,
            SpecificParserConfig::Csv(_) => SourceFormat::Csv,
            SpecificParserConfig::Protobuf(_) => SourceFormat::Protobuf,
            SpecificParserConfig::Json => SourceFormat::Json,
            SpecificParserConfig::UpsertJson => SourceFormat::UpsertJson,
            SpecificParserConfig::DebeziumJson => SourceFormat::DebeziumJson,
            SpecificParserConfig::Maxwell => SourceFormat::Maxwell,
            SpecificParserConfig::CanalJson => SourceFormat::CanalJson,
            SpecificParserConfig::Native => SourceFormat::Native,
            SpecificParserConfig::DebeziumAvro(_) => SourceFormat::DebeziumAvro,
            SpecificParserConfig::DebeziumMongoJson => SourceFormat::DebeziumMongoJson,
        }
    }

    pub fn is_upsert(&self) -> bool {
        matches!(
            self,
            SpecificParserConfig::UpsertJson | SpecificParserConfig::UpsertAvro(_)
        )
    }

    pub async fn new(
        format: SourceFormat,
        info: &StreamSourceInfo,
        props: &HashMap<String, String>,
    ) -> Result<Self> {
        let conf = match format {
            SourceFormat::Csv => SpecificParserConfig::Csv(CsvParserConfig {
                delimiter: info.csv_delimiter as u8,
                has_header: info.csv_has_header,
            }),
            SourceFormat::Avro => SpecificParserConfig::Avro(
                AvroParserConfig::new(
                    props,
                    &info.row_schema_location,
                    info.use_schema_registry,
                    false,
                    None,
                )
                .await?,
            ),
            SourceFormat::UpsertAvro => SpecificParserConfig::UpsertAvro(
                AvroParserConfig::new(
                    props,
                    &info.row_schema_location,
                    info.use_schema_registry,
                    true,
                    if info.upsert_avro_primary_key.is_empty() {
                        None
                    } else {
                        Some(info.upsert_avro_primary_key.to_string())
                    },
                )
                .await?,
            ),
            SourceFormat::Protobuf => SpecificParserConfig::Protobuf(
                ProtobufParserConfig::new(
                    props,
                    &info.row_schema_location,
                    &info.proto_message_name,
                    info.use_schema_registry,
                )
                .await?,
            ),
            SourceFormat::Json => SpecificParserConfig::Json,
            SourceFormat::UpsertJson => SpecificParserConfig::UpsertJson,
            SourceFormat::DebeziumJson => SpecificParserConfig::DebeziumJson,
            SourceFormat::DebeziumMongoJson => SpecificParserConfig::DebeziumMongoJson,
            SourceFormat::Maxwell => SpecificParserConfig::Maxwell,
            SourceFormat::CanalJson => SpecificParserConfig::CanalJson,
            SourceFormat::Native => SpecificParserConfig::Native,
            SourceFormat::DebeziumAvro => SpecificParserConfig::DebeziumAvro(
                DebeziumAvroParserConfig::new(props, &info.row_schema_location).await?,
            ),
            _ => {
                return Err(RwError::from(ProtocolError(
                    "invalid source format".to_string(),
                )));
            }
        };
        Ok(conf)
    }
}

impl ParserConfig {
    pub async fn new(
        format: SourceFormat,
        info: &StreamSourceInfo,
        props: &HashMap<String, String>,
        rw_columns: &Vec<SourceColumnDesc>,
    ) -> Result<Self> {
        let common = CommonParserConfig {
            rw_columns: rw_columns.to_owned(),
        };
        let specific = SpecificParserConfig::new(format, info, props).await?;

        Ok(Self { common, specific })
    }
}
