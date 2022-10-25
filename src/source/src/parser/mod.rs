// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

pub use avro_parser::*;
pub use debezium::*;
use itertools::Itertools;
pub use json_parser::*;
pub use pb_parser::*;
use risingwave_common::array::{ArrayBuilderImpl, Op, StreamChunk};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::Datum;

use crate::{SourceColumnDesc, SourceFormat};

mod avro_parser;
mod common;
mod debezium;
mod json_parser;
mod pb_parser;
// mod protobuf_parser;

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

    fn finish(writer: SourceStreamChunkRowWriter<'_>);
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
    fn finish(writer: SourceStreamChunkRowWriter<'_>) {
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
    fn finish(writer: SourceStreamChunkRowWriter<'_>) {
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
    fn finish(writer: SourceStreamChunkRowWriter<'_>) {
        writer.op_builder.push(Op::UpdateDelete);
        writer.op_builder.push(Op::UpdateInsert);
    }
}

impl SourceStreamChunkRowWriter<'_> {
    fn do_action<A: OpAction>(
        self,
        mut f: impl FnMut(&SourceColumnDesc) -> Result<A::Output>,
    ) -> Result<WriteGuard> {
        // The closure `f` may fail so that a part of builders were appended incompletely.
        // Loop invariant: `builders[0..appended_idx)` has been appended on every iter ended or loop
        // exited.
        let mut appended_idx = 0;

        self.descs
            .iter()
            .zip_eq(self.builders.iter_mut())
            .enumerate()
            .try_for_each(|(idx, (desc, builder))| -> Result<()> {
                let output = if desc.skip_parse {
                    A::DEFAULT_OUTPUT
                } else {
                    f(desc)?
                };
                A::apply(builder, output);
                appended_idx = idx + 1;

                Ok(())
            })
            .inspect_err(|_e| {
                self.builders[..appended_idx]
                    .iter_mut()
                    .for_each(A::rollback);
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
    pub fn insert(self, f: impl FnMut(&SourceColumnDesc) -> Result<Datum>) -> Result<WriteGuard> {
        self.do_action::<OpActionInsert>(f)
    }

    /// Write a `Delete` record to the [`StreamChunk`].
    ///
    /// # Arguments
    ///
    /// * `self`: Ownership is consumed so only one record can be written.
    /// * `f`: A failable closure that produced one [`Datum`] by corresponding [`SourceColumnDesc`].
    pub fn delete(self, f: impl FnMut(&SourceColumnDesc) -> Result<Datum>) -> Result<WriteGuard> {
        self.do_action::<OpActionDelete>(f)
    }

    /// Write a `Delete` record to the [`StreamChunk`].
    ///
    /// # Arguments
    ///
    /// * `self`: Ownership is consumed so only one record can be written.
    /// * `f`: A failable closure that produced two [`Datum`]s as old and new value by corresponding
    ///   [`SourceColumnDesc`].
    pub fn update(
        self,
        f: impl FnMut(&SourceColumnDesc) -> Result<(Datum, Datum)>,
    ) -> Result<WriteGuard> {
        self.do_action::<OpActionUpdate>(f)
    }
}

/// `SourceParser` is the message parser, `ChunkReader` will parse the messages in `SourceReader`
/// one by one through `SourceParser` and assemble them into `DataChunk`
/// Note that the `skip_parse` parameter in `SourceColumnDesc`, when it is true, should skip the
/// parse and return `Datum` of `None`
pub trait SourceParser: Send + Sync + Debug + 'static {
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
    fn parse(&self, payload: &[u8], writer: SourceStreamChunkRowWriter<'_>) -> Result<WriteGuard>;
}

#[derive(Debug)]
pub enum SourceParserImpl {
    Json(JsonParser),
    Protobuf(ProtobufParser),
    DebeziumJson(DebeziumJsonParser),
    Avro(AvroParser),
}

impl SourceParserImpl {
    pub fn parse(
        &self,
        payload: &[u8],
        writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        match self {
            Self::Json(parser) => parser.parse(payload, writer),
            Self::Protobuf(parser) => parser.parse(payload, writer),
            Self::DebeziumJson(parser) => parser.parse(payload, writer),
            Self::Avro(avro_parser) => avro_parser.parse(payload, writer),
        }
    }

    pub async fn create(
        format: &SourceFormat,
        properties: &HashMap<String, String>,
        schema_location: &str,
    ) -> Result<Arc<Self>> {
        const PROTOBUF_MESSAGE_KEY: &str = "proto.message";
        let parser = match format {
            SourceFormat::Json => SourceParserImpl::Json(JsonParser),
            SourceFormat::Protobuf => {
                let message_name = properties.get(PROTOBUF_MESSAGE_KEY).ok_or_else(|| {
                    RwError::from(ProtocolError(format!(
                        "Must specify '{}' in WITH clause",
                        PROTOBUF_MESSAGE_KEY
                    )))
                })?;
                SourceParserImpl::Protobuf(
                    ProtobufParser::new(schema_location, message_name, properties.clone()).await?,
                )
            }
            SourceFormat::DebeziumJson => SourceParserImpl::DebeziumJson(DebeziumJsonParser),
            SourceFormat::Avro => {
                SourceParserImpl::Avro(AvroParser::new(schema_location, properties.clone()).await?)
            }
            _ => {
                return Err(RwError::from(ProtocolError(
                    "format not support".to_string(),
                )));
            }
        };
        Ok(Arc::new(parser))
    }
}
