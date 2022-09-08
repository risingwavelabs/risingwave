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
pub use protobuf_parser::*;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilderImpl, Op, StreamChunk};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::Datum;

use crate::{SourceColumnDesc, SourceFormat};

mod avro_parser;
mod common;
mod debezium;
mod json_parser;
mod protobuf_parser;

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

    pub fn finish(self) -> Result<StreamChunk> {
        Ok(StreamChunk::new(
            self.op_builder,
            self.builders
                .into_iter()
                .map(|builder| -> Result<_> { Ok(Column::new(Arc::new(builder.finish()?))) })
                .try_collect()?,
            None,
        ))
    }
}

/// `SourceStreamChunkRowWriter` is responsible to write one row (Insert/Delete) or two rows
/// (Update) to the [`StreamChunk`].
pub struct SourceStreamChunkRowWriter<'a> {
    descs: &'a [SourceColumnDesc],
    builders: &'a mut [ArrayBuilderImpl],
    op_builder: &'a mut Vec<Op>,
}

/// `WriteGuard` can't be constructed directly in other mods, so it can be used to ensure that the
/// write methods are called at least once.
pub struct WriteGuard(());

impl SourceStreamChunkRowWriter<'_> {
    pub fn insert(
        self,
        mut f: impl FnMut(&SourceColumnDesc) -> Result<Datum>,
    ) -> Result<WriteGuard> {
        self.descs
            .iter()
            .zip_eq(self.builders.iter_mut())
            .try_for_each(|(desc, builder)| -> Result<()> {
                let datum = if desc.skip_parse { None } else { f(desc)? };
                builder.append_datum(&datum)?;
                Ok(())
            })?;
        self.op_builder.push(Op::Insert);

        Ok(WriteGuard(()))
    }

    pub fn delete(
        self,
        mut f: impl FnMut(&SourceColumnDesc) -> Result<Datum>,
    ) -> Result<WriteGuard> {
        self.descs
            .iter()
            .zip_eq(self.builders.iter_mut())
            .try_for_each(|(desc, builder)| -> Result<()> {
                let datum = if desc.skip_parse { None } else { f(desc)? };
                builder.append_datum(&datum)?;
                Ok(())
            })?;
        self.op_builder.push(Op::Delete);

        Ok(WriteGuard(()))
    }

    pub fn update(
        self,
        mut f: impl FnMut(&SourceColumnDesc) -> Result<(Datum, Datum)>,
    ) -> Result<WriteGuard> {
        self.descs
            .iter()
            .zip_eq(self.builders.iter_mut())
            .try_for_each(|(desc, builder)| -> Result<()> {
                let (old, new) = if desc.skip_parse {
                    (None, None)
                } else {
                    f(desc)?
                };
                builder.append_datum(&old)?;
                builder.append_datum(&new)?;
                Ok(())
            })?;
        self.op_builder.push(Op::UpdateDelete);
        self.op_builder.push(Op::UpdateInsert);

        Ok(WriteGuard(()))
    }
}

/// `SourceParser` is the message parser, `ChunkReader` will parse the messages in `SourceReader`
/// one by one through `SourceParser` and assemble them into `DataChunk`
/// Note that the `skip_parse` parameter in `SourceColumnDesc`, when it is true, should skip the
/// parse and return `Datum` of `None`
pub trait SourceParser: Send + Sync + Debug + 'static {
    /// parse needs to be a member method because some format like Protobuf needs to be pre-compiled
    fn parse(&self, payload: &[u8], writer: SourceStreamChunkRowWriter<'_>) -> Result<WriteGuard>;
}

#[derive(Debug)]
pub enum SourceParserImpl {
    Json(JSONParser),
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
            SourceFormat::Json => SourceParserImpl::Json(JSONParser {}),
            SourceFormat::Protobuf => {
                let message_name = properties.get(PROTOBUF_MESSAGE_KEY).ok_or_else(|| {
                    RwError::from(ProtocolError(format!(
                        "Must specify '{}' in WITH clause",
                        PROTOBUF_MESSAGE_KEY
                    )))
                })?;
                SourceParserImpl::Protobuf(ProtobufParser::new(schema_location, message_name)?)
            }
            SourceFormat::DebeziumJson => SourceParserImpl::DebeziumJson(DebeziumJsonParser {}),
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
