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

use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};

use super::{
    AccessBuilderImpl, ByteStreamSourceParser, EncodingProperties, EncodingType,
    SourceStreamChunkRowWriter, SpecificParserConfig,
};
use crate::parser::bytes_parser::BytesAccessBuilder;
use crate::parser::unified::upsert::UpsertChangeEvent;
use crate::parser::unified::util::apply_row_operation_on_stream_chunk_writer_with_op;
use crate::parser::unified::{AccessImpl, ChangeEventOperation};
use crate::parser::upsert_parser::get_key_column_name;
use crate::parser::{BytesProperties, ParserFormat};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct PlainParser {
    pub key_builder: Option<AccessBuilderImpl>,
    pub payload_builder: AccessBuilderImpl,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    pub source_ctx: SourceContextRef,
}

impl PlainParser {
    pub async fn new(
        props: SpecificParserConfig,
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> Result<Self> {
        let key_builder = if let Some(key_column_name) = get_key_column_name(&rw_columns) {
            Some(AccessBuilderImpl::Bytes(BytesAccessBuilder::new(
                EncodingProperties::Bytes(BytesProperties {
                    column_name: Some(key_column_name),
                }),
            )?))
        } else {
            None
        };

        let payload_builder = match props.encoding_config {
            EncodingProperties::Json(_)
            | EncodingProperties::Protobuf(_)
            | EncodingProperties::Avro(_)
            | EncodingProperties::Bytes(_) => {
                AccessBuilderImpl::new_default(props.encoding_config, EncodingType::Value).await?
            }
            _ => {
                return Err(RwError::from(ProtocolError(
                    "unsupported encoding for Plain".to_string(),
                )));
            }
        };
        Ok(Self {
            key_builder,
            payload_builder,
            rw_columns,
            source_ctx,
        })
    }

    pub async fn parse_inner(
        &mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<()> {
        // reuse upsert component but always insert
        let mut row_op: UpsertChangeEvent<AccessImpl<'_, '_>, AccessImpl<'_, '_>> =
            UpsertChangeEvent::default();
        let change_event_op = ChangeEventOperation::Upsert;

        if let Some(data) = key && let Some(key_builder) = self.key_builder.as_mut() {
            // key is optional in format plain
            row_op = row_op.with_key(key_builder.generate_accessor(data).await?);
        }
        if let Some(data) = payload {
            // the data part also can be an empty vec
            row_op = row_op.with_value(self.payload_builder.generate_accessor(data).await?);
        }

        apply_row_operation_on_stream_chunk_writer_with_op(row_op, &mut writer, change_event_op)
            .map_err(Into::into)
    }
}

impl ByteStreamSourceParser for PlainParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    fn parser_format(&self) -> ParserFormat {
        ParserFormat::Plain
    }

    async fn parse_one<'a>(
        &'a mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> Result<()> {
        self.parse_inner(key, payload, writer).await
    }
}
