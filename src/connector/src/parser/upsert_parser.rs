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
use risingwave_pb::plan_common::AdditionalColumnType;

use super::bytes_parser::BytesAccessBuilder;
use super::unified::upsert::UpsertChangeEvent;
use super::unified::util::apply_row_operation_on_stream_chunk_writer_with_op;
use super::unified::{AccessImpl, ChangeEventOperation};
use super::{
    AccessBuilderImpl, ByteStreamSourceParser, BytesProperties, EncodingProperties, EncodingType,
    SourceStreamChunkRowWriter, SpecificParserConfig,
};
use crate::parser::ParserFormat;
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct UpsertParser {
    key_builder: AccessBuilderImpl,
    payload_builder: AccessBuilderImpl,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

async fn build_accessor_builder(
    config: EncodingProperties,
    encoding_type: EncodingType,
) -> Result<AccessBuilderImpl> {
    match config {
        EncodingProperties::Json(_)
        | EncodingProperties::Protobuf(_)
        | EncodingProperties::Avro(_) => {
            Ok(AccessBuilderImpl::new_default(config, encoding_type).await?)
        }
        _ => Err(RwError::from(ProtocolError(
            "unsupported encoding for Upsert".to_string(),
        ))),
    }
}

pub fn get_key_column_name(columns: &[SourceColumnDesc]) -> Option<String> {
    columns.iter().find_map(|column| {
        if column.additional_column_type == AdditionalColumnType::Key {
            Some(column.name.clone())
        } else {
            None
        }
    })
}

impl UpsertParser {
    pub async fn new(
        props: SpecificParserConfig,
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> Result<Self> {
        // check whether columns has Key as AdditionalColumnType, if so, the key accessor should be
        // bytes
        let key_builder = if let Some(key_column_name) = get_key_column_name(&rw_columns) {
            // later: if key column has other type other than bytes, build other accessor.
            // For now, all key columns are bytes
            AccessBuilderImpl::Bytes(BytesAccessBuilder::new(EncodingProperties::Bytes(
                BytesProperties {
                    column_name: Some(key_column_name),
                },
            ))?)
        } else {
            unreachable!("format upsert must have key column")
        };
        let payload_builder =
            build_accessor_builder(props.encoding_config, EncodingType::Value).await?;
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
        let mut row_op: UpsertChangeEvent<AccessImpl<'_, '_>, AccessImpl<'_, '_>> =
            UpsertChangeEvent::default();
        let mut change_event_op = ChangeEventOperation::Delete;
        if let Some(data) = key {
            row_op = row_op.with_key(self.key_builder.generate_accessor(data).await?);
        }
        // Empty payload of kafka is Some(vec![])
        if let Some(data) = payload
            && !data.is_empty()
        {
            row_op = row_op.with_value(self.payload_builder.generate_accessor(data).await?);
            change_event_op = ChangeEventOperation::Upsert;
        }

        apply_row_operation_on_stream_chunk_writer_with_op(row_op, &mut writer, change_event_op)
            .map_err(Into::into)
    }
}

impl ByteStreamSourceParser for UpsertParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    fn parser_format(&self) -> ParserFormat {
        ParserFormat::Upsert
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
