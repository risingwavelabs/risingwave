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

use risingwave_common::catalog::DEFAULT_KEY_COLUMN_NAME;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};

use super::bytes_parser::BytesAccessBuilder;
use super::unified::upsert::UpsertChangeEvent;
use super::unified::util::apply_row_operation_on_stream_chunk_writer_with_op;
use super::unified::{AccessImpl, ChangeEventOperation};
use super::{
    AccessBuilderImpl, ByteStreamSourceParser, BytesProperties, EncodingProperties, EncodingType,
    SourceStreamChunkRowWriter, SpecificParserConfig, WriteGuard,
};
use crate::extract_key_config;
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct UpsertParser {
    key_builder: AccessBuilderImpl,
    payload_builder: AccessBuilderImpl,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
    avro_primary_key_column_name: Option<String>,
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

fn check_rw_default_key(columns: &Vec<SourceColumnDesc>) -> bool {
    for col in columns {
        if col.name.starts_with(DEFAULT_KEY_COLUMN_NAME) {
            return true;
        }
    }
    false
}

impl UpsertParser {
    pub async fn new(
        props: SpecificParserConfig,
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> Result<Self> {
        let mut avro_primary_key_column_name = None;
        let key_builder: AccessBuilderImpl;
        // check whether columns has `DEFAULT_KEY_COLUMN_NAME`, if so, the key accessor should be
        // bytes
        if check_rw_default_key(&rw_columns) {
            key_builder = AccessBuilderImpl::Bytes(BytesAccessBuilder::new(
                EncodingProperties::Bytes(BytesProperties {
                    column_name: Some(DEFAULT_KEY_COLUMN_NAME.into()),
                }),
            )?);
        } else {
            if let EncodingProperties::Avro(config) = &props.encoding_config {
                avro_primary_key_column_name = Some(config.upsert_primary_key.clone())
            }
            let (key_config, key_type) = extract_key_config!(props);
            key_builder = build_accessor_builder(key_config, key_type).await?;
        }
        let payload_builder =
            build_accessor_builder(props.encoding_config, EncodingType::Value).await?;
        Ok(Self {
            key_builder,
            payload_builder,
            rw_columns,
            source_ctx,
            avro_primary_key_column_name,
        })
    }

    pub async fn parse_inner(
        &mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let mut row_op: UpsertChangeEvent<AccessImpl<'_, '_>, AccessImpl<'_, '_>> =
            UpsertChangeEvent::default();
        let mut change_event_op = ChangeEventOperation::Delete;
        if let Some(data) = key {
            row_op = row_op.with_key(self.key_builder.generate_accessor(data).await?);
        }
        // Empty payload of kafka is Some(vec![])
        if let Some(data) = payload && !data.is_empty() {
            row_op = row_op.with_value(self.payload_builder.generate_accessor(data).await?);
            change_event_op = ChangeEventOperation::Upsert;
        }
        if let Some(primary_key_name) = &self.avro_primary_key_column_name {
            row_op = row_op.with_key_as_column_name(primary_key_name);
        }

        apply_row_operation_on_stream_chunk_writer_with_op(row_op, &mut writer, change_event_op)
    }
}

impl ByteStreamSourceParser for UpsertParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    async fn parse_one<'a>(
        &'a mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> Result<WriteGuard> {
        self.parse_inner(key, payload, writer).await
    }
}
