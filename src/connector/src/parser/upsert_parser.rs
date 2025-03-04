// Copyright 2025 RisingWave Labs
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

use risingwave_common::bail;
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;

use super::bytes_parser::BytesAccessBuilder;
use super::unified::{AccessImpl, ChangeEventOperation};
use super::{
    AccessBuilderImpl, ByteStreamSourceParser, BytesProperties, EncodingProperties,
    SourceStreamChunkRowWriter, SpecificParserConfig,
};
use crate::error::ConnectorResult;
use crate::parser::ParserFormat;
use crate::parser::unified::kv_event::KvEvent;
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct UpsertParser {
    key_builder: AccessBuilderImpl,
    payload_builder: AccessBuilderImpl,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

async fn build_accessor_builder(config: EncodingProperties) -> ConnectorResult<AccessBuilderImpl> {
    match config {
        EncodingProperties::Json(_)
        | EncodingProperties::Protobuf(_)
        | EncodingProperties::Avro(_) => Ok(AccessBuilderImpl::new_default(config).await?),
        _ => bail!("unsupported encoding for Upsert"),
    }
}

pub fn get_key_column_name(columns: &[SourceColumnDesc]) -> Option<String> {
    columns.iter().find_map(|column| {
        if matches!(
            column.additional_column.column_type,
            Some(AdditionalColumnType::Key(_))
        ) {
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
    ) -> ConnectorResult<Self> {
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
        let payload_builder = build_accessor_builder(props.encoding_config).await?;
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
    ) -> ConnectorResult<()> {
        let meta = writer.source_meta();
        let mut row_op: KvEvent<AccessImpl<'_>, AccessImpl<'_>> = KvEvent::default();
        if let Some(data) = key {
            row_op.with_key(self.key_builder.generate_accessor(data, meta).await?);
        }
        // Empty payload of kafka is Some(vec![])
        let change_event_op;
        if let Some(data) = payload
            && !data.is_empty()
        {
            row_op.with_value(self.payload_builder.generate_accessor(data, meta).await?);
            change_event_op = ChangeEventOperation::Upsert;
        } else {
            change_event_op = ChangeEventOperation::Delete;
        }

        match change_event_op {
            ChangeEventOperation::Upsert => {
                let f = |column: &SourceColumnDesc| row_op.access_field::<false>(column);
                writer.do_insert(f)?
            }
            ChangeEventOperation::Delete => {
                let f = |column: &SourceColumnDesc| row_op.access_field::<true>(column);
                writer.do_delete(f)?
            }
        }
        Ok(())
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
    ) -> ConnectorResult<()> {
        self.parse_inner(key, payload, writer).await
    }
}
