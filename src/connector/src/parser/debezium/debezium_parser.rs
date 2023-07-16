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

use risingwave_common::error::Result;

use super::DebeziumAvroAccessBuilder;
use crate::parser::unified::debezium::DebeziumChangeEvent;
use crate::parser::unified::util::apply_row_operation_on_stream_chunk_writer;
use crate::parser::{
    AccessBuilderImpl, ByteStreamSourceParser, EncodingProperties, EncodingType, ParserProperties,
    SourceStreamChunkRowWriter, WriteGuard,
};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct DebeziumParser {
    key_builder: AccessBuilderImpl,
    payload_builder: AccessBuilderImpl,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

impl DebeziumParser {
    pub async fn new(
        props: ParserProperties,
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> Result<Self> {
        match props.encoding_config {
            EncodingProperties::Avro(config) => {
                let payload_builder =
                    DebeziumAvroAccessBuilder::new(config.clone(), EncodingType::Value).await?;
                let key_builder = DebeziumAvroAccessBuilder::new(config, EncodingType::Key).await?;
                Ok(Self {
                    key_builder: AccessBuilderImpl::DebeziumAvro(key_builder),
                    payload_builder: AccessBuilderImpl::DebeziumAvro(payload_builder),
                    rw_columns,
                    source_ctx,
                })
            }
            EncodingProperties::Json(_) | EncodingProperties::Protobuf(_) => {
                let key_builder = match props.key_encoding_config {
                    None => {
                        AccessBuilderImpl::new_default(
                            props.encoding_config.clone(),
                            EncodingType::Key,
                        )
                        .await?
                    }
                    Some(config) => {
                        AccessBuilderImpl::new_default(config, EncodingType::Key).await?
                    }
                };
                let payload_builder =
                    AccessBuilderImpl::new_default(props.encoding_config, EncodingType::Value)
                        .await?;
                Ok(Self {
                    key_builder,
                    payload_builder,
                    rw_columns,
                    source_ctx,
                })
            }
            _ => unreachable!(),
        }
    }

    pub async fn parse_inner(
        &mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let key_accessor = match key {
            None => None,
            Some(data) => Some(self.key_builder.generate_accessor(data).await?),
        };
        let payload_accessor = match payload {
            None => None,
            Some(data) => Some(self.payload_builder.generate_accessor(data).await?),
        };
        let row_op = DebeziumChangeEvent::new(key_accessor, payload_accessor);

        apply_row_operation_on_stream_chunk_writer(row_op, &mut writer)
    }
}

impl ByteStreamSourceParser for DebeziumParser {
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
