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
use risingwave_common::error::{ErrorCode, Result, RwError};

use super::unified::util::apply_row_accessor_on_stream_chunk_writer;
use super::{
    AccessBuilderImpl, ByteStreamSourceParser, EncodingProperties, EncodingType,
    SourceStreamChunkRowWriter, SpecificParserConfig, WriteGuard,
};
use crate::only_parse_payload;
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct PlainParser {
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
        let payload_builder = match props.encoding_config {
            EncodingProperties::Protobuf(_)
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
            payload_builder,
            rw_columns,
            source_ctx,
        })
    }

    pub async fn parse_inner(
        &mut self,
        payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let accessor = self.payload_builder.generate_accessor(payload).await?;

        apply_row_accessor_on_stream_chunk_writer(accessor, &mut writer)
    }
}

impl ByteStreamSourceParser for PlainParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    async fn parse_one<'a>(
        &'a mut self,
        _key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> Result<WriteGuard> {
        only_parse_payload!(self, payload, writer)
    }
}
