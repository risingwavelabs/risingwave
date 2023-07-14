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

use risingwave_common::error::{Result, RwError, ErrorCode};

use crate::parser::unified::maxwell::MaxwellChangeEvent;
use crate::parser::unified::util::apply_row_operation_on_stream_chunk_writer;
use crate::parser::{AccessBuilder, ParserProperties, EncodingProperties, EncodingType, SourceStreamChunkRowWriter, WriteGuard, ByteStreamSourceParser};
use crate::source::{SourceColumnDesc, SourceContextRef, SourceContext};

#[derive(Debug)]
pub struct MaxwellParser {
    payload_builder: AccessBuilder,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

impl MaxwellParser {
    pub async fn new(
        props: ParserProperties,
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> Result<Self> {
        match props.encoding_config {
            EncodingProperties::Json(_) => {
                let payload_builder =
                    AccessBuilder::new_default(props.encoding_config, EncodingType::Value).await?;
                Ok(Self {
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
        mut payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let payload_accessor = self.payload_builder.generate_accessor(payload).await?;
        let row_op = MaxwellChangeEvent::new(payload_accessor);

        apply_row_operation_on_stream_chunk_writer(row_op, &mut writer)
    }
}

impl ByteStreamSourceParser for MaxwellParser {
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
        if payload.is_some() {
            self.parse_inner(payload.unwrap(), writer).await
        } else {
            Err(RwError::from(ErrorCode::InternalError(
                "Empty payload with nonempty key".into(),
            )))
        }
    }
}
