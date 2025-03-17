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

use crate::error::ConnectorResult;
use crate::only_parse_payload;
use crate::parser::unified::maxwell::MaxwellChangeEvent;
use crate::parser::unified::util::apply_row_operation_on_stream_chunk_writer;
use crate::parser::{
    AccessBuilderImpl, ByteStreamSourceParser, EncodingProperties, ParserFormat,
    SourceStreamChunkRowWriter, SpecificParserConfig,
};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct MaxwellParser {
    payload_builder: AccessBuilderImpl,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

impl MaxwellParser {
    pub async fn new(
        props: SpecificParserConfig,
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> ConnectorResult<Self> {
        match props.encoding_config {
            EncodingProperties::Json(_) => {
                let payload_builder = AccessBuilderImpl::new_default(props.encoding_config).await?;
                Ok(Self {
                    payload_builder,
                    rw_columns,
                    source_ctx,
                })
            }
            _ => bail!("unsupported encoding for Maxwell"),
        }
    }

    pub async fn parse_inner(
        &mut self,
        payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> ConnectorResult<()> {
        let m = writer.source_meta();
        let payload_accessor = self.payload_builder.generate_accessor(payload, m).await?;
        let row_op = MaxwellChangeEvent::new(payload_accessor);

        apply_row_operation_on_stream_chunk_writer(row_op, &mut writer).map_err(Into::into)
    }
}

impl ByteStreamSourceParser for MaxwellParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    fn parser_format(&self) -> ParserFormat {
        ParserFormat::Maxwell
    }

    async fn parse_one<'a>(
        &'a mut self,
        _key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> ConnectorResult<()> {
        // restrict the behaviours since there is no corresponding
        // key/value test for maxwell yet.
        only_parse_payload!(self, payload, writer)
    }
}
