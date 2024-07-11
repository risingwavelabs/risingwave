// Copyright 2024 RisingWave Labs
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

use crate::error::ConnectorResult;
use crate::only_parse_payload;
use crate::parser::dynamodb::{build_dynamodb_json_accessor_builder, DynamodbChangeEvent};
use crate::parser::unified::util::apply_row_operation_on_stream_chunk_writer;
use crate::parser::{
    AccessBuilderImpl, ByteStreamSourceParser, ParserFormat, SourceStreamChunkRowWriter,
    SpecificParserConfig,
};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct DynamodbCdcJsonParser {
    payload_builder: AccessBuilderImpl,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
    single_blob_column: String,
}

impl DynamodbCdcJsonParser {
    pub async fn new(
        props: SpecificParserConfig,
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> ConnectorResult<Self> {
        // the key of Dynamodb CDC are embedded value of primary key and partition key, which is not used here.
        let (payload_builder, single_blob_column) =
            build_dynamodb_json_accessor_builder(props.encoding_config).await?;
        Ok(Self {
            payload_builder,
            rw_columns,
            source_ctx,
            single_blob_column,
        })
    }

    pub async fn parse_inner(
        &mut self,
        payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> ConnectorResult<()> {
        let payload_accessor = self.payload_builder.generate_accessor(payload).await?;
        let row_op = DynamodbChangeEvent::new(payload_accessor, self.single_blob_column.clone());
        match apply_row_operation_on_stream_chunk_writer(&row_op, &mut writer) {
            Ok(_) => Ok(()),
            Err(err) => Err(err)?,
        }
    }
}

impl ByteStreamSourceParser for DynamodbCdcJsonParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    fn parser_format(&self) -> ParserFormat {
        ParserFormat::DynamodbCdcJson
    }

    async fn parse_one<'a>(
        &'a mut self,
        _key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> ConnectorResult<()> {
        only_parse_payload!(self, payload, writer)
    }
}
