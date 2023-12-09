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

use risingwave_common::error::{ErrorCode, Result, RwError};
use simd_json::BorrowedValue;

use super::CockroachChangeEvent;
use crate::only_parse_payload;
use crate::parser::unified::json::{JsonAccess, JsonParseOptions};
use crate::parser::unified::util::apply_row_operation_on_stream_chunk_writer;
use crate::parser::{ByteStreamSourceParser, ParserFormat, SourceStreamChunkRowWriter};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct CockroachJsonParser {
    rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

impl CockroachJsonParser {
    pub fn new(rw_columns: Vec<SourceColumnDesc>, source_ctx: SourceContextRef) -> Self {
        Self {
            rw_columns,
            source_ctx,
        }
    }

    #[allow(clippy::unused_async)]
    pub async fn parse_inner(
        &self,
        mut payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<()> {
        let event: BorrowedValue<'_> = simd_json::to_borrowed_value(&mut payload)
            .map_err(|e| RwError::from(ErrorCode::ProtocolError(e.to_string())))?;
        let accessor = JsonAccess::new_with_options(event, &JsonParseOptions::DEBEZIUM);
        let event_op = CockroachChangeEvent::new(accessor);

        apply_row_operation_on_stream_chunk_writer(event_op, &mut writer).map_err(Into::into)
    }
}

impl ByteStreamSourceParser for CockroachJsonParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    fn parser_format(&self) -> ParserFormat {
        ParserFormat::Cockroach
    }

    async fn parse_one<'a>(
        &'a mut self,
        _key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> Result<()> {
        only_parse_payload!(self, payload, writer)
    }
}
