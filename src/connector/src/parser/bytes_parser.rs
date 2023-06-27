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

use risingwave_common::error::{Result, RwError};

use super::unified::ChangeEvent;
use super::unified::bytes::{BytesAccess, BytesChangeEvent};
use super::unified::util::apply_row_operation_on_stream_chunk_writer;
use super::{ByteStreamSourceParser, SourceStreamChunkRowWriter, WriteGuard};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

/// Parser for BYTES format
#[derive(Debug)]
pub struct BytesParser {
    rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

impl BytesParser {
    pub fn new(rw_columns: Vec<SourceColumnDesc>, source_ctx: SourceContextRef) -> Result<Self> {
        Ok(Self {
            rw_columns,
            source_ctx,            
        })
    }

    #[allow(clippy::unused_async)]
    pub async fn parse_inner(
        &self,
        mut payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let accessor = BytesChangeEvent::with_value(BytesAccess::new(payload));
        writer.insert(|column| {
            let res = accessor.access_field(&column.name, &column.data_type)?;
            tracing::trace!(
                "inserted {:?} {:?} is_pk:{:?} {:?} ",
                &column.name,
                &column.data_type,
                &column.is_pk,
                res
            );
            Ok(res)
        })
        // apply_row_operation_on_stream_chunk_writer(accessor, &mut writer)
    }
}

impl ByteStreamSourceParser for BytesParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    async fn parse_one<'a>(
        &'a mut self,
        payload: Vec<u8>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> Result<WriteGuard> {
        self.parse_inner(payload, writer).await
    }
}
