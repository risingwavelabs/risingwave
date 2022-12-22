// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::future::ready;
use risingwave_common::error::Result;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::ToOwnedDatum;

use crate::{ParseFuture, SourceParser, SourceStreamChunkRowWriter, WriteGuard};

#[derive(Debug)]
pub struct NativeParser;

impl NativeParser {
    fn parse_inner(
        &self,
        payload: &[u8],
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        // Reclaim the ownership of the memory.
        // Previously leak the memory in `DatagenEventGenerator`.
        let boxed_row: Box<OwnedRow> = unsafe { Box::from_raw(payload.as_ptr() as *mut OwnedRow) };
        writer.insert(|idx, _desc| {
            let datum = boxed_row.datum_at(idx).to_owned_datum();
            Ok(datum)
        })
    }
}

impl SourceParser for NativeParser {
    type ParseResult<'a> = impl ParseFuture<'a, Result<WriteGuard>>;

    fn parse<'a, 'b, 'c>(
        &'a self,
        payload: &'b [u8],
        writer: SourceStreamChunkRowWriter<'c>,
    ) -> Self::ParseResult<'a>
    where
        'b: 'a,
        'c: 'a,
    {
        ready(self.parse_inner(payload, writer))
    }
}
