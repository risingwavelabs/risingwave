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

use std::fmt::Debug;

use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use simd_json::BorrowedValue;

use crate::parser::unified::json::{JsonAccess, JsonParseOptions};
use crate::parser::unified::maxwell::MaxwellChangeEvent;
use crate::parser::unified::util::apply_row_operation_on_stream_chunk_writer;
use crate::parser::{ByteStreamSourceParser, SourceStreamChunkRowWriter, WriteGuard};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

const AFTER: &str = "data";
const BEFORE: &str = "old";
const OP: &str = "type";

#[derive(Debug)]
pub struct MaxwellParser {
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

impl MaxwellParser {
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
        let event: BorrowedValue<'_> = simd_json::to_borrowed_value(&mut payload)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        let accessor = JsonAccess::new_with_options(event, &JsonParseOptions::DEFAULT);

        let row_op = MaxwellChangeEvent::new(accessor);

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
        payload: Vec<u8>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> Result<WriteGuard> {
        self.parse_inner(payload, writer).await
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::Op;
    use risingwave_common::cast::str_to_timestamp;
    use risingwave_common::row::Row;
    use risingwave_common::types::{DataType, ScalarImpl, ToOwnedDatum};

    use super::*;
    use crate::parser::{SourceColumnDesc, SourceStreamChunkBuilder};
    #[tokio::test]
    async fn test_json_parser() {
        let descs = vec![
            SourceColumnDesc::simple("ID", DataType::Int32, 0.into()),
            SourceColumnDesc::simple("NAME", DataType::Varchar, 1.into()),
            SourceColumnDesc::simple("is_adult", DataType::Int16, 2.into()),
            SourceColumnDesc::simple("birthday", DataType::Timestamp, 3.into()),
        ];

        let parser = MaxwellParser::new(descs.clone(), Default::default()).unwrap();

        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 4);
        let payloads = vec![
            br#"{"database":"test","table":"t","type":"insert","ts":1666937996,"xid":1171,"commit":true,"data":{"id":1,"name":"tom","is_adult":0,"birthday":"2017-12-31 16:00:01"}}"#.to_vec(),
            br#"{"database":"test","table":"t","type":"insert","ts":1666938023,"xid":1254,"commit":true,"data":{"id":2,"name":"alex","is_adult":1,"birthday":"1999-12-31 16:00:01"}}"#.to_vec(),
            br#"{"database":"test","table":"t","type":"update","ts":1666938068,"xid":1373,"commit":true,"data":{"id":2,"name":"chi","is_adult":1,"birthday":"1999-12-31 16:00:01"},"old":{"name":"alex"}}"#.to_vec()
        ];

        for payload in payloads {
            let writer = builder.row_writer();
            parser.parse_inner(payload, writer).await.unwrap();
        }

        let chunk = builder.finish();

        let mut rows = chunk.rows();

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(row.datum_at(0).to_owned_datum(), Some(ScalarImpl::Int32(1)));
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("tom".into())))
            );
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int16(0)))
            );
            assert_eq!(
                row.datum_at(3).to_owned_datum(),
                (Some(ScalarImpl::Timestamp(
                    str_to_timestamp("2017-12-31 16:00:01").unwrap()
                )))
            )
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(row.datum_at(0).to_owned_datum(), Some(ScalarImpl::Int32(2)));
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("alex".into())))
            );
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int16(1)))
            );
            assert_eq!(
                row.datum_at(3).to_owned_datum(),
                (Some(ScalarImpl::Timestamp(
                    str_to_timestamp("1999-12-31 16:00:01").unwrap()
                )))
            )
        }

      

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(row.datum_at(0).to_owned_datum(), Some(ScalarImpl::Int32(2)));
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("chi".into())))
            );
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int16(1)))
            );
            assert_eq!(
                row.datum_at(3).to_owned_datum(),
                (Some(ScalarImpl::Timestamp(
                    str_to_timestamp("1999-12-31 16:00:01").unwrap()
                )))
            )
        }
    }
}
