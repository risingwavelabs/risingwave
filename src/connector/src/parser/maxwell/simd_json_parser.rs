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

use futures_async_stream::try_stream;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use simd_json::{BorrowedValue, ValueAccess};

use super::operators::*;
use crate::impl_common_parser_logic;
use crate::parser::common::simd_json_parse_value;
use crate::parser::{SourceStreamChunkRowWriter, WriteGuard};
use crate::source::{SourceColumnDesc, SourceContextRef};

const AFTER: &str = "data";
const BEFORE: &str = "old";
const OP: &str = "type";

impl_common_parser_logic!(MaxwellParser);

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

        let op = event.get(OP).and_then(|v| v.as_str()).ok_or_else(|| {
            RwError::from(ProtocolError(
                "op field not found in maxwell json".to_owned(),
            ))
        })?;

        match op {
            MAXWELL_INSERT_OP => {
                let after = event.get(AFTER).ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "data is missing for creating event".to_string(),
                    ))
                })?;
                writer.insert(|column| {
                    simd_json_parse_value(
                        &column.data_type,
                        after.get(column.name_in_lower_case.as_str()),
                    )
                    .map_err(Into::into)
                })
            }
            MAXWELL_UPDATE_OP => {
                let after = event.get(AFTER).ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "data is missing for updating event".to_string(),
                    ))
                })?;
                let before = event.get(BEFORE).ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "old is missing for updating event".to_string(),
                    ))
                })?;

                writer.update(|column| {
                    // old only contains the changed columns but data contains all columns.
                    let col_name_lc = column.name_in_lower_case.as_str();
                    let before_value = before.get(col_name_lc).or_else(|| after.get(col_name_lc));
                    let before = simd_json_parse_value(&column.data_type, before_value)?;
                    let after = simd_json_parse_value(&column.data_type, after.get(col_name_lc))?;
                    Ok((before, after))
                })
            }
            MAXWELL_DELETE_OP => {
                let before = event.get(AFTER).ok_or_else(|| {
                    RwError::from(ProtocolError("old is missing for delete event".to_string()))
                })?;
                writer.delete(|column| {
                    simd_json_parse_value(
                        &column.data_type,
                        before.get(column.name_in_lower_case.as_str()),
                    )
                    .map_err(Into::into)
                })
            }
            other => Err(RwError::from(ProtocolError(format!(
                "unknown Maxwell op: {}",
                other
            )))),
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::Op;
    use risingwave_common::row::Row;
    use risingwave_common::types::{DataType, ScalarImpl, ToOwnedDatum};
    use risingwave_expr::vector_op::cast::str_to_timestamp;

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
            assert_eq!(op, Op::UpdateDelete);
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
            assert_eq!(op, Op::UpdateInsert);
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
