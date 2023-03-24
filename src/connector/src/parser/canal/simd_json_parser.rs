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

use std::str::FromStr;

use anyhow::anyhow;
use futures_async_stream::try_stream;
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, Datum, Decimal, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::vector_op::cast::{
    str_to_date, str_to_timestamp, str_with_time_zone_to_timestamptz,
};
use simd_json::{BorrowedValue, StaticNode, ValueAccess};

use crate::parser::canal::operators::*;
use crate::parser::util::at_least_one_ok;
use crate::parser::{SourceStreamChunkRowWriter, WriteGuard};
use crate::source::{SourceColumnDesc, SourceContextRef};
use crate::{ensure_rust_type, ensure_str, impl_common_parser_logic};

const AFTER: &str = "data";
const BEFORE: &str = "old";
const OP: &str = "type";
const IS_DDL: &str = "isddl";

impl_common_parser_logic!(CanalJsonParser);
#[derive(Debug)]
pub struct CanalJsonParser {
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

impl CanalJsonParser {
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

        let is_ddl = event.get(IS_DDL).and_then(|v| v.as_bool()).ok_or_else(|| {
            RwError::from(ProtocolError(
                "isDdl field not found in canal json".to_owned(),
            ))
        })?;
        if is_ddl {
            return Err(RwError::from(ProtocolError(
                "received a DDL message, please set `canal.instance.filter.query.dml` to true."
                    .to_string(),
            )));
        }

        let op = event.get(OP).and_then(|v| v.as_str()).ok_or_else(|| {
            RwError::from(ProtocolError("op field not found in canal json".to_owned()))
        })?;

        match op {
            CANAL_INSERT_EVENT => {
                let inserted = event
                    .get(AFTER)
                    .and_then(|v| match v {
                        BorrowedValue::Array(array) => Some(array.iter()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        RwError::from(ProtocolError(
                            "data is missing for creating event".to_string(),
                        ))
                    })?;
                let results = inserted
                    .into_iter()
                    .map(|v| {
                        writer.insert(|column| {
                            cannal_simd_json_parse_value(
                                &column.data_type,
                                v.get(column.name_in_lower_case.as_str()),
                            )
                        })
                    })
                    .collect::<Vec<Result<_>>>();

                at_least_one_ok(results)
            }
            CANAL_UPDATE_EVENT => {
                let after = event
                    .get(AFTER)
                    .and_then(|v| match v {
                        BorrowedValue::Array(array) => Some(array.iter()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        RwError::from(ProtocolError(
                            "data is missing for updating event".to_string(),
                        ))
                    })?;
                let before = event
                    .get(BEFORE)
                    .and_then(|v| match v {
                        BorrowedValue::Array(array) => Some(array.iter()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        RwError::from(ProtocolError(
                            "old is missing for updating event".to_string(),
                        ))
                    })?;

                let results = before
                    .zip_eq_fast(after)
                    .map(|(before, after)| {
                        writer.update(|column| {
                            // in origin canal, old only contains the changed columns but data
                            // contains all columns.
                            // in ticdc, old contains all fields
                            let col_name_lc = column.name_in_lower_case.as_str();
                            let before_value =
                                before.get(col_name_lc).or_else(|| after.get(col_name_lc));
                            let before =
                                cannal_simd_json_parse_value(&column.data_type, before_value)?;
                            let after = cannal_simd_json_parse_value(
                                &column.data_type,
                                after.get(col_name_lc),
                            )?;
                            Ok((before, after))
                        })
                    })
                    .collect::<Vec<Result<_>>>();

                at_least_one_ok(results)
            }
            CANAL_DELETE_EVENT => {
                let deleted = event
                    .get(AFTER)
                    .and_then(|v| match v {
                        BorrowedValue::Array(array) => Some(array.iter()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        RwError::from(ProtocolError("old is missing for delete event".to_string()))
                    })?;

                let results = deleted
                    .into_iter()
                    .map(|v| {
                        writer.delete(|column| {
                            cannal_simd_json_parse_value(
                                &column.data_type,
                                v.get(column.name_in_lower_case.as_str()),
                            )
                        })
                    })
                    .collect::<Vec<Result<_>>>();

                at_least_one_ok(results)
            }
            other => Err(RwError::from(ProtocolError(format!(
                "unknown canal json op: {}",
                other
            )))),
        }
    }
}

#[inline]
fn cannal_simd_json_parse_value(
    dtype: &DataType,
    value: Option<&BorrowedValue<'_>>,
) -> Result<Datum> {
    match value {
        None | Some(BorrowedValue::Static(StaticNode::Null)) => Ok(None),
        Some(v) => Ok(Some(cannal_do_parse_simd_json_value(dtype, v).map_err(
            |e| {
                tracing::warn!("failed to parse type '{}' from json: {}", dtype, e);
                anyhow!("failed to parse type '{}' from json: {}", dtype, e)
            },
        )?)),
    }
}

#[inline]
fn cannal_do_parse_simd_json_value(dtype: &DataType, v: &BorrowedValue<'_>) -> Result<ScalarImpl> {
    let v = match dtype {
        // mysql use tinyint to represent boolean
        DataType::Boolean => ScalarImpl::Bool(ensure_rust_type!(v, i16) != 0),
        DataType::Int16 => ScalarImpl::Int16(ensure_rust_type!(v, i16)),
        DataType::Int32 => ScalarImpl::Int32(ensure_rust_type!(v, i32)),
        DataType::Int64 => ScalarImpl::Int64(ensure_rust_type!(v, i64)),
        DataType::Float32 => ScalarImpl::Float32(ensure_rust_type!(v, f32).into()),
        DataType::Float64 => ScalarImpl::Float64(ensure_rust_type!(v, f64).into()),
        // FIXME: decimal should have more precision than f64
        DataType::Decimal => Decimal::from_str(ensure_str!(v, "string"))
            .map_err(|_| anyhow!("parse decimal from string err {}", v))?
            .into(),
        DataType::Varchar => ensure_str!(v, "varchar").to_string().into(),
        DataType::Date => str_to_date(ensure_str!(v, "date"))?.into(),
        DataType::Time => str_to_date(ensure_str!(v, "time"))?.into(),
        DataType::Timestamp => str_to_timestamp(ensure_str!(v, "string"))?.into(),
        DataType::Timestamptz => {
            str_with_time_zone_to_timestamptz(ensure_str!(v, "string"))?.into()
        }
        _ => {
            return Err(RwError::from(InternalError(format!(
                "cannal data source not support type {}",
                dtype
            ))))
        }
    };
    Ok(v)
}

#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use risingwave_common::array::Op;
    use risingwave_common::row::Row;
    use risingwave_common::types::{DataType, Decimal, ScalarImpl, ToOwnedDatum};
    use risingwave_expr::vector_op::cast::str_to_timestamp;

    use super::*;
    use crate::parser::SourceStreamChunkBuilder;
    use crate::source::SourceColumnDesc;

    #[tokio::test]
    async fn test_json_parser() {
        let payload = br#"{"data":[{"id":"1","name":"mike","is_adult":"0","balance":"1500.62","reg_time":"2018-01-01 00:00:01","win_rate":"0.65"}],"database":"demo","es":1668673476000,"id":7,"isDdl":false,"mysqlType":{"id":"int","name":"varchar(40)","is_adult":"boolean","balance":"decimal(10,2)","reg_time":"timestamp","win_rate":"double"},"old":[{"balance":"1000.62"}],"pkNames":null,"sql":"","sqlType":{"id":4,"name":12,"is_adult":-6,"balance":3,"reg_time":93,"win_rate":8},"table":"demo","ts":1668673476732,"type":"UPDATE"}"#;

        let descs = vec![
            SourceColumnDesc::simple("ID", DataType::Int64, 0.into()),
            SourceColumnDesc::simple("NAME", DataType::Varchar, 1.into()),
            SourceColumnDesc::simple("is_adult", DataType::Boolean, 2.into()),
            SourceColumnDesc::simple("balance", DataType::Decimal, 3.into()),
            SourceColumnDesc::simple("reg_time", DataType::Timestamp, 4.into()),
            SourceColumnDesc::simple("win_rate", DataType::Float64, 5.into()),
        ];

        let parser = CanalJsonParser::new(descs.clone(), Default::default()).unwrap();

        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 2);

        let writer = builder.row_writer();
        parser.parse_inner(payload.to_vec(), writer).await.unwrap();

        let chunk = builder.finish();

        let mut rows = chunk.rows();

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::UpdateDelete);
            assert_eq!(row.datum_at(0).to_owned_datum(), Some(ScalarImpl::Int64(1)));
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("mike".into())))
            );
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Bool(false)))
            );
            assert_eq!(
                row.datum_at(3).to_owned_datum(),
                (Some(Decimal::from_str("1000.62").unwrap().into()))
            );
            assert_eq!(
                row.datum_at(4).to_owned_datum(),
                (Some(ScalarImpl::NaiveDateTime(
                    str_to_timestamp("2018-01-01 00:00:01").unwrap()
                )))
            );
            assert_eq!(
                row.datum_at(5).to_owned_datum(),
                (Some(ScalarImpl::Float64(0.65.into())))
            );
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::UpdateInsert);
            assert_eq!(row.datum_at(0).to_owned_datum(), Some(ScalarImpl::Int64(1)));
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("mike".into())))
            );
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Bool(false)))
            );
            assert_eq!(
                row.datum_at(3).to_owned_datum(),
                (Some(Decimal::from_str("1500.62").unwrap().into()))
            );
            assert_eq!(
                row.datum_at(4).to_owned_datum(),
                (Some(ScalarImpl::NaiveDateTime(
                    str_to_timestamp("2018-01-01 00:00:01").unwrap()
                )))
            );
            assert_eq!(
                row.datum_at(5).to_owned_datum(),
                (Some(ScalarImpl::Float64(0.65.into())))
            );
        }
    }

    #[tokio::test]
    async fn test_parse_multi_rows() {
        let payload = br#"{"data": [{"v1": "1", "v2": "2"}, {"v1": "3", "v2": "4"}], "old": null, "mysqlType":{"v1": "int", "v2": "int"}, "sqlType":{"v1": 4, "v2": 4}, "database":"demo","es":1668673394000,"id":5,"isDdl":false, "table":"demo","ts":1668673394788,"type":"INSERT"}"#;

        let descs = vec![
            SourceColumnDesc::simple("v1", DataType::Int32, 0.into()),
            SourceColumnDesc::simple("v2", DataType::Int32, 1.into()),
        ];

        let parser = CanalJsonParser::new(descs.clone(), Default::default()).unwrap();

        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 2);

        let writer = builder.row_writer();
        parser.parse_inner(payload.to_vec(), writer).await.unwrap();

        let chunk = builder.finish();

        let mut rows = chunk.rows();

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(row.datum_at(0).to_owned_datum(), Some(ScalarImpl::Int32(1)));
            assert_eq!(row.datum_at(1).to_owned_datum(), Some(ScalarImpl::Int32(2)));
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(row.datum_at(0).to_owned_datum(), Some(ScalarImpl::Int32(3)));
            assert_eq!(row.datum_at(1).to_owned_datum(), Some(ScalarImpl::Int32(4)));
        }
    }
}
