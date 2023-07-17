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

use risingwave_common::error::ErrorCode::{self, ProtocolError};
use risingwave_common::error::{Result, RwError};
use simd_json::{BorrowedValue, Mutable, ValueAccess};

use crate::parser::canal::operators::*;
use crate::parser::unified::json::{JsonAccess, JsonParseOptions};
use crate::parser::unified::util::apply_row_operation_on_stream_chunk_writer;
use crate::parser::unified::ChangeEventOperation;
use crate::parser::{ByteStreamSourceParser, SourceStreamChunkRowWriter, WriteGuard};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

const DATA: &str = "data";
const OP: &str = "type";
const IS_DDL: &str = "isDdl";

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
        let mut event: BorrowedValue<'_> = simd_json::to_borrowed_value(&mut payload)
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

        let op = match event.get(OP).and_then(|v| v.as_str()) {
            Some(CANAL_INSERT_EVENT | CANAL_UPDATE_EVENT) => ChangeEventOperation::Upsert,
            Some(CANAL_DELETE_EVENT) => ChangeEventOperation::Delete,
            _ => Err(RwError::from(ProtocolError(
                "op field not found in canal json".to_owned(),
            )))?,
        };

        let events = event
            .get_mut(DATA)
            .and_then(|v| match v {
                BorrowedValue::Array(array) => Some(array),
                _ => None,
            })
            .ok_or_else(|| {
                RwError::from(ProtocolError(
                    "'data' is missing for creating event".to_string(),
                ))
            })?;
        let mut errors = Vec::new();
        let mut guard = None;
        for event in events.drain(..) {
            let accessor = JsonAccess::new_with_options(event, &JsonParseOptions::CANAL);
            match apply_row_operation_on_stream_chunk_writer((op, accessor), &mut writer) {
                Ok(this_guard) => guard = Some(this_guard),
                Err(err) => errors.push(err),
            }
        }
        if let Some(guard) = guard {
            if !errors.is_empty() {
                tracing::error!(?errors, "failed to parse some columns");
            }
            Ok(guard)
        } else {
            Err(RwError::from(ErrorCode::InternalError(format!(
                "failed to parse all columns: {:?}",
                errors
            ))))
        }
    }
}

impl ByteStreamSourceParser for CanalJsonParser {
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
    use std::str::FromStr;

    use risingwave_common::array::Op;
    use risingwave_common::cast::str_to_timestamp;
    use risingwave_common::row::Row;
    use risingwave_common::types::{DataType, Decimal, JsonbVal, ScalarImpl, ToOwnedDatum};
    use serde_json::Value;

    use super::*;
    use crate::parser::SourceStreamChunkBuilder;
    use crate::source::SourceColumnDesc;

    #[tokio::test]
    async fn test_data_types() {
        let payload = br#"{"id":0,"database":"test","table":"data_type","pkNames":["id"],"isDdl":false,"type":"INSERT","es":1682057341424,"ts":1682057382913,"sql":"","sqlType":{"id":4,"tinyint":-6,"smallint":5,"mediumint":4,"int":4,"bigint":-5,"float":7,"double":8,"decimal":3,"date":91,"datetime":93,"time":92,"timestamp":93,"char":1,"varchar":12,"binary":2004,"varbinary":2004,"blob":2004,"text":2005,"enum":4,"set":-7,"json":12},"mysqlType":{"binary":"binary","varbinary":"varbinary","enum":"enum","set":"set","bigint":"bigint","float":"float","datetime":"datetime","varchar":"varchar","smallint":"smallint","mediumint":"mediumint","double":"double","date":"date","char":"char","id":"int","tinyint":"tinyint","decimal":"decimal","blob":"blob","text":"text","int":"int","time":"time","timestamp":"timestamp","json":"json"},"old":null,"data":[{"id":"1","tinyint":"5","smallint":"136","mediumint":"172113","int":"1801160058","bigint":"3916589616287113937","float":"0","double":"0.15652","decimal":"1.20364700","date":"2023-04-20","datetime":"2023-02-15 13:01:36","time":"20:23:41","timestamp":"2022-10-13 12:12:54","char":"Kathleen","varchar":"atque esse fugiat et quibusdam qui.","binary":"Joseph\u0000\u0000\u0000\u0000","varbinary":"Douglas","blob":"ducimus ut in commodi necessitatibus error magni repellat exercitationem!","text":"rerum sunt nulla quo quibusdam velit doloremque.","enum":"1","set":"1","json":"{\"a\": 1, \"b\": 2}"}]}"#;
        let descs = vec![
            SourceColumnDesc::simple("id", DataType::Int32, 0.into()),
            SourceColumnDesc::simple("date", DataType::Date, 1.into()),
            SourceColumnDesc::simple("datetime", DataType::Timestamp, 2.into()),
            SourceColumnDesc::simple("time", DataType::Time, 3.into()),
            SourceColumnDesc::simple("timestamp", DataType::Timestamp, 4.into()),
            SourceColumnDesc::simple("char", DataType::Varchar, 5.into()),
            SourceColumnDesc::simple("binary", DataType::Bytea, 6.into()),
            SourceColumnDesc::simple("json", DataType::Jsonb, 7.into()),
        ];
        let parser = CanalJsonParser::new(descs.clone(), Default::default()).unwrap();

        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 1);

        let writer = builder.row_writer();
        parser.parse_inner(payload.to_vec(), writer).await.unwrap();

        let chunk = builder.finish();
        let (op, row) = chunk.rows().next().unwrap();
        assert_eq!(op, Op::Insert);
        assert_eq!(row.datum_at(0).to_owned_datum(), Some(ScalarImpl::Int32(1)));
        assert_eq!(
            row.datum_at(1).to_owned_datum(),
            Some(ScalarImpl::Date(
                chrono::NaiveDate::from_ymd_opt(2023, 4, 20).unwrap().into()
            ))
        );
        assert_eq!(
            row.datum_at(2).to_owned_datum(),
            Some(ScalarImpl::Timestamp(
                str_to_timestamp("2023-02-15 13:01:36").unwrap()
            ))
        );
        assert_eq!(
            row.datum_at(3).to_owned_datum(),
            Some(ScalarImpl::Time(
                chrono::NaiveTime::from_hms_opt(20, 23, 41).unwrap().into()
            ))
        );
        assert_eq!(
            row.datum_at(4).to_owned_datum(),
            Some(ScalarImpl::Timestamp(
                str_to_timestamp("2022-10-13 12:12:54").unwrap()
            ))
        );
        assert_eq!(
            row.datum_at(5).to_owned_datum(),
            Some(ScalarImpl::Utf8(Box::from("Kathleen".to_string())))
        );
        assert_eq!(
            row.datum_at(6).to_owned_datum(),
            Some(ScalarImpl::Bytea(Box::from(
                "Joseph\u{0}\u{0}\u{0}\u{0}".as_bytes()
            )))
        );
        assert_eq!(
            row.datum_at(7).to_owned_datum(),
            Some(ScalarImpl::Jsonb(JsonbVal::from(Value::from(
                "{\"a\": 1, \"b\": 2}".to_string()
            ))))
        );
    }

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
            assert_eq!(op, Op::Insert);
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
                (Some(ScalarImpl::Timestamp(
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
