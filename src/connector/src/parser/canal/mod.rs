// Copyright 2023 Singularity Data
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

mod simd_json_parser;

mod operators;
mod util;

pub use simd_json_parser::*;

#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use risingwave_common::array::Op;
    use risingwave_common::row::Row;
    use risingwave_common::types::{DataType, Decimal, ScalarImpl, ToOwnedDatum};
    use risingwave_expr::vector_op::cast::str_to_timestamp;

    use super::*;
    use crate::parser::{SourceParser, SourceStreamChunkBuilder};
    use crate::SourceColumnDesc;

    #[tokio::test]
    async fn test_json_parser() {
        let payload = br#"{"data":[{"id":"1","name":"mike","is_adult":"0","balance":"1500.62","reg_time":"2018-01-01 00:00:01","win_rate":"0.65"}],"database":"demo","es":1668673476000,"id":7,"isDdl":false,"mysqlType":{"id":"int","name":"varchar(40)","is_adult":"boolean","balance":"decimal(10,2)","reg_time":"timestamp","win_rate":"double"},"old":[{"balance":"1000.62"}],"pkNames":null,"sql":"","sqlType":{"id":4,"name":12,"is_adult":-6,"balance":3,"reg_time":93,"win_rate":8},"table":"demo","ts":1668673476732,"type":"UPDATE"}"#;
        let parser = CanalJsonParser;
        let descs = vec![
            SourceColumnDesc::simple("ID", DataType::Int64, 0.into()),
            SourceColumnDesc::simple("NAME", DataType::Varchar, 1.into()),
            SourceColumnDesc::simple("is_adult", DataType::Boolean, 2.into()),
            SourceColumnDesc::simple("balance", DataType::Decimal, 3.into()),
            SourceColumnDesc::simple("reg_time", DataType::Timestamp, 4.into()),
            SourceColumnDesc::simple("win_rate", DataType::Float64, 5.into()),
        ];

        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 2);

        let writer = builder.row_writer();
        parser.parse(payload, writer).await.unwrap();

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

        let parser = CanalJsonParser;
        let descs = vec![
            SourceColumnDesc::simple("v1", DataType::Int32, 0.into()),
            SourceColumnDesc::simple("v2", DataType::Int32, 1.into()),
        ];

        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 2);

        let writer = builder.row_writer();
        parser.parse(payload, writer).await.unwrap();

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
