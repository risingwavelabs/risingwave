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

use std::collections::BTreeMap;
use std::fmt::Debug;

use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::chrono_wrapper::NANOSECONDS_PER_DAY;
use risingwave_common::types::{DataType, Datum, NaiveDateTimeWrapper, Scalar};
use risingwave_expr::vector_op::cast::{timestamp_to_date, timestamp_to_time};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

use crate::parser::common::json_parse_value;
use crate::{SourceParser, SourceStreamChunkRowWriter, WriteGuard};

const DEBEZIUM_READ_OP: &str = "r";
const DEBEZIUM_CREATE_OP: &str = "c";
const DEBEZIUM_UPDATE_OP: &str = "u";
const DEBEZIUM_DELETE_OP: &str = "d";

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DebeziumEvent {
    pub payload: Payload,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Payload {
    pub before: Option<BTreeMap<String, Value>>,
    pub after: Option<BTreeMap<String, Value>>,
    pub op: String,
    #[serde(rename = "ts_ms")]
    pub ts_ms: i64,
}

#[derive(Debug)]
pub struct DebeziumJsonParser;

impl SourceParser for DebeziumJsonParser {
    fn parse(&self, payload: &[u8], writer: SourceStreamChunkRowWriter<'_>) -> Result<WriteGuard> {
        let event: DebeziumEvent = serde_json::from_slice(payload)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        let mut payload = event.payload;

        match payload.op.as_str() {
            DEBEZIUM_UPDATE_OP => {
                let before = payload.before.as_mut().ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "before is missing for updating event. If you are using postgres, you may want to try ALTER TABLE $TABLE_NAME REPLICA IDENTITY FULL;".to_string(),
                    ))
                })?;

                let after = payload.after.as_mut().ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "after is missing for updating event".to_string(),
                    ))
                })?;

                writer.update(|column| {
                    let before =
                        debezium_json_parse_value(&column.data_type, before.get(&column.name))?;
                    let after =
                        debezium_json_parse_value(&column.data_type, after.get(&column.name))?;

                    Ok((before, after))
                })
            }
            DEBEZIUM_CREATE_OP | DEBEZIUM_READ_OP => {
                let after = payload.after.as_ref().ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "after is missing for creating event".to_string(),
                    ))
                })?;

                writer.insert(|column| {
                    debezium_json_parse_value(&column.data_type, after.get(&column.name))
                        .map_err(Into::into)
                })
            }
            DEBEZIUM_DELETE_OP => {
                let before = payload.before.as_ref().ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "before is missing for delete event".to_string(),
                    ))
                })?;

                writer.delete(|column| {
                    debezium_json_parse_value(&column.data_type, before.get(&column.name))
                        .map_err(Into::into)
                })
            }
            _ => Err(RwError::from(ProtocolError(format!(
                "unknown debezium op: {}",
                payload.op
            )))),
        }
    }
}

fn parse_unix_timestamp(dtype: &DataType, unix: i64) -> anyhow::Result<Datum> {
    Ok(Some(match *dtype {
        DataType::Date => timestamp_to_date(NaiveDateTimeWrapper::from_protobuf(
            unix * NANOSECONDS_PER_DAY,
        )?)?
        .to_scalar_value(),
        DataType::Time => {
            timestamp_to_time(NaiveDateTimeWrapper::from_protobuf(unix * 1000)?)?.to_scalar_value()
        }
        DataType::Timestamp => NaiveDateTimeWrapper::from_protobuf(unix * 1000)?.to_scalar_value(),
        _ => unreachable!(),
    }))
}

fn debezium_json_parse_value(dtype: &DataType, value: Option<&Value>) -> anyhow::Result<Datum> {
    if let Some(v) = value && let Some(unix) = v.as_i64() && vec![DataType::Timestamp, DataType::Time, DataType::Date].contains(dtype) {
        parse_unix_timestamp(dtype, unix)
    } else {
        json_parse_value(dtype, value)
    }
}

#[cfg(test)]
mod test {

    use risingwave_common::array::{Op, Row};
    use risingwave_common::catalog::ColumnId;
    use risingwave_common::types::{DataType, NaiveDateTimeWrapper, NaiveDateWrapper, ScalarImpl};

    use crate::parser::debezium::json::DebeziumJsonParser;
    use crate::{SourceColumnDesc, SourceParser, SourceStreamChunkBuilder};

    const INPUT: &[(&str, DataType)] = &[
        ("id", DataType::Int32),
        ("name", DataType::Varchar),
        ("description", DataType::Varchar),
        ("weight", DataType::Float64),
    ];

    fn get_test_columns(input: &Vec<(&str, DataType)>) -> Vec<SourceColumnDesc> {
        input
            .iter()
            .enumerate()
            .map(|(i, (name, data_type))| SourceColumnDesc {
                name: name.to_string(),
                data_type: data_type.clone(),
                column_id: ColumnId::from(i as i32),
                skip_parse: false,
                fields: vec![],
            })
            .collect()
    }

    fn parse_one(
        parser: impl SourceParser,
        columns: Vec<SourceColumnDesc>,
        payload: &[u8],
    ) -> Vec<(Op, Row)> {
        let mut builder = SourceStreamChunkBuilder::with_capacity(columns, 2);
        {
            let writer = builder.row_writer();
            parser.parse(payload, writer).unwrap();
        }
        let chunk = builder.finish();
        chunk
            .rows()
            .map(|(op, row_ref)| (op, row_ref.to_owned_row()))
            .collect::<Vec<_>>()
    }

    #[test]
    fn test_debezium_json_parser_read() {
        //     "before": null,
        //     "after": {
        //       "id": 101,
        //       "name": "scooter",
        //       "description": "Small 2-wheel scooter",
        //       "weight": 1.234
        //     },
        let data = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"before"},{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"dbserver1.inventory.products.Envelope"},"payload":{"before":null,"after":{"id":101,"name":"scooter","description":"Small 2-wheel scooter","weight":1.234},"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639547113601,"snapshot":"true","db":"inventory","sequence":null,"table":"products","server_id":0,"gtid":null,"file":"mysql-bin.000003","pos":156,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1639547113602,"transaction":null}}"#;
        let parser = DebeziumJsonParser;
        let columns = get_test_columns(&INPUT.to_vec());

        let [(_op, row)]: [_; 1] = parse_one(parser, columns, data).try_into().unwrap();

        assert!(row[0].eq(&Some(ScalarImpl::Int32(101))));
        assert!(row[1].eq(&Some(ScalarImpl::Utf8("scooter".to_string()))));
        assert!(row[2].eq(&Some(ScalarImpl::Utf8("Small 2-wheel scooter".to_string()))));
        assert!(row[3].eq(&Some(ScalarImpl::Float64(1.234.into()))));
    }

    #[test]
    fn test_debezium_json_parser_insert() {
        //     "before": null,
        //     "after": {
        //       "id": 102,
        //       "name": "car battery",
        //       "description": "12V car battery",
        //       "weight": 8.1
        //     },
        let data = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"before"},{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"dbserver1.inventory.products.Envelope"},"payload":{"before":null,"after":{"id":102,"name":"car battery","description":"12V car battery","weight":8.1},"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639551564000,"snapshot":"false","db":"inventory","sequence":null,"table":"products","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":717,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1639551564960,"transaction":null}}"#;
        let parser = DebeziumJsonParser;
        let columns = get_test_columns(&INPUT.to_vec());
        let [(op, row)]: [_; 1] = parse_one(parser, columns, data).try_into().unwrap();
        assert_eq!(op, Op::Insert);

        assert!(row[0].eq(&Some(ScalarImpl::Int32(102))));
        assert!(row[1].eq(&Some(ScalarImpl::Utf8("car battery".to_string()))));
        assert!(row[2].eq(&Some(ScalarImpl::Utf8("12V car battery".to_string()))));
        assert!(row[3].eq(&Some(ScalarImpl::Float64(8.1.into()))));
    }

    #[test]
    fn test_debezium_json_parser_delete() {
        //     "before": {
        //       "id": 101,
        //       "name": "scooter",
        //       "description": "Small 2-wheel scooter",
        //       "weight": 1.234
        //     },
        //     "after": null,
        let data = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"before"},{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"dbserver1.inventory.products.Envelope"},"payload":{"before":{"id":101,"name":"scooter","description":"Small 2-wheel scooter","weight":1.234},"after":null,"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639551767000,"snapshot":"false","db":"inventory","sequence":null,"table":"products","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":1045,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1639551767775,"transaction":null}}"#;
        let parser = DebeziumJsonParser {};
        let columns = get_test_columns(&INPUT.to_vec());
        let [(op, row)]: [_; 1] = parse_one(parser, columns, data).try_into().unwrap();

        assert_eq!(op, Op::Delete);

        assert!(row[0].eq(&Some(ScalarImpl::Int32(101))));
        assert!(row[1].eq(&Some(ScalarImpl::Utf8("scooter".to_string()))));
        assert!(row[2].eq(&Some(ScalarImpl::Utf8("Small 2-wheel scooter".to_string()))));
        assert!(row[3].eq(&Some(ScalarImpl::Float64(1.234.into()))));
    }

    #[test]
    fn test_debezium_json_parser_update() {
        //     "before": {
        //       "id": 102,
        //       "name": "car battery",
        //       "description": "12V car battery",
        //       "weight": 8.1
        //     },
        //     "after": {
        //       "id": 102,
        //       "name": "car battery",
        //       "description": "24V car battery",
        //       "weight": 9.1
        //     },
        let data = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"before"},{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"dbserver1.inventory.products.Envelope"},"payload":{"before":{"id":102,"name":"car battery","description":"12V car battery","weight":8.1},"after":{"id":102,"name":"car battery","description":"24V car battery","weight":9.1},"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639551901000,"snapshot":"false","db":"inventory","sequence":null,"table":"products","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":1382,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1639551901165,"transaction":null}}"#;
        let parser = DebeziumJsonParser {};
        let columns = get_test_columns(&INPUT.to_vec());

        let [(op1, row1), (op2, row2)]: [_; 2] =
            parse_one(parser, columns, data).try_into().unwrap();

        assert_eq!(op1, Op::UpdateDelete);
        assert_eq!(op2, Op::UpdateInsert);

        assert!(row1[0].eq(&Some(ScalarImpl::Int32(102))));
        assert!(row1[1].eq(&Some(ScalarImpl::Utf8("car battery".to_string()))));
        assert!(row1[2].eq(&Some(ScalarImpl::Utf8("12V car battery".to_string()))));
        assert!(row1[3].eq(&Some(ScalarImpl::Float64(8.1.into()))));

        assert!(row2[0].eq(&Some(ScalarImpl::Int32(102))));
        assert!(row2[1].eq(&Some(ScalarImpl::Utf8("car battery".to_string()))));
        assert!(row2[2].eq(&Some(ScalarImpl::Utf8("24V car battery".to_string()))));
        assert!(row2[3].eq(&Some(ScalarImpl::Float64(9.1.into()))));
    }

    #[test]
    fn test_update_with_before_null() {
        // the test case it identical with test_debezium_json_parser_insert but op is 'u'
        //     "before": null,
        //     "after": {
        //       "id": 102,
        //       "name": "car battery",
        //       "description": "12V car battery",
        //       "weight": 8.1
        //     },
        let data = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"before"},{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"dbserver1.inventory.products.Envelope"},"payload":{"before":null,"after":{"id":102,"name":"car battery","description":"12V car battery","weight":8.1},"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639551564000,"snapshot":"false","db":"inventory","sequence":null,"table":"products","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":717,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1639551564960,"transaction":null}}"#;
        let parser = DebeziumJsonParser;
        let columns = get_test_columns(&INPUT.to_vec());

        let mut builder = SourceStreamChunkBuilder::with_capacity(columns, 2);
        let writer = builder.row_writer();
        if let Err(e) = parser.parse(data, writer) {
            println!("{:?}", e.to_string());
        } else {
            panic!("the test case is expected to be failed");
        }
    }

    #[test]
    fn test_debezium_json_parser_read_unix() {
        use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
        use risingwave_common::types::NaiveTimeWrapper;
        //     "before": null,
        //     "after": {
        //       "id": 1,
        //       "ts": 1665791731989360,
        //       "d": 19279,
        //       "t": 86131989360
        //     },
        let data = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int64","optional":false,"field":"id"},{"type":"int64","optional":true,"name":"io.debezium.time.MicroTimestamp","version":1,"field":"ts"},{"type":"int32","optional":true,"name":"io.debezium.time.Date","version":1,"field":"d"},{"type":"int64","optional":true,"name":"io.debezium.time.MicroTime","version":1,"field":"t"}],"optional":true,"name":"postgres.public.t.Value","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":false,"field":"id"},{"type":"int64","optional":true,"name":"io.debezium.time.MicroTimestamp","version":1,"field":"ts"},{"type":"int32","optional":true,"name":"io.debezium.time.Date","version":1,"field":"d"},{"type":"int64","optional":true,"name":"io.debezium.time.MicroTime","version":1,"field":"t"}],"optional":true,"name":"postgres.public.t.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false,incremental"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":false,"field":"schema"},{"type":"string","optional":false,"field":"table"},{"type":"int64","optional":true,"field":"txId"},{"type":"int64","optional":true,"field":"lsn"},{"type":"int64","optional":true,"field":"xmin"}],"optional":false,"name":"io.debezium.connector.postgresql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"postgres.public.t.Envelope"},"payload":{"before":null,"after":{"id":1,"ts":1665791731989360,"d":19279,"t":86131989360},"source":{"version":"1.9.5.Final","connector":"postgresql","name":"postgres","ts_ms":1665806169262,"snapshot":"true","db":"ch_benchmark_db","sequence":"[null,\"25503568\"]","schema":"public","table":"t","txId":4499,"lsn":25503568,"xmin":null},"op":"r","ts_ms":1665806169263,"transaction":null}}"#;
        let parser = DebeziumJsonParser;
        let columns = get_test_columns(&vec![
            ("id", DataType::Int64),
            ("ts", DataType::Timestamp),
            ("d", DataType::Date),
            ("t", DataType::Time),
        ]);

        let [(_op, row)]: [_; 1] = parse_one(parser, columns, data).try_into().unwrap();

        assert!(row[0].eq(&Some(ScalarImpl::Int64(1))));
        assert!(
            row[1].eq(&Some(ScalarImpl::NaiveDateTime(NaiveDateTimeWrapper::new(
                NaiveDateTime::parse_from_str("2022-10-14 23:55:31.989360", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap()
            ))))
        );
        assert!(row[2].eq(&Some(ScalarImpl::NaiveDate(NaiveDateWrapper::new(
            NaiveDate::parse_from_str("2022-10-14", "%Y-%m-%d").unwrap()
        )))));
        assert!(row[3].eq(&Some(ScalarImpl::NaiveTime(NaiveTimeWrapper::new(
            NaiveTime::parse_from_str("23:55:31.989360", "%H:%M:%S%.f").unwrap()
        )))));
    }
}
