use risingwave_common::array::Op;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::Datum;
use serde_json::Value;

use crate::parser::common::json_parse_value;
use crate::{Event, SourceColumnDesc, SourceParser};

/// Parser for JSON format
#[derive(Debug)]
pub struct JSONParser;

impl SourceParser for JSONParser {
    fn parse(&self, payload: &[u8], columns: &[SourceColumnDesc]) -> Result<Event> {
        let value: Value = serde_json::from_slice(payload)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        Ok(Event {
            ops: vec![Op::Insert],
            rows: vec![columns
                .iter()
                .map(|column| {
                    if column.skip_parse {
                        None
                    } else {
                        json_parse_value(column, value.get(&column.name))
                    }
                })
                .collect::<Vec<Datum>>()],
        })
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::{
        BoolType, DataTypeKind, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
        ScalarImpl, StringType,
    };

    use crate::{JSONParser, SourceColumnDesc, SourceParser};

    #[test]
    fn test_json_parser() {
        let parser = JSONParser {};

        let payload = r#"{"i32":1,"char":"char","bool":true,"i16":1,"i64":12345678,"f32":1.23,"f64":1.2345,"varchar":"varchar"}"#.as_bytes();
        let descs = vec![
            SourceColumnDesc {
                name: "i32".to_string(),
                data_type: Int32Type::create(false),
                column_id: 0,
                skip_parse: false,
                is_primary: false,
            },
            SourceColumnDesc {
                name: "char".to_string(),
                data_type: StringType::create(false, 8, DataTypeKind::Char),
                column_id: 1,
                skip_parse: false,
                is_primary: false,
            },
            SourceColumnDesc {
                name: "bool".to_string(),
                data_type: BoolType::create(false),
                column_id: 2,
                skip_parse: false,
                is_primary: false,
            },
            SourceColumnDesc {
                name: "i16".to_string(),
                data_type: Int16Type::create(false),
                column_id: 3,
                skip_parse: false,
                is_primary: false,
            },
            SourceColumnDesc {
                name: "i64".to_string(),
                data_type: Int64Type::create(false),
                column_id: 4,
                skip_parse: false,
                is_primary: false,
            },
            SourceColumnDesc {
                name: "f32".to_string(),
                data_type: Float32Type::create(false),
                column_id: 5,
                skip_parse: false,
                is_primary: false,
            },
            SourceColumnDesc {
                name: "f64".to_string(),
                data_type: Float64Type::create(false),
                column_id: 6,
                skip_parse: false,
                is_primary: false,
            },
            SourceColumnDesc {
                name: "varchar".to_string(),
                data_type: StringType::create(false, 9, DataTypeKind::Varchar),
                column_id: 7,
                skip_parse: false,
                is_primary: false,
            },
        ];

        let result = parser.parse(payload, &descs);
        assert!(result.is_ok());
        let event = result.unwrap();
        let row = event.rows.first().unwrap();
        assert_eq!(row.len(), descs.len());
        assert!(row[0].eq(&Some(ScalarImpl::Int32(1))));
        assert!(row[1].eq(&Some(ScalarImpl::Utf8("char".to_string()))));
        assert!(row[2].eq(&Some(ScalarImpl::Bool(true))));
        assert!(row[3].eq(&Some(ScalarImpl::Int16(1))));
        assert!(row[4].eq(&Some(ScalarImpl::Int64(12345678))));
        assert!(row[5].eq(&Some(ScalarImpl::Float32(1.23.into()))));
        assert!(row[6].eq(&Some(ScalarImpl::Float64(1.2345.into()))));
        assert!(row[7].eq(&Some(ScalarImpl::Utf8("varchar".to_string()))));

        let payload = r#"{"i32":1}"#.as_bytes();
        let result = parser.parse(payload, &descs);
        assert!(result.is_ok());
        let event = result.unwrap();
        let row = event.rows.first().unwrap();
        assert_eq!(row.len(), descs.len());
        assert!(row[0].eq(&Some(ScalarImpl::Int32(1))));
        assert!(row[1].eq(&None));

        let payload = r#"{"i32:1}"#.as_bytes();
        let result = parser.parse(payload, &descs);
        assert!(result.is_err());
    }
}
