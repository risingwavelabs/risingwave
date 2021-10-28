use rust_decimal::Decimal;
use serde_json::Value;

use crate::error::ErrorCode::ProtocolError;
use crate::error::RwError;
use crate::source::{SourceColumnDesc, SourceParser};
use crate::types::{DataTypeKind, Datum, ScalarImpl, ScalarRef};

/// Parser for JSON format
pub struct JSONParser;

impl SourceParser for JSONParser {
    fn parse(
        &self,
        payload: &[u8],
        columns: &[SourceColumnDesc],
    ) -> crate::error::Result<Vec<Datum>> {
        let value: Value = serde_json::from_slice(payload)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        let ret = columns
            .iter()
            .map(|column| {
                let value = value.get(&column.name);
                match column.data_type.data_type_kind() {
                    DataTypeKind::Boolean => value
                        .and_then(|v| v.as_bool())
                        .map(|v| ScalarImpl::Bool(v as bool)),
                    DataTypeKind::Int16 => value
                        .and_then(|v| v.as_i64())
                        .map(|v| ScalarImpl::Int16(v as i16)),
                    DataTypeKind::Int32 => value
                        .and_then(|v| v.as_i64())
                        .map(|v| ScalarImpl::Int32(v as i32)),
                    DataTypeKind::Int64 => value
                        .and_then(|v| v.as_i64())
                        .map(|v| ScalarImpl::Int64(v as i64)),
                    DataTypeKind::Float32 => value
                        .and_then(|v| v.as_f64())
                        .map(|v| ScalarImpl::Float32(v as f32)),
                    DataTypeKind::Float64 => value
                        .and_then(|v| v.as_f64())
                        .map(|v| ScalarImpl::Float64(v as f64)),
                    DataTypeKind::Decimal => value
                        .and_then(|v| v.as_u64())
                        .map(|v| ScalarImpl::Decimal(Decimal::from(v))),
                    DataTypeKind::Char | DataTypeKind::Varchar => value
                        .and_then(|v| v.as_str())
                        .map(|v| ScalarImpl::UTF8(v.to_owned_scalar())),
                    _ => unimplemented!(),
                }
            })
            .collect::<Vec<Datum>>();

        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use crate::source::{JSONParser, SourceColumnDesc, SourceParser};
    use crate::types::{
        BoolType, DataTypeKind, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
        ScalarImpl, StringType,
    };

    #[test]
    fn test_json_parser() {
        let parser = JSONParser {};

        let payload = r#"{"i32":1,"char":"char","bool":true,"i16":1,"i64":12345678,"f32":1.23,"f64":1.2345,"varchar":"varchar"}"#.as_bytes();
        let descs = vec![
            SourceColumnDesc {
                name: "i32".to_string(),
                data_type: Int32Type::create(false),
                column_id: 0,
            },
            SourceColumnDesc {
                name: "char".to_string(),
                data_type: StringType::create(false, 8, DataTypeKind::Char),
                column_id: 1,
            },
            SourceColumnDesc {
                name: "bool".to_string(),
                data_type: BoolType::create(false),
                column_id: 2,
            },
            SourceColumnDesc {
                name: "i16".to_string(),
                data_type: Int16Type::create(false),
                column_id: 3,
            },
            SourceColumnDesc {
                name: "i64".to_string(),
                data_type: Int64Type::create(false),
                column_id: 4,
            },
            SourceColumnDesc {
                name: "f32".to_string(),
                data_type: Float32Type::create(false),
                column_id: 5,
            },
            SourceColumnDesc {
                name: "f64".to_string(),
                data_type: Float64Type::create(false),
                column_id: 6,
            },
            SourceColumnDesc {
                name: "varchar".to_string(),
                data_type: StringType::create(false, 9, DataTypeKind::Varchar),
                column_id: 7,
            },
        ];

        let result = parser.parse(payload, &descs);
        assert!(result.is_ok());
        let datums = result.unwrap();
        assert_eq!(datums.len(), descs.len());
        assert!(datums[0].eq(&Some(ScalarImpl::Int32(1))));
        assert!(datums[1].eq(&Some(ScalarImpl::UTF8("char".to_string()))));
        assert!(datums[2].eq(&Some(ScalarImpl::Bool(true))));
        assert!(datums[3].eq(&Some(ScalarImpl::Int16(1))));
        assert!(datums[4].eq(&Some(ScalarImpl::Int64(12345678))));
        assert!(datums[5].eq(&Some(ScalarImpl::Float32(1.23))));
        assert!(datums[6].eq(&Some(ScalarImpl::Float64(1.2345))));
        assert!(datums[7].eq(&Some(ScalarImpl::UTF8("varchar".to_string()))));

        let payload = r#"{"i32":1}"#.as_bytes();
        let result = parser.parse(payload, &descs);
        assert!(result.is_ok());
        let datums = result.unwrap();
        assert_eq!(datums.len(), descs.len());
        assert!(datums[0].eq(&Some(ScalarImpl::Int32(1))));
        assert!(datums[1].eq(&None));

        let payload = r#"{"i32:1}"#.as_bytes();
        let result = parser.parse(payload, &descs);
        assert!(result.is_err());
    }
}
