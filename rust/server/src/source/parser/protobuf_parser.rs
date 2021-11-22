use std::path::Path;

use itertools::Itertools;
use protobuf::descriptor::FileDescriptorSet;
use protobuf::RepeatedField;
use rust_decimal::Decimal;
use serde::de::Deserialize;
use serde_protobuf::de::Deserializer;
use serde_protobuf::descriptor::Descriptors;
use serde_value::Value;

use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::Result;
use risingwave_common::error::RwError;
use risingwave_common::types::{DataTypeKind, Datum, ScalarImpl};

use crate::source::{SourceColumnDesc, SourceParser};

/// Parser for Protobuf-encoded bytes.
#[derive(Debug)]
pub struct ProtobufParser {
    descriptors: Descriptors,
    message_name: String,
}

impl ProtobufParser {
    /// Generate message name
    fn normalize_message_name(message_name: &str) -> String {
        if message_name.is_empty() || !message_name.contains('.') || message_name.starts_with('.') {
            message_name.to_string()
        } else {
            format!(".{}", message_name)
        }
    }

    /// Decode payload to `SerdeValue`
    fn decode(&self, data: &[u8]) -> Result<Value> {
        let input_stream = protobuf::CodedInputStream::from_bytes(data);
        let mut deserializer =
            Deserializer::for_named_message(&self.descriptors, &self.message_name, input_stream)
                .map_err(|e| {
                    RwError::from(ProtocolError(format!(
                        "Creating an input stream to parse protobuf: {:?}",
                        e
                    )))
                })?;

        let deserialized_message = Value::deserialize(&mut deserializer).map_err(|e| {
            RwError::from(ProtocolError(format!(
                "Creating an input stream to parse protobuf: {:?}",
                e
            )))
        })?;

        Ok(deserialized_message)
    }

    /// Generate a `ProtobufParser`
    /// * `inputs`, `includes`: protobuf files path and include dir
    /// * `message_name`: a message name that needs to correspond
    pub fn new(includes: &[String], inputs: &[String], message_name: &str) -> Result<Self> {
        let includes = includes.iter().map(Path::new).collect_vec();
        let inputs = inputs.iter().map(Path::new).collect_vec();

        let parsed_result = protobuf_codegen_pure::parse_and_typecheck(&includes, &inputs)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        let mut file_descriptor_set = FileDescriptorSet::new();
        file_descriptor_set.set_file(RepeatedField::from(parsed_result.file_descriptors));

        Ok(ProtobufParser {
            descriptors: Descriptors::from_proto(&file_descriptor_set),
            message_name: Self::normalize_message_name(message_name),
        })
    }
}

macro_rules! protobuf_match_type {
  ($value:expr, $target_scalar_type:path, { $($serde_type:tt),* }, $target_type:tt) => {
    $value.and_then(|v| match v {
      $(Value::$serde_type(b) => Some($target_type::from(b)), )*
      Value::Option(Some(boxed_value)) => match *boxed_value {
        $(Value::$serde_type(b) => Some($target_type::from(b)), )*
        _ => None,
      },
      _ => None,
    }).map($target_scalar_type)
  };
}

impl SourceParser for ProtobufParser {
    fn parse(&self, payload: &[u8], columns: &[SourceColumnDesc]) -> Result<Vec<Datum>> {
        let mut map = match self.decode(payload)? {
            Value::Map(m) => m,
            _ => return Err(RwError::from(ProtocolError("".to_string()))),
        };

        let ret =  columns.iter().map(|column| {
      let key = Value::String(column.name.clone());

      // Use `remove` instead of `get` to take the ownership of the value
      let value = map.remove(&key);
      match column.data_type.data_type_kind() {
        DataTypeKind::Boolean => {
          protobuf_match_type!(value, ScalarImpl::Bool, { Bool }, bool)
        }
        DataTypeKind::Int16 => {
          protobuf_match_type!(value, ScalarImpl::Int16, { I8, I16, U8 }, i16)
        },
        DataTypeKind::Int32 => {
          protobuf_match_type!(value, ScalarImpl::Int32, { I8, I16, I32, U8, U16 }, i32)
        }
        DataTypeKind::Int64 => {
          protobuf_match_type!(value, ScalarImpl::Int64, { I8, I16, I32, I64, U8, U16, U32 }, i64)
        }
        DataTypeKind::Float32 => {
          protobuf_match_type!(value, ScalarImpl::Float32, { I8, I16, U8, U16, F32 }, f32)
        }
        DataTypeKind::Float64 => {
          protobuf_match_type!(value, ScalarImpl::Float64, { I8, I16, I32, U8, U16, U32, F32, F64}, f64)
        }
        DataTypeKind::Decimal => {
          protobuf_match_type!(value, ScalarImpl::Decimal, { I8, I16, I32, I64, U8, U16, U32, U64}, Decimal)
        }
        DataTypeKind::Char | DataTypeKind::Varchar => {
          protobuf_match_type!(value, ScalarImpl::UTF8, { String }, String)
        }
        _ => unimplemented!(),
      }
    }).collect::<Vec<Datum>>();

        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use serde_value::Value;
    use tempfile::Builder;

    use risingwave_common::error::Result;
    use risingwave_common::types::{
        DataTypeKind, Float32Type, Int32Type, Int64Type, ScalarImpl, StringType,
    };

    use crate::source::{ProtobufParser, SourceColumnDesc, SourceParser};

    static PROTO_FILE_DATA: &str = r#"
    syntax = "proto3";
    package test;
    message TestRecord {
      int32 id = 1;
      string address = 2;
      string city = 3;
      int64 zipcode = 4;
      float rate = 5;
    }"#;

    //        Id:      123,
    // 		Address: "test address",
    // 		City:    "test city",
    // 		Zipcode: 456,
    // 		Rate:    1.2345,
    static PRE_GEN_PROTO_DATA: &[u8] = b"\x08\x7b\x12\x0c\x74\x65\x73\x74\x20\x61\x64\x64\x72\x65\x73\x73\x1a\x09\x74\x65\x73\x74\x20\x63\x69\x74\x79\x20\xc8\x03\x2d\x19\x04\x9e\x3f";

    fn create_parser() -> Result<ProtobufParser> {
        let temp_file = Builder::new()
            .prefix("temp")
            .suffix(".proto")
            .rand_bytes(5)
            .tempfile()
            .unwrap();

        let dir = temp_file.path().parent().unwrap().to_str().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut file = temp_file.as_file();

        file.write_all(PROTO_FILE_DATA.as_ref())
            .expect("writing binary to test file");

        ProtobufParser::new(&[dir.to_string()], &[path.to_string()], ".test.TestRecord")
    }

    macro_rules! map(
    { $($key:expr => $value:expr),+ } => {
        {
            let mut m = ::std::collections::HashMap::new();
            $(
                m.insert($key, $value);
            )+
            m
        }
     };
);

    #[test]
    fn test_proto_message_name() {
        assert_eq!(ProtobufParser::normalize_message_name(""), "".to_string());
        assert_eq!(
            ProtobufParser::normalize_message_name("test"),
            "test".to_string()
        );
        assert_eq!(
            ProtobufParser::normalize_message_name(".test"),
            ".test".to_string()
        );
        assert_eq!(
            ProtobufParser::normalize_message_name("test.Test"),
            ".test.Test".to_string()
        );
    }

    #[test]
    fn test_create_parser() {
        assert!(create_parser().is_ok())
    }

    #[test]
    fn test_parser_decode() {
        let parser = create_parser().unwrap();

        let value = parser.decode(PRE_GEN_PROTO_DATA).unwrap();

        let map = match value {
            Value::Map(m) => m,
            _ => panic!("value should be map"),
        };

        let hash = map!(
          "id" => Value::Option(Some(Box::new(Value::I32(123)))),
          "address" => Value::Option(Some(Box::new(Value::String("test address".to_string())))),
          "city" => Value::Option(Some(Box::new(Value::String("test city".to_string())))),
          "zipcode" => Value::Option(Some(Box::new(Value::I64(456)))),
          "rate" => Value::Option(Some(Box::new(Value::F32(1.2345))))
        );

        let keys = hash
            .iter()
            .map(|e| e.0)
            .map(|key| key.to_string())
            .collect::<Vec<String>>();

        assert!(hash.iter().all(|e| {
            let key = e.0;
            let val = e.1;

            match map.get(&Value::String(key.to_string())) {
                None => false,
                Some(r) => r == val,
            }
        }));

        assert!(keys
            .iter()
            .all(|key| map.contains_key(&Value::String(key.clone()))));
    }

    #[test]
    fn test_parser_parse() {
        let parser = create_parser().unwrap();
        let descs = vec![
            SourceColumnDesc {
                name: "id".to_string(),
                data_type: Int32Type::create(false),
                column_id: 0,
            },
            SourceColumnDesc {
                name: "address".to_string(),
                data_type: StringType::create(false, 16, DataTypeKind::Char),
                column_id: 1,
            },
            SourceColumnDesc {
                name: "city".to_string(),
                data_type: StringType::create(false, 8, DataTypeKind::Char),
                column_id: 2,
            },
            SourceColumnDesc {
                name: "zipcode".to_string(),
                data_type: Int64Type::create(false),
                column_id: 3,
            },
            SourceColumnDesc {
                name: "rate".to_string(),
                data_type: Float32Type::create(false),
                column_id: 4,
            },
        ];

        let result = parser.parse(PRE_GEN_PROTO_DATA, &descs);
        assert!(result.is_ok());
        let datums = result.unwrap();
        assert_eq!(datums.len(), descs.len());
        assert!(datums[0].eq(&Some(ScalarImpl::Int32(123))));
        assert!(datums[1].eq(&Some(ScalarImpl::UTF8("test address".to_string()))));
        assert!(datums[2].eq(&Some(ScalarImpl::UTF8("test city".to_string()))));
        assert!(datums[3].eq(&Some(ScalarImpl::Int64(456))));
        assert!(datums[4].eq(&Some(ScalarImpl::Float32(1.2345))));
    }
}
