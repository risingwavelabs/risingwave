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

use std::path::Path;

use itertools::Itertools;
use protobuf::descriptor::FileDescriptorSet;
use protobuf::RepeatedField;
use risingwave_common::error::ErrorCode::{self, InternalError, ItemNotFound, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, Decimal, OrderedF32, OrderedF64, ScalarImpl};
use risingwave_expr::vector_op::cast::{str_to_date, str_to_timestamp};
use risingwave_pb::plan_common::ColumnDesc;
use serde::de::Deserialize;
use serde_protobuf::de::Deserializer;
use serde_protobuf::descriptor::{Descriptors, FieldDescriptor, FieldType};
use serde_value::Value;
use url::Url;

use crate::{SourceParser, SourceStreamChunkRowWriter, WriteGuard};

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

    /// Create from local path of protobuf files.
    /// * `inputs`, `includes`: protobuf files path and include dir
    /// * `message_name`: a message name that needs to correspond
    pub fn new_from_local(
        includes: &[&Path],
        inputs: &[&Path],
        message_name: &str,
    ) -> Result<Self> {
        let parsed_result = protobuf_codegen_pure::parse_and_typecheck(includes, inputs)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        let mut file_descriptor_set = FileDescriptorSet::new();
        file_descriptor_set.set_file(RepeatedField::from(parsed_result.file_descriptors));

        Ok(ProtobufParser {
            descriptors: Descriptors::from_proto(&file_descriptor_set),
            message_name: Self::normalize_message_name(message_name),
        })
    }

    /// Create a protobuf parser from a URL.
    pub fn new(location: &str, message_name: &str) -> Result<Self> {
        let url = Url::parse(location)
            .map_err(|e| InternalError(format!("failed to parse url ({}): {}", location, e)))?;

        match url.scheme() {
            "file" => {
                let path = url.to_file_path().map_err(|_| {
                    RwError::from(InternalError(format!("illegal path: {}", location)))
                })?;

                if path.is_dir() {
                    // TODO(TaoWu): Allow user to specify a directory of protos.
                    return Err(RwError::from(ProtocolError(
                        "schema file location must not be a directory".to_string(),
                    )));
                }
                Self::new_from_local(&[path.parent().unwrap()], &[path.as_path()], message_name)
            }
            scheme => Err(RwError::from(ProtocolError(format!(
                "path scheme {} is not supported",
                scheme
            )))),
        }
    }

    /// Maps the protobuf schema to relational schema.
    pub fn map_to_columns(&self) -> Result<Vec<ColumnDesc>> {
        let msg = match self.descriptors.message_by_name(self.message_name.as_str()) {
            Some(msg) => msg,
            None => {
                return Err(
                    ItemNotFound(format!("{} is not found in proto", self.message_name)).into(),
                )
            }
        };
        let mut index = 0;
        msg.fields()
            .iter()
            .map(|f| Self::pb_field_to_col_desc(f, &self.descriptors, &mut index))
            .collect::<Result<Vec<ColumnDesc>>>()
    }

    // Use pb field to create column_desc, use index to create increment column_id
    fn pb_field_to_col_desc(
        field_descriptor: &FieldDescriptor,
        descriptors: &Descriptors,
        index: &mut i32,
    ) -> Result<ColumnDesc> {
        let field_type = field_descriptor.field_type(descriptors);
        let data_type = protobuf_type_mapping(field_descriptor, descriptors)?;
        if let FieldType::Message(m) = field_type {
            let column_vec = m
                .fields()
                .iter()
                .map(|f| Self::pb_field_to_col_desc(f, descriptors, index))
                .collect::<Result<Vec<_>>>()?;
            *index += 1;
            Ok(ColumnDesc {
                column_id: *index, // need increment
                name: field_descriptor.name().to_string(),
                column_type: Some(data_type.to_protobuf()),
                field_descs: column_vec,
                type_name: m.name().to_string(),
            })
        } else {
            *index += 1;
            Ok(ColumnDesc {
                column_id: *index, // need increment
                name: field_descriptor.name().to_string(),
                column_type: Some(data_type.to_protobuf()),
                ..Default::default()
            })
        }
    }
}

macro_rules! protobuf_match_type {
    ($value:expr, $target_scalar_type:path, { $($serde_type:ident),* }, $target_type:ty) => {
        $value.and_then(|v| match v {
            $(Value::$serde_type(b) => Some(<$target_type>::from(b)), )*
            Value::Option(Some(boxed_value)) => match *boxed_value {
                $(Value::$serde_type(b) => Some(<$target_type>::from(b)), )*
                _ => None,
            },
            _ => None,
        }).map($target_scalar_type)
    };
}

/// Maps a protobuf field type to a DB column type.
fn protobuf_type_mapping(f: &FieldDescriptor, descriptors: &Descriptors) -> Result<DataType> {
    let is_repeated = f.is_repeated();
    let field_type = &f.field_type(descriptors);
    if is_repeated {
        return Err(ErrorCode::NotImplemented(
            "repeated field is not supported".to_string(),
            None.into(),
        )
        .into());
    }
    let t = match field_type {
        FieldType::Double => DataType::Float64,
        FieldType::Float => DataType::Float32,
        FieldType::Int64 | FieldType::SFixed64 | FieldType::SInt64 => DataType::Int64,
        FieldType::Int32 | FieldType::SFixed32 | FieldType::SInt32 => DataType::Int32,
        FieldType::Bool => DataType::Boolean,
        FieldType::String => DataType::Varchar,
        FieldType::Message(m) => {
            let fields = m
                .fields()
                .iter()
                .map(|f| protobuf_type_mapping(f, descriptors))
                .collect::<Result<Vec<_>>>()?;
            let field_names = m
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect_vec();
            DataType::new_struct(fields, field_names)
        }
        actual_type => {
            return Err(ErrorCode::NotImplemented(
                format!("unsupported field type: {:?}", actual_type),
                None.into(),
            )
            .into());
        }
    };
    Ok(t)
}

impl SourceParser for ProtobufParser {
    fn parse(&self, payload: &[u8], writer: SourceStreamChunkRowWriter<'_>) -> Result<WriteGuard> {
        let mut map = match self.decode(payload)? {
            Value::Map(m) => m,
            _ => return Err(RwError::from(ProtocolError("".to_string()))),
        };

        writer.insert(|column| {
            let key = Value::String(column.name.clone());

            // Use `remove` instead of `get` to take the ownership of the value
            let value = map.remove(&key);
            Ok(match column.data_type {
                DataType::Boolean => {
                    protobuf_match_type!(value, ScalarImpl::Bool, { Bool }, bool)
                }
                DataType::Int16 => {
                    protobuf_match_type!(value, ScalarImpl::Int16, { I8, I16, U8 }, i16)
                }
                DataType::Int32 => {
                    protobuf_match_type!(value, ScalarImpl::Int32, { I8, I16, I32, U8, U16 }, i32)
                }
                DataType::Int64 => {
                    protobuf_match_type!(value, ScalarImpl::Int64, { I8, I16, I32, I64, U8, U16, U32 }, i64)
                }
                DataType::Float32 => {
                    protobuf_match_type!(value, ScalarImpl::Float32, { I8, I16, U8, U16, F32 }, OrderedF32)
                }
                DataType::Float64 => {
                    protobuf_match_type!(value, ScalarImpl::Float64, { I8, I16, I32, U8, U16, U32, F32, F64}, OrderedF64)
                }
                DataType::Decimal => {
                    protobuf_match_type!(value, ScalarImpl::Decimal, { I8, I16, I32, I64, U8, U16, U32, U64}, Decimal)
                }
                DataType::Varchar => {
                    protobuf_match_type!(value, ScalarImpl::Utf8, { String }, String)
                }
                DataType::Date => {
                    value.and_then(|v| match v {
                        Value::String(b) => str_to_date(&b).ok(),
                        Value::Option(Some(boxed_value)) => match *boxed_value {
                            Value::String(b) => str_to_date(&b).ok(),
                            _ => None,
                        }
                        _ => None,
                    }).map(ScalarImpl::NaiveDate)
                }
                DataType::Timestamp =>{
                    value.and_then(|v| match v {
                        Value::String(b) => str_to_timestamp(&b).ok(),
                        Value::Option(Some(boxed_value)) => match *boxed_value {
                            Value::String(b) => str_to_timestamp(&b).ok(),
                            _ => None,
                        }
                        _ => None,
                    }).map(ScalarImpl::NaiveDateTime)
                }
                _ => unimplemented!(),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use itertools::Itertools;
    use maplit::hashmap;
    use risingwave_common::array::Op;
    use risingwave_common::catalog::ColumnId;
    use risingwave_common::error::Result;
    use risingwave_common::test_prelude::*;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_expr::vector_op::cast::str_to_date;
    use risingwave_pb::plan_common::ColumnDesc;
    use serde_value::Value;
    use tempfile::Builder;

    use crate::{ProtobufParser, SourceColumnDesc, SourceParser, SourceStreamChunkBuilder};

    static PROTO_FILE_DATA: &str = r#"
    syntax = "proto3";
    package test;
    message TestRecord {
      int32 id = 1;
      string address = 2;
      string city = 3;
      int64 zipcode = 4;
      float rate = 5;
      string date = 6;
    }"#;

    // Id:      123,
    // Address: "test address",
    // City:    "test city",
    // Zipcode: 456,
    // Rate:    1.2345,
    // Date:    "2021-01-01"
    static PRE_GEN_PROTO_DATA: &[u8] = b"\x08\x7b\x12\x0c\x74\x65\x73\x74\x20\x61\x64\x64\x72\x65\x73\x73\x1a\x09\x74\x65\x73\x74\x20\x63\x69\x74\x79\x20\xc8\x03\x2d\x19\x04\x9e\x3f\x32\x0a\x32\x30\x32\x31\x2d\x30\x31\x2d\x30\x31";

    fn create_parser(proto_data: &str) -> Result<ProtobufParser> {
        let temp_file = Builder::new()
            .prefix("temp")
            .suffix(".proto")
            .rand_bytes(5)
            .tempfile()
            .unwrap();

        let path = temp_file.path().to_str().unwrap();
        let mut file = temp_file.as_file();
        file.write_all(proto_data.as_ref())
            .expect("writing binary to test file");

        ProtobufParser::new(format!("file://{}", path).as_str(), ".test.TestRecord")
    }

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
        create_parser(PROTO_FILE_DATA).unwrap();
    }

    #[test]
    fn test_parser_decode() {
        let parser = create_parser(PROTO_FILE_DATA).unwrap();

        let value = parser.decode(PRE_GEN_PROTO_DATA).unwrap();

        let map = match value {
            Value::Map(m) => m,
            _ => panic!("value should be map"),
        };

        let hash = hashmap!(
            "id" => Value::Option(Some(Box::new(Value::I32(123)))),
            "address" => Value::Option(Some(Box::new(Value::String("test address".to_string())))),
            "city" => Value::Option(Some(Box::new(Value::String("test city".to_string())))),
            "zipcode" => Value::Option(Some(Box::new(Value::I64(456)))),
            "rate" => Value::Option(Some(Box::new(Value::F32(1.2345)))),
            "date" => Value::Option(Some(Box::new(Value::String("2021-01-01".to_string()))))
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
        let parser = create_parser(PROTO_FILE_DATA).unwrap();
        let descs = vec![
            SourceColumnDesc {
                name: "id".to_string(),
                data_type: DataType::Int32,
                column_id: ColumnId::from(0),
                skip_parse: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "address".to_string(),
                data_type: DataType::Varchar,
                column_id: ColumnId::from(1),
                skip_parse: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "city".to_string(),
                data_type: DataType::Varchar,
                column_id: ColumnId::from(2),
                skip_parse: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "zipcode".to_string(),
                data_type: DataType::Int64,
                column_id: ColumnId::from(3),
                skip_parse: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "rate".to_string(),
                data_type: DataType::Float32,
                column_id: ColumnId::from(4),
                skip_parse: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "date".to_string(),
                data_type: DataType::Date,
                column_id: ColumnId::from(5),
                skip_parse: false,
                fields: vec![],
            },
        ];

        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 1);
        {
            let writer = builder.row_writer();
            parser.parse(PRE_GEN_PROTO_DATA, writer).unwrap();
        }
        let chunk = builder.finish().unwrap();
        let (op, row) = chunk.rows().next().unwrap();
        assert_eq!(op, Op::Insert);
        let row = row.to_owned_row();
        assert!(row[0].eq(&Some(ScalarImpl::Int32(123))));
        assert!(row[1].eq(&Some(ScalarImpl::Utf8("test address".to_string()))));
        assert!(row[2].eq(&Some(ScalarImpl::Utf8("test city".to_string()))));
        assert!(row[3].eq(&Some(ScalarImpl::Int64(456))));
        assert!(row[4].eq(&Some(ScalarImpl::Float32(1.2345.into()))));
        assert!(row[5].eq(&Some(ScalarImpl::NaiveDate(
            str_to_date("2021-01-01").unwrap()
        ))))
    }

    static PROTO_NESTED_FILE_DATA: &str = r#"
    syntax = "proto3";
    package test;
    message TestRecord {
      int32 id = 1;
      Country country = 3;
      int64 zipcode = 4;
      float rate = 5;
    }
    message Country {
      string address = 1;
      City city = 2;
      string zipcode = 3;
    }
    message City {
      string address = 1;
      string zipcode = 2;
    }"#;

    #[test]
    fn test_map_to_columns() {
        use risingwave_common::types::*;

        let parser = create_parser(PROTO_NESTED_FILE_DATA).unwrap();
        let columns = parser.map_to_columns().unwrap();
        let city = vec![
            ColumnDesc::new_atomic(DataType::Varchar.to_protobuf(), "address", 3),
            ColumnDesc::new_atomic(DataType::Varchar.to_protobuf(), "zipcode", 4),
        ];
        let country = vec![
            ColumnDesc::new_atomic(DataType::Varchar.to_protobuf(), "address", 2),
            ColumnDesc::new_struct("city", 5, ".test.City", city),
            ColumnDesc::new_atomic(DataType::Varchar.to_protobuf(), "zipcode", 6),
        ];
        assert_eq!(
            columns,
            vec![
                ColumnDesc::new_atomic(DataType::Int32.to_protobuf(), "id", 1),
                ColumnDesc::new_struct("country", 7, ".test.Country", country),
                ColumnDesc::new_atomic(DataType::Int64.to_protobuf(), "zipcode", 8),
                ColumnDesc::new_atomic(DataType::Float32.to_protobuf(), "rate", 9),
            ]
        );
    }

    #[test]
    fn test_struct_field_names() {
        use risingwave_common::catalog::ColumnDesc as RwColumnDesc;
        use risingwave_common::types::*;

        let parser = create_parser(PROTO_NESTED_FILE_DATA).unwrap();
        let columns = parser.map_to_columns().unwrap();
        let columns = columns.iter().map(RwColumnDesc::from).collect_vec();
        if let DataType::Struct(t) = columns[1].data_type.clone() {
            // country
            if let DataType::Struct(tf1) = t.fields[1].clone() {
                // city
                assert_eq!(
                    tf1.field_names.to_vec(),
                    vec!["address".to_string(), "zipcode".to_string()]
                );
            } else {
                unreachable!()
            }
            assert_eq!(
                t.field_names.to_vec(),
                vec![
                    "address".to_string(),
                    "city".to_string(),
                    "zipcode".to_string()
                ]
            );
        } else {
            unreachable!()
        }
    }
}
