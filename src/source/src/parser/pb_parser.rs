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

use prost_reflect::{
    Cardinality, DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, Value,
};
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::error::ErrorCode::{InternalError, NotImplemented, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, Decimal, OrderedF32, OrderedF64, ScalarImpl};
use risingwave_expr::vector_op::cast::{str_to_date, str_to_timestamp};
use risingwave_pb::plan_common::ColumnDesc;
use url::Url;

use crate::{dtype_to_source_column_desc, SourceColumnDesc, SourceParser, WriteGuard};

#[derive(Debug, Clone)]
struct ProtobufParser {
    pub message_descriptor: MessageDescriptor,
    message_name: String,
}

impl ProtobufParser {
    pub fn new(location: &str, message_name: &str) -> Result<Self> {
        let url = Url::parse(location)
            .map_err(|e| InternalError(format!("failed to parse url ({}): {}", location, e)))?;

        let schema_bytes = match url.scheme() {
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
                Self::local_read_to_bytes(&path)
            }
            "s3" => {
                // TODO(tabVersion): Support load from s3.
                return Err(RwError::from(ProtocolError(
                    "s3 schema location is not supported".to_string(),
                )));
            }
            scheme => Err(RwError::from(ProtocolError(format!(
                "path scheme {} is not supported",
                scheme
            )))),
        }?;

        let pool = DescriptorPool::decode(&schema_bytes[..]).map_err(|e| {
            ProtocolError(format!(
                "cannot build descriptor pool from schema: {}, error: {}",
                location, e
            ))
        })?;
        let message_descriptor = pool.get_message_by_name(message_name).ok_or_else(|| {
            ProtocolError(format!(
                "cannot find message {} in schema: {}",
                message_name, location,
            ))
        })?;
        Ok(Self {
            message_descriptor,
            message_name: Self::normalize_message_name(message_name),
        })
    }

    /// read binary schema from a local file
    fn local_read_to_bytes(path: &Path) -> Result<Vec<u8>> {
        std::fs::read(path).map_err(|e| {
            RwError::from(InternalError(format!(
                "failed to read file {}: {}",
                path.display(),
                e
            )))
        })
    }

    /// Maps the protobuf schema to relational schema.
    pub fn map_to_columns(&self) -> Result<Vec<ColumnDesc>> {
        let mut columns = Vec::with_capacity(self.message_descriptor.fields().len());
        let mut index = 0;
        for field in self.message_descriptor.fields() {
            columns.push(Self::pb_field_to_col_desc(&field, &mut index)?);
        }

        Ok(columns)
    }

    /// Maps a protobuf field to a RW column.
    fn pb_field_to_col_desc(
        field_descriptor: &FieldDescriptor,
        index: &mut i32,
    ) -> Result<ColumnDesc> {
        let field_type = protobuf_type_mapping(field_descriptor)?;
        if let Kind::Message(m) = field_descriptor.kind() {
            let column_vec = m
                .fields()
                .map(|f| Self::pb_field_to_col_desc(&f, index))
                .collect::<Result<Vec<_>>>()?;
            *index += 1;
            Ok(ColumnDesc {
                column_id: *index,
                name: field_descriptor.name().to_string(),
                column_type: Some(field_type.to_protobuf()),
                field_descs: column_vec,
                type_name: m.name().to_string(),
            })
        } else {
            *index += 1;
            Ok(ColumnDesc {
                column_id: *index,
                name: field_descriptor.name().to_string(),
                column_type: Some(field_type.to_protobuf()),
                ..Default::default()
            })
        }
    }

    /// Generate message name
    fn normalize_message_name(message_name: &str) -> String {
        if message_name.is_empty() || !message_name.contains('.') || message_name.starts_with('.') {
            message_name.to_string()
        } else {
            format!(".{}", message_name)
        }
    }
}

// All field in pb3 is optional, and fields equal to default value will not be serialized,
// so parser generally uses the default value of the corresponding type directly
// serde-protobuf will parse that as None, which violates the semantics of pb3
// so convert None to the default value
// TODO: migrate to prost based parser, which support higher versions of protobuf
// so user can used optional keyword to distinguish between default value and null
macro_rules! protobuf_match_type {
    ($value:expr, $target_scalar_type:path, { $($serde_type:ident),* }, $target_type:ty) => {
        $value.and_then(|v| match v {
            $( Value::$serde_type(b) => Some(<$target_type>::from(b.to_owned())), )*
            _ => None,
        })
        .or_else(|| Some(<$target_type>::default()))
        .map($target_scalar_type)
    };
}

fn from_protobuf_value(column: &SourceColumnDesc, value: Option<Value>) -> Option<ScalarImpl> {
    match &column.data_type {
        DataType::Boolean => {
            protobuf_match_type!(value, ScalarImpl::Bool, { Bool }, bool)
        }
        DataType::Int32 => {
            protobuf_match_type!(value, ScalarImpl::Int32, { I32 }, i32)
        }
        DataType::Int64 => {
            protobuf_match_type!(value, ScalarImpl::Int64, { I32, I64, U32 }, i64)
        }
        DataType::Float32 => {
            protobuf_match_type!(value, ScalarImpl::Float32, { F32 }, OrderedF32)
        }
        DataType::Float64 => {
            protobuf_match_type!(value, ScalarImpl::Float64, { I32, U32, F32, F64}, OrderedF64)
        }
        DataType::Decimal => {
            protobuf_match_type!(value, ScalarImpl::Decimal, { I32, I64, U32, U64}, Decimal)
        }
        DataType::Varchar => {
            protobuf_match_type!(value, ScalarImpl::Utf8, { String }, String)
        }

        // TODO: consider if these type is available in protobuf
        DataType::Date => value
            .and_then(|v| match v {
                Value::String(b) => str_to_date(&b).ok(),
                _ => None,
            })
            .map(ScalarImpl::NaiveDate),
        DataType::Timestamp => value
            .and_then(|v| match v {
                Value::String(b) => str_to_timestamp(&b).ok(),
                _ => None,
            })
            .map(ScalarImpl::NaiveDateTime),
        DataType::Struct(_) => {
            let struct_map = value.and_then(|v| match v {
                Value::Map(map) => Some(map),
                _ => None,
            });

            struct_map.map(|mut struct_map| {
                let field_values = column
                    .fields
                    .iter()
                    .map(|field_desc| {
                        let filed_value =
                        // TODO: check if it in this format
                            struct_map.remove(&prost_reflect::MapKey::String(field_desc.name.to_owned()));
                        from_protobuf_value(&field_desc.into(), filed_value)
                    })
                    .collect();
                ScalarImpl::Struct(StructValue::new(field_values))
            })
        }
        DataType::List {
            datatype: item_type,
        } => {
            let value_list = value.and_then(|v| match v {
                Value::List(l) => Some(l),
                _ => None,
            });
            let item_schema = dtype_to_source_column_desc(item_type);
            value_list.map(|values| {
                let values = values
                    .into_iter()
                    .map(|value| from_protobuf_value(&item_schema, Some(value)))
                    .collect::<Vec<Option<ScalarImpl>>>();
                ScalarImpl::List(ListValue::new(values))
            })
        }
        _ => unimplemented!(),
    }
}

/// Maps protobuf type to RW type.
fn protobuf_type_mapping(field_descriptor: &FieldDescriptor) -> Result<DataType> {
    let field_type = field_descriptor.kind();
    let mut t = match field_type {
        Kind::Bool => DataType::Boolean,
        Kind::Double => DataType::Float64,
        Kind::Float => DataType::Float32,
        Kind::Int32 | Kind::Sfixed32 | Kind::Fixed32 => DataType::Int32,
        Kind::Int64 | Kind::Sfixed64 | Kind::Fixed64 => DataType::Int64,
        Kind::String => DataType::Varchar,
        Kind::Message(m) => {
            let fields = m
                .fields()
                .map(|f| protobuf_type_mapping(&f))
                .collect::<Result<Vec<_>>>()?;
            let field_names = m.fields().map(|f| f.name().to_string()).collect::<Vec<_>>();
            DataType::new_struct(fields, field_names)
        }
        // TODO(tabVersion): lack enum support
        actual_type => {
            return Err(NotImplemented(
                format!("unsupported field type: {:?}", actual_type),
                None.into(),
            )
            .into());
        }
    };
    if field_descriptor.cardinality() == Cardinality::Repeated {
        t = DataType::List {
            datatype: Box::new(t),
        }
    }
    Ok(t)
}

impl SourceParser for ProtobufParser {
    fn parse(
        &self,
        payload: &[u8],
        writer: crate::SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let message = DynamicMessage::decode(self.message_descriptor.clone(), payload)
            .map_err(|e| ProtocolError(format!("parse message failed: {}", e)))?;

        writer.insert(|column_desc| {
            let dyn_message = message.clone();
            let value = dyn_message
                .get_field_by_name(column_desc.name.as_str())
                .map(|f| f.into_owned());
            Ok(from_protobuf_value(column_desc, value))
        })
    }
}

mod test {
    use itertools::Itertools;

    use crate::SourceStreamChunkBuilder;

    use super::*;

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
    fn test_new_proto_parser() -> Result<()> {
        let parser = ProtobufParser::new(
            "file:///Users/tabversion/Desktop/risingwave/src/source/src/test_data/SCHEMA",
            "test.User",
        )?;
        println!("{:#?}", parser.map_to_columns());
        Ok(())
    }

    // Id:      123,
    // Address: "test address",
    // City:    "test city",
    // Zipcode: 456,
    // Rate:    1.2345,
    // Date:    "2021-01-01"
    static PRE_GEN_PROTO_DATA: &[u8] = b"\x08\x7b\x12\x0c\x74\x65\x73\x74\x20\x61\x64\x64\x72\x65\x73\x73\x1a\x09\x74\x65\x73\x74\x20\x63\x69\x74\x79\x20\xc8\x03\x2d\x19\x04\x9e\x3f\x32\x0a\x32\x30\x32\x31\x2d\x30\x31\x2d\x30\x31";


    #[test]
    fn test_simple_schema() -> Result<()> {
        let location = "file:///Users/tabversion/Desktop/risingwave/src/source/src/test_data/simple_schema";
        let message_name = "test.TestRecord";

        let parser = ProtobufParser::new(
            location.clone(),
            message_name.clone(),
        )?;

        let column_desc = parser.map_to_columns().unwrap();
        let mut builder = SourceStreamChunkBuilder::with_capacity(column_desc.iter().map().collect_vec(), 1);

        let value = DynamicMessage::decode(parser.message_descriptor.clone(), PRE_GEN_PROTO_DATA).unwrap();
        println!("{:?}", value);

        Ok(())
    }
}
