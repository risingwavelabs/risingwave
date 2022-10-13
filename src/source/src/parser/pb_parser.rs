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
use prost_reflect::{
    Cardinality, DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MessageDescriptor,
    ReflectMessage, Value,
};
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::error::ErrorCode::{InternalError, NotImplemented, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, Datum, Decimal, OrderedF32, OrderedF64, ScalarImpl};
use risingwave_pb::plan_common::ColumnDesc;
use url::Url;

use crate::{SourceParser, WriteGuard};

#[derive(Debug, Clone)]
pub struct ProtobufParser {
    pub message_descriptor: MessageDescriptor,
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
                "cannot find message {} in schema: {}.\n poll is {:?}",
                message_name, location, pool
            ))
        })?;
        Ok(Self { message_descriptor })
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
            let field_descs = if let DataType::List { .. } = field_type {
                vec![]
            } else {
                m.fields()
                    .map(|f| Self::pb_field_to_col_desc(&f, index))
                    .collect::<Result<Vec<_>>>()?
            };
            *index += 1;
            Ok(ColumnDesc {
                column_id: *index,
                name: field_descriptor.name().to_string(),
                column_type: Some(field_type.to_protobuf()),
                field_descs,
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
}

fn from_protobuf_value(field_desc: &FieldDescriptor, value: &Value) -> Result<Datum> {
    let v = match value {
        Value::Bool(v) => ScalarImpl::Bool(*v),
        Value::I32(i) => ScalarImpl::Int32(*i),
        Value::U32(i) => ScalarImpl::Int64(*i as i64),
        Value::I64(i) => ScalarImpl::Int64(*i),
        Value::U64(i) => ScalarImpl::Decimal(Decimal::from(*i)),
        Value::F32(f) => ScalarImpl::Float32(OrderedF32::from(*f)),
        Value::F64(f) => ScalarImpl::Float64(OrderedF64::from(*f)),
        Value::String(s) => ScalarImpl::Utf8(s.to_owned()),
        Value::EnumNumber(idx) => {
            let kind = field_desc.kind();
            let enum_desc = kind.as_enum().ok_or_else(|| {
                let err_msg = format!("protobuf parse error.not a enum desc {:?}", field_desc);
                RwError::from(InternalError(err_msg))
            })?;
            let enum_symbol = enum_desc.get_value(*idx).ok_or_else(|| {
                let err_msg = format!(
                    "protobuf parse error.unknown enum index {} of enum {:?}",
                    idx, enum_desc
                );
                RwError::from(InternalError(err_msg))
            })?;
            ScalarImpl::Utf8(enum_symbol.name().to_owned())
        }
        Value::Message(dyn_msg) => {
            let mut rw_values = Vec::with_capacity(dyn_msg.descriptor().fields().len());
            // fields is a btree map in descriptor
            // so it's order is the same as datatype
            for field_desc in dyn_msg.descriptor().fields() {
                tracing::info!("field {}", field_desc.name());
                // missing field
                if !dyn_msg.has_field(&field_desc)
                    && field_desc.cardinality() == Cardinality::Required
                {
                    let err_msg = format!(
                        "protobuf parse error.missing required field {:?}",
                        field_desc
                    );
                    return Err(RwError::from(InternalError(err_msg)));
                }
                // use default value if dyn_msg doesn't has this field
                let value = dyn_msg.get_field(&field_desc);
                rw_values.push(from_protobuf_value(&field_desc, &value)?);
            }
            ScalarImpl::Struct(StructValue::new(rw_values))
        }
        Value::List(values) => {
            let rw_values = values
                .iter()
                .map(|value| from_protobuf_value(field_desc, value))
                .collect::<Result<Vec<_>>>()?;
            tracing::info!("list {}, value {:?}", field_desc.name(), rw_values);
            ScalarImpl::List(ListValue::new(rw_values))
        }
        _ => {
            let err_msg = format!(
                "protobuf parse error.unsupported type {:?}, value {:?}",
                field_desc, value
            );
            return Err(RwError::from(InternalError(err_msg)));
        }
    };
    Ok(Some(v))
}

/// Maps protobuf type to RW type.
fn protobuf_type_mapping(field_descriptor: &FieldDescriptor) -> Result<DataType> {
    let field_type = field_descriptor.kind();
    let mut t = match field_type {
        Kind::Bool => DataType::Boolean,
        Kind::Double => DataType::Float64,
        Kind::Float => DataType::Float32,
        Kind::Int32 | Kind::Sfixed32 | Kind::Fixed32 => DataType::Int32,
        Kind::Int64 | Kind::Sfixed64 | Kind::Fixed64 | Kind::Uint32 => DataType::Int64,
        Kind::Uint64 => DataType::Decimal,
        Kind::String => DataType::Varchar,
        Kind::Message(m) => {
            let fields = m
                .fields()
                .map(|f| protobuf_type_mapping(&f))
                .collect::<Result<Vec<_>>>()?;
            let field_names = m.fields().map(|f| f.name().to_string()).collect_vec();
            DataType::new_struct(fields, field_names)
        }
        Kind::Enum(_) => DataType::Varchar,
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
            let field_desc = message
                .descriptor()
                .get_field_by_name(&column_desc.name)
                .ok_or_else(|| {
                    let err_msg = format!("protobuf schema don't have field {}", column_desc.name);
                    tracing::error!(err_msg);
                    RwError::from(InternalError(err_msg))
                })?;
            let value = message.get_field(&field_desc);
            from_protobuf_value(&field_desc, &value).map_err(|e| {
                tracing::error!(
                    "failed to process value ({}): {}",
                    String::from_utf8_lossy(payload),
                    e
                );
                e
            })
        })
    }
}

#[cfg(test)]
mod test {

    use std::path::PathBuf;

    use risingwave_pb::data::data_type::TypeName as ProstTypeName;

    use super::*;

    fn schema_dir() -> String {
        let dir = PathBuf::from("src/test_data");
        format!(
            "file://{}",
            std::fs::canonicalize(&dir).unwrap().to_str().unwrap()
        )
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
        let location = schema_dir() + "/simple-schema";
        let message_name = "test.TestRecord";
        println!("location: {}", location);
        let parser = ProtobufParser::new(&location, message_name)?;
        let value = DynamicMessage::decode(parser.message_descriptor, PRE_GEN_PROTO_DATA).unwrap();

        assert_eq!(
            value.get_field_by_name("id").unwrap().into_owned(),
            Value::I32(123)
        );
        assert_eq!(
            value.get_field_by_name("address").unwrap().into_owned(),
            Value::String("test address".to_string())
        );
        assert_eq!(
            value.get_field_by_name("city").unwrap().into_owned(),
            Value::String("test city".to_string())
        );
        assert_eq!(
            value.get_field_by_name("zipcode").unwrap().into_owned(),
            Value::I64(456)
        );
        assert_eq!(
            value.get_field_by_name("rate").unwrap().into_owned(),
            Value::F32(1.2345)
        );
        assert_eq!(
            value.get_field_by_name("date").unwrap().into_owned(),
            Value::String("2021-01-01".to_string())
        );

        Ok(())
    }

    #[test]
    fn test_complex_schema() -> Result<()> {
        let location = schema_dir() + "/complex-schema";
        let message_name = "test.User";

        let parser = ProtobufParser::new(&location, message_name)?;
        let columns = parser.map_to_columns().unwrap();

        assert_eq!(columns[0].name, "id".to_string());
        assert_eq!(columns[1].name, "code".to_string());
        assert_eq!(columns[2].name, "timestamp".to_string());

        let data_type = columns[3].column_type.as_ref().unwrap();
        assert_eq!(data_type.get_type_name().unwrap(), ProstTypeName::List);
        let inner_field_type = data_type.field_type.clone();
        assert_eq!(
            inner_field_type[0].get_type_name().unwrap(),
            ProstTypeName::Struct
        );
        let struct_inner = inner_field_type[0].field_type.clone();
        assert_eq!(
            struct_inner[0].get_type_name().unwrap(),
            ProstTypeName::Int32
        );
        assert_eq!(
            struct_inner[1].get_type_name().unwrap(),
            ProstTypeName::Int32
        );
        assert_eq!(
            struct_inner[2].get_type_name().unwrap(),
            ProstTypeName::Varchar
        );

        assert_eq!(columns[4].name, "contacts".to_string());
        let inner_field_type = columns[4].column_type.as_ref().unwrap().field_type.clone();
        assert_eq!(
            inner_field_type[0].get_type_name().unwrap(),
            ProstTypeName::List
        );
        assert_eq!(
            inner_field_type[1].get_type_name().unwrap(),
            ProstTypeName::List
        );
        Ok(())
    }
}
