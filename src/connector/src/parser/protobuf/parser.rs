// Copyright 2024 RisingWave Labs
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

use anyhow::Context;
use itertools::Itertools;
use prost_reflect::{
    Cardinality, DescriptorPool, DynamicMessage, FieldDescriptor, FileDescriptor, Kind,
    MessageDescriptor, ReflectMessage, Value,
};
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::types::{
    DataType, DatumCow, Decimal, JsonbVal, ScalarImpl, ToOwnedDatum, F32, F64,
};
use risingwave_common::{bail, try_match_expand};
use risingwave_pb::plan_common::{AdditionalColumn, ColumnDesc, ColumnDescVersion};
use thiserror::Error;
use thiserror_ext::Macro;

use crate::error::ConnectorResult;
use crate::parser::unified::protobuf::ProtobufAccess;
use crate::parser::unified::{uncategorized, AccessError, AccessImpl, AccessResult};
use crate::parser::util::bytes_from_url;
use crate::parser::{AccessBuilder, EncodingProperties};
use crate::schema::schema_registry::{extract_schema_id, handle_sr_list, Client, WireFormatError};
use crate::schema::SchemaLoader;

#[derive(Debug)]
pub struct ProtobufAccessBuilder {
    confluent_wire_type: bool,
    message_descriptor: MessageDescriptor,
}

impl AccessBuilder for ProtobufAccessBuilder {
    #[allow(clippy::unused_async)]
    async fn generate_accessor(&mut self, payload: Vec<u8>) -> ConnectorResult<AccessImpl<'_>> {
        let payload = if self.confluent_wire_type {
            resolve_pb_header(&payload)?
        } else {
            &payload
        };

        let message = DynamicMessage::decode(self.message_descriptor.clone(), payload)
            .context("failed to parse message")?;

        Ok(AccessImpl::Protobuf(ProtobufAccess::new(message)))
    }
}

impl ProtobufAccessBuilder {
    pub fn new(config: ProtobufParserConfig) -> ConnectorResult<Self> {
        let ProtobufParserConfig {
            confluent_wire_type,
            message_descriptor,
        } = config;

        Ok(Self {
            confluent_wire_type,
            message_descriptor,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ProtobufParserConfig {
    confluent_wire_type: bool,
    pub(crate) message_descriptor: MessageDescriptor,
}

impl ProtobufParserConfig {
    pub async fn new(encoding_properties: EncodingProperties) -> ConnectorResult<Self> {
        let protobuf_config = try_match_expand!(encoding_properties, EncodingProperties::Protobuf)?;
        let location = &protobuf_config.row_schema_location;
        let message_name = &protobuf_config.message_name;
        let url = handle_sr_list(location.as_str())?;

        if protobuf_config.key_message_name.is_some() {
            // https://docs.confluent.io/platform/7.5/control-center/topics/schema.html#c3-schemas-best-practices-key-value-pairs
            bail!("protobuf key is not supported");
        }
        let pool = if protobuf_config.use_schema_registry {
            let client = Client::new(url, &protobuf_config.client_config)?;
            let loader = SchemaLoader {
                client,
                name_strategy: protobuf_config.name_strategy,
                topic: protobuf_config.topic,
                key_record_name: None,
                val_record_name: Some(message_name.clone()),
            };
            let (_schema_id, root_file_descriptor) = loader
                .load_val_schema::<FileDescriptor>()
                .await
                .context("load schema failed")?;
            root_file_descriptor.parent_pool().clone()
        } else {
            let url = url.first().unwrap();
            let schema_bytes = bytes_from_url(url, protobuf_config.aws_auth_props.as_ref()).await?;
            DescriptorPool::decode(schema_bytes.as_slice())
                .with_context(|| format!("cannot build descriptor pool from schema `{location}`"))?
        };

        let message_descriptor = pool.get_message_by_name(message_name).with_context(|| {
            format!(
                "cannot find message `{}` in schema `{}`",
                message_name, location,
            )
        })?;

        Ok(Self {
            message_descriptor,
            confluent_wire_type: protobuf_config.use_schema_registry,
        })
    }

    /// Maps the protobuf schema to relational schema.
    pub fn map_to_columns(&self) -> ConnectorResult<Vec<ColumnDesc>> {
        let mut columns = Vec::with_capacity(self.message_descriptor.fields().len());
        let mut index = 0;
        let mut parse_trace: Vec<String> = vec![];
        for field in self.message_descriptor.fields() {
            columns.push(Self::pb_field_to_col_desc(
                &field,
                &mut index,
                &mut parse_trace,
            )?);
        }

        Ok(columns)
    }

    /// Maps a protobuf field to a RW column.
    fn pb_field_to_col_desc(
        field_descriptor: &FieldDescriptor,
        index: &mut i32,
        parse_trace: &mut Vec<String>,
    ) -> ConnectorResult<ColumnDesc> {
        let field_type = protobuf_type_mapping(field_descriptor, parse_trace)
            .context("failed to map protobuf type")?;
        if let Kind::Message(m) = field_descriptor.kind() {
            let field_descs = if let DataType::List { .. } = field_type {
                vec![]
            } else {
                m.fields()
                    .map(|f| Self::pb_field_to_col_desc(&f, index, parse_trace))
                    .try_collect()?
            };
            *index += 1;
            Ok(ColumnDesc {
                column_id: *index,
                name: field_descriptor.name().to_string(),
                column_type: Some(field_type.to_protobuf()),
                field_descs,
                type_name: m.full_name().to_string(),
                generated_or_default_column: None,
                description: None,
                additional_column_type: 0, // deprecated
                additional_column: Some(AdditionalColumn { column_type: None }),
                version: ColumnDescVersion::Pr13707 as i32,
            })
        } else {
            *index += 1;
            Ok(ColumnDesc {
                column_id: *index,
                name: field_descriptor.name().to_string(),
                column_type: Some(field_type.to_protobuf()),
                additional_column: Some(AdditionalColumn { column_type: None }),
                version: ColumnDescVersion::Pr13707 as i32,
                ..Default::default()
            })
        }
    }
}

#[derive(Error, Debug, Macro)]
#[error("{0}")]
struct ProtobufTypeError(#[message] String);

fn detect_loop_and_push(
    trace: &mut Vec<String>,
    fd: &FieldDescriptor,
) -> std::result::Result<(), ProtobufTypeError> {
    let identifier = format!("{}({})", fd.name(), fd.full_name());
    if trace.iter().any(|s| s == identifier.as_str()) {
        bail_protobuf_type_error!(
            "circular reference detected: {}, conflict with {}, kind {:?}",
            trace.iter().format("->"),
            identifier,
            fd.kind(),
        );
    }
    trace.push(identifier);
    Ok(())
}

pub fn from_protobuf_value<'a>(
    field_desc: &FieldDescriptor,
    value: &'a Value,
    type_expected: &DataType,
) -> AccessResult<DatumCow<'a>> {
    let kind = field_desc.kind();

    macro_rules! borrowed {
        ($v:expr) => {
            return Ok(DatumCow::Borrowed(Some($v.into())))
        };
    }

    let v: ScalarImpl = match value {
        Value::Bool(v) => ScalarImpl::Bool(*v),
        Value::I32(i) => ScalarImpl::Int32(*i),
        Value::U32(i) => ScalarImpl::Int64(*i as i64),
        Value::I64(i) => ScalarImpl::Int64(*i),
        Value::U64(i) => ScalarImpl::Decimal(Decimal::from(*i)),
        Value::F32(f) => ScalarImpl::Float32(F32::from(*f)),
        Value::F64(f) => ScalarImpl::Float64(F64::from(*f)),
        Value::String(s) => borrowed!(s.as_str()),
        Value::EnumNumber(idx) => {
            let enum_desc = kind.as_enum().ok_or_else(|| AccessError::TypeError {
                expected: "enum".to_owned(),
                got: format!("{kind:?}"),
                value: value.to_string(),
            })?;
            let enum_symbol = enum_desc.get_value(*idx).ok_or_else(|| {
                uncategorized!("unknown enum index {} of enum {:?}", idx, enum_desc)
            })?;
            ScalarImpl::Utf8(enum_symbol.name().into())
        }
        Value::Message(dyn_msg) => {
            if dyn_msg.descriptor().full_name() == "google.protobuf.Any" {
                ScalarImpl::Jsonb(JsonbVal::from(
                    serde_json::to_value(dyn_msg).map_err(AccessError::ProtobufAnyToJson)?,
                ))
            } else {
                let desc = dyn_msg.descriptor();
                let DataType::Struct(st) = type_expected else {
                    return Err(AccessError::TypeError {
                        expected: type_expected.to_string(),
                        got: desc.full_name().to_string(),
                        value: value.to_string(), // Protobuf TEXT
                    });
                };

                let mut rw_values = Vec::with_capacity(st.len());
                for (name, expected_field_type) in st.iter() {
                    let Some(field_desc) = desc.get_field_by_name(name) else {
                        // Field deleted in protobuf. Fallback to SQL NULL (of proper RW type).
                        rw_values.push(None);
                        continue;
                    };
                    let value = dyn_msg.get_field(&field_desc);
                    rw_values.push(
                        from_protobuf_value(&field_desc, &value, expected_field_type)?
                            .to_owned_datum(),
                    );
                }
                ScalarImpl::Struct(StructValue::new(rw_values))
            }
        }
        Value::List(values) => {
            let DataType::List(element_type) = type_expected else {
                return Err(AccessError::TypeError {
                    expected: type_expected.to_string(),
                    got: format!("repeated {:?}", kind),
                    value: value.to_string(), // Protobuf TEXT
                });
            };
            let mut builder = element_type.create_array_builder(values.len());
            for value in values {
                builder.append(from_protobuf_value(field_desc, value, element_type)?);
            }
            ScalarImpl::List(ListValue::new(builder.finish()))
        }
        Value::Bytes(value) => borrowed!(&**value),
        _ => {
            return Err(AccessError::UnsupportedType {
                ty: format!("{kind:?}"),
            });
        }
    };
    Ok(Some(v).into())
}

/// Maps protobuf type to RW type.
fn protobuf_type_mapping(
    field_descriptor: &FieldDescriptor,
    parse_trace: &mut Vec<String>,
) -> std::result::Result<DataType, ProtobufTypeError> {
    detect_loop_and_push(parse_trace, field_descriptor)?;
    let field_type = field_descriptor.kind();
    let mut t = match field_type {
        Kind::Bool => DataType::Boolean,
        Kind::Double => DataType::Float64,
        Kind::Float => DataType::Float32,
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => DataType::Int32,
        // Fixed32 represents [0, 2^32 - 1]. It's equal to u32.
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 | Kind::Uint32 | Kind::Fixed32 => {
            DataType::Int64
        }
        Kind::Uint64 | Kind::Fixed64 => DataType::Decimal,
        Kind::String => DataType::Varchar,
        Kind::Message(m) => match m.full_name() {
            // Well-Known Types are identified by their full name
            "google.protobuf.Any" => DataType::Jsonb,
            _ => {
                let fields = m
                    .fields()
                    .map(|f| protobuf_type_mapping(&f, parse_trace))
                    .try_collect()?;
                let field_names = m.fields().map(|f| f.name().to_string()).collect_vec();
                DataType::new_struct(fields, field_names)
            }
        },
        Kind::Enum(_) => DataType::Varchar,
        Kind::Bytes => DataType::Bytea,
    };
    if field_descriptor.is_map() {
        bail_protobuf_type_error!(
            "protobuf map type (on field `{}`) is not supported",
            field_descriptor.full_name()
        );
    }
    if field_descriptor.cardinality() == Cardinality::Repeated {
        t = DataType::List(Box::new(t))
    }
    _ = parse_trace.pop();
    Ok(t)
}

/// A port from the implementation of confluent's Varint Zig-zag deserialization.
/// See `ReadVarint` in <https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/utils/ByteUtils.java>
fn decode_varint_zigzag(buffer: &[u8]) -> ConnectorResult<(i32, usize)> {
    // We expect the decoded number to be 4 bytes.
    let mut value = 0u32;
    let mut shift = 0;
    let mut len = 0usize;

    for &byte in buffer {
        len += 1;
        // The Varint encoding is limited to 5 bytes.
        if len > 5 {
            break;
        }
        // The byte is cast to u32 to avoid shifting overflow.
        let byte_ext = byte as u32;
        // In Varint encoding, the lowest 7 bits are used to represent number,
        // while the highest zero bit indicates the end of the number with Varint encoding.
        value |= (byte_ext & 0x7F) << shift;
        if byte_ext & 0x80 == 0 {
            return Ok((((value >> 1) as i32) ^ -((value & 1) as i32), len));
        }

        shift += 7;
    }

    Err(WireFormatError::ParseMessageIndexes.into())
}

/// Reference: <https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>
/// Wire format for Confluent pb header is:
/// | 0          | 1-4        | 5-x             | x+1-end
/// | magic-byte | schema-id  | message-indexes | protobuf-payload
pub(crate) fn resolve_pb_header(payload: &[u8]) -> ConnectorResult<&[u8]> {
    // there's a message index array at the front of payload
    // if it is the first message in proto def, the array is just and `0`
    let (_, remained) = extract_schema_id(payload)?;
    // The message indexes are encoded as int using variable-length zig-zag encoding,
    // prefixed by the length of the array.
    // Note that if the first byte is 0, it is equivalent to (1, 0) as an optimization.
    match remained.first() {
        Some(0) => Ok(&remained[1..]),
        Some(_) => {
            let (index_len, mut offset) = decode_varint_zigzag(remained)?;
            for _ in 0..index_len {
                offset += decode_varint_zigzag(&remained[offset..])?.1;
            }
            Ok(&remained[offset..])
        }
        None => bail!("The proto payload is empty"),
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use prost::Message;
    use risingwave_common::types::StructType;
    use risingwave_connector_codec::decoder::AccessExt;
    use risingwave_pb::catalog::StreamSourceInfo;
    use risingwave_pb::data::data_type::PbTypeName;
    use risingwave_pb::plan_common::{PbEncodeType, PbFormatType};
    use serde_json::json;
    use thiserror_ext::AsReport as _;

    use super::*;
    use crate::parser::protobuf::recursive::all_types::{EnumType, ExampleOneof, NestedMessage};
    use crate::parser::protobuf::recursive::AllTypes;
    use crate::parser::SpecificParserConfig;

    fn schema_dir() -> String {
        let dir = PathBuf::from("src/test_data");
        format!(
            "file://{}",
            std::fs::canonicalize(dir).unwrap().to_str().unwrap()
        )
    }

    // Id:      123,
    // Address: "test address",
    // City:    "test city",
    // Zipcode: 456,
    // Rate:    1.2345,
    // Date:    "2021-01-01"
    static PRE_GEN_PROTO_DATA: &[u8] = b"\x08\x7b\x12\x0c\x74\x65\x73\x74\x20\x61\x64\x64\x72\x65\x73\x73\x1a\x09\x74\x65\x73\x74\x20\x63\x69\x74\x79\x20\xc8\x03\x2d\x19\x04\x9e\x3f\x32\x0a\x32\x30\x32\x31\x2d\x30\x31\x2d\x30\x31";

    #[tokio::test]
    async fn test_simple_schema() -> crate::error::ConnectorResult<()> {
        let location = schema_dir() + "/simple-schema";
        println!("location: {}", location);
        let message_name = "test.TestRecord";
        let info = StreamSourceInfo {
            proto_message_name: message_name.to_string(),
            row_schema_location: location.to_string(),
            use_schema_registry: false,
            format: PbFormatType::Plain.into(),
            row_encode: PbEncodeType::Protobuf.into(),
            ..Default::default()
        };
        let parser_config = SpecificParserConfig::new(&info, &Default::default())?;
        let conf = ProtobufParserConfig::new(parser_config.encoding_config).await?;
        let value = DynamicMessage::decode(conf.message_descriptor, PRE_GEN_PROTO_DATA).unwrap();

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

    #[tokio::test]
    async fn test_complex_schema() -> crate::error::ConnectorResult<()> {
        let location = schema_dir() + "/complex-schema";
        let message_name = "test.User";

        let info = StreamSourceInfo {
            proto_message_name: message_name.to_string(),
            row_schema_location: location.to_string(),
            use_schema_registry: false,
            format: PbFormatType::Plain.into(),
            row_encode: PbEncodeType::Protobuf.into(),
            ..Default::default()
        };
        let parser_config = SpecificParserConfig::new(&info, &Default::default())?;
        let conf = ProtobufParserConfig::new(parser_config.encoding_config).await?;
        let columns = conf.map_to_columns().unwrap();

        assert_eq!(columns[0].name, "id".to_string());
        assert_eq!(columns[1].name, "code".to_string());
        assert_eq!(columns[2].name, "timestamp".to_string());

        let data_type = columns[3].column_type.as_ref().unwrap();
        assert_eq!(data_type.get_type_name().unwrap(), PbTypeName::List);
        let inner_field_type = data_type.field_type.clone();
        assert_eq!(
            inner_field_type[0].get_type_name().unwrap(),
            PbTypeName::Struct
        );
        let struct_inner = inner_field_type[0].field_type.clone();
        assert_eq!(struct_inner[0].get_type_name().unwrap(), PbTypeName::Int32);
        assert_eq!(struct_inner[1].get_type_name().unwrap(), PbTypeName::Int32);
        assert_eq!(
            struct_inner[2].get_type_name().unwrap(),
            PbTypeName::Varchar
        );

        assert_eq!(columns[4].name, "contacts".to_string());
        let inner_field_type = columns[4].column_type.as_ref().unwrap().field_type.clone();
        assert_eq!(
            inner_field_type[0].get_type_name().unwrap(),
            PbTypeName::List
        );
        assert_eq!(
            inner_field_type[1].get_type_name().unwrap(),
            PbTypeName::List
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_refuse_recursive_proto_message() {
        let location = schema_dir() + "/proto_recursive/recursive.pb";
        let message_name = "recursive.ComplexRecursiveMessage";

        let info = StreamSourceInfo {
            proto_message_name: message_name.to_string(),
            row_schema_location: location.to_string(),
            use_schema_registry: false,
            format: PbFormatType::Plain.into(),
            row_encode: PbEncodeType::Protobuf.into(),
            ..Default::default()
        };
        let parser_config = SpecificParserConfig::new(&info, &Default::default()).unwrap();
        let conf = ProtobufParserConfig::new(parser_config.encoding_config)
            .await
            .unwrap();
        let columns = conf.map_to_columns();
        // expect error message:
        // "Err(Protocol error: circular reference detected:
        // parent(recursive.ComplexRecursiveMessage.parent)->siblings(recursive.
        // ComplexRecursiveMessage.Parent.siblings), conflict with
        // parent(recursive.ComplexRecursiveMessage.parent), kind
        // recursive.ComplexRecursiveMessage.Parent"
        assert!(columns.is_err());
    }

    async fn create_recursive_pb_parser_config(
        location: &str,
        message_name: &str,
    ) -> ProtobufParserConfig {
        let location = schema_dir() + location;

        let info = StreamSourceInfo {
            proto_message_name: message_name.to_string(),
            row_schema_location: location.to_string(),
            use_schema_registry: false,
            format: PbFormatType::Plain.into(),
            row_encode: PbEncodeType::Protobuf.into(),
            ..Default::default()
        };
        let parser_config = SpecificParserConfig::new(&info, &Default::default()).unwrap();

        ProtobufParserConfig::new(parser_config.encoding_config)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_all_types_create_source() {
        let conf = create_recursive_pb_parser_config(
            "/proto_recursive/recursive.pb",
            "recursive.AllTypes",
        )
        .await;

        // Ensure that the parser can recognize the schema.
        let columns = conf
            .map_to_columns()
            .unwrap()
            .into_iter()
            .map(|c| DataType::from(&c.column_type.unwrap()))
            .collect_vec();
        assert_eq!(
            columns,
            vec![
                DataType::Float64, // double_field
                DataType::Float32, // float_field
                DataType::Int32,   // int32_field
                DataType::Int64,   // int64_field
                DataType::Int64,   // uint32_field
                DataType::Decimal, // uint64_field
                DataType::Int32,   // sint32_field
                DataType::Int64,   // sint64_field
                DataType::Int64,   // fixed32_field
                DataType::Decimal, // fixed64_field
                DataType::Int32,   // sfixed32_field
                DataType::Int64,   // sfixed64_field
                DataType::Boolean, // bool_field
                DataType::Varchar, // string_field
                DataType::Bytea,   // bytes_field
                DataType::Varchar, // enum_field
                DataType::Struct(StructType::new(vec![
                    ("id", DataType::Int32),
                    ("name", DataType::Varchar)
                ])), // nested_message_field
                DataType::List(DataType::Int32.into()), // repeated_int_field
                DataType::Varchar, // oneof_string
                DataType::Int32,   // oneof_int32
                DataType::Varchar, // oneof_enum
                DataType::Struct(StructType::new(vec![
                    ("seconds", DataType::Int64),
                    ("nanos", DataType::Int32)
                ])), // timestamp_field
                DataType::Struct(StructType::new(vec![
                    ("seconds", DataType::Int64),
                    ("nanos", DataType::Int32)
                ])), // duration_field
                DataType::Jsonb,   // any_field
                DataType::Struct(StructType::new(vec![("value", DataType::Int32)])), /* int32_value_field */
                DataType::Struct(StructType::new(vec![("value", DataType::Varchar)])), /* string_value_field */
            ]
        )
    }

    #[tokio::test]
    async fn test_all_types_data_parsing() {
        let m = create_all_types_message();
        let mut payload = Vec::new();
        m.encode(&mut payload).unwrap();

        let conf = create_recursive_pb_parser_config(
            "/proto_recursive/recursive.pb",
            "recursive.AllTypes",
        )
        .await;
        let mut access_builder = ProtobufAccessBuilder::new(conf).unwrap();
        let access = access_builder.generate_accessor(payload).await.unwrap();
        if let AccessImpl::Protobuf(a) = access {
            assert_all_types_eq(&a, &m);
        } else {
            panic!("unexpected")
        }
    }

    fn assert_all_types_eq(a: &ProtobufAccess, m: &AllTypes) {
        type S = ScalarImpl;

        pb_eq(a, "double_field", S::Float64(m.double_field.into()));
        pb_eq(a, "float_field", S::Float32(m.float_field.into()));
        pb_eq(a, "int32_field", S::Int32(m.int32_field));
        pb_eq(a, "int64_field", S::Int64(m.int64_field));
        pb_eq(a, "uint32_field", S::Int64(m.uint32_field.into()));
        pb_eq(a, "uint64_field", S::Decimal(m.uint64_field.into()));
        pb_eq(a, "sint32_field", S::Int32(m.sint32_field));
        pb_eq(a, "sint64_field", S::Int64(m.sint64_field));
        pb_eq(a, "fixed32_field", S::Int64(m.fixed32_field.into()));
        pb_eq(a, "fixed64_field", S::Decimal(m.fixed64_field.into()));
        pb_eq(a, "sfixed32_field", S::Int32(m.sfixed32_field));
        pb_eq(a, "sfixed64_field", S::Int64(m.sfixed64_field));
        pb_eq(a, "bool_field", S::Bool(m.bool_field));
        pb_eq(a, "string_field", S::Utf8(m.string_field.as_str().into()));
        pb_eq(a, "bytes_field", S::Bytea(m.bytes_field.clone().into()));
        pb_eq(a, "enum_field", S::Utf8("OPTION1".into()));
        pb_eq(
            a,
            "nested_message_field",
            S::Struct(StructValue::new(vec![
                Some(ScalarImpl::Int32(100)),
                Some(ScalarImpl::Utf8("Nested".into())),
            ])),
        );
        pb_eq(
            a,
            "repeated_int_field",
            S::List(ListValue::from_iter(m.repeated_int_field.clone())),
        );
        pb_eq(
            a,
            "timestamp_field",
            S::Struct(StructValue::new(vec![
                Some(ScalarImpl::Int64(1630927032)),
                Some(ScalarImpl::Int32(500000000)),
            ])),
        );
        pb_eq(
            a,
            "duration_field",
            S::Struct(StructValue::new(vec![
                Some(ScalarImpl::Int64(60)),
                Some(ScalarImpl::Int32(500000000)),
            ])),
        );
        pb_eq(
            a,
            "int32_value_field",
            S::Struct(StructValue::new(vec![Some(ScalarImpl::Int32(42))])),
        );
        pb_eq(
            a,
            "string_value_field",
            S::Struct(StructValue::new(vec![Some(ScalarImpl::Utf8(
                m.string_value_field.as_ref().unwrap().as_str().into(),
            ))])),
        );
        pb_eq(a, "oneof_string", S::Utf8("".into()));
        pb_eq(a, "oneof_int32", S::Int32(123));
        pb_eq(a, "oneof_enum", S::Utf8("DEFAULT".into()));
    }

    fn pb_eq(a: &ProtobufAccess, field_name: &str, value: ScalarImpl) {
        let field = a.descriptor().get_field_by_name(field_name).unwrap();
        let dummy_type = protobuf_type_mapping(&field, &mut vec![]).unwrap();
        let d = a.access_owned(&[field_name], &dummy_type).unwrap().unwrap();
        assert_eq!(d, value, "field: {} value: {:?}", field_name, d);
    }

    fn create_all_types_message() -> AllTypes {
        AllTypes {
            double_field: 1.2345,
            float_field: 1.2345,
            int32_field: 42,
            int64_field: 1234567890,
            uint32_field: 98765,
            uint64_field: 9876543210,
            sint32_field: -12345,
            sint64_field: -987654321,
            fixed32_field: 1234,
            fixed64_field: 5678,
            sfixed32_field: -56789,
            sfixed64_field: -123456,
            bool_field: true,
            string_field: "Hello, Prost!".to_string(),
            bytes_field: b"byte data".to_vec(),
            enum_field: EnumType::Option1 as i32,
            nested_message_field: Some(NestedMessage {
                id: 100,
                name: "Nested".to_string(),
            }),
            repeated_int_field: vec![1, 2, 3, 4, 5],
            timestamp_field: Some(::prost_types::Timestamp {
                seconds: 1630927032,
                nanos: 500000000,
            }),
            duration_field: Some(::prost_types::Duration {
                seconds: 60,
                nanos: 500000000,
            }),
            any_field: Some(::prost_types::Any {
                type_url: "type.googleapis.com/my_custom_type".to_string(),
                value: b"My custom data".to_vec(),
            }),
            int32_value_field: Some(42),
            string_value_field: Some("Hello, Wrapper!".to_string()),
            example_oneof: Some(ExampleOneof::OneofInt32(123)),
        }
    }

    // id: 12345
    // name {
    //    type_url: "type.googleapis.com/test.StringValue"
    //    value: "\n\010John Doe"
    // }
    static ANY_GEN_PROTO_DATA: &[u8] = b"\x08\xb9\x60\x12\x32\x0a\x24\x74\x79\x70\x65\x2e\x67\x6f\x6f\x67\x6c\x65\x61\x70\x69\x73\x2e\x63\x6f\x6d\x2f\x74\x65\x73\x74\x2e\x53\x74\x72\x69\x6e\x67\x56\x61\x6c\x75\x65\x12\x0a\x0a\x08\x4a\x6f\x68\x6e\x20\x44\x6f\x65";

    #[tokio::test]
    async fn test_any_schema() -> crate::error::ConnectorResult<()> {
        let conf = create_recursive_pb_parser_config("/any-schema.pb", "test.TestAny").await;

        println!("Current conf: {:#?}", conf);
        println!("---------------------------");

        let message =
            DynamicMessage::decode(conf.message_descriptor.clone(), ANY_GEN_PROTO_DATA).unwrap();

        println!("Test ANY_GEN_PROTO_DATA, current value: {:#?}", message);
        println!("---------------------------");

        let field = conf
            .message_descriptor
            .get_field_by_name("any_value")
            .unwrap();
        let value = message.get_field(&field);

        let ret = from_protobuf_value(&field, &value, &DataType::Jsonb)
            .unwrap()
            .to_owned_datum();
        println!("Decoded Value for ANY_GEN_PROTO_DATA: {:#?}", ret);
        println!("---------------------------");

        match ret {
            Some(ScalarImpl::Jsonb(jv)) => {
                assert_eq!(
                    jv,
                    JsonbVal::from(json!({
                        "@type": "type.googleapis.com/test.StringValue",
                        "value": "John Doe"
                    }))
                );
            }
            _ => panic!("Expected ScalarImpl::Jsonb"),
        }

        Ok(())
    }

    // id: 12345
    // name {
    // type_url: "type.googleapis.com/test.Int32Value"
    // value: "\010\322\376\006"
    // }
    // Unpacked Int32Value from Any: value: 114514
    static ANY_GEN_PROTO_DATA_1: &[u8] = b"\x08\xb9\x60\x12\x2b\x0a\x23\x74\x79\x70\x65\x2e\x67\x6f\x6f\x67\x6c\x65\x61\x70\x69\x73\x2e\x63\x6f\x6d\x2f\x74\x65\x73\x74\x2e\x49\x6e\x74\x33\x32\x56\x61\x6c\x75\x65\x12\x04\x08\xd2\xfe\x06";

    #[tokio::test]
    async fn test_any_schema_1() -> crate::error::ConnectorResult<()> {
        let conf = create_recursive_pb_parser_config("/any-schema.pb", "test.TestAny").await;

        println!("Current conf: {:#?}", conf);
        println!("---------------------------");

        let message =
            DynamicMessage::decode(conf.message_descriptor.clone(), ANY_GEN_PROTO_DATA_1).unwrap();

        println!("Current Value: {:#?}", message);
        println!("---------------------------");

        let field = conf
            .message_descriptor
            .get_field_by_name("any_value")
            .unwrap();
        let value = message.get_field(&field);

        let ret = from_protobuf_value(&field, &value, &DataType::Jsonb)
            .unwrap()
            .to_owned_datum();
        println!("Decoded Value for ANY_GEN_PROTO_DATA: {:#?}", ret);
        println!("---------------------------");

        match ret {
            Some(ScalarImpl::Jsonb(jv)) => {
                assert_eq!(
                    jv,
                    JsonbVal::from(json!({
                        "@type": "type.googleapis.com/test.Int32Value",
                        "value": 114514
                    }))
                );
            }
            _ => panic!("Expected ScalarImpl::Jsonb"),
        }

        Ok(())
    }

    // "id": 12345,
    // "any_value": {
    //     "type_url": "type.googleapis.com/test.AnyValue",
    //     "value": {
    //         "any_value_1": {
    //             "type_url": "type.googleapis.com/test.StringValue",
    //             "value": "114514"
    //         },
    //         "any_value_2": {
    //             "type_url": "type.googleapis.com/test.Int32Value",
    //             "value": 114514
    //         }
    //     }
    // }
    static ANY_RECURSIVE_GEN_PROTO_DATA: &[u8] = b"\x08\xb9\x60\x12\x84\x01\x0a\x21\x74\x79\x70\x65\x2e\x67\x6f\x6f\x67\x6c\x65\x61\x70\x69\x73\x2e\x63\x6f\x6d\x2f\x74\x65\x73\x74\x2e\x41\x6e\x79\x56\x61\x6c\x75\x65\x12\x5f\x0a\x30\x0a\x24\x74\x79\x70\x65\x2e\x67\x6f\x6f\x67\x6c\x65\x61\x70\x69\x73\x2e\x63\x6f\x6d\x2f\x74\x65\x73\x74\x2e\x53\x74\x72\x69\x6e\x67\x56\x61\x6c\x75\x65\x12\x08\x0a\x06\x31\x31\x34\x35\x31\x34\x12\x2b\x0a\x23\x74\x79\x70\x65\x2e\x67\x6f\x6f\x67\x6c\x65\x61\x70\x69\x73\x2e\x63\x6f\x6d\x2f\x74\x65\x73\x74\x2e\x49\x6e\x74\x33\x32\x56\x61\x6c\x75\x65\x12\x04\x08\xd2\xfe\x06";

    #[tokio::test]
    async fn test_any_recursive() -> crate::error::ConnectorResult<()> {
        let conf = create_recursive_pb_parser_config("/any-schema.pb", "test.TestAny").await;

        println!("Current conf: {:#?}", conf);
        println!("---------------------------");

        let message = DynamicMessage::decode(
            conf.message_descriptor.clone(),
            ANY_RECURSIVE_GEN_PROTO_DATA,
        )
        .unwrap();

        println!("Current Value: {:#?}", message);
        println!("---------------------------");

        let field = conf
            .message_descriptor
            .get_field_by_name("any_value")
            .unwrap();
        let value = message.get_field(&field);

        let ret = from_protobuf_value(&field, &value, &DataType::Jsonb)
            .unwrap()
            .to_owned_datum();
        println!("Decoded Value for ANY_RECURSIVE_GEN_PROTO_DATA: {:#?}", ret);
        println!("---------------------------");

        match ret {
            Some(ScalarImpl::Jsonb(jv)) => {
                assert_eq!(
                    jv,
                    JsonbVal::from(json!({
                        "@type": "type.googleapis.com/test.AnyValue",
                        "anyValue1": {
                            "@type": "type.googleapis.com/test.StringValue",
                            "value": "114514",
                        },
                        "anyValue2": {
                            "@type": "type.googleapis.com/test.Int32Value",
                            "value": 114514,
                        }
                    }))
                );
            }
            _ => panic!("Expected ScalarImpl::Jsonb"),
        }

        Ok(())
    }

    // id: 12345
    // any_value: {
    //    type_url: "type.googleapis.com/test.StringXalue"
    //    value: "\n\010John Doe"
    // }
    static ANY_GEN_PROTO_DATA_INVALID: &[u8] = b"\x08\xb9\x60\x12\x32\x0a\x24\x74\x79\x70\x65\x2e\x67\x6f\x6f\x67\x6c\x65\x61\x70\x69\x73\x2e\x63\x6f\x6d\x2f\x74\x65\x73\x74\x2e\x53\x74\x72\x69\x6e\x67\x58\x61\x6c\x75\x65\x12\x0a\x0a\x08\x4a\x6f\x68\x6e\x20\x44\x6f\x65";

    #[tokio::test]
    async fn test_any_invalid() -> crate::error::ConnectorResult<()> {
        let conf = create_recursive_pb_parser_config("/any-schema.pb", "test.TestAny").await;

        let message =
            DynamicMessage::decode(conf.message_descriptor.clone(), ANY_GEN_PROTO_DATA_INVALID)
                .unwrap();

        let field = conf
            .message_descriptor
            .get_field_by_name("any_value")
            .unwrap();
        let value = message.get_field(&field);

        let err = from_protobuf_value(&field, &value, &DataType::Jsonb).unwrap_err();

        let expected = expect_test::expect![[r#"
            Fail to convert protobuf Any into jsonb

            Caused by:
              message 'test.StringXalue' not found
        "#]];
        expected.assert_eq(err.to_report_string_pretty().as_str());

        Ok(())
    }

    #[test]
    fn test_decode_varint_zigzag() {
        // 1. Positive number
        let buffer = vec![0x02];
        let (value, len) = decode_varint_zigzag(&buffer).unwrap();
        assert_eq!(value, 1);
        assert_eq!(len, 1);

        // 2. Negative number
        let buffer = vec![0x01];
        let (value, len) = decode_varint_zigzag(&buffer).unwrap();
        assert_eq!(value, -1);
        assert_eq!(len, 1);

        // 3. Larger positive number
        let buffer = vec![0x9E, 0x03];
        let (value, len) = decode_varint_zigzag(&buffer).unwrap();
        assert_eq!(value, 207);
        assert_eq!(len, 2);

        // 4. Larger negative number
        let buffer = vec![0xBF, 0x07];
        let (value, len) = decode_varint_zigzag(&buffer).unwrap();
        assert_eq!(value, -480);
        assert_eq!(len, 2);

        // 5. Maximum positive number
        let buffer = vec![0xFE, 0xFF, 0xFF, 0xFF, 0x0F];
        let (value, len) = decode_varint_zigzag(&buffer).unwrap();
        assert_eq!(value, i32::MAX);
        assert_eq!(len, 5);

        // 6. Maximum negative number
        let buffer = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x0F];
        let (value, len) = decode_varint_zigzag(&buffer).unwrap();
        assert_eq!(value, i32::MIN);
        assert_eq!(len, 5);

        // 7. More than 32 bits
        let buffer = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x7F];
        let (value, len) = decode_varint_zigzag(&buffer).unwrap();
        assert_eq!(value, i32::MIN);
        assert_eq!(len, 5);

        // 8. Invalid input (more than 5 bytes)
        let buffer = vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        let result = decode_varint_zigzag(&buffer);
        assert!(result.is_err());
    }
}
