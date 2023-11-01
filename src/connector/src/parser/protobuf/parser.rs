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

use std::path::Path;
use std::sync::Arc;

use itertools::Itertools;
use prost_reflect::{
    Cardinality, DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MessageDescriptor,
    ReflectMessage, Value,
};
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::try_match_expand;
use risingwave_common::types::{DataType, Datum, Decimal, JsonbVal, ScalarImpl, F32, F64};
use risingwave_pb::plan_common::ColumnDesc;

use super::schema_resolver::*;
use crate::aws_utils::load_file_descriptor_from_s3;
use crate::parser::unified::protobuf::ProtobufAccess;
use crate::parser::unified::AccessImpl;
use crate::parser::{AccessBuilder, EncodingProperties};
use crate::schema::schema_registry::{
    extract_schema_id, get_subject_by_strategy, handle_sr_list, Client,
};

#[derive(Debug)]
pub struct ProtobufAccessBuilder {
    confluent_wire_type: bool,
    message_descriptor: MessageDescriptor,
    descriptor_pool: Arc<DescriptorPool>,
}

impl AccessBuilder for ProtobufAccessBuilder {
    #[allow(clippy::unused_async)]
    async fn generate_accessor(&mut self, payload: Vec<u8>) -> Result<AccessImpl<'_, '_>> {
        let payload = if self.confluent_wire_type {
            resolve_pb_header(&payload)?
        } else {
            &payload
        };

        let message = DynamicMessage::decode(self.message_descriptor.clone(), payload)
            .map_err(|e| ProtocolError(format!("parse message failed: {}", e)))?;

        Ok(AccessImpl::Protobuf(ProtobufAccess::new(
            message,
            Arc::clone(&self.descriptor_pool),
        )))
    }
}

impl ProtobufAccessBuilder {
    pub fn new(config: ProtobufParserConfig) -> Result<Self> {
        let ProtobufParserConfig {
            confluent_wire_type,
            message_descriptor,
            descriptor_pool,
        } = config;

        Ok(Self {
            confluent_wire_type,
            message_descriptor,
            descriptor_pool,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ProtobufParserConfig {
    confluent_wire_type: bool,
    pub(crate) message_descriptor: MessageDescriptor,
    /// Note that the pub(crate) here is merely for testing
    pub(crate) descriptor_pool: Arc<DescriptorPool>,
}

impl ProtobufParserConfig {
    pub async fn new(encoding_properties: EncodingProperties) -> Result<Self> {
        let protobuf_config = try_match_expand!(encoding_properties, EncodingProperties::Protobuf)?;
        let location = &protobuf_config.row_schema_location;
        let message_name = &protobuf_config.message_name;
        let url = handle_sr_list(location.as_str())?;

        if let Some(name) = protobuf_config.key_message_name {
            // https://docs.confluent.io/platform/7.5/control-center/topics/schema.html#c3-schemas-best-practices-key-value-pairs
            return Err(RwError::from(ProtocolError(format!(
                "key.message = {name} not used. Protobuf key unsupported."
            ))));
        }
        let schema_bytes = if protobuf_config.use_schema_registry {
            let schema_value = get_subject_by_strategy(
                &protobuf_config.name_strategy,
                protobuf_config.topic.as_str(),
                Some(message_name.as_ref()),
                false,
            )?;
            tracing::debug!("infer value subject {schema_value}");

            let client = Client::new(url, &protobuf_config.client_config)?;
            compile_file_descriptor_from_schema_registry(schema_value.as_str(), &client).await?
        } else {
            let url = url.get(0).unwrap();
            match url.scheme() {
                // TODO(Tao): support local file only when it's compiled in debug mode.
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
                    load_file_descriptor_from_s3(
                        url,
                        protobuf_config.aws_auth_props.as_ref().unwrap(),
                    )
                    .await
                }
                "https" | "http" => load_file_descriptor_from_http(url).await,
                scheme => Err(RwError::from(ProtocolError(format!(
                    "path scheme {} is not supported",
                    scheme
                )))),
            }?
        };

        let pool = DescriptorPool::decode(schema_bytes.as_slice()).map_err(|e| {
            ProtocolError(format!(
                "cannot build descriptor pool from schema: {}, error: {}",
                location, e
            ))
        })?;

        let message_descriptor = pool.get_message_by_name(message_name).ok_or_else(|| {
            ProtocolError(format!(
                "Cannot find message {} in schema: {}.\nDescriptor pool is {:?}",
                message_name, location, pool
            ))
        })?;

        Ok(Self {
            message_descriptor,
            confluent_wire_type: protobuf_config.use_schema_registry,
            descriptor_pool: Arc::new(pool),
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
    ) -> Result<ColumnDesc> {
        let field_type = protobuf_type_mapping(field_descriptor, parse_trace)?;
        if let Kind::Message(m) = field_descriptor.kind() {
            let field_descs = if let DataType::List { .. } = field_type {
                vec![]
            } else {
                m.fields()
                    .map(|f| Self::pb_field_to_col_desc(&f, index, parse_trace))
                    .collect::<Result<Vec<_>>>()?
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

fn detect_loop_and_push(trace: &mut Vec<String>, fd: &FieldDescriptor) -> Result<()> {
    let identifier = format!("{}({})", fd.name(), fd.full_name());
    if trace.iter().any(|s| s == identifier.as_str()) {
        return Err(RwError::from(ProtocolError(format!(
            "circular reference detected: {}, conflict with {}, kind {:?}",
            trace.iter().join("->"),
            identifier,
            fd.kind(),
        ))));
    }
    trace.push(identifier);
    Ok(())
}

fn extract_any_info(dyn_msg: &DynamicMessage) -> (String, Value) {
    debug_assert!(
        dyn_msg.fields().count() == 2,
        "Expected only two fields for Any Type MessageDescriptor"
    );

    let type_url = dyn_msg
        .get_field_by_name("type_url")
        .expect("Expect type_url in dyn_msg")
        .to_string()
        .split('/')
        .nth(1)
        .map(|part| part[..part.len() - 1].to_string())
        .unwrap_or_default();

    let payload = dyn_msg
        .get_field_by_name("value")
        .expect("Expect value (payload) in dyn_msg")
        .as_ref()
        .clone();

    (type_url, payload)
}

/// TODO: Resolve the potential naming conflict in the map
/// i.e., If the two anonymous type shares the same key (e.g., "Int32"),
/// the latter will overwrite the former one in `serde_json::Map`.
/// Possible solution, maintaining a global id map, for the same types
/// In the same level of fields, add the unique id at the tail of the name.
/// e.g., "Int32.1" & "Int32.2" in the above example
fn recursive_parse_json(
    fields: &[Datum],
    full_name_vec: Option<Vec<String>>,
    full_name: Option<String>,
) -> serde_json::Value {
    // Note that the key is of no order
    let mut ret: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();

    // The hidden type hint for user's convenience
    // i.e., `"_type": message.full_name()`
    if let Some(full_name) = full_name {
        ret.insert("_type".to_string(), serde_json::Value::String(full_name));
    }

    for (idx, field) in fields.iter().enumerate() {
        let mut key;
        if let Some(k) = full_name_vec.as_ref() {
            key = k[idx].to_string();
        } else {
            key = "".to_string();
        }

        match field.clone() {
            Some(ScalarImpl::Int16(v)) => {
                if key.is_empty() {
                    key = "Int16".to_string();
                }
                ret.insert(key, serde_json::Value::Number(serde_json::Number::from(v)));
            }
            Some(ScalarImpl::Int32(v)) => {
                if key.is_empty() {
                    key = "Int32".to_string();
                }
                ret.insert(key, serde_json::Value::Number(serde_json::Number::from(v)));
            }
            Some(ScalarImpl::Int64(v)) => {
                if key.is_empty() {
                    key = "Int64".to_string();
                }
                ret.insert(key, serde_json::Value::Number(serde_json::Number::from(v)));
            }
            Some(ScalarImpl::Bool(v)) => {
                if key.is_empty() {
                    key = "Bool".to_string();
                }
                ret.insert(key, serde_json::Value::Bool(v));
            }
            Some(ScalarImpl::Bytea(v)) => {
                if key.is_empty() {
                    key = "Bytea".to_string();
                }
                let s = String::from_utf8(v.to_vec()).unwrap();
                ret.insert(key, serde_json::Value::String(s));
            }
            Some(ScalarImpl::Float32(v)) => {
                if key.is_empty() {
                    key = "Int16".to_string();
                }
                ret.insert(
                    key,
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(v.into_inner() as f64).unwrap(),
                    ),
                );
            }
            Some(ScalarImpl::Float64(v)) => {
                if key.is_empty() {
                    key = "Float64".to_string();
                }
                ret.insert(
                    key,
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(v.into_inner()).unwrap(),
                    ),
                );
            }
            Some(ScalarImpl::Utf8(v)) => {
                if key.is_empty() {
                    key = "Utf8".to_string();
                }
                ret.insert(key, serde_json::Value::String(v.to_string()));
            }
            Some(ScalarImpl::Struct(v)) => {
                if key.is_empty() {
                    key = "Struct".to_string();
                }
                ret.insert(key, recursive_parse_json(v.fields(), None, None));
            }
            Some(ScalarImpl::Jsonb(v)) => {
                if key.is_empty() {
                    key = "Jsonb".to_string();
                }
                ret.insert(key, v.take());
            }
            r#type => panic!("Not yet support ScalarImpl type: {:?}", r#type),
        }
    }

    serde_json::Value::Object(ret)
}

pub fn from_protobuf_value(
    field_desc: &FieldDescriptor,
    value: &Value,
    descriptor_pool: &Arc<DescriptorPool>,
) -> Result<Datum> {
    let v = match value {
        Value::Bool(v) => ScalarImpl::Bool(*v),
        Value::I32(i) => ScalarImpl::Int32(*i),
        Value::U32(i) => ScalarImpl::Int64(*i as i64),
        Value::I64(i) => ScalarImpl::Int64(*i),
        Value::U64(i) => ScalarImpl::Decimal(Decimal::from(*i)),
        Value::F32(f) => ScalarImpl::Float32(F32::from(*f)),
        Value::F64(f) => ScalarImpl::Float64(F64::from(*f)),
        Value::String(s) => ScalarImpl::Utf8(s.as_str().into()),
        Value::EnumNumber(idx) => {
            let kind = field_desc.kind();
            let enum_desc = kind.as_enum().ok_or_else(|| {
                let err_msg = format!("protobuf parse error.not a enum desc {:?}", field_desc);
                RwError::from(ProtocolError(err_msg))
            })?;
            let enum_symbol = enum_desc.get_value(*idx).ok_or_else(|| {
                let err_msg = format!(
                    "protobuf parse error.unknown enum index {} of enum {:?}",
                    idx, enum_desc
                );
                RwError::from(ProtocolError(err_msg))
            })?;
            ScalarImpl::Utf8(enum_symbol.name().into())
        }
        Value::Message(dyn_msg) => {
            if dyn_msg.descriptor().full_name() == "google.protobuf.Any" {
                // If the fields are not presented, default value is an empty string
                if !dyn_msg.has_field_by_name("type_url") || !dyn_msg.has_field_by_name("value") {
                    return Ok(Some(ScalarImpl::Jsonb(JsonbVal::from(
                        serde_json::json! {""},
                    ))));
                }

                // Sanity check
                debug_assert!(
                    dyn_msg.has_field_by_name("type_url") && dyn_msg.has_field_by_name("value"),
                    "`type_url` & `value` must exist in fields of `dyn_msg`"
                );

                // The message is of type `Any`
                let (type_url, payload) = extract_any_info(dyn_msg);

                let payload_field_desc = dyn_msg.descriptor().get_field_by_name("value").unwrap();

                let Some(ScalarImpl::Bytea(payload)) =
                    from_protobuf_value(&payload_field_desc, &payload, descriptor_pool)?
                else {
                    let err_msg = "Expected ScalarImpl::Bytea for payload".to_string();
                    return Err(RwError::from(ProtocolError(err_msg)));
                };

                // Get the corresponding schema from the descriptor pool
                let msg_desc = descriptor_pool
                    .get_message_by_name(&type_url)
                    .ok_or_else(|| {
                        ProtocolError(format!(
                        "Cannot find message {} in from_protobuf_value.\nDescriptor pool is {:#?}",
                        type_url, descriptor_pool
                    ))
                    })?;

                let f = msg_desc
                    .clone()
                    .fields()
                    .map(|f| f.name().to_string())
                    .collect::<Vec<String>>();

                let full_name = msg_desc.clone().full_name().to_string();

                // Decode the payload based on the `msg_desc`
                let decoded_value = DynamicMessage::decode(msg_desc, payload.as_ref()).unwrap();
                let decoded_value = from_protobuf_value(
                    field_desc,
                    &Value::Message(decoded_value),
                    descriptor_pool,
                )?
                .unwrap();

                // Extract the struct value
                let ScalarImpl::Struct(v) = decoded_value else {
                    panic!("Expect ScalarImpl::Struct");
                };

                ScalarImpl::Jsonb(JsonbVal::from(serde_json::json!(recursive_parse_json(
                    v.fields(),
                    Some(f),
                    Some(full_name),
                ))))
            } else {
                let mut rw_values = Vec::with_capacity(dyn_msg.descriptor().fields().len());
                // fields is a btree map in descriptor
                // so it's order is the same as datatype
                for field_desc in dyn_msg.descriptor().fields() {
                    // missing field
                    if !dyn_msg.has_field(&field_desc)
                        && field_desc.cardinality() == Cardinality::Required
                    {
                        let err_msg = format!(
                            "protobuf parse error.missing required field {:?}",
                            field_desc
                        );
                        return Err(RwError::from(ProtocolError(err_msg)));
                    }
                    // use default value if dyn_msg doesn't has this field
                    let value = dyn_msg.get_field(&field_desc);
                    rw_values.push(from_protobuf_value(&field_desc, &value, descriptor_pool)?);
                }
                ScalarImpl::Struct(StructValue::new(rw_values))
            }
        }
        Value::List(values) => {
            let rw_values = values
                .iter()
                .map(|value| from_protobuf_value(field_desc, value, descriptor_pool))
                .collect::<Result<Vec<_>>>()?;
            ScalarImpl::List(ListValue::new(rw_values))
        }
        Value::Bytes(value) => ScalarImpl::Bytea(value.to_vec().into_boxed_slice()),
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
fn protobuf_type_mapping(
    field_descriptor: &FieldDescriptor,
    parse_trace: &mut Vec<String>,
) -> Result<DataType> {
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
        Kind::Message(m) => {
            let fields = m
                .fields()
                .map(|f| protobuf_type_mapping(&f, parse_trace))
                .collect::<Result<Vec<_>>>()?;
            let field_names = m.fields().map(|f| f.name().to_string()).collect_vec();

            // Note that this part is useful for actual parsing
            // Since RisingWave will parse message to `ScalarImpl::Jsonb`
            // Please do NOT modify it
            if field_names.len() == 2
                && field_names.contains(&"value".to_string())
                && field_names.contains(&"type_url".to_string())
            {
                DataType::Jsonb
            } else {
                DataType::new_struct(fields, field_names)
            }
        }
        Kind::Enum(_) => DataType::Varchar,
        Kind::Bytes => DataType::Bytea,
    };
    if field_descriptor.is_map() {
        return Err(RwError::from(ProtocolError(format!(
            "map type is unsupported (field: '{}')",
            field_descriptor.full_name()
        ))));
    }
    if field_descriptor.cardinality() == Cardinality::Repeated {
        t = DataType::List(Box::new(t))
    }
    _ = parse_trace.pop();
    Ok(t)
}

pub(crate) fn resolve_pb_header(payload: &[u8]) -> Result<&[u8]> {
    // there's a message index array at the front of payload
    // if it is the first message in proto def, the array is just and `0`
    // TODO: support parsing more complex index array
    let (_, remained) = extract_schema_id(payload)?;
    match remained.first() {
        Some(0) => Ok(&remained[1..]),
        Some(i) => {
            Err(RwError::from(ProtocolError(format!("The payload message must be the first message in protobuf schema def, but the message index is {}", i))))
        }
        None => {
            Err(RwError::from(ProtocolError("The proto payload is empty".to_owned())))
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use prost::Message;
    use risingwave_common::types::{DataType, StructType};
    use risingwave_pb::catalog::StreamSourceInfo;
    use risingwave_pb::data::data_type::PbTypeName;
    use risingwave_pb::plan_common::{PbEncodeType, PbFormatType};
    use serde_json::json;

    use super::*;
    use crate::parser::protobuf::recursive::all_types::{EnumType, ExampleOneof, NestedMessage};
    use crate::parser::protobuf::recursive::AllTypes;
    use crate::parser::unified::Access;
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
    async fn test_simple_schema() -> Result<()> {
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
        let parser_config = SpecificParserConfig::new(&info, &HashMap::new())?;
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
    async fn test_complex_schema() -> Result<()> {
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
        let parser_config = SpecificParserConfig::new(&info, &HashMap::new())?;
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
        let parser_config = SpecificParserConfig::new(&info, &HashMap::new()).unwrap();
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
        let parser_config = SpecificParserConfig::new(&info, &HashMap::new()).unwrap();

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
            S::List(ListValue::new(
                m.repeated_int_field
                    .iter()
                    .map(|&x| Some(x.into()))
                    .collect(),
            )),
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
        let d = a.access(&[field_name], None).unwrap().unwrap();
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
    async fn test_any_schema() -> Result<()> {
        let conf = create_recursive_pb_parser_config("/any-schema.pb", "test.TestAny").await;

        println!("Current conf: {:#?}", conf);
        println!("---------------------------");

        let value =
            DynamicMessage::decode(conf.message_descriptor.clone(), ANY_GEN_PROTO_DATA).unwrap();

        println!("Test ANY_GEN_PROTO_DATA, current value: {:#?}", value);
        println!("---------------------------");

        // This is of no use
        let field = value.fields().next().unwrap().0;

        if let Some(ret) =
            from_protobuf_value(&field, &Value::Message(value), &conf.descriptor_pool).unwrap()
        {
            println!("Decoded Value for ANY_GEN_PROTO_DATA: {:#?}", ret);
            println!("---------------------------");

            let ScalarImpl::Struct(struct_value) = ret else {
                panic!("Expected ScalarImpl::Struct");
            };

            let fields = struct_value.fields();

            match fields[0].clone() {
                Some(ScalarImpl::Int32(v)) => {
                    println!("Successfully decode field[0]");
                    assert_eq!(v, 12345);
                }
                _ => panic!("Expected ScalarImpl::Int32"),
            }

            match fields[1].clone() {
                Some(ScalarImpl::Jsonb(jv)) => {
                    assert_eq!(
                        jv,
                        JsonbVal::from(json!({
                            "_type": "test.StringValue",
                            "value": "John Doe"
                        }))
                    );
                }
                _ => panic!("Expected ScalarImpl::Jsonb"),
            }
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
    async fn test_any_schema_1() -> Result<()> {
        let conf = create_recursive_pb_parser_config("/any-schema.pb", "test.TestAny").await;

        println!("Current conf: {:#?}", conf);
        println!("---------------------------");

        let value =
            DynamicMessage::decode(conf.message_descriptor.clone(), ANY_GEN_PROTO_DATA_1).unwrap();

        println!("Current Value: {:#?}", value);
        println!("---------------------------");

        // This is of no use
        let field = value.fields().next().unwrap().0;

        if let Some(ret) =
            from_protobuf_value(&field, &Value::Message(value), &conf.descriptor_pool).unwrap()
        {
            println!("Decoded Value for ANY_GEN_PROTO_DATA: {:#?}", ret);
            println!("---------------------------");

            let ScalarImpl::Struct(struct_value) = ret else {
                panic!("Expected ScalarImpl::Struct");
            };

            let fields = struct_value.fields();

            match fields[0].clone() {
                Some(ScalarImpl::Int32(v)) => {
                    println!("Successfully decode field[0]");
                    assert_eq!(v, 12345);
                }
                _ => panic!("Expected ScalarImpl::Int32"),
            }

            match fields[1].clone() {
                Some(ScalarImpl::Jsonb(jv)) => {
                    assert_eq!(
                        jv,
                        JsonbVal::from(json!({
                            "_type": "test.Int32Value",
                            "value": 114514
                        }))
                    );
                }
                _ => panic!("Expected ScalarImpl::Jsonb"),
            }
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
    async fn test_any_recursive() -> Result<()> {
        let conf = create_recursive_pb_parser_config("/any-schema.pb", "test.TestAny").await;

        println!("Current conf: {:#?}", conf);
        println!("---------------------------");

        let value = DynamicMessage::decode(
            conf.message_descriptor.clone(),
            ANY_RECURSIVE_GEN_PROTO_DATA,
        )
        .unwrap();

        println!("Current Value: {:#?}", value);
        println!("---------------------------");

        // This is of no use
        let field = value.fields().next().unwrap().0;

        if let Some(ret) =
            from_protobuf_value(&field, &Value::Message(value), &conf.descriptor_pool).unwrap()
        {
            println!("Decoded Value for ANY_RECURSIVE_GEN_PROTO_DATA: {:#?}", ret);
            println!("---------------------------");

            let ScalarImpl::Struct(struct_value) = ret else {
                panic!("Expected ScalarImpl::Struct");
            };

            let fields = struct_value.fields();

            match fields[0].clone() {
                Some(ScalarImpl::Int32(v)) => {
                    println!("Successfully decode field[0]");
                    assert_eq!(v, 12345);
                }
                _ => panic!("Expected ScalarImpl::Int32"),
            }

            match fields[1].clone() {
                Some(ScalarImpl::Jsonb(jv)) => {
                    assert_eq!(
                        jv,
                        JsonbVal::from(json!({
                            "_type": "test.AnyValue",
                            "any_value_1": {
                                "_type": "test.StringValue",
                                "value": "114514",
                            },
                            "any_value_2": {
                                "_type": "test.Int32Value",
                                "value": 114514,
                            }
                        }))
                    );
                }
                _ => panic!("Expected ScalarImpl::Jsonb"),
            }
        }

        Ok(())
    }
}
