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

use bytes::Bytes;
use prost::Message;
use prost_reflect::{
    DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, ReflectMessage, Value,
};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, DatumRef, ScalarRefImpl, StructType};
use risingwave_common::util::iter_util::ZipEqDebug;

use super::{FieldEncodeError, Result as SinkResult, RowEncoder, SerTo};

type Result<T> = std::result::Result<T, FieldEncodeError>;

pub struct ProtoEncoder {
    schema: Schema,
    col_indices: Option<Vec<usize>>,
    descriptor: MessageDescriptor,
}

impl ProtoEncoder {
    pub fn new(
        schema: Schema,
        col_indices: Option<Vec<usize>>,
        descriptor: MessageDescriptor,
    ) -> SinkResult<Self> {
        match &col_indices {
            Some(col_indices) => validate_fields(
                col_indices.iter().map(|idx| {
                    let f = &schema[*idx];
                    (f.name.as_str(), &f.data_type)
                }),
                &descriptor,
            )?,
            None => validate_fields(
                schema
                    .fields
                    .iter()
                    .map(|f| (f.name.as_str(), &f.data_type)),
                &descriptor,
            )?,
        };

        Ok(Self {
            schema,
            col_indices,
            descriptor,
        })
    }
}

impl RowEncoder for ProtoEncoder {
    type Output = DynamicMessage;

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn col_indices(&self) -> Option<&[usize]> {
        self.col_indices.as_deref()
    }

    fn encode_cols(
        &self,
        row: impl Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> SinkResult<Self::Output> {
        encode_fields(
            col_indices.map(|idx| {
                let f = &self.schema[idx];
                ((f.name.as_str(), &f.data_type), row.datum_at(idx))
            }),
            &self.descriptor,
        )
        .map_err(Into::into)
    }
}

impl SerTo<Vec<u8>> for DynamicMessage {
    fn ser_to(self) -> SinkResult<Vec<u8>> {
        Ok(self.encode_to_vec())
    }
}

/// A trait that assists code reuse between `validate` and `encode`.
/// * For `validate`, the inputs are (RisingWave type, ProtoBuf type).
/// * For `encode`, the inputs are (RisingWave type, RisingWave data, ProtoBuf type).
///
/// Thus we impl [`MaybeData`] for both [`()`] and [`ScalarRefImpl`].
trait MaybeData: std::fmt::Debug {
    type Out;

    fn on_base(self, f: impl FnOnce(ScalarRefImpl<'_>) -> Result<Value>) -> Result<Self::Out>;

    fn on_struct(self, st: &StructType, pb: &MessageDescriptor) -> Result<Self::Out>;

    fn on_list(self, elem: &DataType, pb: &FieldDescriptor) -> Result<Self::Out>;
}

impl MaybeData for () {
    type Out = ();

    fn on_base(self, _: impl FnOnce(ScalarRefImpl<'_>) -> Result<Value>) -> Result<Self::Out> {
        Ok(self)
    }

    fn on_struct(self, st: &StructType, pb: &MessageDescriptor) -> Result<Self::Out> {
        validate_fields(st.iter(), pb)
    }

    fn on_list(self, elem: &DataType, pb: &FieldDescriptor) -> Result<Self::Out> {
        encode_field(elem, (), pb, true)
    }
}

/// Nullability is not part of type system in proto.
/// * Top level is always a message.
/// * All message fields can be omitted in proto3.
/// * All repeated elements must have a value.
/// So we handle [`ScalarRefImpl`] rather than [`DatumRef`] here.
impl MaybeData for ScalarRefImpl<'_> {
    type Out = Value;

    fn on_base(self, f: impl FnOnce(ScalarRefImpl<'_>) -> Result<Value>) -> Result<Self::Out> {
        f(self)
    }

    fn on_struct(self, st: &StructType, pb: &MessageDescriptor) -> Result<Self::Out> {
        let d = self.into_struct();
        let message = encode_fields(st.iter().zip_eq_debug(d.iter_fields_ref()), pb)?;
        Ok(Value::Message(message))
    }

    fn on_list(self, elem: &DataType, pb: &FieldDescriptor) -> Result<Self::Out> {
        let d = self.into_list();
        let vs = d
            .iter()
            .map(|d| {
                encode_field(
                    elem,
                    d.ok_or_else(|| {
                        FieldEncodeError::new("array containing null not allowed as repeated field")
                    })?,
                    pb,
                    true,
                )
            })
            .try_collect()?;
        Ok(Value::List(vs))
    }
}

fn validate_fields<'a>(
    fields: impl Iterator<Item = (&'a str, &'a DataType)>,
    descriptor: &MessageDescriptor,
) -> Result<()> {
    for (name, t) in fields {
        let Some(proto_field) = descriptor.get_field_by_name(name) else {
            return Err(FieldEncodeError::new("field not in proto").with_name(name));
        };
        if proto_field.cardinality() == prost_reflect::Cardinality::Required {
            return Err(FieldEncodeError::new("`required` not supported").with_name(name));
        }
        encode_field(t, (), &proto_field, false).map_err(|e| e.with_name(name))?;
    }
    Ok(())
}

fn encode_fields<'a>(
    fields_with_datums: impl Iterator<Item = ((&'a str, &'a DataType), DatumRef<'a>)>,
    descriptor: &MessageDescriptor,
) -> Result<DynamicMessage> {
    let mut message = DynamicMessage::new(descriptor.clone());
    for ((name, t), d) in fields_with_datums {
        let proto_field = descriptor.get_field_by_name(name).unwrap();
        // On `null`, simply skip setting the field.
        if let Some(scalar) = d {
            let value =
                encode_field(t, scalar, &proto_field, false).map_err(|e| e.with_name(name))?;
            message
                .try_set_field(&proto_field, value)
                .map_err(|e| FieldEncodeError::new(e).with_name(name))?;
        }
    }
    Ok(message)
}

// Full name of Well-Known Types
const WKT_TIMESTAMP: &str = "google.protobuf.Timestamp";
const WKT_BOOL_VALUE: &str = "google.protobuf.BoolValue";

/// Handles both `validate` (without actual data) and `encode`.
/// See [`MaybeData`] for more info.
fn encode_field<D: MaybeData>(
    data_type: &DataType,
    maybe: D,
    proto_field: &FieldDescriptor,
    in_repeated: bool,
) -> Result<D::Out> {
    // Regarding (proto_field.is_list, in_repeated):
    // (F, T) => impossible
    // (F, F) => encoding to a non-repeated field
    // (T, F) => encoding to a repeated field
    // (T, T) => encoding to an element of a repeated field
    // In the bottom 2 cases, we need to distinguish the same `proto_field` with the help of `in_repeated`.
    assert!(proto_field.is_list() || !in_repeated);
    let expect_list = proto_field.is_list() && !in_repeated;
    if proto_field.is_map() || proto_field.is_group() {
        return Err(FieldEncodeError::new(
            "proto map or group not supported yet",
        ));
    }

    let no_match_err = || {
        Err(FieldEncodeError::new(format!(
            "cannot encode {} column as {}{:?} field",
            data_type,
            if expect_list { "repeated " } else { "" },
            proto_field.kind()
        )))
    };

    let value = match &data_type {
        // Group A: perfect match between RisingWave types and ProtoBuf types
        DataType::Boolean => match (expect_list, proto_field.kind()) {
            (false, Kind::Bool) => maybe.on_base(|s| Ok(Value::Bool(s.into_bool())))?,
            _ => return no_match_err(),
        },
        DataType::Varchar => match (expect_list, proto_field.kind()) {
            (false, Kind::String) => maybe.on_base(|s| Ok(Value::String(s.into_utf8().into())))?,
            _ => return no_match_err(),
        },
        DataType::Bytea => match (expect_list, proto_field.kind()) {
            (false, Kind::Bytes) => {
                maybe.on_base(|s| Ok(Value::Bytes(Bytes::copy_from_slice(s.into_bytea()))))?
            }
            _ => return no_match_err(),
        },
        DataType::Float32 => match (expect_list, proto_field.kind()) {
            (false, Kind::Float) => maybe.on_base(|s| Ok(Value::F32(s.into_float32().into())))?,
            _ => return no_match_err(),
        },
        DataType::Float64 => match (expect_list, proto_field.kind()) {
            (false, Kind::Double) => maybe.on_base(|s| Ok(Value::F64(s.into_float64().into())))?,
            _ => return no_match_err(),
        },
        DataType::Int32 => match (expect_list, proto_field.kind()) {
            (false, Kind::Int32 | Kind::Sint32 | Kind::Sfixed32) => {
                maybe.on_base(|s| Ok(Value::I32(s.into_int32())))?
            }
            _ => return no_match_err(),
        },
        DataType::Int64 => match (expect_list, proto_field.kind()) {
            (false, Kind::Int64 | Kind::Sint64 | Kind::Sfixed64) => {
                maybe.on_base(|s| Ok(Value::I64(s.into_int64())))?
            }
            _ => return no_match_err(),
        },
        DataType::Struct(st) => match (expect_list, proto_field.kind()) {
            (false, Kind::Message(pb)) => maybe.on_struct(st, &pb)?,
            _ => return no_match_err(),
        },
        DataType::List(elem) => match expect_list {
            true => maybe.on_list(elem, proto_field)?,
            false => return no_match_err(),
        },
        // Group B: match between RisingWave types and ProtoBuf Well-Known types
        DataType::Timestamptz => match (expect_list, proto_field.kind()) {
            (false, Kind::Message(pb)) if pb.full_name() == WKT_TIMESTAMP => {
                maybe.on_base(|s| {
                    let d = s.into_timestamptz();
                    let message = prost_types::Timestamp {
                        seconds: d.timestamp(),
                        nanos: d.timestamp_subsec_nanos().try_into().unwrap(),
                    };
                    Ok(Value::Message(message.transcode_to_dynamic()))
                })?
            }
            _ => return no_match_err(),
        },
        DataType::Jsonb => return no_match_err(), // Value, NullValue, Struct (map), ListValue
        // Group C: experimental
        DataType::Int16 => return no_match_err(),
        DataType::Date => return no_match_err(), // google.type.Date
        DataType::Time => return no_match_err(), // google.type.TimeOfDay
        DataType::Timestamp => return no_match_err(), // google.type.DateTime
        DataType::Decimal => return no_match_err(), // google.type.Decimal
        DataType::Interval => return no_match_err(),
        // Group D: unsupported
        DataType::Serial | DataType::Int256 => {
            return no_match_err();
        }
    };

    Ok(value)
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::Field;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{ListValue, ScalarImpl, StructValue, Timestamptz};

    use super::*;

    #[test]
    fn test_encode_proto_ok() {
        let pool_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/test_data/proto_recursive/recursive.pb");
        let pool_bytes = std::fs::read(pool_path).unwrap();
        let pool = prost_reflect::DescriptorPool::decode(pool_bytes.as_ref()).unwrap();
        let descriptor = pool.get_message_by_name("recursive.AllTypes").unwrap();

        let schema = Schema::new(vec![
            Field::with_name(DataType::Boolean, "bool_field"),
            Field::with_name(DataType::Varchar, "string_field"),
            Field::with_name(DataType::Bytea, "bytes_field"),
            Field::with_name(DataType::Float32, "float_field"),
            Field::with_name(DataType::Float64, "double_field"),
            Field::with_name(DataType::Int32, "int32_field"),
            Field::with_name(DataType::Int64, "int64_field"),
            Field::with_name(DataType::Int32, "sint32_field"),
            Field::with_name(DataType::Int64, "sint64_field"),
            Field::with_name(DataType::Int32, "sfixed32_field"),
            Field::with_name(DataType::Int64, "sfixed64_field"),
            Field::with_name(
                DataType::Struct(StructType::new(vec![
                    ("id", DataType::Int32),
                    ("name", DataType::Varchar),
                ])),
                "nested_message_field",
            ),
            Field::with_name(DataType::List(DataType::Int32.into()), "repeated_int_field"),
            Field::with_name(DataType::Timestamptz, "timestamp_field"),
        ]);
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Utf8("RisingWave".into())),
            Some(ScalarImpl::Bytea([0xbe, 0xef].into())),
            Some(ScalarImpl::Float32(3.5f32.into())),
            Some(ScalarImpl::Float64(4.25f64.into())),
            Some(ScalarImpl::Int32(22)),
            Some(ScalarImpl::Int64(23)),
            Some(ScalarImpl::Int32(24)),
            None,
            Some(ScalarImpl::Int32(26)),
            Some(ScalarImpl::Int64(27)),
            Some(ScalarImpl::Struct(StructValue::new(vec![
                Some(ScalarImpl::Int32(1)),
                Some(ScalarImpl::Utf8("".into())),
            ]))),
            Some(ScalarImpl::List(ListValue::from_iter([4, 0, 4]))),
            Some(ScalarImpl::Timestamptz(Timestamptz::from_micros(3))),
        ]);

        let encoder = ProtoEncoder::new(schema, None, descriptor.clone()).unwrap();
        let m = encoder.encode(row).unwrap();
        let encoded: Vec<u8> = m.ser_to().unwrap();
        assert_eq!(
            encoded,
            // Hint: write the binary output to a file `test.binpb`, and view it with `protoc`:
            // ```
            // protoc --decode_raw < test.binpb
            // protoc --decode=recursive.AllTypes recursive.proto < test.binpb
            // ```
            [
                9, 0, 0, 0, 0, 0, 0, 17, 64, 21, 0, 0, 96, 64, 24, 22, 32, 23, 56, 48, 93, 26, 0,
                0, 0, 97, 27, 0, 0, 0, 0, 0, 0, 0, 104, 1, 114, 10, 82, 105, 115, 105, 110, 103,
                87, 97, 118, 101, 122, 2, 190, 239, 138, 1, 2, 8, 1, 146, 1, 3, 4, 0, 4, 186, 1, 3,
                16, 184, 23
            ]
        );
    }

    #[test]
    fn test_encode_proto_repeated() {
        let pool_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/test_data/proto_recursive/recursive.pb");
        let pool_bytes = std::fs::read(pool_path).unwrap();
        let pool = prost_reflect::DescriptorPool::decode(pool_bytes.as_ref()).unwrap();
        let message_descriptor = pool.get_message_by_name("recursive.AllTypes").unwrap();

        let schema = Schema::new(vec![Field::with_name(
            DataType::List(DataType::List(DataType::Int32.into()).into()),
            "repeated_int_field",
        )]);

        let err = validate_fields(
            schema
                .fields
                .iter()
                .map(|f| (f.name.as_str(), &f.data_type)),
            &message_descriptor,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "encode repeated_int_field error: cannot encode integer[] column as int32 field"
        );

        let schema = Schema::new(vec![Field::with_name(
            DataType::List(DataType::Int32.into()),
            "repeated_int_field",
        )]);
        let row = OwnedRow::new(vec![Some(ScalarImpl::List(ListValue::from_iter([
            Some(0),
            None,
            Some(2),
            Some(3),
        ])))]);

        let err = encode_fields(
            schema
                .fields
                .iter()
                .map(|f| (f.name.as_str(), &f.data_type))
                .zip_eq_debug(row.iter()),
            &message_descriptor,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "encode repeated_int_field error: array containing null not allowed as repeated field"
        );
    }

    #[test]
    fn test_encode_proto_err() {
        let pool_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/test_data/proto_recursive/recursive.pb");
        let pool_bytes = std::fs::read(pool_path).unwrap();
        let pool = prost_reflect::DescriptorPool::decode(pool_bytes.as_ref()).unwrap();
        let message_descriptor = pool.get_message_by_name("recursive.AllTypes").unwrap();

        let err = validate_fields(
            std::iter::once(("not_exists", &DataType::Int16)),
            &message_descriptor,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "encode not_exists error: field not in proto"
        );

        let err = validate_fields(
            std::iter::once(("map_field", &DataType::Jsonb)),
            &message_descriptor,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "encode map_field error: field not in proto"
        );
    }
}
