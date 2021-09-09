use crate::array2::{ArrayBuilder, ArrayBuilderImpl, ArrayRef, DataChunk};
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};
use crate::expr::Expression;
use crate::types::{build_from_proto, DataType, DataTypeKind, DataTypeRef};
use std::convert::TryFrom;
use std::convert::TryInto;

use protobuf::Message;

use risingwave_proto::data::DataType_TypeName;
use risingwave_proto::expr::{ConstantValue, ExprNode, ExprNode_ExprNodeType};

use crate::array::interval_array::IntervalValue;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum Datum {
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Decimal(Decimal),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    UTF8String(String),
    Interval(IntervalValue),
}

pub(super) struct LiteralExpression {
    return_type: DataTypeRef,
    literal: Datum,
}

impl Expression for LiteralExpression {
    fn return_type(&self) -> &dyn DataType {
        &*self.return_type
    }

    fn return_type_ref(&self) -> DataTypeRef {
        self.return_type.clone()
    }

    fn eval(&mut self, _input: &DataChunk) -> Result<ArrayRef> {
        let mut array_builder =
            DataType::create_array_builder(self.return_type.clone(), _input.cardinality())?;
        for _ in 0.._input.cardinality() {
            match &mut array_builder {
                // FIXME: refactor in a generic/macro way
                ArrayBuilderImpl::Int32(inner) => {
                    let v = match &self.literal {
                        Datum::Int32(v) => *v,
                        _ => unimplemented!(),
                    };
                    inner.append(Some(v))?;
                }
                ArrayBuilderImpl::UTF8(inner) => {
                    let v = match &self.literal {
                        Datum::UTF8String(ref v) => v,
                        _ => unimplemented!(),
                    };
                    inner.append(Some(v))?;
                }

                _ => unimplemented!(),
            }
        }
        array_builder.finish().map(Arc::new)
    }
}

fn literal_type_match(return_type: DataTypeKind, literal: Datum) -> bool {
    matches!(
        (return_type, literal),
        (DataTypeKind::Boolean, Datum::Bool(_))
            | (DataTypeKind::Int16, Datum::Int16(_))
            | (DataTypeKind::Int32, Datum::Int32(_))
            | (DataTypeKind::Int64, Datum::Int64(_))
            | (DataTypeKind::Float32, Datum::Float32(_))
            | (DataTypeKind::Float64, Datum::Float64(_))
            | (DataTypeKind::Decimal, Datum::Decimal(_))
            | (DataTypeKind::Date, Datum::Int32(_))
            | (DataTypeKind::Char, Datum::UTF8String(_))
            | (DataTypeKind::Varchar, Datum::UTF8String(_))
            | (DataTypeKind::Interval, Datum::Int32(_))
    )
}

impl LiteralExpression {
    pub fn new(return_type: DataTypeRef, literal: Datum) -> Self {
        assert!(literal_type_match(
            return_type.deref().data_type_kind(),
            literal.clone()
        ));
        LiteralExpression {
            return_type,
            literal,
        }
    }
}

impl<'a> TryFrom<&'a ExprNode> for LiteralExpression {
    type Error = RwError;

    fn try_from(proto: &'a ExprNode) -> Result<Self> {
        ensure!(proto.expr_type == ExprNode_ExprNodeType::CONSTANT_VALUE);
        let data_type = build_from_proto(proto.get_return_type())?;

        let proto_value =
            ConstantValue::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;

        // TODO: We need to unify these
        let value = match proto.get_return_type().get_type_name() {
            DataType_TypeName::INT16 => Datum::Int16(i16::from_be_bytes(
                proto_value.get_body().try_into().map_err(|e| {
                    InternalError(format!("Failed to deserialize i16, reason: {:?}", e))
                })?,
            )),
            DataType_TypeName::INT32 => Datum::Int32(i32::from_be_bytes(
                proto_value.get_body().try_into().map_err(|e| {
                    InternalError(format!("Failed to deserialize i32, reason: {:?}", e))
                })?,
            )),
            DataType_TypeName::INT64 => Datum::Int64(i64::from_be_bytes(
                proto_value.get_body().try_into().map_err(|e| {
                    InternalError(format!("Failed to deserialize i64, reason: {:?}", e))
                })?,
            )),
            DataType_TypeName::FLOAT => Datum::Float32(f32::from_be_bytes(
                proto_value.get_body().try_into().map_err(|e| {
                    InternalError(format!("Failed to deserialize f32, reason: {:?}", e))
                })?,
            )),
            DataType_TypeName::DOUBLE => Datum::Float64(f64::from_be_bytes(
                proto_value.get_body().try_into().map_err(|e| {
                    InternalError(format!("Failed to deserialize f64, reason: {:?}", e))
                })?,
            )),
            DataType_TypeName::CHAR => Datum::UTF8String(
                std::str::from_utf8(proto_value.get_body())
                    .map_err(|e| {
                        InternalError(format!("Failed to deserialize char, reason: {:?}", e))
                    })?
                    .to_string(),
            ),
            DataType_TypeName::VARCHAR => Datum::UTF8String(
                std::str::from_utf8(proto_value.get_body())
                    .map_err(|e| {
                        InternalError(format!("Failed to deserialize varchar, reason: {:?}", e))
                    })?
                    .to_string(),
            ),
            DataType_TypeName::DECIMAL => Datum::Decimal(
                Decimal::from_str(std::str::from_utf8(proto_value.get_body()).unwrap()).map_err(
                    |e| InternalError(format!("Failed to deserialize decimal, reason: {:?}", e)),
                )?,
            ),
            _ => {
                return Err(InternalError(format!(
                    "Unrecognized type name: {:?}",
                    proto.get_return_type().get_type_name()
                ))
                .into())
            }
        };

        Ok(Self {
            return_type: data_type,
            literal: value,
        })
    }
}
