use crate::array::{Array, ArrayBuilder, ArrayBuilderImpl, ArrayRef, DataChunk};
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};
use crate::expr::Expression;
use crate::types::{build_from_proto, DataType, DataTypeKind, DataTypeRef, Datum, ScalarImpl};
use std::convert::TryFrom;
use std::convert::TryInto;

use protobuf::Message;

use risingwave_proto::data::DataType_TypeName;
use risingwave_proto::expr::{ConstantValue, ExprNode, ExprNode_Type};

use rust_decimal::Decimal;
use std::ops::Deref;
use std::sync::Arc;

use crate::types::Scalar;
use std::str::FromStr;

macro_rules! array_impl_literal_append {
  ([$arr_builder: ident, $literal: ident, $cardinality: ident], $( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
    match ($arr_builder, $literal) {
      $(
      (ArrayBuilderImpl::$variant_name(inner), Some(ScalarImpl::$variant_name(v))) => {
       append_literal_to_arr(inner, Some(v.as_scalar_ref()), $cardinality)?;
      }
      (ArrayBuilderImpl::$variant_name(inner), None) => {
       append_literal_to_arr(inner, None, $cardinality)?;
      }
      )*
      (_, _) => unimplemented!("Do not support values in insert values executor"),
    }
  };
}

pub struct LiteralExpression {
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

    fn eval(&mut self, input: &DataChunk) -> Result<ArrayRef> {
        let mut array_builder =
            DataType::create_array_builder(self.return_type.clone(), input.cardinality())?;
        let cardinality = input.cardinality();
        let builder = &mut array_builder;
        let literal = &self.literal;
        for_all_variants! {array_impl_literal_append, builder, literal, cardinality}
        array_builder.finish().map(Arc::new)
    }
}

fn append_literal_to_arr<'a, A1>(
    a: &'a mut A1,
    v: Option<<<A1 as ArrayBuilder>::ArrayType as Array>::RefItem<'a>>,
    cardinality: usize,
) -> Result<()>
where
    A1: ArrayBuilder,
{
    for _ in 0..cardinality {
        a.append(v)?
    }
    Ok(())
}

fn literal_type_match(return_type: DataTypeKind, literal: Option<&ScalarImpl>) -> bool {
    match literal {
        Some(datum) => {
            matches!(
                (return_type, datum),
                (DataTypeKind::Boolean, ScalarImpl::Bool(_))
                    | (DataTypeKind::Int16, ScalarImpl::Int16(_))
                    | (DataTypeKind::Int32, ScalarImpl::Int32(_))
                    | (DataTypeKind::Int64, ScalarImpl::Int64(_))
                    | (DataTypeKind::Float32, ScalarImpl::Float32(_))
                    | (DataTypeKind::Float64, ScalarImpl::Float64(_))
                    | (DataTypeKind::Date, ScalarImpl::Int32(_))
                    | (DataTypeKind::Char, ScalarImpl::UTF8(_))
                    | (DataTypeKind::Varchar, ScalarImpl::UTF8(_))
            )
        }

        None => true,
    }
}

impl LiteralExpression {
    pub fn new(return_type: DataTypeRef, literal: Datum) -> Self {
        assert!(literal_type_match(
            return_type.deref().data_type_kind(),
            literal.as_ref()
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
        ensure!(proto.expr_type == ExprNode_Type::CONSTANT_VALUE);
        let data_type = build_from_proto(proto.get_return_type())?;

        let proto_value =
            ConstantValue::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;

        // TODO: We need to unify these
        // TODO: Add insert NULL
        let value = match proto.get_return_type().get_type_name() {
            DataType_TypeName::INT16 => ScalarImpl::Int16(i16::from_be_bytes(
                proto_value.get_body().try_into().map_err(|e| {
                    InternalError(format!("Failed to deserialize i16, reason: {:?}", e))
                })?,
            )),
            DataType_TypeName::INT32 => ScalarImpl::Int32(i32::from_be_bytes(
                proto_value.get_body().try_into().map_err(|e| {
                    InternalError(format!("Failed to deserialize i32, reason: {:?}", e))
                })?,
            )),
            DataType_TypeName::INT64 => ScalarImpl::Int64(i64::from_be_bytes(
                proto_value.get_body().try_into().map_err(|e| {
                    InternalError(format!("Failed to deserialize i64, reason: {:?}", e))
                })?,
            )),
            DataType_TypeName::FLOAT => ScalarImpl::Float32(f32::from_be_bytes(
                proto_value.get_body().try_into().map_err(|e| {
                    InternalError(format!("Failed to deserialize f32, reason: {:?}", e))
                })?,
            )),
            DataType_TypeName::DOUBLE => ScalarImpl::Float64(f64::from_be_bytes(
                proto_value.get_body().try_into().map_err(|e| {
                    InternalError(format!("Failed to deserialize f64, reason: {:?}", e))
                })?,
            )),
            DataType_TypeName::CHAR => ScalarImpl::UTF8(
                std::str::from_utf8(proto_value.get_body())
                    .map_err(|e| {
                        InternalError(format!("Failed to deserialize char, reason: {:?}", e))
                    })?
                    .to_string(),
            ),
            DataType_TypeName::VARCHAR => ScalarImpl::UTF8(
                std::str::from_utf8(proto_value.get_body())
                    .map_err(|e| {
                        InternalError(format!("Failed to deserialize varchar, reason: {:?}", e))
                    })?
                    .to_string(),
            ),
            DataType_TypeName::DECIMAL => ScalarImpl::Decimal(
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
            // FIXME: add NULL value
            literal: Some(value),
        })
    }
}
