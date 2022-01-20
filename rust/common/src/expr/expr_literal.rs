use std::convert::{TryFrom, TryInto};
use std::str::FromStr;
use std::sync::Arc;

use prost::DecodeError;
use risingwave_pb::data::data_type::IntervalType::*;
use risingwave_pb::data::data_type::{IntervalType, TypeName};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::array::{Array, ArrayBuilder, ArrayBuilderImpl, ArrayRef, DataChunk};
use crate::error::ErrorCode::InternalError;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::Expression;
use crate::types::{DataTypeKind, Datum, Decimal, IntervalUnit, Scalar, ScalarImpl};

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

#[derive(Debug)]
pub struct LiteralExpression {
    return_type: DataTypeKind,
    literal: Datum,
}

impl Expression for LiteralExpression {
    fn return_type(&self) -> DataTypeKind {
        self.return_type
    }

    fn eval(&mut self, input: &DataChunk) -> Result<ArrayRef> {
        let mut array_builder = self.return_type.create_array_builder(input.cardinality())?;
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
                    | (DataTypeKind::Char, ScalarImpl::Utf8(_))
                    | (DataTypeKind::Varchar, ScalarImpl::Utf8(_))
            )
        }
        None => true,
    }
}

fn make_interval(bytes: &[u8], ty: IntervalType) -> Result<IntervalUnit> {
    match ty {
        // the unit is months
        Year | YearToMonth | Month => {
            let bytes = bytes.try_into().map_err(|e| {
                InternalError(format!("Failed to deserialize i32, reason: {:?}", e))
            })?;
            let mouths = i32::from_be_bytes(bytes);
            Ok(IntervalUnit::from_month(mouths))
        }
        // the unit is ms
        Day | DayToHour | DayToMinute | DayToSecond | Hour | HourToMinute | HourToSecond
        | Minute | MinuteToSecond | Second => {
            let bytes = bytes.try_into().map_err(|e| {
                InternalError(format!("Failed to deserialize i64, reason: {:?}", e))
            })?;
            let ms = i64::from_be_bytes(bytes);
            Ok(IntervalUnit::from_millis(ms))
        }
        Invalid => Err(InternalError(format!("Invalid interval type {:?}", ty)).into()),
    }
}

impl LiteralExpression {
    pub fn new(return_type: DataTypeKind, literal: Datum) -> Self {
        assert!(literal_type_match(return_type, literal.as_ref()));
        LiteralExpression {
            return_type,
            literal,
        }
    }

    fn literal(&self) -> Datum {
        self.literal.clone()
    }
}

impl<'a> TryFrom<&'a ExprNode> for LiteralExpression {
    type Error = RwError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.expr_type == Type::ConstantValue as i32);
        let ret_type = DataTypeKind::from(prost.get_return_type()?);
        if prost.rex_node.is_none() {
            return Ok(Self {
                return_type: ret_type,
                literal: None,
            });
        }

        if let RexNode::Constant(prost_value) = prost.get_rex_node()? {
            // TODO: We need to unify these
            let value = match prost.get_return_type()?.get_type_name()? {
                TypeName::Boolean => ScalarImpl::Bool(
                    i8::from_be_bytes(prost_value.get_body().as_slice().try_into().map_err(
                        |e| InternalError(format!("Failed to deserialize bool, reason: {:?}", e)),
                    )?) == 1,
                ),
                TypeName::Int16 => ScalarImpl::Int16(i16::from_be_bytes(
                    prost_value.get_body().as_slice().try_into().map_err(|e| {
                        InternalError(format!("Failed to deserialize i16, reason: {:?}", e))
                    })?,
                )),
                TypeName::Int32 => ScalarImpl::Int32(i32::from_be_bytes(
                    prost_value.get_body().as_slice().try_into().map_err(|e| {
                        InternalError(format!("Failed to deserialize i32, reason: {:?}", e))
                    })?,
                )),
                TypeName::Int64 => ScalarImpl::Int64(i64::from_be_bytes(
                    prost_value.get_body().as_slice().try_into().map_err(|e| {
                        InternalError(format!("Failed to deserialize i64, reason: {:?}", e))
                    })?,
                )),
                TypeName::Float => ScalarImpl::Float32(
                    f32::from_be_bytes(prost_value.get_body().as_slice().try_into().map_err(
                        |e| InternalError(format!("Failed to deserialize f32, reason: {:?}", e)),
                    )?)
                    .into(),
                ),
                TypeName::Double => ScalarImpl::Float64(
                    f64::from_be_bytes(prost_value.get_body().as_slice().try_into().map_err(
                        |e| InternalError(format!("Failed to deserialize f64, reason: {:?}", e)),
                    )?)
                    .into(),
                ),
                TypeName::Char | TypeName::Symbol => ScalarImpl::Utf8(
                    std::str::from_utf8(prost_value.get_body())
                        .map_err(|e| {
                            InternalError(format!("Failed to deserialize char, reason: {:?}", e))
                        })?
                        .to_string(),
                ),
                TypeName::Varchar => ScalarImpl::Utf8(
                    std::str::from_utf8(prost_value.get_body())
                        .map_err(|e| {
                            InternalError(format!("Failed to deserialize varchar, reason: {:?}", e))
                        })?
                        .to_string(),
                ),
                TypeName::Decimal => ScalarImpl::Decimal(
                    Decimal::from_str(std::str::from_utf8(prost_value.get_body()).unwrap())
                        .map_err(|e| {
                            InternalError(format!("Failed to deserialize decimal, reason: {:?}", e))
                        })?,
                ),
                TypeName::Interval => {
                    let bytes = prost_value.get_body();
                    ScalarImpl::Interval(make_interval(
                        bytes,
                        prost.get_return_type()?.get_interval_type()?,
                    )?)
                }
                _ => {
                    return Err(InternalError(format!(
                        "Unrecognized type name: {:?}",
                        prost.get_return_type()?.get_type_name()
                    ))
                    .into())
                }
            };

            Ok(Self {
                return_type: ret_type,
                literal: Some(value),
            })
        } else {
            Err(RwError::from(ErrorCode::ProstError(DecodeError::new(
                "Cannot parse the RexNode",
            ))))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_pb::data::data_type::IntervalType;
    use risingwave_pb::data::DataType;
    use risingwave_pb::expr::expr_node::Type;
    use risingwave_pb::expr::{ConstantValue, ExprNode};

    use super::*;
    use crate::array::column::Column;
    use crate::array::PrimitiveArray;
    use crate::types::IntoOrdered;

    #[test]
    fn test_expr_literal_from() {
        let v = true;
        let t = TypeName::Boolean;
        let bytes = (v as i8).to_be_bytes().to_vec();
        // construct LiteralExpression in various types below with value 1i8, and expect Err
        for typ in [
            TypeName::Int16,
            TypeName::Int32,
            TypeName::Int64,
            TypeName::Float,
            TypeName::Double,
            TypeName::Interval,
            TypeName::Date,
        ] {
            assert!(
                LiteralExpression::try_from(&make_expression(Some(bytes.clone()), typ)).is_err()
            );
        }
        let expr = LiteralExpression::try_from(&make_expression(Some(bytes), t)).unwrap();
        assert_eq!(v.to_scalar_value(), expr.literal().unwrap());

        let v = 1i16;
        let t = TypeName::Int16;
        let bytes = v.to_be_bytes().to_vec();
        assert!(LiteralExpression::try_from(&make_expression(
            Some(bytes.clone()),
            TypeName::Boolean
        ))
        .is_err());
        let expr = LiteralExpression::try_from(&make_expression(Some(bytes), t)).unwrap();
        assert_eq!(v.to_scalar_value(), expr.literal().unwrap());

        let v = 1i32;
        let t = TypeName::Int32;
        let bytes = v.to_be_bytes().to_vec();
        let expr = LiteralExpression::try_from(&make_expression(Some(bytes), t)).unwrap();
        assert_eq!(v.to_scalar_value(), expr.literal().unwrap());

        let v = 1i64;
        let t = TypeName::Int64;
        let bytes = v.to_be_bytes().to_vec();
        let expr = LiteralExpression::try_from(&make_expression(Some(bytes), t)).unwrap();
        assert_eq!(v.to_scalar_value(), expr.literal().unwrap());

        let v = 1f32.into_ordered();
        let t = TypeName::Float;
        let bytes = v.to_be_bytes().to_vec();
        let expr = LiteralExpression::try_from(&make_expression(Some(bytes), t)).unwrap();
        assert_eq!(v.to_scalar_value(), expr.literal().unwrap());

        let v = 1f64.into_ordered();
        let t = TypeName::Double;
        let bytes = v.to_be_bytes().to_vec();
        let expr = LiteralExpression::try_from(&make_expression(Some(bytes), t)).unwrap();
        assert_eq!(v.to_scalar_value(), expr.literal().unwrap());

        let v = None;
        let t = TypeName::Float;
        let expr = LiteralExpression::try_from(&make_expression(None, t)).unwrap();
        assert_eq!(v, expr.literal());

        let v = String::from("varchar");
        let t = TypeName::Varchar;
        let bytes = v.as_bytes().to_vec();
        let expr = LiteralExpression::try_from(&make_expression(Some(bytes), t)).unwrap();
        assert_eq!(v.to_scalar_value(), expr.literal().unwrap());

        let v = Decimal::new(3141, 3);
        let t = TypeName::Decimal;
        let bytes = v.to_string().as_bytes().to_vec();
        let expr = LiteralExpression::try_from(&make_expression(Some(bytes), t)).unwrap();
        assert_eq!(v.to_scalar_value(), expr.literal().unwrap());

        let v = String::from("NaN");
        let t = TypeName::Decimal;
        let bytes = v.as_bytes().to_vec();
        assert!(LiteralExpression::try_from(&make_expression(Some(bytes), t)).is_ok());

        let v = 32i32;
        let t = TypeName::Interval;
        let bytes = v.to_be_bytes().to_vec();
        let expr = LiteralExpression::try_from(&make_expression(Some(bytes), t)).unwrap();
        assert_eq!(
            IntervalUnit::from_month(v).to_scalar_value(),
            expr.literal().unwrap()
        );
    }

    fn make_expression(bytes: Option<Vec<u8>>, data_type: TypeName) -> ExprNode {
        ExprNode {
            expr_type: Type::ConstantValue as i32,
            return_type: Some(DataType {
                type_name: data_type as i32,
                interval_type: IntervalType::Month as i32,
                ..Default::default()
            }),
            rex_node: bytes.map(|bs| RexNode::Constant(ConstantValue { body: bs })),
        }
    }

    fn create_column(vec: &[Option<i32>]) -> Result<Column> {
        let array = PrimitiveArray::from_slice(vec).map(|x| Arc::new(x.into()))?;
        Ok(Column::new(array))
    }
}
