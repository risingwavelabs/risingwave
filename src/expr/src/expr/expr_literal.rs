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

use std::convert::TryFrom;
use std::sync::Arc;

use risingwave_common::array::{Array, ArrayBuilder, ArrayBuilderImpl, ArrayRef, DataChunk};
use risingwave_common::for_all_variants;
use risingwave_common::row::Row;
use risingwave_common::types::{literal_type_match, DataType, Datum, Scalar, ScalarImpl};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::Expression;
use crate::{bail, ensure, ExprError, Result};

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
            (_, _) => $crate::bail!(
                "Do not support values in insert values executor".to_string()
            ),
        }
    };
}

#[derive(Debug)]
pub struct LiteralExpression {
    return_type: DataType,
    literal: Datum,
}

impl Expression for LiteralExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let mut array_builder = self.return_type.create_array_builder(input.capacity());
        let capacity = input.capacity();
        let builder = &mut array_builder;
        let literal = &self.literal;
        for_all_variants! {array_impl_literal_append, builder, literal, capacity}
        array_builder.finish().map(Arc::new).map_err(Into::into)
    }

    fn eval_row(&self, _input: &Row) -> Result<Datum> {
        Ok(self.literal.as_ref().cloned())
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

impl LiteralExpression {
    pub fn new(return_type: DataType, literal: Datum) -> Self {
        assert!(literal_type_match(&return_type, literal.as_ref()));
        LiteralExpression {
            return_type,
            literal,
        }
    }

    pub fn literal(&self) -> Datum {
        self.literal.clone()
    }
}

impl<'a> TryFrom<&'a ExprNode> for LiteralExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type().unwrap() == Type::ConstantValue);
        let ret_type = DataType::from(prost.get_return_type().unwrap());
        if prost.rex_node.is_none() {
            return Ok(Self {
                return_type: ret_type,
                literal: None,
            });
        }

        if let RexNode::Constant(prost_value) = prost.get_rex_node().unwrap() {
            // TODO: We need to unify these
            let value = ScalarImpl::from_proto_bytes(
                prost_value.get_body(),
                prost.get_return_type().unwrap(),
            )?;
            Ok(Self {
                return_type: ret_type,
                literal: Some(value),
            })
        } else {
            bail!("Cannot parse the RexNode");
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{I32Array, StructValue};
    use risingwave_common::array_nonnull;
    use risingwave_common::types::{Decimal, IntervalUnit, IntoOrdered};
    use risingwave_pb::data::data_type::{IntervalType, TypeName};
    use risingwave_pb::data::DataType as ProstDataType;
    use risingwave_pb::expr::expr_node::RexNode::Constant;
    use risingwave_pb::expr::expr_node::Type;
    use risingwave_pb::expr::{ConstantValue, ExprNode};

    use super::*;

    #[test]
    fn test_struct_expr_literal_from() {
        let value = StructValue::new(vec![
            Some(ScalarImpl::Utf8("12222".to_string())),
            Some(2.into()),
            None,
        ]);
        let body = value.to_protobuf_owned();
        let expr = ExprNode {
            expr_type: Type::ConstantValue as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Struct as i32,
                field_type: vec![
                    ProstDataType {
                        type_name: TypeName::Varchar as i32,
                        ..Default::default()
                    },
                    ProstDataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    },
                    ProstDataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            rex_node: Some(Constant(ConstantValue { body })),
        };
        let expr = LiteralExpression::try_from(&expr).unwrap();
        assert_eq!(value.to_scalar_value(), expr.literal().unwrap());
    }

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
            TypeName::Struct,
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
            TypeName::Boolean,
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
            return_type: Some(ProstDataType {
                type_name: data_type as i32,
                interval_type: IntervalType::Month as i32,
                ..Default::default()
            }),
            rex_node: bytes.map(|bs| RexNode::Constant(ConstantValue { body: bs })),
        }
    }

    #[test]
    fn test_literal_eval_dummy_chunk() {
        let literal = LiteralExpression::new(DataType::Int32, Some(1.into()));
        let result = literal.eval(&DataChunk::new_dummy(1)).unwrap();
        assert_eq!(*result, array_nonnull!(I32Array, [1]).into());
    }

    #[test]
    fn test_literal_eval_row_dummy_chunk() {
        let literal = LiteralExpression::new(DataType::Int32, Some(1.into()));
        let result = literal.eval_row(&Row::new(vec![])).unwrap();
        assert_eq!(result, Some(1.into()))
    }
}
