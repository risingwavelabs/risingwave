// Copyright 2025 RisingWave Labs
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

use risingwave_common::array::DataChunk;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, literal_type_match};
use risingwave_common::util::value_encoding::DatumFromProtoExt;
use risingwave_pb::expr::ExprNode;

use super::{Build, ValueImpl};
use crate::expr::Expression;
use crate::{ExprError, Result};

/// A literal expression.
#[derive(Clone, Debug)]
pub struct LiteralExpression {
    return_type: DataType,
    literal: Datum,
}

#[async_trait::async_trait]
impl Expression for LiteralExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        Ok(ValueImpl::Scalar {
            value: self.literal.clone(),
            capacity: input.capacity(),
        })
    }

    async fn eval_row(&self, _input: &OwnedRow) -> Result<Datum> {
        Ok(self.literal.as_ref().cloned())
    }

    fn eval_const(&self) -> Result<Datum> {
        Ok(self.literal.clone())
    }
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

impl Build for LiteralExpression {
    fn build(
        prost: &ExprNode,
        _build_child: impl Fn(&ExprNode) -> Result<super::BoxedExpression>,
    ) -> Result<Self> {
        let ret_type = DataType::from(prost.get_return_type().unwrap());

        let prost_value = prost.get_rex_node().unwrap().as_constant().unwrap();

        let value = Datum::from_protobuf(
            prost_value,
            &DataType::from(prost.get_return_type().unwrap()),
        )
        .map_err(|e| ExprError::Internal(e.into()))?;
        Ok(Self {
            return_type: ret_type,
            literal: value,
        })
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{I32Array, StructValue};
    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_common::types::{Decimal, Interval, IntoOrdered, Scalar, ScalarImpl};
    use risingwave_common::util::value_encoding::{DatumToProtoExt, serialize_datum};
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::{PbDataType, PbDatum};
    use risingwave_pb::expr::expr_node::RexNode::{self, Constant};
    use risingwave_pb::expr::expr_node::Type;

    use super::*;

    #[test]
    fn test_struct_expr_literal_from() {
        let value = StructValue::new(vec![
            Some(ScalarImpl::Utf8("12222".into())),
            Some(2.into()),
            None,
        ]);
        let pb_datum = Some(value.clone().to_scalar_value()).to_protobuf();
        let expr = ExprNode {
            function_type: Type::Unspecified as i32,
            return_type: Some(PbDataType {
                type_name: TypeName::Struct as i32,
                field_type: vec![
                    PbDataType {
                        type_name: TypeName::Varchar as i32,
                        ..Default::default()
                    },
                    PbDataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    },
                    PbDataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            rex_node: Some(Constant(pb_datum)),
        };
        let expr = LiteralExpression::build_for_test(&expr).unwrap();
        assert_eq!(value.to_scalar_value(), expr.literal().unwrap());
    }

    #[test]
    fn test_expr_literal_from() {
        let v = true;
        let t = TypeName::Boolean;
        let bytes = serialize_datum(Some(v.to_scalar_value()).as_ref());

        let expr = LiteralExpression::build_for_test(&make_expression(bytes, t)).unwrap();
        assert_eq!(v.to_scalar_value(), expr.literal().unwrap());

        let v = 1i16;
        let t = TypeName::Int16;
        let bytes = serialize_datum(Some(v.to_scalar_value()).as_ref());

        let expr = LiteralExpression::build_for_test(&make_expression(bytes, t)).unwrap();
        assert_eq!(v.to_scalar_value(), expr.literal().unwrap());

        let v = 1i32;
        let t = TypeName::Int32;
        let bytes = serialize_datum(Some(v.to_scalar_value()).as_ref());
        let expr = LiteralExpression::build_for_test(&make_expression(bytes, t)).unwrap();
        assert_eq!(v.to_scalar_value(), expr.literal().unwrap());

        let v = 1i64;
        let t = TypeName::Int64;
        let bytes = serialize_datum(Some(v.to_scalar_value()).as_ref());
        let expr = LiteralExpression::build_for_test(&make_expression(bytes, t)).unwrap();
        assert_eq!(v.to_scalar_value(), expr.literal().unwrap());

        let v = 1f32.into_ordered();
        let t = TypeName::Float;
        let bytes = serialize_datum(Some(v.to_scalar_value()).as_ref());
        let expr = LiteralExpression::build_for_test(&make_expression(bytes, t)).unwrap();
        assert_eq!(v.to_scalar_value(), expr.literal().unwrap());

        let v = 1f64.into_ordered();
        let t = TypeName::Double;
        let bytes = serialize_datum(Some(v.to_scalar_value()).as_ref());
        let expr = LiteralExpression::build_for_test(&make_expression(bytes, t)).unwrap();
        assert_eq!(v.to_scalar_value(), expr.literal().unwrap());

        let v = None;
        let t = TypeName::Float;
        let bytes = serialize_datum(Datum::None);
        let expr = LiteralExpression::build_for_test(&make_expression(bytes, t)).unwrap();
        assert_eq!(v, expr.literal());

        let v: Box<str> = "varchar".into();
        let t = TypeName::Varchar;
        let bytes = serialize_datum(Some(v.clone().to_scalar_value()).as_ref());
        let expr = LiteralExpression::build_for_test(&make_expression(bytes, t)).unwrap();
        assert_eq!(v.to_scalar_value(), expr.literal().unwrap());

        let v = Decimal::from_i128_with_scale(3141, 3);
        let t = TypeName::Decimal;
        let bytes = serialize_datum(Some(v.to_scalar_value()).as_ref());
        let expr = LiteralExpression::build_for_test(&make_expression(bytes, t)).unwrap();
        assert_eq!(v.to_scalar_value(), expr.literal().unwrap());

        let v = 32i32;
        let t = TypeName::Interval;
        let bytes = serialize_datum(Some(Interval::from_month(v).to_scalar_value()).as_ref());
        let expr = LiteralExpression::build_for_test(&make_expression(bytes, t)).unwrap();
        assert_eq!(
            Interval::from_month(v).to_scalar_value(),
            expr.literal().unwrap()
        );
    }

    fn make_expression(bytes: Vec<u8>, data_type: TypeName) -> ExprNode {
        ExprNode {
            function_type: Type::Unspecified as i32,
            return_type: Some(PbDataType {
                type_name: data_type as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::Constant(PbDatum { body: bytes })),
        }
    }

    #[tokio::test]
    async fn test_literal_eval_dummy_chunk() {
        let literal = LiteralExpression::new(DataType::Int32, Some(1.into()));
        let result = literal.eval(&DataChunk::new_dummy(1)).await.unwrap();
        assert_eq!(*result, I32Array::from_iter([1]).into());
    }

    #[tokio::test]
    async fn test_literal_eval_row_dummy_chunk() {
        let literal = LiteralExpression::new(DataType::Int32, Some(1.into()));
        let result = literal.eval_row(&OwnedRow::new(vec![])).await.unwrap();
        assert_eq!(result, Some(1.into()))
    }
}
