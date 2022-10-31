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

use anyhow::anyhow;
use risingwave_common::array::{ArrayImpl, ArrayRef, DataChunk, Row};
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::value_encoding::deserialize_datum;
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression, Expression};
use crate::{bail, ensure, ExprError, Result};

/// `FieldExpression` access a field from a struct.
#[derive(Debug)]
pub struct FieldExpression {
    return_type: DataType,
    input: BoxedExpression,
    index: usize,
}

impl Expression for FieldExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let array = self.input.eval_checked(input)?;
        if let ArrayImpl::Struct(struct_array) = array.as_ref() {
            Ok(struct_array.field_at(self.index))
        } else {
            Err(anyhow!("expects a struct array ref").into())
        }
    }

    fn eval_row(&self, _input: &Row) -> Result<Datum> {
        Err(anyhow!("expects a struct array ref").into())
    }
}

impl FieldExpression {
    pub fn new(return_type: DataType, input: BoxedExpression, index: usize) -> Self {
        FieldExpression {
            return_type,
            input,
            index,
        }
    }
}

impl<'a> TryFrom<&'a ExprNode> for FieldExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type().unwrap() == Type::Field);

        let ret_type = DataType::from(prost.get_return_type().unwrap());
        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };

        let children = func_call_node.children.to_vec();
        // Field `func_call_node` have 2 child nodes, the first is Field `FuncCall` or
        // `InputRef`, the second is i32 `Literal`.
        let [first, second]: [_; 2] = children.try_into().unwrap();
        let input = expr_build_from_prost(&first)?;
        let RexNode::Constant(value) = second.get_rex_node().unwrap() else {
            bail!("Expected Constant as 1st argument");
        };
        let index = deserialize_datum(value.body.as_slice(), &DataType::Int32)
            .map_err(|e| anyhow!("Failed to deserialize i32, reason: {:?}", e))?
            .unwrap()
            .as_int32()
            .to_owned();

        Ok(FieldExpression::new(ret_type, input, index as usize))
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::array;
    use risingwave_common::array::{DataChunk, F32Array, I32Array, StructArray};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_pb::data::data_type::TypeName;

    use crate::expr::expr_field::FieldExpression;
    use crate::expr::test_utils::{make_field_function, make_i32_literal, make_input_ref};
    use crate::expr::Expression;

    #[test]
    fn test_field_expr() {
        let input_node = make_input_ref(0, TypeName::Struct);
        let literal_node = make_i32_literal(0);
        let field_expr = FieldExpression::try_from(&make_field_function(
            vec![input_node, literal_node],
            TypeName::Int32,
        ))
        .unwrap();
        let array = StructArray::from_slices(
            &[true],
            vec![
                array! { I32Array, [Some(1),Some(2),Some(3),Some(4),Some(5)] }.into(),
                array! { F32Array, [Some(2.0)] }.into(),
            ],
            vec![DataType::Int32, DataType::Float32],
        );

        let data_chunk = DataChunk::new(vec![array.into()], 1);
        let res = field_expr.eval(&data_chunk).unwrap();
        assert_eq!(res.datum_at(0), Some(ScalarImpl::Int32(1)));
        assert_eq!(res.datum_at(1), Some(ScalarImpl::Int32(2)));
        assert_eq!(res.datum_at(2), Some(ScalarImpl::Int32(3)));
        assert_eq!(res.datum_at(3), Some(ScalarImpl::Int32(4)));
        assert_eq!(res.datum_at(4), Some(ScalarImpl::Int32(5)));
    }

    #[test]
    fn test_nested_field_expr() {
        let field_node = make_field_function(
            vec![make_input_ref(0, TypeName::Struct), make_i32_literal(0)],
            TypeName::Int32,
        );
        let field_expr = FieldExpression::try_from(&make_field_function(
            vec![field_node, make_i32_literal(1)],
            TypeName::Int32,
        ))
        .unwrap();

        let struct_array = StructArray::from_slices(
            &[true],
            vec![
                array! { I32Array, [Some(1),Some(2),Some(3),Some(4),Some(5)] }.into(),
                array! { F32Array, [Some(1.0),Some(2.0),Some(3.0),Some(4.0),Some(5.0)] }.into(),
            ],
            vec![DataType::Int32, DataType::Float32],
        );
        let array = StructArray::from_slices(
            &[true],
            vec![
                struct_array.into(),
                array! { F32Array, [Some(2.0),Some(2.0),Some(2.0),Some(2.0),Some(2.0)] }.into(),
            ],
            vec![DataType::Int32, DataType::Float32],
        );

        let data_chunk = DataChunk::new(vec![array.into()], 1);
        let res = field_expr.eval(&data_chunk).unwrap();
        assert_eq!(res.datum_at(0), Some(ScalarImpl::Float32(1.0.into())));
        assert_eq!(res.datum_at(1), Some(ScalarImpl::Float32(2.0.into())));
        assert_eq!(res.datum_at(2), Some(ScalarImpl::Float32(3.0.into())));
        assert_eq!(res.datum_at(3), Some(ScalarImpl::Float32(4.0.into())));
        assert_eq!(res.datum_at(4), Some(ScalarImpl::Float32(5.0.into())));
    }
}
