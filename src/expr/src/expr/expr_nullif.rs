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

use itertools::Itertools;
use risingwave_common::array::{Array, ArrayImpl, ArrayRef, DataChunk};
use risingwave_common::error::{internal_error, Result, RwError};
use risingwave_common::types::{DataType, ToOwnedDatum};
use risingwave_common::{ensure, ensure_eq, try_match_expand};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression, Expression};

#[derive(Debug)]
pub struct NullifExpression {
    return_type: DataType,
    origin: BoxedExpression,
    is_equal: BoxedExpression,
}

impl Expression for NullifExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let origin = self.origin.eval(input)?;
        let is_equal = self.is_equal.eval(input)?;
        let mut builder = self.return_type.create_array_builder(input.cardinality())?;

        if let ArrayImpl::Bool(bool_array) = is_equal.as_ref() {
            // If `is_equal` is true, add null otherwise add `origin` value.
            origin
                .iter()
                .zip_eq(bool_array.iter())
                .try_for_each(|(c, d)| {
                    if let Some(false) = d {
                        builder.append_datum(&c.to_owned_datum())
                    } else {
                        builder.append_null()
                    }
                })?;
            Ok(Arc::new(builder.finish()?))
        } else {
            Err(internal_error("expects a bool array ref"))
        }
    }
}

impl NullifExpression {
    pub fn new(return_type: DataType, origin: BoxedExpression, is_equal: BoxedExpression) -> Self {
        NullifExpression {
            return_type,
            origin,
            is_equal,
        }
    }
}

impl<'a> TryFrom<&'a ExprNode> for NullifExpression {
    type Error = RwError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type()? == Type::Nullif);

        let ret_type = DataType::from(prost.get_return_type()?);
        let func_call_node = try_match_expand!(prost.get_rex_node().unwrap(), RexNode::FuncCall)?;

        let children = func_call_node.children.to_vec();
        // Nullif `func_call_node` have 2 child nodes.
        ensure_eq!(children.len(), 2);
        let origin = expr_build_from_prost(&children[0])?;
        let is_equal = expr_build_from_prost(&children[1])?;
        Ok(NullifExpression::new(ret_type, origin, is_equal))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::column::Column;
    use risingwave_common::array::{BoolArray, DataChunk, PrimitiveArray, Utf8Array};
    use risingwave_common::types::ScalarImpl;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType as ProstDataType;
    use risingwave_pb::expr::expr_node::RexNode;
    use risingwave_pb::expr::expr_node::Type::Nullif;
    use risingwave_pb::expr::{ExprNode, FunctionCall};

    use crate::expr::expr_nullif::NullifExpression;
    use crate::expr::test_utils::make_input_ref;
    use crate::expr::Expression;

    pub fn make_nullif_function(children: Vec<ExprNode>, ret: TypeName) -> ExprNode {
        ExprNode {
            expr_type: Nullif as i32,
            return_type: Some(ProstDataType {
                type_name: ret as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall { children })),
        }
    }

    #[test]
    fn test_nullif_expr() {
        let input_node1 = make_input_ref(0, TypeName::Int32);
        let input_node2 = make_input_ref(1, TypeName::Int32);
        let input_node3 = make_input_ref(2, TypeName::Varchar);

        let array = PrimitiveArray::<i32>::from_slice(&[Some(2), Some(2), Some(4), Some(3)])
            .map(|x| Arc::new(x.into()))
            .unwrap();
        let col1 = Column::new(array);
        let array = BoolArray::from_slice(&[Some(false), Some(false), Some(true), Some(true)])
            .map(|x| Arc::new(x.into()))
            .unwrap();
        let col2 = Column::new(array);
        let array = Utf8Array::from_slice(&[Some("2"), Some("2"), Some("4"), Some("3")])
            .map(|x| Arc::new(x.into()))
            .unwrap();
        let col3 = Column::new(array);

        let data_chunk = DataChunk::builder().columns(vec![col1, col2, col3]).build();

        let nullif_expr = NullifExpression::try_from(&make_nullif_function(
            vec![input_node1, input_node2.clone()],
            TypeName::Int32,
        ))
        .unwrap();
        let res = nullif_expr.eval(&data_chunk).unwrap();
        assert_eq!(res.datum_at(0), Some(ScalarImpl::Int32(2)));
        assert_eq!(res.datum_at(1), Some(ScalarImpl::Int32(2)));
        assert_eq!(res.datum_at(2), None);
        assert_eq!(res.datum_at(3), None);

        let nullif_expr = NullifExpression::try_from(&make_nullif_function(
            vec![input_node3, input_node2],
            TypeName::Varchar,
        ))
        .unwrap();
        let res = nullif_expr.eval(&data_chunk).unwrap();
        assert_eq!(res.datum_at(0), Some(ScalarImpl::Utf8("2".to_string())));
        assert_eq!(res.datum_at(1), Some(ScalarImpl::Utf8("2".to_string())));
        assert_eq!(res.datum_at(2), None);
        assert_eq!(res.datum_at(3), None);
    }
}
