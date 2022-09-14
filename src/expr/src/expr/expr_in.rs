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

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::{ArrayBuilder, ArrayRef, BoolArrayBuilder, DataChunk, Row};
use risingwave_common::types::{DataType, Datum, Scalar, ToOwnedDatum};
use risingwave_common::{bail, ensure};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost, BoxedExpression, Expression};
use crate::{ExprError, Result};

#[derive(Debug)]
pub(crate) struct InExpression {
    left: BoxedExpression,
    set: HashSet<Datum>,
    return_type: DataType,
}

impl InExpression {
    pub fn new(
        left: BoxedExpression,
        data: impl Iterator<Item = Datum>,
        return_type: DataType,
    ) -> Self {
        let mut sarg = HashSet::new();
        for datum in data {
            sarg.insert(datum);
        }
        Self {
            left,
            set: sarg,
            return_type,
        }
    }

    // Returns true if datum exists in set, null if datum is null or datum does not exist in set
    // but null does, and false if neither datum nor null exists in set.
    fn exists(&self, datum: &Datum) -> Option<bool> {
        if datum.is_none() {
            None
        } else if self.set.contains(datum) {
            Some(true)
        } else if self.set.contains(&None) {
            None
        } else {
            Some(false)
        }
    }
}

impl Expression for InExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let input_array = self.left.eval_checked(input)?;
        let mut output_array = BoolArrayBuilder::new(input_array.len());
        for (data, vis) in input_array.iter().zip_eq(input.vis().iter()) {
            if vis {
                let ret = self.exists(&data.to_owned_datum());
                output_array.append(ret)?;
            } else {
                output_array.append(None)?;
            }
        }
        Ok(Arc::new(output_array.finish().into()))
    }

    fn eval_row(&self, input: &Row) -> Result<Datum> {
        let data = self.left.eval_row(input)?;
        let ret = self.exists(&data);
        Ok(ret.map(|b| b.to_scalar_value()))
    }
}

impl<'a> TryFrom<&'a ExprNode> for InExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type().unwrap() == Type::In);

        let ret_type = DataType::from(prost.get_return_type().unwrap());
        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };
        let children = &func_call_node.children;

        let left_expr = build_from_prost(&children[0])?;
        let mut data = Vec::new();
        // Used for const expression below to generate datum.
        // Frontend has made sure these can all be folded to constants.
        let data_chunk = DataChunk::new_dummy(1);
        for child in &children[1..] {
            let const_expr = build_from_prost(child)?;
            let array = const_expr.eval(&data_chunk)?;
            let datum = array.value_at(0).to_owned_datum();
            data.push(datum);
        }
        Ok(InExpression::new(left_expr, data.into_iter(), ret_type))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, Row};
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::{DataType, Scalar, ScalarImpl};
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType as ProstDataType;
    use risingwave_pb::expr::expr_node::{RexNode, Type};
    use risingwave_pb::expr::{ConstantValue, ExprNode, FunctionCall, InputRefExpr};

    use crate::expr::expr_in::InExpression;
    use crate::expr::{Expression, InputRefExpression};

    #[test]
    fn test_in_expr() {
        let input_ref = InputRefExpr { column_idx: 0 };
        let input_ref_expr_node = ExprNode {
            expr_type: Type::InputRef as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Varchar as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::InputRef(input_ref)),
        };
        let constant_values = vec![
            ExprNode {
                expr_type: Type::ConstantValue as i32,
                return_type: Some(ProstDataType {
                    type_name: TypeName::Varchar as i32,
                    ..Default::default()
                }),
                rex_node: Some(RexNode::Constant(ConstantValue {
                    body: "ABC".as_bytes().to_vec(),
                })),
            },
            ExprNode {
                expr_type: Type::ConstantValue as i32,
                return_type: Some(ProstDataType {
                    type_name: TypeName::Varchar as i32,
                    ..Default::default()
                }),
                rex_node: Some(RexNode::Constant(ConstantValue {
                    body: "def".as_bytes().to_vec(),
                })),
            },
        ];
        let mut in_children = vec![input_ref_expr_node];
        in_children.extend(constant_values.into_iter());
        let call = FunctionCall {
            children: in_children,
        };
        let p = ExprNode {
            expr_type: Type::In as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Boolean as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(call)),
        };
        assert!(InExpression::try_from(&p).is_ok());
    }

    #[test]
    fn test_eval_search_expr() {
        let input_refs = [
            Box::new(InputRefExpression::new(DataType::Varchar, 0)),
            Box::new(InputRefExpression::new(DataType::Varchar, 0)),
        ];
        let data = [
            vec![
                Some(ScalarImpl::Utf8("abc".to_string())),
                Some(ScalarImpl::Utf8("def".to_string())),
            ],
            vec![None, Some(ScalarImpl::Utf8("abc".to_string()))],
        ];

        let data_chunks = [
            DataChunk::from_pretty(
                "T
                 abc
                 a
                 def
                 abc
                 .",
            )
            .with_invisible_holes(),
            DataChunk::from_pretty(
                "T
                abc
                a
                .",
            )
            .with_invisible_holes(),
        ];

        let expected = vec![
            vec![Some(true), Some(false), Some(true), Some(true), None],
            vec![Some(true), None, None],
        ];

        for (i, input_ref) in input_refs.into_iter().enumerate() {
            let search_expr =
                InExpression::new(input_ref, data[i].clone().into_iter(), DataType::Boolean);
            let vis = data_chunks[i].get_visibility_ref();
            let res = search_expr
                .eval(&data_chunks[i])
                .unwrap()
                .compact(vis.unwrap(), expected[i].len())
                .unwrap();

            for (i, expect) in expected[i].iter().enumerate() {
                assert_eq!(res.datum_at(i), expect.map(ScalarImpl::Bool));
            }
        }
    }

    #[test]
    fn test_eval_row_search_expr() {
        let input_refs = [
            Box::new(InputRefExpression::new(DataType::Varchar, 0)),
            Box::new(InputRefExpression::new(DataType::Varchar, 0)),
        ];

        let data = [
            vec![
                Some(ScalarImpl::Utf8("abc".to_string())),
                Some(ScalarImpl::Utf8("def".to_string())),
            ],
            vec![None, Some(ScalarImpl::Utf8("abc".to_string()))],
        ];

        let row_inputs = vec![
            vec![Some("abc"), Some("a"), Some("def"), None],
            vec![Some("abc"), Some("a"), None],
        ];

        let expected = [
            vec![Some(true), Some(false), Some(true), None],
            vec![Some(true), None, None],
        ];

        for (i, input_ref) in input_refs.into_iter().enumerate() {
            let search_expr =
                InExpression::new(input_ref, data[i].clone().into_iter(), DataType::Boolean);

            for (j, row_input) in row_inputs[i].iter().enumerate() {
                let row_input = vec![row_input.map(|s| s.to_string().to_scalar_value())];
                let row = Row::new(row_input);
                let result = search_expr.eval_row(&row).unwrap();
                assert_eq!(result, expected[i][j].map(ScalarImpl::Bool));
            }
        }
    }
}
