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

use itertools::Itertools;
use risingwave_common::array::{ArrayRef, DataChunk, Row};
use risingwave_common::types::{DataType, Datum, ScalarImpl, ScalarRefImpl, ToOwnedDatum};
use risingwave_common::{bail, ensure};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost, BoxedExpression, Expression};
use crate::{ExprError, Result};

#[derive(Debug)]
pub struct WhenClause {
    pub when: BoxedExpression,
    pub then: BoxedExpression,
}

impl WhenClause {
    pub fn new(when: BoxedExpression, then: BoxedExpression) -> Self {
        WhenClause { when, then }
    }
}

#[derive(Debug)]
pub struct CaseExpression {
    return_type: DataType,
    when_clauses: Vec<WhenClause>,
    else_clause: Option<BoxedExpression>,
}

impl CaseExpression {
    pub fn new(
        return_type: DataType,
        when_clauses: Vec<WhenClause>,
        else_clause: Option<BoxedExpression>,
    ) -> Self {
        Self {
            return_type,
            when_clauses,
            else_clause,
        }
    }
}

impl Expression for CaseExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let vis = input.vis();
        let mut els = self
            .else_clause
            .as_deref()
            .map(|else_clause| else_clause.eval_checked(input).unwrap());
        let when_thens = self
            .when_clauses
            .iter()
            .map(|when_clause| {
                (
                    when_clause.when.eval_checked(input).unwrap(),
                    when_clause.then.eval_checked(input).unwrap(),
                )
            })
            .collect_vec();
        let mut output_array = self.return_type().create_array_builder(input.capacity());
        for idx in 0..input.capacity() {
            if vis.is_set(idx) {
                if let Some((_, t)) = when_thens
                    .iter()
                    .map(|(w, t)| (w.value_at(idx), t.value_at(idx)))
                    .find(|(w, _)| {
                        *w.unwrap_or(ScalarRefImpl::Bool(false))
                            .into_scalar_impl()
                            .as_bool()
                    })
                {
                    output_array.append_datum(&t.to_owned_datum());
                } else if let Some(els) = els.as_mut() {
                    let t = els.datum_at(idx);
                    output_array.append_datum(&t);
                } else {
                    output_array.append_null();
                };
            } else {
                output_array.append_null();
            }
        }
        let output_array = output_array.finish().into();
        Ok(output_array)
    }

    fn eval_row(&self, input: &Row) -> Result<Datum> {
        let els = self
            .else_clause
            .as_deref()
            .map(|else_clause| else_clause.eval_row(input).unwrap());
        let when_then_first = self
            .when_clauses
            .iter()
            .map(|when_clause| {
                (
                    when_clause.when.eval_row(input).unwrap(),
                    when_clause.then.eval_row(input).unwrap(),
                )
            })
            .find(|(w, _)| *(w.as_ref().unwrap_or(&ScalarImpl::Bool(false)).as_bool()));

        let ret = if let Some((_, t)) = when_then_first {
            t
        } else {
            els.unwrap_or(None)
        };

        Ok(ret)
    }
}

impl<'a> TryFrom<&'a ExprNode> for CaseExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type().unwrap() == Type::Case);

        let ret_type = DataType::from(prost.get_return_type().unwrap());
        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };
        let children = &func_call_node.children;
        // children: (when, then)+, (else_clause)?
        let len = children.len();
        let else_clause = if len % 2 == 1 {
            let else_clause = build_from_prost(&children[len - 1])?;
            if else_clause.return_type() != ret_type {
                bail!("Type mismatched between else and case.");
            }
            Some(else_clause)
        } else {
            None
        };
        let mut when_clauses = vec![];
        for i in 0..len / 2 {
            let when_index = i * 2;
            let then_index = i * 2 + 1;
            let when_expr = build_from_prost(&children[when_index])?;
            let then_expr = build_from_prost(&children[then_index])?;
            if when_expr.return_type() != DataType::Boolean {
                bail!("Type mismatched between when clause and condition");
            }
            if then_expr.return_type() != ret_type {
                bail!("Type mismatched between then clause and case");
            }
            let when_clause = WhenClause::new(when_expr, then_expr);
            when_clauses.push(when_clause);
        }
        Ok(CaseExpression::new(ret_type, when_clauses, else_clause))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::Scalar;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType as ProstDataType;
    use risingwave_pb::expr::expr_node::Type;
    use risingwave_pb::expr::FunctionCall;

    use super::*;
    use crate::expr::expr_binary_nonnull::new_binary_expr;
    use crate::expr::{InputRefExpression, LiteralExpression};

    #[test]
    fn test_case_expr() {
        let call = FunctionCall {
            children: vec![
                ExprNode {
                    expr_type: Type::ConstantValue as i32,
                    return_type: Some(ProstDataType {
                        type_name: TypeName::Boolean as i32,
                        ..Default::default()
                    }),
                    rex_node: None,
                },
                ExprNode {
                    expr_type: Type::ConstantValue as i32,
                    return_type: Some(ProstDataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    }),
                    rex_node: None,
                },
            ],
        };
        let p = ExprNode {
            expr_type: Type::Case as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Int32 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(call)),
        };
        assert!(CaseExpression::try_from(&p).is_ok());
    }

    fn test_eval_row(expr: CaseExpression, row_inputs: Vec<i32>, expected: Vec<Option<f32>>) {
        for (i, row_input) in row_inputs.iter().enumerate() {
            let row = Row::new(vec![Some(row_input.to_scalar_value())]);
            let datum = expr.eval_row(&row).unwrap();
            let expected = expected[i].map(|f| f.into());
            assert_eq!(datum, expected)
        }
    }

    #[test]
    fn test_eval_searched_case() {
        let ret_type = DataType::Float32;
        // when x <= 2 then 3.1
        let when_clauses = vec![WhenClause::new(
            new_binary_expr(
                Type::LessThanOrEqual,
                DataType::Boolean,
                Box::new(InputRefExpression::new(DataType::Int32, 0)),
                Box::new(LiteralExpression::new(DataType::Float32, Some(2f32.into()))),
            ),
            Box::new(LiteralExpression::new(
                DataType::Float32,
                Some(3.1f32.into()),
            )),
        )];
        // else 4.1
        let els = Box::new(LiteralExpression::new(
            DataType::Float32,
            Some(4.1f32.into()),
        ));
        let searched_case_expr = CaseExpression::new(ret_type, when_clauses, Some(els));
        let input = DataChunk::from_pretty(
            "i
             1
             2
             3
             4
             5",
        );
        let output = searched_case_expr.eval(&input).unwrap();
        assert_eq!(output.datum_at(0), Some(3.1f32.into()));
        assert_eq!(output.datum_at(1), Some(3.1f32.into()));
        assert_eq!(output.datum_at(2), Some(4.1f32.into()));
        assert_eq!(output.datum_at(3), Some(4.1f32.into()));
        assert_eq!(output.datum_at(4), Some(4.1f32.into()));
    }

    #[test]
    fn test_eval_without_else() {
        let ret_type = DataType::Float32;
        // when x <= 3 then 3.1
        let when_clauses = vec![WhenClause::new(
            new_binary_expr(
                Type::LessThanOrEqual,
                DataType::Boolean,
                Box::new(InputRefExpression::new(DataType::Int32, 0)),
                Box::new(LiteralExpression::new(DataType::Float32, Some(3f32.into()))),
            ),
            Box::new(LiteralExpression::new(
                DataType::Float32,
                Some(3.1f32.into()),
            )),
        )];
        let searched_case_expr = CaseExpression::new(ret_type, when_clauses, None);
        let input = DataChunk::from_pretty(
            "i
             3
             4
             3
             4",
        );
        let output = searched_case_expr.eval(&input).unwrap();
        assert_eq!(output.datum_at(0), Some(3.1f32.into()));
        assert_eq!(output.datum_at(1), None);
        assert_eq!(output.datum_at(2), Some(3.1f32.into()));
        assert_eq!(output.datum_at(3), None);
    }

    #[test]
    fn test_eval_row_searched_case() {
        let ret_type = DataType::Float32;
        // when x <= 2 then 3.1
        let when_clauses = vec![WhenClause::new(
            new_binary_expr(
                Type::LessThanOrEqual,
                DataType::Boolean,
                Box::new(InputRefExpression::new(DataType::Int32, 0)),
                Box::new(LiteralExpression::new(DataType::Float32, Some(2f32.into()))),
            ),
            Box::new(LiteralExpression::new(
                DataType::Float32,
                Some(3.1f32.into()),
            )),
        )];
        // else 4.1
        let els = Box::new(LiteralExpression::new(
            DataType::Float32,
            Some(4.1f32.into()),
        ));
        let searched_case_expr = CaseExpression::new(ret_type, when_clauses, Some(els));

        let row_inputs = vec![1, 2, 3, 4, 5];
        let expected = vec![
            Some(3.1f32),
            Some(3.1f32),
            Some(4.1f32),
            Some(4.1f32),
            Some(4.1f32),
        ];

        test_eval_row(searched_case_expr, row_inputs, expected);
    }

    #[test]
    fn test_eval_row_without_else() {
        let ret_type = DataType::Float32;
        // when x <= 3 then 3.1
        let when_clauses = vec![WhenClause::new(
            new_binary_expr(
                Type::LessThanOrEqual,
                DataType::Boolean,
                Box::new(InputRefExpression::new(DataType::Int32, 0)),
                Box::new(LiteralExpression::new(DataType::Float32, Some(3f32.into()))),
            ),
            Box::new(LiteralExpression::new(
                DataType::Float32,
                Some(3.1f32.into()),
            )),
        )];
        let searched_case_expr = CaseExpression::new(ret_type, when_clauses, None);

        let row_inputs = vec![2, 3, 4, 5];
        let expected = vec![Some(3.1f32), Some(3.1f32), None, None];

        test_eval_row(searched_case_expr, row_inputs, expected);
    }
}
