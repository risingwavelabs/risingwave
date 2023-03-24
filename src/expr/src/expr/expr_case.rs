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

use std::sync::Arc;

use risingwave_common::array::{ArrayRef, DataChunk, Vis};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_common::{bail, ensure};
use risingwave_pb::expr::expr_node::{PbType, RexNode};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost, BoxedExpression, Expression};
use crate::{ExprError, Result};

#[derive(Debug)]
pub struct WhenClause {
    when: BoxedExpression,
    then: BoxedExpression,
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

#[async_trait::async_trait]
impl Expression for CaseExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let mut input = input.clone();
        let input_len = input.capacity();
        let mut selection = vec![None; input_len];
        let when_len = self.when_clauses.len();
        let mut result_array = Vec::with_capacity(when_len + 1);
        for (when_idx, WhenClause { when, then }) in self.when_clauses.iter().enumerate() {
            let calc_then_vis: Vis = when
                .eval_checked(&input)
                .await?
                .as_bool()
                .to_bitmap()
                .into();
            let input_vis = input.vis().clone();
            input.set_vis(calc_then_vis.clone());
            let then_res = then.eval_checked(&input).await?;
            calc_then_vis
                .iter_ones()
                .for_each(|pos| selection[pos] = Some(when_idx));
            input.set_vis(&input_vis & (!&calc_then_vis));
            result_array.push(then_res);
        }
        if let Some(ref else_expr) = self.else_clause {
            let else_res = else_expr.eval_checked(&input).await?;
            input
                .vis()
                .iter_ones()
                .for_each(|pos| selection[pos] = Some(when_len));
            result_array.push(else_res);
        }
        let mut builder = self.return_type().create_array_builder(input.capacity());
        for (i, sel) in selection.into_iter().enumerate() {
            if let Some(when_idx) = sel {
                builder.append_datum(result_array[when_idx].value_at(i));
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        for WhenClause { when, then } in &self.when_clauses {
            if when.eval_row(input).await?.map_or(false, |w| w.into_bool()) {
                return then.eval_row(input).await;
            }
        }
        if let Some(ref else_expr) = self.else_clause {
            else_expr.eval_row(input).await
        } else {
            Ok(None)
        }
    }
}

impl<'a> TryFrom<&'a ExprNode> for CaseExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type().unwrap() == PbType::Case);

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
            let when_clause = WhenClause {
                when: when_expr,
                then: then_expr,
            };
            when_clauses.push(when_clause);
        }
        Ok(CaseExpression::new(ret_type, when_clauses, else_clause))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::Scalar;
    use risingwave_pb::expr::expr_node::PbType;

    use super::*;
    use crate::expr::{build, InputRefExpression, LiteralExpression};

    async fn test_eval_row(expr: CaseExpression, row_inputs: Vec<i32>, expected: Vec<Option<f32>>) {
        for (i, row_input) in row_inputs.iter().enumerate() {
            let row = OwnedRow::new(vec![Some(row_input.to_scalar_value())]);
            let datum = expr.eval_row(&row).await.unwrap();
            let expected = expected[i].map(|f| f.into());
            assert_eq!(datum, expected)
        }
    }

    #[tokio::test]
    async fn test_eval_searched_case() {
        let ret_type = DataType::Float32;
        // when x <= 2 then 3.1
        let when_clauses = vec![WhenClause {
            when: build(
                PbType::LessThanOrEqual,
                DataType::Boolean,
                vec![
                    Box::new(InputRefExpression::new(DataType::Int32, 0)),
                    Box::new(LiteralExpression::new(DataType::Float32, Some(2f32.into()))),
                ],
            )
            .unwrap(),
            then: Box::new(LiteralExpression::new(
                DataType::Float32,
                Some(3.1f32.into()),
            )),
        }];
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
        let output = searched_case_expr.eval(&input).await.unwrap();
        assert_eq!(output.datum_at(0), Some(3.1f32.into()));
        assert_eq!(output.datum_at(1), Some(3.1f32.into()));
        assert_eq!(output.datum_at(2), Some(4.1f32.into()));
        assert_eq!(output.datum_at(3), Some(4.1f32.into()));
        assert_eq!(output.datum_at(4), Some(4.1f32.into()));
    }

    #[tokio::test]
    async fn test_eval_without_else() {
        let ret_type = DataType::Float32;
        // when x <= 3 then 3.1
        let when_clauses = vec![WhenClause {
            when: build(
                PbType::LessThanOrEqual,
                DataType::Boolean,
                vec![
                    Box::new(InputRefExpression::new(DataType::Int32, 0)),
                    Box::new(LiteralExpression::new(DataType::Float32, Some(3f32.into()))),
                ],
            )
            .unwrap(),
            then: Box::new(LiteralExpression::new(
                DataType::Float32,
                Some(3.1f32.into()),
            )),
        }];
        let searched_case_expr = CaseExpression::new(ret_type, when_clauses, None);
        let input = DataChunk::from_pretty(
            "i
             3
             4
             3
             4",
        );
        let output = searched_case_expr.eval(&input).await.unwrap();
        assert_eq!(output.datum_at(0), Some(3.1f32.into()));
        assert_eq!(output.datum_at(1), None);
        assert_eq!(output.datum_at(2), Some(3.1f32.into()));
        assert_eq!(output.datum_at(3), None);
    }

    #[tokio::test]
    async fn test_eval_row_searched_case() {
        let ret_type = DataType::Float32;
        // when x <= 2 then 3.1
        let when_clauses = vec![WhenClause {
            when: build(
                PbType::LessThanOrEqual,
                DataType::Boolean,
                vec![
                    Box::new(InputRefExpression::new(DataType::Int32, 0)),
                    Box::new(LiteralExpression::new(DataType::Float32, Some(2f32.into()))),
                ],
            )
            .unwrap(),
            then: Box::new(LiteralExpression::new(
                DataType::Float32,
                Some(3.1f32.into()),
            )),
        }];
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

        test_eval_row(searched_case_expr, row_inputs, expected).await;
    }

    #[tokio::test]
    async fn test_eval_row_without_else() {
        let ret_type = DataType::Float32;
        // when x <= 3 then 3.1
        let when_clauses = vec![WhenClause {
            when: build(
                PbType::LessThanOrEqual,
                DataType::Boolean,
                vec![
                    Box::new(InputRefExpression::new(DataType::Int32, 0)),
                    Box::new(LiteralExpression::new(DataType::Float32, Some(3f32.into()))),
                ],
            )
            .unwrap(),
            then: Box::new(LiteralExpression::new(
                DataType::Float32,
                Some(3.1f32.into()),
            )),
        }];
        let searched_case_expr = CaseExpression::new(ret_type, when_clauses, None);

        let row_inputs = vec![2, 3, 4, 5];
        let expected = vec![Some(3.1f32), Some(3.1f32), None, None];

        test_eval_row(searched_case_expr, row_inputs, expected).await;
    }
}
