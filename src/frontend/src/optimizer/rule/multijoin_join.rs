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



use super::super::plan_node::*;
use super::Rule;

use crate::optimizer::rule::BoxedRule;


/// Merges adjacent inner joins into a single `LogicalMultiJoin`.
/// The `LogicalMultiJoin` is short-lived and will be immediately
/// rewritten into a join tree of binary joins.
pub struct MultiJoinJoinRule {}

impl Rule for MultiJoinJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = LogicalMultiJoin::from_join(&plan)?;
        Some(join.into())
    }
}

impl MultiJoinJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(MultiJoinJoinRule {})
    }
}
#[cfg(test)]
mod tests {
    use rand::Rng;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, Datum};
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, InputRef};
    use crate::optimizer::heuristic::{ApplyOrder, HeuristicOptimizer};
    use crate::session::OptimizerContext;

    #[tokio::test]
    async fn test_joins_get_merged_into_multijoin() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..10)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx.clone(),
        );
        let mid = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[6..9].to_vec(),
            },
            ctx,
        );

        let join_type = JoinType::Inner;
        let on_0: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(3, ty.clone()))),
                ],
            )
            .unwrap(),
        ));
        let join_0 = LogicalJoin::new(
            left.clone().into(),
            right.clone().into(),
            join_type,
            Condition::with_expr(on_0),
        );

        let on_1: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(2, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(8, ty))),
                ],
            )
            .unwrap(),
        ));
        let join_1 = LogicalJoin::new(
            mid.clone().into(),
            LogicalMultiJoin::from_join(&join_0.into()).unwrap().into(),
            join_type,
            Condition::with_expr(on_1),
        );
        let multi_join = LogicalMultiJoin::from_join(&join_1.into()).unwrap();
        for (input, schema) in
            multi_join
                .inputs()
                .iter()
                .zip(vec![mid.schema(), left.schema(), right.schema()])
        {
            assert_eq!(input.schema(), schema);
        }
    }
}
