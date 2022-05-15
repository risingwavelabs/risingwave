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


/// Pushes predicates above and within a join node into the join node and/or its children nodes.
///
/// # Which predicates can be pushed
///
/// For inner join, we can do all kinds of pushdown.
///
/// For left/right semi join, we can push filter to left/right and on-clause,
/// and push on-clause to left/right.
///
/// For left/right anti join, we can push filter to left/right, but on-clause can not be pushed
///
/// ## Outer Join
///
/// Preserved Row table
/// : The table in an Outer Join that must return all rows.
///
/// Null Supplying table
/// : This is the table that has nulls filled in for its columns in unmatched rows.
///
/// |                          | Preserved Row table | Null Supplying table |
/// |--------------------------|---------------------|----------------------|
/// | Join predicate (on)      | Not Pushed          | Pushed               |
/// | Where predicate (filter) | Pushed              | Not Pushed           |
pub struct ReorderMultiJoinRule {}

impl Rule for ReorderMultiJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_logical_multi_join()?;
        // check if join is inner and can be merged into multijoin
        let left_deep_join = join.to_left_deep_join_with_heuristic_ordering().ok()?;
        Some(left_deep_join)
    }
}

impl ReorderMultiJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(ReorderMultiJoinRule {})
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
    async fn test_heuristic_join_reorder_from_multijoin() {
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
        println!("{:?}", multi_join);

        println!(
            "{:?}",
            multi_join.to_left_deep_join_with_heuristic_ordering()
        );
    }
}
