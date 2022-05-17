// Copyrelation_c 2022 Singularity Data
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
/// For relation_a/relation_c semi join, we can push filter to relation_a/relation_c and on-clause,
/// and push on-clause to relation_a/relation_c.
///
/// For relation_a/relation_c anti join, we can push filter to relation_a/relation_c, but on-clause
/// can not be pushed
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
        let relation_a_deep_join = join.heuristic_ordering().ok()?;
        Some(relation_a_deep_join)
    }
}

impl ReorderMultiJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(ReorderMultiJoinRule {})
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_pb::expr::expr_node::Type;
    use risingwave_pb::plan_common::JoinType;

    use super::*;
    use crate::expr::{ExprImpl, FunctionCall, InputRef};
    use crate::session::OptimizerContext;
    use crate::utils::Condition;

    #[tokio::test]
    async fn test_heuristic_join_reorder_from_multijoin() {
        // Converts a join graph
        // A-B C
        //
        // with initial ordering:
        //
        //      inner
        //     /   |
        //  cross  B
        //  / |
        // A  C
        //
        // to:
        //
        //     cross
        //     /   |
        //  inner  C
        //  / |
        // A  B

        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..10)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let relation_a = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let relation_c = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx.clone(),
        );
        let relation_b = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[6..9].to_vec(),
            },
            ctx,
        );

        let join_type = JoinType::Inner;
        let join_0 = LogicalJoin::new(
            relation_a.clone().into(),
            relation_c.clone().into(),
            join_type,
            Condition::true_cond(),
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
            relation_b.clone().into(),
            LogicalMultiJoin::from_join(&join_0.into()).unwrap().into(),
            join_type,
            Condition::with_expr(on_1),
        );
        let multi_join = LogicalMultiJoin::from_join(&join_1.into()).unwrap();
        for (input, schema) in multi_join.inputs().iter().zip_eq(vec![
            relation_a.schema(),
            relation_b.schema(),
            relation_c.schema(),
        ]) {
            assert_eq!(input.schema(), schema);
        }
        println!("{:?}", multi_join);

        println!("{:?}", multi_join.heuristic_ordering());

        // TODO: continue this test
    }
}
