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

use super::super::plan_node::*;
use super::Rule;
use crate::optimizer::rule::BoxedRule;

/// Reorders a multi join into a left deep join via the heuristic ordering
pub struct ReorderMultiJoinRule {}

impl Rule for ReorderMultiJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        if plan
            .ctx()
            .session_ctx()
            .config()
            .get_streaming_enable_delta_join()
        {
            let join = plan.as_logical_multi_join()?;
            join.as_bushy_tree_join().ok()
        } else {
            let join = plan.as_logical_multi_join()?;
            // check if join is inner and can be merged into multijoin
            let join_ordering = join.heuristic_ordering().ok()?; // maybe panic here instead?
            let left_deep_join = join.as_reordered_left_deep_join(&join_ordering);
            Some(left_deep_join)
        }
    }
}

impl ReorderMultiJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(ReorderMultiJoinRule {})
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::iter_util::ZipEqFast;
    use risingwave_pb::expr::expr_node::Type;
    use risingwave_pb::plan_common::JoinType;

    use super::*;
    use crate::expr::{ExprImpl, FunctionCall, InputRef};
    use crate::optimizer::optimizer_context::OptimizerContext;
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
            join_0.into(),
            relation_b.clone().into(),
            join_type,
            Condition::with_expr(on_1),
        );
        let multijoin_builder = LogicalMultiJoinBuilder::new(join_1.into());
        let multi_join = multijoin_builder.build();
        for (input, schema) in multi_join.inputs().iter().zip_eq_fast(vec![
            relation_a.schema(),
            relation_c.schema(),
            relation_b.schema(),
        ]) {
            assert_eq!(input.schema(), schema);
        }

        assert_eq!(multi_join.heuristic_ordering().unwrap(), vec![0, 2, 1]);
    }
}
