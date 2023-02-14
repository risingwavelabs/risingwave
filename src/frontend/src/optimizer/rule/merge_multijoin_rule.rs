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

/// Merges adjacent inner joins, filters and projections into a single `LogicalMultiJoin`.
pub struct MergeMultiJoinRule {}

impl Rule for MergeMultiJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let multijoin_builder = LogicalMultiJoinBuilder::new(plan);
        if multijoin_builder.inputs().len() <= 2 {
            return None;
        }
        Some(multijoin_builder.build().into())
    }
}

impl MergeMultiJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(MergeMultiJoinRule {})
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
    async fn test_merge_multijoin_join() {
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
            Condition::true_cond(),
        );
        let filter_on_join = LogicalFilter::new(join_0.into(), Condition::with_expr(on_0));

        let on_1: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(2, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(8, ty.clone()))),
                ],
            )
            .unwrap(),
        ));
        let join_1 = LogicalJoin::new(
            mid.clone().into(),
            filter_on_join.into(),
            join_type,
            Condition::with_expr(on_1.clone()),
        );
        let multijoin_builder = LogicalMultiJoinBuilder::new(join_1.into());
        let multi_join = multijoin_builder.build();

        for (input, schema) in multi_join.inputs().iter().zip_eq_fast(vec![
            mid.schema(),
            left.schema(),
            right.schema(),
        ]) {
            assert_eq!(input.schema(), schema);
        }

        assert_eq!(multi_join.on().conjunctions.len(), 2);
        assert!(multi_join.on().conjunctions.contains(&on_1));

        let on_0_shifted: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(4, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(6, ty))),
                ],
            )
            .unwrap(),
        ));

        assert!(multi_join.on().conjunctions.contains(&on_0_shifted));
    }
}
