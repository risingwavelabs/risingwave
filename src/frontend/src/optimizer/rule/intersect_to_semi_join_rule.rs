// Copyright 2025 RisingWave Labs
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

use risingwave_common::types::DataType::Boolean;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::generic::Agg;
use crate::optimizer::plan_node::{LogicalIntersect, LogicalJoin, PlanTreeNode};

pub struct IntersectToSemiJoinRule {}
impl Rule for IntersectToSemiJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_intersect: &LogicalIntersect = plan.as_logical_intersect()?;
        let all = logical_intersect.all();
        if all {
            return None;
        }

        let inputs = logical_intersect.inputs();
        let join = inputs
            .into_iter()
            .fold(None, |left, right| match left {
                None => Some(right),
                Some(left) => {
                    let on =
                        IntersectToSemiJoinRule::gen_null_safe_equal(left.clone(), right.clone());
                    Some(LogicalJoin::create(left, right, JoinType::LeftSemi, on))
                }
            })
            .unwrap();

        Some(Agg::new(vec![], (0..join.schema().len()).collect(), join).into())
    }
}

impl IntersectToSemiJoinRule {
    pub(crate) fn gen_null_safe_equal(left: PlanRef, right: PlanRef) -> ExprImpl {
        let arms = (left
            .schema()
            .fields()
            .iter()
            .zip_eq_debug(right.schema().fields())
            .enumerate())
        .map(|(i, (left_field, right_field))| {
            ExprImpl::FunctionCall(Box::new(FunctionCall::new_unchecked(
                ExprType::IsNotDistinctFrom,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(i, left_field.data_type()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(
                        i + left.schema().len(),
                        right_field.data_type(),
                    ))),
                ],
                Boolean,
            )))
        });
        ExprImpl::and(arms)
    }
}

impl IntersectToSemiJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(IntersectToSemiJoinRule {})
    }
}
