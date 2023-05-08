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

use fixedbitset::FixedBitSet;
use risingwave_common::types::DataType::Boolean;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::Agg;
use crate::optimizer::plan_node::{LogicalExcept, LogicalJoin, PlanTreeNode};
use crate::optimizer::PlanRef;

pub struct ExceptToAntiJoinRule {}
impl Rule for ExceptToAntiJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_except: &LogicalExcept = plan.as_logical_except()?;
        let all = logical_except.all();
        if all {
            return None;
        }

        let inputs = logical_except.inputs();
        let join = inputs
            .into_iter()
            .fold(None, |left, right| match left {
                None => Some(right),
                Some(left) => {
                    let on =
                        ExceptToAntiJoinRule::gen_null_safe_equal(left.clone(), right.clone());
                    Some(LogicalJoin::create(left, right, JoinType::LeftAnti, on))
                }
            })
            .unwrap();

        let mut bit_set = FixedBitSet::with_capacity(join.schema().len());
        bit_set.toggle_range(..);
        Some(Agg::new(vec![], bit_set, join).into())
    }
}

impl ExceptToAntiJoinRule {
    fn gen_null_safe_equal(left: PlanRef, right: PlanRef) -> ExprImpl {
        (left
            .schema()
            .fields()
            .iter()
            .zip_eq_debug(right.schema().fields())
            .enumerate())
            .fold(None, |expr, (i, (left_field, right_field))| {
                let equal = ExprImpl::FunctionCall(Box::new(FunctionCall::new_unchecked(
                    ExprType::IsNotDistinctFrom,
                    vec![
                        ExprImpl::InputRef(Box::new(InputRef::new(i, left_field.data_type()))),
                        ExprImpl::InputRef(Box::new(InputRef::new(
                            i + left.schema().len(),
                            right_field.data_type(),
                        ))),
                    ],
                    Boolean,
                )));

                match expr {
                    None => Some(equal),
                    Some(expr) => Some(ExprImpl::FunctionCall(Box::new(
                        FunctionCall::new_unchecked(ExprType::And, vec![expr, equal], Boolean),
                    ))),
                }
            })
            .unwrap()
    }
}

impl ExceptToAntiJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(ExceptToAntiJoinRule {})
    }
}
