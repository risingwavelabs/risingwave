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

use std::collections::HashMap;

use risingwave_common::types::DataType;
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::{LogicalJoin, LogicalProject, PlanTreeNodeBinary};
use crate::optimizer::PlanRef;

pub struct ApplyScan {}
impl Rule for ApplyScan {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        let (left, right, on, join_type) = apply.clone().decompose();
        let apply_left_len = left.schema().len();
        assert_eq!(join_type, JoinType::Inner);
        if let (None, None) = (right.as_logical_scan(), right.as_logical_join()) {
            return None;
        }

        let mut column_mapping = HashMap::new();
        on.conjunctions.iter().for_each(|expr| {
            if let ExprImpl::FunctionCall(func_call) = expr {
                if let Some((left, right, data_type)) = Self::check(func_call, apply_left_len) {
                    column_mapping.insert(left, (right, data_type));
                }
            }
        });
        if column_mapping.len() == apply_left_len {
            // We can eliminate join now!
            let mut exprs: Vec<ExprImpl> = (0..apply_left_len)
                .into_iter()
                .map(|left| {
                    let (right, data_type) = column_mapping.get(&left).unwrap();
                    InputRef::new(*right - apply_left_len, data_type.clone()).into()
                })
                .collect();
            exprs.extend(
                right
                    .schema()
                    .data_types()
                    .into_iter()
                    .enumerate()
                    .map(|(index, data_type)| InputRef::new(index, data_type).into()),
            );
            let project = LogicalProject::create(right, exprs);
            // TODO: add LogicalFilter here to do null-check.
            Some(project)
        } else {
            let left = apply.left();
            let new_left = if let Some(agg) = left.as_logical_agg() {
                agg.rewrite_agg()
            } else {
                None
            };
            let join = LogicalJoin::new(new_left.unwrap(), right, join_type, on);
            Some(join.into())
        }
    }
}

impl ApplyScan {
    pub fn create() -> BoxedRule {
        Box::new(ApplyScan {})
    }

    fn check(func_call: &FunctionCall, apply_left_len: usize) -> Option<(usize, usize, DataType)> {
        let inputs = func_call.inputs();
        if func_call.get_expr_type() == ExprType::Equal && inputs.len() == 2 {
            let left = &inputs[0];
            let right = &inputs[1];
            match (left, right) {
                (ExprImpl::InputRef(left), ExprImpl::InputRef(right)) => {
                    let left_type = left.return_type();
                    let left = left.index();
                    let right_type = right.return_type();
                    let right = right.index();
                    if left < apply_left_len && right >= apply_left_len {
                        Some((left, right, right_type))
                    } else if left >= apply_left_len && right < apply_left_len {
                        Some((right, left, left_type))
                    } else {
                        None
                    }
                }
                _ => None,
            }
        } else {
            None
        }
    }
}
