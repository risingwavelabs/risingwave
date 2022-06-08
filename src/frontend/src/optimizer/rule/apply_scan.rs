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
use crate::expr::{
    CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef,
};
use crate::optimizer::plan_node::{LogicalJoin, LogicalProject};
use crate::optimizer::PlanRef;
use crate::utils::Condition;

pub struct ApplyScan {}
impl Rule for ApplyScan {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_indices, index_mapping) =
            apply.clone().decompose();
        let apply_left_len = left.schema().len();
        assert_eq!(join_type, JoinType::Inner);
        // TODO: Push `LogicalApply` down `LogicalJoin`.
        if let (None, None) = (right.as_logical_scan(), right.as_logical_join()) {
            return None;
        }

        // Record the mapping from `CorrelatedInputRef`'s index to `InputRef`'s index.
        // We currently can remove DAG only if ALL the `CorrelatedInputRef` are equal joined to
        // `InputRef`.
        // TODO: Do some transformation for IN, and try to remove DAG for it.
        let mut column_mapping = HashMap::new();
        on.conjunctions.iter().for_each(|expr| {
            if let ExprImpl::FunctionCall(func_call) = expr {
                if let Some((left, right, data_type)) = Self::check(func_call) {
                    column_mapping.insert(left, (right, data_type));
                }
            }
        });
        if column_mapping.len() == apply_left_len {
            // Remove DAG.

            // Replace `LogicalApply` with `LogicalProject` and insert the `InputRef`s which is
            // equal to `CorrelatedInputRef` at the beginning of `LogicalProject`.
            // See the fourth section of Unnesting Arbitrary Queries for how to do the optimization.
            let mut exprs: Vec<ExprImpl> = correlated_indices
                .into_iter()
                .map(|correlated_index| {
                    let (col_index, data_type) = column_mapping.get(&correlated_index).unwrap();
                    InputRef::new(*col_index - apply_left_len, data_type.clone()).into()
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
            let mut rewriter = Rewriter { index_mapping };
            let rewritten_exprs = on
                .into_iter()
                .map(|expr| rewriter.rewrite_expr(expr))
                .collect();

            let join = LogicalJoin::new(
                left,
                right,
                join_type,
                Condition {
                    conjunctions: rewritten_exprs,
                },
            );
            Some(join.into())
        }
    }
}

impl ApplyScan {
    pub fn create() -> BoxedRule {
        Box::new(ApplyScan {})
    }

    /// Check whether the `func_call` is like `CorrelatedInputRef` = `InputRef`.
    fn check(func_call: &FunctionCall) -> Option<(usize, usize, DataType)> {
        let inputs = func_call.inputs();
        if func_call.get_expr_type() == ExprType::Equal && inputs.len() == 2 {
            let left = &inputs[0];
            let right = &inputs[1];
            match (left, right) {
                (ExprImpl::CorrelatedInputRef(left), ExprImpl::InputRef(right)) => {
                    Some((left.index(), right.index(), right.return_type()))
                }
                (ExprImpl::InputRef(right), ExprImpl::CorrelatedInputRef(left)) => {
                    Some((left.index(), right.index(), right.return_type()))
                }
                _ => None,
            }
        } else {
            None
        }
    }
}

struct Rewriter {
    index_mapping: HashMap<usize, usize>,
}

impl ExprRewriter for Rewriter {
    fn rewrite_correlated_input_ref(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        let new_index = *self
            .index_mapping
            .get(&correlated_input_ref.index())
            .unwrap();
        InputRef::new(new_index, correlated_input_ref.return_type()).into()
    }
}
