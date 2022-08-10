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

use itertools::Itertools;
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{CorrelatedId, CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::plan_node::{LogicalApply, LogicalProject};
use crate::optimizer::PlanRef;
use crate::utils::ColIndexMapping;

/// Push `LogicalApply` down `LogicalProject`.
pub struct ApplyProjRule {}
impl Rule for ApplyProjRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices) =
            apply.clone().decompose();
        let project = right.as_logical_project()?;
        assert_eq!(join_type, JoinType::Inner);

        // Insert all the columns of `LogicalApply`'s left at the beginning of the new
        // `LogicalProject`.
        let mut exprs: Vec<ExprImpl> = left
            .schema()
            .data_types()
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| InputRef::new(index, data_type).into())
            .collect();

        let (proj_exprs, proj_input) = project.clone().decompose();

        // replace correlated_input_ref in project exprs
        let mut rewriter = Rewriter {
            offset: left.schema().len(),
            index_mapping: ColIndexMapping::new(
                correlated_indices
                    .clone()
                    .into_iter()
                    .map(Some)
                    .collect_vec(),
            )
            .inverse(),
            correlated_id,
        };

        let new_proj_exprs: Vec<ExprImpl> = proj_exprs
            .into_iter()
            .map(|expr| rewriter.rewrite_expr(expr))
            .collect_vec();

        exprs.extend(new_proj_exprs.clone().into_iter());

        let mut rewriter = ApplyOnConditionRewriter {
            left_input_len: left.schema().len(),
            mapping: new_proj_exprs,
        };
        let new_on = on.rewrite_expr(&mut rewriter);
        let new_apply = LogicalApply::create(
            left,
            proj_input,
            join_type,
            new_on,
            correlated_id,
            correlated_indices,
        );

        let new_project = LogicalProject::create(new_apply, exprs);
        Some(new_project)
    }
}

impl ApplyProjRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyProjRule {})
    }
}

pub struct ApplyOnConditionRewriter {
    pub left_input_len: usize,
    pub mapping: Vec<ExprImpl>,
}

impl ExprRewriter for ApplyOnConditionRewriter {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        if input_ref.index >= self.left_input_len {
            self.mapping[input_ref.index() - self.left_input_len].clone()
        } else {
            input_ref.into()
        }
    }
}

/// Convert `CorrelatedInputRef` to `InputRef` and shift `InputRef` with offset.
struct Rewriter {
    offset: usize,
    index_mapping: ColIndexMapping,
    correlated_id: CorrelatedId,
}
impl ExprRewriter for Rewriter {
    fn rewrite_correlated_input_ref(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        if correlated_input_ref.correlated_id() == self.correlated_id {
            InputRef::new(
                self.index_mapping.map(correlated_input_ref.index()),
                correlated_input_ref.return_type(),
            )
            .into()
        } else {
            correlated_input_ref.into()
        }
    }

    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new(input_ref.index() + self.offset, input_ref.return_type()).into()
    }
}
