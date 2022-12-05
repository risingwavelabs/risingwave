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

use itertools::{Either, Itertools};
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{CorrelatedId, CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::plan_node::{LogicalApply, LogicalFilter, PlanTreeNodeUnary};
use crate::optimizer::PlanRef;
use crate::utils::{ColIndexMapping, Condition};

/// Push `LogicalFilter` down `LogicalApply`
pub struct ApplyFilterRule {}
impl Rule for ApplyFilterRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();

        if max_one_row {
            return None;
        }

        assert_eq!(join_type, JoinType::Inner);
        let filter = right.as_logical_filter()?;
        let input = filter.input();

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
            has_correlated_input_ref: false,
            correlated_id,
        };
        // Split predicates in LogicalFilter into correlated expressions and uncorrelated
        // expressions.
        let (cor_exprs, uncor_exprs) =
            filter
                .predicate()
                .clone()
                .into_iter()
                .partition_map(|expr| {
                    let expr = rewriter.rewrite_expr(expr);
                    if rewriter.has_correlated_input_ref {
                        rewriter.has_correlated_input_ref = false;
                        Either::Left(expr)
                    } else {
                        Either::Right(expr)
                    }
                });

        let new_on = on.and(Condition {
            conjunctions: cor_exprs,
        });
        let new_apply = LogicalApply::create(
            left,
            input,
            join_type,
            new_on,
            apply.correlated_id(),
            correlated_indices,
            false,
        );
        let new_filter = LogicalFilter::new(
            new_apply,
            Condition {
                conjunctions: uncor_exprs,
            },
        )
        .into();
        Some(new_filter)
    }
}

impl ApplyFilterRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyFilterRule {})
    }
}

/// Convert `CorrelatedInputRef` to `InputRef` and shift `InputRef` with offset.
struct Rewriter {
    offset: usize,
    index_mapping: ColIndexMapping,
    has_correlated_input_ref: bool,
    correlated_id: CorrelatedId,
}
impl ExprRewriter for Rewriter {
    fn rewrite_correlated_input_ref(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        let found = correlated_input_ref.correlated_id() == self.correlated_id;
        self.has_correlated_input_ref |= found;
        if found {
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
