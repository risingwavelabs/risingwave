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
use crate::expr::{CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::plan_node::{LogicalApply, LogicalFilter, PlanTreeNodeUnary};
use crate::optimizer::PlanRef;
use crate::utils::Condition;

/// Push `LogicalFilter` down `LogicalApply`
pub struct ApplyFilter {}
impl Rule for ApplyFilter {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        let (left, right, on, join_type) = apply.clone().decompose();
        assert_eq!(join_type, JoinType::Inner);
        let filter = right.as_logical_filter()?;

        let mut rewriter = Rewriter {
            offset: left.schema().len(),
        };
        // Split predicates in LogicalFilter into correlated expressions and uncorrelated
        // expressions.
        let (cor_exprs, uncor_exprs) =
            filter
                .predicate()
                .clone()
                .into_iter()
                .partition_map(|expr| {
                    if expr.has_correlated_input_ref() {
                        Either::Left(rewriter.rewrite_expr(expr))
                    } else {
                        Either::Right(rewriter.rewrite_expr(expr))
                    }
                });

        let new_on = on.and(Condition {
            conjunctions: cor_exprs,
        });
        let new_apply = LogicalApply::create(left, filter.input(), join_type, new_on);
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

impl ApplyFilter {
    pub fn create() -> BoxedRule {
        Box::new(ApplyFilter {})
    }
}

/// Convert `CorrelatedInputRef` to `InputRef` and shift `InputRef` with offset.
struct Rewriter {
    offset: usize,
}

impl ExprRewriter for Rewriter {
    fn rewrite_correlated_input_ref(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        // Convert correlated_input_ref to input_ref.
        // TODO: use LiftCorrelatedInputRef here.
        InputRef::new(
            correlated_input_ref.index(),
            correlated_input_ref.return_type(),
        )
        .into()
    }

    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new(input_ref.index() + self.offset, input_ref.return_type()).into()
    }
}
