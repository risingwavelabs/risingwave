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
use crate::expr::ExprRewriter;
use crate::optimizer::plan_node::{LogicalApply, LogicalFilter, PlanTreeNodeUnary};
use crate::optimizer::PlanRef;
use crate::utils::{ColIndexMapping, Condition};

/// Push `LogicalFilter` down `LogicalApply`
pub struct ApplyFilter {}
impl Rule for ApplyFilter {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_indices, index_mapping) =
            apply.clone().decompose();
        assert_eq!(join_type, JoinType::Inner);
        let filter = right.as_logical_filter()?;
        let input = filter.input();

        let mut col_index_mapping =
            ColIndexMapping::with_shift_offset(input.schema().len(), left.schema().len() as isize);
        // Split predicates in LogicalFilter into correlated expressions and uncorrelated
        // expressions.
        let (cor_exprs, uncor_exprs) =
            filter
                .predicate()
                .clone()
                .into_iter()
                .partition_map(|expr| {
                    let expr = col_index_mapping.rewrite_expr(expr);
                    if expr.has_correlated_input_ref() {
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
            correlated_indices,
            index_mapping,
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

impl ApplyFilter {
    pub fn create() -> BoxedRule {
        Box::new(ApplyFilter {})
    }
}
