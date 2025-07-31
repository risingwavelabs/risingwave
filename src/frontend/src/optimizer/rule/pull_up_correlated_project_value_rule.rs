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

use itertools::{Either, Itertools};
use risingwave_pb::plan_common::JoinType;

use super::correlated_expr_rewriter::ProjectValueRewriter;
use super::prelude::{PlanRef, *};
use crate::expr::{ExprRewriter, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::*;
use crate::optimizer::plan_visitor::{PlanCorrelatedIdFinder, PlanVisitor};

/// This rule is for pattern: Apply->Project->Values.
///
/// To unnest simple scalar subqueries, we pull correlated expressions from the Project node
/// that operates on Values and replace the Apply operator with a Project that concatenates
/// the left input expressions and the new right input expressions. This also handles cases
/// where there are no correlated expressions (e.g., SELECT (SELECT 1)) by inlining the
/// constant values directly.
///
/// This rule is restricted to Values nodes with empty schemas (no columns) to avoid the
/// complexity of constant folding. This covers the common case of scalar subqueries that
/// don't reference any tables in their FROM clause.
pub struct PullUpCorrelatedProjectValueRule {}

impl Rule<Logical> for PullUpCorrelatedProjectValueRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        let (apply_left, apply_right, apply_on, join_type, correlated_id, _, max_one_row) =
            apply.clone().decompose();

        // Only handle the max_one_row case (scalar subqueries)
        if !max_one_row {
            return None;
        }

        // Must be a LeftOuter join to be a scalar subquery
        if join_type != JoinType::LeftOuter {
            return None;
        }

        // The apply condition must be trivial (true)
        if !apply_on.always_true() {
            return None;
        }

        let project = apply_right.as_logical_project()?;
        let (proj_exprs, proj_input) = project.clone().decompose();

        // Check if the project input is LogicalValues
        let values = proj_input.as_logical_values()?;

        // For scalar subqueries, we expect exactly one row in Values
        if values.rows().len() != 1 {
            return None;
        }

        // Restrict to Values with empty schema (no columns) to avoid complex constant folding
        // This handles cases like SELECT (SELECT 1) where the Values has no input columns
        if !values.schema().fields().is_empty() {
            return None;
        }

        let mut rewriter = ProjectValueRewriter::new(correlated_id);

        // Split project expressions into correlated and uncorrelated expressions
        let (cor_exprs, uncor_exprs): (Vec<_>, Vec<_>) =
            proj_exprs.into_iter().partition_map(|expr| {
                if expr.has_correlated_input_ref_by_correlated_id(correlated_id) {
                    Either::Left(rewriter.rewrite_expr(expr))
                } else {
                    Either::Right(expr)
                }
            });

        // Check if any correlated expressions reference the same correlated_id in the values
        let mut plan_correlated_id_finder = PlanCorrelatedIdFinder::default();
        plan_correlated_id_finder.visit(values.clone().into());
        if plan_correlated_id_finder.contains(&correlated_id) {
            return None;
        }

        // Create new project expressions by concatenating left input expressions with
        // the correlated expressions that were pulled up
        let mut new_proj_exprs = Vec::new();

        // Add all expressions from the left input (outer query)
        for (i, field) in apply_left.schema().fields().iter().enumerate() {
            new_proj_exprs.push(InputRef::new(i, field.data_type().clone()).into());
        }

        // Add the correlated expressions (these now reference the left input)
        new_proj_exprs.extend(cor_exprs);

        // Handle uncorrelated expressions - since Values has empty schema,
        // these expressions are pure constants that can be added directly
        new_proj_exprs.extend(uncor_exprs);

        // Create the new project node that replaces the Apply
        Some(LogicalProject::new(apply_left, new_proj_exprs).into())
    }
}

impl PullUpCorrelatedProjectValueRule {
    pub fn create() -> BoxedRule {
        Box::new(PullUpCorrelatedProjectValueRule {})
    }
}
