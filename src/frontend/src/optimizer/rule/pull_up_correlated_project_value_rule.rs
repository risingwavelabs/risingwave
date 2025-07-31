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
use risingwave_common::util::column_index_mapping::ColIndexMapping;

use super::prelude::{PlanRef, *};
use crate::expr::{CorrelatedId, CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::*;
 use risingwave_pb::plan_common::JoinType;
use crate::optimizer::plan_visitor::{PlanCorrelatedIdFinder, PlanVisitor};

/// This rule is for pattern: Apply->Project->Values.
///
/// To unnest simple scalar subqueries, we pull correlated expressions from the Project node 
/// that operates on Values and replace the Apply operator with a Project that concatenates 
/// the left input expressions and the new right input expressions. This also handles cases
/// where there are no correlated expressions (e.g., SELECT (SELECT 1)) by inlining the
/// constant values directly.
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

        let mut rewriter = Rewriter {
            input_refs: vec![],
            index: apply_left.schema().fields().len(),
            correlated_id,
        };

        // Split project expressions into correlated and uncorrelated expressions
        let (cor_exprs, uncor_exprs): (Vec<_>, Vec<_>) = proj_exprs
            .into_iter()
            .partition_map(|expr| {
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
            new_proj_exprs.push(
                InputRef::new(i, field.data_type().clone()).into()
            );
        }

        // Add the correlated expressions (these now reference the left input)
        new_proj_exprs.extend(cor_exprs);

        // Handle uncorrelated expressions by evaluating them from the Values
        if !uncor_exprs.is_empty() {
            let values_row = &values.rows()[0];
            
            for expr in uncor_exprs.iter() {
                // For uncorrelated expressions, evaluate them against the Values row
                if let Some(const_expr) = Self::try_evaluate_const_expr(expr, values_row) {
                    new_proj_exprs.push(const_expr);
                } else {
                    // If we can't evaluate as constant, fall back to original expression
                    // This shouldn't happen for simple scalar subqueries with Values
                    new_proj_exprs.push(expr.clone());
                }
            }
        }

        // Create the new project node that replaces the Apply
        Some(
            LogicalProject::new(apply_left, new_proj_exprs).into()
        )
    }
}

impl PullUpCorrelatedProjectValueRule {
    pub fn create() -> BoxedRule {
        Box::new(PullUpCorrelatedProjectValueRule {})
    }

    /// Try to evaluate a constant expression from a Values row
    fn try_evaluate_const_expr(expr: &ExprImpl, values_row: &[ExprImpl]) -> Option<ExprImpl> {
        match expr {
            // If the expression is an InputRef to the Values,
            // we can directly return the corresponding expression from the Values row
            ExprImpl::InputRef(input_ref) => {
                if input_ref.index() < values_row.len() {
                    Some(values_row[input_ref.index()].clone())
                } else {
                    None
                }
            }
            // If the expression is already a literal, return it as-is
            ExprImpl::Literal(_) => Some(expr.clone()),
            // For other expressions, we could implement more sophisticated constant folding
            // For now, return the expression as-is for simple cases
            _ => Some(expr.clone()),
        }
    }
}

/// Rewrites a pulled expression from project to reference the left input instead of correlated input.
///
/// Rewrites `correlated_input_ref` (referencing left side) to `input_ref` and shifts `input_ref`
/// indices as needed.
struct Rewriter {
    // All uncorrelated `InputRef`s in the expression.
    pub input_refs: Vec<InputRef>,
    
    pub index: usize,
    
    pub correlated_id: CorrelatedId,
}

impl ExprRewriter for Rewriter {
    fn rewrite_correlated_input_ref(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        // Convert correlated_input_ref to input_ref pointing to the left input
        // only rewrite the correlated_input_ref with the same correlated_id
        if correlated_input_ref.correlated_id() == self.correlated_id {
            InputRef::new(
                correlated_input_ref.index(),
                correlated_input_ref.return_type(),
            )
            .into()
        } else {
            correlated_input_ref.into()
        }
    }

    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        // For this rule, input_refs in the project should reference Values
        // Since we're inlining, we don't need to preserve these references
        // They should be constant-folded or handled separately
        input_ref.into()
    }
}
