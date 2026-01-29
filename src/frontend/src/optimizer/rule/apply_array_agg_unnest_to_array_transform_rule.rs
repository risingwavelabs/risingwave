// Copyright 2026 RisingWave Labs
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

use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_expr::sig::FuncName;
use risingwave_pb::plan_common::JoinType;

use super::prelude::{BoxedRule, PlanRef, *};
use crate::expr::{
    CorrelatedId, CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall,
    FunctionCallWithLambda, InputRef, Literal, TableFunctionType, collect_input_refs,
    default_visit_expr, infer_type,
};
use crate::optimizer::plan_node::{
    LogicalApply, LogicalAgg, LogicalProject, LogicalValues, PlanTreeNodeUnary,
};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::utils::Substitute;

/// Rewrite the pattern of `unnest(array) -> [project] -> array_agg` in a correlated scalar subquery
/// into `array_transform(array, |x| ...)` in a single `LogicalProject` on the outer input.
///
/// This avoids translating the subquery into stateful streaming operators.
///
/// Semantics caveat: `array_agg` over `unnest` returns `NULL` for both `NULL` and empty arrays
/// (since `unnest` yields zero rows). But `array_transform` returns empty array for empty inputs.
/// Therefore we wrap it with `CASE WHEN cardinality(arr)=0 THEN NULL ELSE array_transform(...) END`.
pub struct ApplyArrayAggUnnestToArrayTransformRule {}

impl Rule<Logical> for ApplyArrayAggUnnestToArrayTransformRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        if apply.translated() {
            return None;
        }

        let (left, right, on, join_type, correlated_id, _correlated_indices, _max_one_row) =
            apply.clone().decompose();

        // Correlated scalar subquery pattern.
        if !matches!(join_type, JoinType::LeftOuter | JoinType::Inner) || !on.always_true() {
            return None;
        }

        // Unwrap `LogicalProject` on RHS (often exists for scalar subqueries).
        let (right_project_exprs, right_agg_plan): (Option<Vec<ExprImpl>>, PlanRef) =
            if let Some(right_project) = right.as_logical_project() {
                // Be conservative: only allow selecting from agg outputs.
                if !right_project
                    .exprs()
                    .iter()
                    .all(|e| matches!(e, ExprImpl::InputRef(_)))
                {
                    return None;
                }
                (Some(right_project.exprs().clone()), right_project.input())
            } else {
                (None, right.clone())
            };
        let right_agg: &LogicalAgg = right_agg_plan.as_logical_agg()?;

        if !right_agg.group_key().is_empty() || !right_agg.grouping_sets().is_empty() {
            return None;
        }

        let [agg_call] = right_agg.agg_calls().as_slice() else {
            return None;
        };
        if agg_call.distinct || !agg_call.order_by.is_empty() || !agg_call.filter.always_true() {
            return None;
        }
        let AggType::Builtin(kind) = &agg_call.agg_type else {
            return None;
        };
        if *kind != PbAggKind::ArrayAgg {
            return None;
        }
        let [agg_input] = agg_call.inputs.as_slice() else {
            return None;
        };
        let agg_arg_col_idx = agg_input.index;
        let agg_ret_ty = agg_call.return_type.clone(); // list(T)

        // Get the mapped element expr (argument of `array_agg`) and locate the ProjectSet built from
        // the correlated `unnest(array_expr)` table function.
        //
        // There might be multiple `LogicalProject`s between `LogicalAgg` and `LogicalProjectSet`
        // (e.g., schema alignment projects inserted by `TableFunctionToProjectSetRule`).
        let agg_input_plan = right_agg.input();
        let (mut cur_plan, mut mapped_elem_expr): (PlanRef, ExprImpl) =
            if let Some(pre_proj) = agg_input_plan.as_logical_project() {
                let mapped = pre_proj.exprs().get(agg_arg_col_idx)?.clone();
                (pre_proj.input(), mapped)
            } else {
                if agg_arg_col_idx != 0 {
                    return None;
                }
                (agg_input_plan, agg_input.clone().into())
            };

        // Inline intermediate projects into `mapped_elem_expr` until we reach the ProjectSet.
        while let Some(proj) = cur_plan.as_logical_project() {
            mapped_elem_expr = Substitute {
                mapping: proj.exprs().clone(),
            }
            .rewrite_expr(mapped_elem_expr);
            cur_plan = proj.input();
        }

        let (array_expr, elem_idx, base_schema_len): (ExprImpl, usize, usize) =
            if let Some(project_set) = cur_plan.as_logical_project_set() {
                let values_plan = project_set.input();
                let values: &LogicalValues = values_plan.as_logical_values()?;
                if !values.is_empty_scalar() {
                    return None;
                }
                let [tf_expr] = project_set.select_list().as_slice() else {
                    return None;
                };
                let ExprImpl::TableFunction(unnest_tf) = tf_expr else {
                    return None;
                };
                if unnest_tf.function_type != TableFunctionType::Unnest {
                    return None;
                }
                let [array_expr] = unnest_tf.args.as_slice() else {
                    return None;
                };
                (array_expr.clone(), 1, project_set.schema().len())
            } else if let Some(table_func) = cur_plan.as_logical_table_function() {
                if table_func.with_ordinality || table_func.schema().len() != 1 {
                    return None;
                }
                let unnest_tf = table_func.table_function();
                if unnest_tf.function_type != TableFunctionType::Unnest {
                    return None;
                }
                let [array_expr] = unnest_tf.args.as_slice() else {
                    return None;
                };
                (array_expr.clone(), 0, table_func.schema().len())
            } else {
                return None;
            };

        let elem_ty = match array_expr.return_type() {
            risingwave_common::types::DataType::List(list) => list.into_elem(),
            _ => return None,
        };

        // Correlated array expr must refer to LHS.
        let left_len = left.schema().len();
        let mut corr_rewriter = CorrelatedInputRefToInputRefRewriter::new(correlated_id, left_len);
        let array_expr = corr_rewriter.rewrite_expr(array_expr.clone());
        if !corr_rewriter.touched() {
            return None;
        }

        // The mapped element expression must depend only on the unnested element column.
        if mapped_elem_expr.has_table_function() || has_correlated_input_ref(&mapped_elem_expr) {
            return None;
        }
        let input_ref_bits =
            collect_input_refs(base_schema_len, std::iter::once(&mapped_elem_expr));
        if input_ref_bits.ones().any(|idx| idx != elem_idx) {
            // No index in lambda. We can only rewrite when the expr depends solely on the element.
            return None;
        }

        // Build the lambda body by rewriting `InputRef(elem_idx)` (unnested element) to lambda arg `InputRef(0)`.
        let mut lambda_rewriter = ProjectSetElemToLambdaArgRewriter::new(elem_idx, elem_ty.clone());
        let lambda_body = lambda_rewriter.rewrite_expr(mapped_elem_expr);

        let array_transform_expr: ExprImpl = ExprImpl::FunctionCallWithLambda(Box::new(
            FunctionCallWithLambda::new_unchecked(
                ExprType::ArrayTransform,
                vec![array_expr.clone()],
                lambda_body,
                agg_ret_ty.clone(),
            ),
        ));

        // CASE WHEN cardinality(array)=0 THEN NULL ELSE array_transform(...) END
        let cardinality = build_scalar_call(ExprType::Cardinality, vec![array_expr.clone()])?;
        let is_empty = build_scalar_call(ExprType::Equal, vec![cardinality, ExprImpl::literal_int(0)])?;
        let null_list = Literal::new(None, agg_ret_ty.clone()).into();
        let case_expr = build_scalar_call(
            ExprType::Case,
            vec![is_empty, null_list, array_transform_expr],
        )?;

        // Build a Project over LHS producing the same output schema as Apply: [LHS..., RHS...].
        let mut out_exprs: Vec<ExprImpl> = left
            .schema()
            .data_types()
            .into_iter()
            .enumerate()
            .map(|(idx, ty)| InputRef::new(idx, ty).into())
            .collect();

        // Only support single RHS output for now.
        if let Some(rhs_exprs) = right_project_exprs {
            let [ExprImpl::InputRef(input_ref)] = rhs_exprs.as_slice() else {
                return None;
            };
            if input_ref.index != 0 {
                return None;
            }
        } else if right.schema().len() != 1 {
            return None;
        }
        out_exprs.push(case_expr);

        Some(LogicalProject::new(left, out_exprs).into())
    }
}

impl ApplyArrayAggUnnestToArrayTransformRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyArrayAggUnnestToArrayTransformRule {})
    }
}

fn build_scalar_call(func_type: ExprType, mut inputs: Vec<ExprImpl>) -> Option<ExprImpl> {
    let return_type = infer_type(FuncName::Scalar(func_type), &mut inputs).ok()?;
    Some(FunctionCall::new_unchecked(func_type, inputs, return_type).into())
}

fn has_correlated_input_ref(expr: &ExprImpl) -> bool {
    struct Finder {
        found: bool,
    }
    impl crate::expr::ExprVisitor for Finder {
        fn visit_correlated_input_ref(&mut self, _: &CorrelatedInputRef) {
            self.found = true;
        }
    }

    let mut finder = Finder { found: false };
    default_visit_expr(&mut finder, expr);
    finder.found
}

struct CorrelatedInputRefToInputRefRewriter {
    correlated_id: CorrelatedId,
    left_len: usize,
    touched: bool,
}

impl CorrelatedInputRefToInputRefRewriter {
    fn new(correlated_id: CorrelatedId, left_len: usize) -> Self {
        Self {
            correlated_id,
            left_len,
            touched: false,
        }
    }

    fn touched(&self) -> bool {
        self.touched
    }
}

impl ExprRewriter for CorrelatedInputRefToInputRefRewriter {
    fn rewrite_correlated_input_ref(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        if correlated_input_ref.correlated_id() != self.correlated_id {
            return correlated_input_ref.into();
        }
        let idx = correlated_input_ref.index();
        if idx >= self.left_len {
            return correlated_input_ref.into();
        }
        self.touched = true;
        InputRef::new(idx, correlated_input_ref.return_type()).into()
    }
}

struct ProjectSetElemToLambdaArgRewriter {
    elem_idx: usize,
    elem_ty: risingwave_common::types::DataType,
}

impl ProjectSetElemToLambdaArgRewriter {
    fn new(elem_idx: usize, elem_ty: risingwave_common::types::DataType) -> Self {
        Self { elem_idx, elem_ty }
    }
}

impl ExprRewriter for ProjectSetElemToLambdaArgRewriter {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        if input_ref.index == self.elem_idx {
            InputRef::new(0, self.elem_ty.clone()).into()
        } else {
            input_ref.into()
        }
    }
}
