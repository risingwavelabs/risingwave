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

use risingwave_common::types::ScalarImpl;
use risingwave_expr::aggregate::AggType;
use risingwave_pb::plan_common::JoinType;

use crate::expr::{ExprImpl, ExprType};
use crate::handler::create_index::IndexColumnExprValidator;
use crate::optimizer::plan_node::{
    Logical, LogicalPlanNodeType, LogicalVectorSearchLookupJoin, PlanTreeNodeBinary,
    PlanTreeNodeUnary,
};
use crate::optimizer::rule::prelude::PlanRef;
use crate::optimizer::rule::{
    BoxedRule, PbAggKind, ProjectMergeRule, Rule, TopNToVectorSearchRule,
};

pub struct CorrelatedTopNToVectorSearchRule;

impl CorrelatedTopNToVectorSearchRule {
    pub fn create() -> BoxedRule<Logical> {
        Box::new(CorrelatedTopNToVectorSearchRule)
    }
}

impl Rule<Logical> for CorrelatedTopNToVectorSearchRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        // match pattern LogicalApply { type: LeftOuter, on: true, correlated_id, max_one_row: true }
        if apply.join_type() != JoinType::LeftOuter {
            return None;
        }
        if !apply.max_one_row() {
            return None;
        }
        if !apply.on_condition().always_true() {
            return None;
        }
        let correlated_id = apply.correlated_id();
        let input = apply.left();

        // match pattern LogicalProject { exprs: [[Coalesce(array_agg($expr1 order_by($expr2 ASC)), ARRAY[]) as $expr3] }
        let right = apply.right();
        let project = right.as_logical_project()?;
        let Ok(expr) = project.exprs().as_slice().try_into() else {
            return None;
        };
        let [expr]: &[_; 1] = expr;
        let func_call = expr.as_function_call()?;
        if func_call.func_type() != ExprType::Coalesce {
            return None;
        }
        let Ok(inputs) = func_call.inputs().try_into() else {
            return None;
        };
        let [first, second]: &[_; 2] = inputs;
        let empty_array = second.as_literal()?;
        let Some(ScalarImpl::List(empty_list)) = empty_array.get_data() else {
            return None;
        };
        if !empty_list.is_empty() {
            return None;
        }

        // match pattern of LogicalAgg { aggs: [array_agg($expr1 order_by($expr2 ASC))] }
        let array_agg_input = first.as_input_ref()?;
        let project_input = project.input();
        let agg = project_input.as_logical_agg()?;
        if !agg.group_key().is_empty() || !agg.grouping_sets().is_empty() {
            return None;
        }
        let Ok(array_agg) = agg.agg_calls().as_slice().try_into() else {
            return None;
        };
        assert_eq!(array_agg_input.index, 0);
        let [array_agg]: &[_; 1] = array_agg;

        if array_agg.agg_type != AggType::Builtin(PbAggKind::ArrayAgg) {
            return None;
        }
        if array_agg.distinct {
            return None;
        }
        if !array_agg.filter.always_true() {
            return None;
        }
        if !array_agg.direct_args.is_empty() {
            return None;
        }

        let Ok(array_agg_input) = array_agg.inputs.as_slice().try_into() else {
            return None;
        };
        let [array_agg_input]: &[_; 1] = array_agg_input;

        let ((top_n, distance_type, left, right, lookup_input), project_exprs) = {
            let mut prev_proj_exprs: Option<Vec<_>> = None;
            let mut input = agg.input();
            loop {
                match input.node_type() {
                    LogicalPlanNodeType::LogicalProject => {
                        let proj = input.as_logical_project().expect("checked node type");
                        prev_proj_exprs = Some(if let Some(prev_proj_exprs) = prev_proj_exprs {
                            ProjectMergeRule::merge_project_exprs(
                                prev_proj_exprs.as_slice(),
                                proj.exprs(),
                                false,
                            )?
                        } else {
                            proj.exprs().clone()
                        });
                        input = proj.input();
                    }
                    LogicalPlanNodeType::LogicalTopN => {
                        let (resolved_info, mut project_exprs) =
                            TopNToVectorSearchRule::resolve_vector_search(
                                input.as_logical_top_n().expect("checked node type"),
                            )?;
                        if let Some(prev_proj_exprs) = prev_proj_exprs {
                            project_exprs = ProjectMergeRule::merge_project_exprs(
                                prev_proj_exprs.as_slice(),
                                &project_exprs,
                                false,
                            )?;
                        }
                        break (resolved_info, project_exprs);
                    }
                    _ => {
                        return None;
                    }
                }
            }
        };

        let (input_vector_idx, lookup_expr) = match (left, right) {
            (ExprImpl::CorrelatedInputRef(correlated), lookup_expr)
            | (lookup_expr, ExprImpl::CorrelatedInputRef(correlated))
                if correlated.correlated_id() == correlated_id
                    && IndexColumnExprValidator::validate(&lookup_expr, true).is_ok() =>
            {
                (correlated.index(), lookup_expr)
            }
            _ => {
                return None;
            }
        };

        // match pattern Row(lookup.col1, lookup.col2, ..)
        let array_agg_input_expr = &project_exprs[array_agg_input.index];
        let row_input_func = array_agg_input_expr.as_function_call()?;
        if row_input_func.func_type() != ExprType::Row {
            return None;
        }
        let mut row_input_indices = vec![];
        let mut include_distance = false;
        for (idx, row_input) in row_input_func.inputs().iter().enumerate() {
            let input_index = row_input.as_input_ref()?.index;
            if input_index == lookup_input.schema().len() {
                // distance column included in the row output
                if idx != row_input_func.inputs().len() - 1 {
                    // for simplicity, we require that distance column should be the last column in the row
                    return None;
                } else {
                    include_distance = true;
                }
            } else {
                row_input_indices.push(input_index);
            }
        }

        Some(
            LogicalVectorSearchLookupJoin::new(
                top_n,
                distance_type,
                input,
                input_vector_idx,
                lookup_input,
                lookup_expr,
                row_input_indices,
                include_distance,
            )
            .into(),
        )
    }
}
