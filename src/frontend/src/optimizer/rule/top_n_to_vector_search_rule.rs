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

use std::assert_matches::assert_matches;

use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_expr::aggregate::AggType;
use risingwave_pb::common::PbDistanceType;
use risingwave_pb::plan_common::JoinType;

use crate::expr::{Expr, ExprImpl, ExprType, InputRef};
use crate::optimizer::LogicalPlanRef;
use crate::optimizer::plan_node::generic::{GenericPlanRef, TopNLimit};
use crate::optimizer::plan_node::{
    LogicalCorrelatedVectorSearch, LogicalPlanRef as PlanRef, LogicalProject, LogicalTopN,
    LogicalVectorSearch, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::optimizer::rule::prelude::*;
use crate::optimizer::rule::{BoxedRule, PbAggKind, ProjectMergeRule, Rule};

pub struct TopNToVectorSearchRule;

impl TopNToVectorSearchRule {
    pub fn create() -> BoxedRule<Logical> {
        Box::new(TopNToVectorSearchRule)
    }
}

fn merge_consecutive_projections(input: LogicalPlanRef) -> Option<(Vec<ExprImpl>, LogicalPlanRef)> {
    let projection = input.as_logical_project()?;
    let mut exprs = projection.exprs().clone();
    let mut input = projection.input();
    while let Some(projection) = input.as_logical_project() {
        exprs = ProjectMergeRule::merge_project_exprs(&exprs, projection.exprs(), false)?;
        input = projection.input();
    }
    Some((exprs, input))
}

impl TopNToVectorSearchRule {
    #[expect(clippy::type_complexity)]
    fn resolve_vector_search(
        top_n: &LogicalTopN,
    ) -> Option<(
        (u64, PbDistanceType, ExprImpl, ExprImpl, PlanRef),
        Vec<ExprImpl>,
    )> {
        if !top_n.group_key().is_empty() {
            // vector search applies for only singleton top n
            return None;
        }
        if top_n.offset() > 0 {
            return None;
        }
        let TopNLimit::Simple(limit) = top_n.limit_attr() else {
            // vector index applies for only simple top n
            return None;
        };
        // vector index applies for only top n with one order column
        let [order]: &[ColumnOrder; 1] = top_n
            .topn_order()
            .column_orders
            .as_slice()
            .try_into()
            .ok()?;
        if order.order_type.is_descending() || order.order_type.nulls_are_smallest() {
            // vector index applies for only ascending order with nulls last
            return None;
        }

        // TODO: may merge the projections in a finer way so as not to break potential common sub expr.
        let (exprs, projection_input) = merge_consecutive_projections(top_n.input())?;

        let order_expr = &exprs[order.column_index];
        let ExprImpl::FunctionCall(call) = order_expr else {
            return None;
        };
        let (call, distance_type) = match call.func_type() {
            ExprType::L1Distance => (call, PbDistanceType::L1),
            ExprType::L2Distance => (call, PbDistanceType::L2Sqr),
            ExprType::CosineDistance => (call, PbDistanceType::Cosine),
            ExprType::Neg => {
                let [neg_input] = call.inputs() else {
                    return None;
                };
                let ExprImpl::FunctionCall(call) = neg_input else {
                    return None;
                };
                if let ExprType::InnerProduct = call.func_type() {
                    (call, PbDistanceType::InnerProduct)
                } else {
                    return None;
                }
            }
            _ => {
                return None;
            }
        };
        assert_eq!(
            call.inputs().len(),
            2,
            "vector distance function should have exactly two arguments",
        );

        let [left, right]: &[_; 2] = call.inputs().try_into().unwrap();
        assert_matches!(left.return_type(), DataType::Vector(_));
        assert_matches!(right.return_type(), DataType::Vector(_));

        let mut output_exprs = Vec::with_capacity(exprs.len());
        for expr in &exprs[0..order.column_index] {
            output_exprs.push(expr.clone());
        }
        output_exprs.push(ExprImpl::InputRef(
            InputRef {
                index: projection_input.schema().len(),
                data_type: DataType::Float64,
            }
            .into(),
        ));
        for expr in &exprs[order.column_index + 1..exprs.len()] {
            output_exprs.push(expr.clone());
        }
        Some((
            (
                limit,
                distance_type,
                left.clone(),
                right.clone(),
                projection_input,
            ),
            output_exprs,
        ))
    }
}

/// This rule converts the following TopN pattern to `LogicalVectorSearch`
/// ```text
///     LogicalTopN { order: [$expr1 ASC], limit: TOP_N, offset: 0 }
///       └─LogicalProject { exprs: [VectorDistanceFunc(vector_expr1, vector_expr2) as $expr1, other_exprs...] }
/// ```
/// to
/// ```text
///     LogicalProject { exprs: [other_exprs... + distance_column] }
///       └─LogicalVectorSearch { distance_type: `PbDistanceType`, top_n: TOP_N, left: vector_expr1, right: vector_expr2, output_columns: [...] }
/// ```
impl Rule<Logical> for TopNToVectorSearchRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let top_n = plan.as_logical_top_n()?;
        let ((top_n, distance_type, left, right, input), project_exprs) =
            TopNToVectorSearchRule::resolve_vector_search(top_n)?;
        let vector_search = LogicalVectorSearch::new(
            top_n,
            distance_type,
            left,
            right,
            (0..input.schema().len()).collect(),
            input,
        );
        Some(LogicalProject::create(vector_search.into(), project_exprs))
    }
}

pub struct CorrelatedTopNToVectorSearchRule;

impl CorrelatedTopNToVectorSearchRule {
    pub fn create() -> BoxedRule<Logical> {
        Box::new(CorrelatedTopNToVectorSearchRule)
    }
}

impl Rule<Logical> for CorrelatedTopNToVectorSearchRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        if apply.join_type() != JoinType::LeftOuter {
            return None;
        }
        if !apply.max_one_row() {
            return None;
        }
        let correlated_id = apply.correlated_id();
        let base = apply.left();

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
        if !agg.group_key().is_empty() {
            return None;
        }
        let Ok(array_agg) = agg.agg_calls().as_slice().try_into() else {
            return None;
        };
        let [array_agg]: &[_; 1] = array_agg;
        if array_agg.agg_type != AggType::Builtin(PbAggKind::ArrayAgg) {
            return None;
        }
        assert_eq!(array_agg_input.index, 0);
        let Ok(array_agg_input) = array_agg.inputs.as_slice().try_into() else {
            return None;
        };
        let [array_agg_input]: &[_; 1] = array_agg_input;
        let Ok(array_agg_order) = array_agg.order_by.as_slice().try_into() else {
            return None;
        };
        let [array_agg_order]: &[_; 1] = array_agg_order;
        if array_agg_order.order_type != OrderType::ascending() {
            return None;
        }

        let agg_input = agg.input();
        let ((top_n, distance_type, left, right, lookup_input), project_exprs) =
            TopNToVectorSearchRule::resolve_vector_search(agg_input.as_logical_top_n()?)?;

        let array_agg_order_input = project_exprs[array_agg_order.column_index].as_input_ref()?;
        // vector search order by distance column in the end
        if array_agg_order_input.index != lookup_input.schema().len() {
            return None;
        }
        // match pattern Row(lookup.col1, lookup.col2, ..)
        let array_agg_input_expr = &project_exprs[array_agg_input.index];
        let array_agg_input_func = array_agg_input_expr.as_function_call()?;
        if array_agg_input_func.func_type() != ExprType::Row {
            return None;
        }
        let mut lookup_input_indices = vec![];
        for input in array_agg_input_func.inputs() {
            lookup_input_indices.push(input.as_input_ref()?.index);
        }
        let base_input = left.as_correlated_input_ref()?;
        if base_input.correlated_id() != correlated_id {
            return None;
        }

        // TODO: check right has no correlated input
        Some(
            LogicalCorrelatedVectorSearch::new(
                top_n,
                distance_type,
                base,
                base_input.index(),
                lookup_input,
                right,
                lookup_input_indices,
            )
            .into(),
        )
    }
}
