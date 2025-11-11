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

use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::common::PbDistanceType;

use crate::expr::{Expr, ExprImpl, ExprType, InputRef};
use crate::optimizer::LogicalPlanRef;
use crate::optimizer::plan_node::generic::{GenericPlanRef, TopNLimit};
use crate::optimizer::plan_node::{
    LogicalPlanRef as PlanRef, LogicalProject, LogicalTopN, LogicalVectorSearch, PlanTreeNodeUnary,
};
use crate::optimizer::rule::prelude::*;
use crate::optimizer::rule::{BoxedRule, ProjectMergeRule, Rule};

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
    pub(super) fn resolve_vector_search(
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
        let vector_search = LogicalVectorSearch::new(top_n, distance_type, left, right, input);
        Some(LogicalProject::create(vector_search.into(), project_exprs))
    }
}
