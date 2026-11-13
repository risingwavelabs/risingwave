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

//! Turns the result of a match into a plan over a scan of the materialized view:
//!
//! ```text
//! Project(output)
//!   Agg(group keys, calls)
//!     Project(inputs of the aggregation)
//!       Filter(compensating predicate)
//!         Join(inner, on true)          -- one per extra input
//!           Scan(view)
//!           the plan of an extra input
//! ```
//!
//! Parts that are not needed are left out. Join conditions stay in the `Filter`; the predicate
//! pushdown and join ordering that follow put them in place.

use std::sync::Arc;

use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::plan_common::JoinType;

use super::extract::ExtractedBlock;
use super::matcher::Compensation;
use crate::catalog::TableCatalog;
use crate::catalog::index_catalog::TableIndex;
use crate::expr::{Expr, ExprImpl, InputRef};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef, PlanAggCall, TableScan};
use crate::optimizer::plan_node::{
    LogicalFilter, LogicalJoin, LogicalPlanRef as PlanRef, LogicalProject, LogicalScan,
};
use crate::utils::{ColIndexMapping, Condition, IndexSet};

/// A rewritten block, with the scan of the view it reads.
pub struct RewrittenBlock {
    pub plan: PlanRef,
    /// The scan of the view, carrying the compensating predicates that only read the view.
    pub view_scan: LogicalScan,
}

/// Builds the plan that replaces `original`, the plan of the query block.
///
/// `view_columns` are the indices of the visible columns of the view in its table.
pub fn build_plan(
    compensation: &Compensation,
    view_table: &Arc<TableCatalog>,
    view_indexes: &[Arc<TableIndex>],
    view_columns: &[usize],
    block: &ExtractedBlock,
    original: &PlanRef,
) -> Option<RewrittenBlock> {
    let ctx = original.ctx();

    // Predicates that only read the view go into its scan, so that the cost of the scan can
    // be estimated before any pushdown runs.
    let column_count = view_columns.len()
        + compensation
            .extra_inputs
            .iter()
            .map(|input| block.input_cols[*input].len())
            .sum::<usize>();
    let (on_view, rest): (Vec<ExprImpl>, Vec<ExprImpl>) =
        compensation.predicate.iter().cloned().partition(|expr| {
            expr.collect_input_refs(column_count)
                .ones()
                .all(|idx| idx < view_columns.len())
        });
    let mut to_table_columns = ColIndexMapping::new(
        view_columns.iter().map(|idx| Some(*idx)).collect(),
        view_table.columns().len(),
    );
    let scan_predicate = Condition {
        conjunctions: on_view,
    }
    .rewrite_expr(&mut to_table_columns);
    let view_scan: LogicalScan = TableScan::new(
        view_columns.to_vec(),
        view_table.clone(),
        view_indexes.to_vec(),
        vec![],
        ctx,
        scan_predicate,
        None,
    )
    .into();

    let mut plan: PlanRef = view_scan.clone().into();
    for input in &compensation.extra_inputs {
        plan = LogicalJoin::new(
            plan,
            block.input_plans[*input].clone(),
            JoinType::Inner,
            Condition::true_cond(),
        )
        .into();
    }
    if !rest.is_empty() {
        plan = LogicalFilter::create(plan, Condition { conjunctions: rest });
    }

    if let Some(agg) = &compensation.agg {
        // An aggregation reads columns, so compute everything it needs in a project first.
        let mut exprs = agg.group.clone();
        let mut add = |expr: &ExprImpl| -> usize {
            exprs.push(expr.clone());
            exprs.len() - 1
        };
        let calls: Vec<PlanAggCall> = agg
            .calls
            .iter()
            .map(|call| PlanAggCall {
                agg_type: call.agg_type.clone(),
                return_type: call.return_type.clone(),
                inputs: call
                    .args
                    .iter()
                    .map(|arg| InputRef::new(add(arg), arg.return_type()))
                    .collect(),
                distinct: call.distinct,
                order_by: call
                    .order_by
                    .iter()
                    .map(|(expr, order)| ColumnOrder::new(add(expr), *order))
                    .collect(),
                filter: Condition {
                    conjunctions: call
                        .filter
                        .iter()
                        .map(|expr| InputRef::new(add(expr), DataType::Boolean).into())
                        .collect(),
                },
                direct_args: call.direct_args.clone(),
            })
            .collect();
        plan = LogicalProject::create(plan, exprs);
        plan = Agg::new(calls, IndexSet::from_iter(0..agg.group.len()), plan).into();
    }
    plan = LogicalProject::create(plan, compensation.output.clone());

    let same_schema = plan
        .schema()
        .fields()
        .iter()
        .map(|field| field.data_type())
        .eq(original
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type()));
    if !same_schema {
        // A bug in the matcher. Keep the original plan rather than produce a wrong one.
        if cfg!(debug_assertions) {
            panic!(
                "materialized view {} rewrites a block into a different schema:\n{:?}\ninstead of\n{:?}",
                view_table.name,
                plan.schema(),
                original.schema()
            );
        }
        tracing::warn!(
            view = view_table.name,
            "discarded a materialized view rewrite whose schema differs from the query block"
        );
        return None;
    }
    Some(RewrittenBlock { plan, view_scan })
}
