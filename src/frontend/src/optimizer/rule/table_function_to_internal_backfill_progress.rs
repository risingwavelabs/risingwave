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

use std::rc::Rc;
use std::sync::Arc;

use anyhow::bail;
use itertools::Itertools;
use risingwave_common::catalog::is_backfill_table;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_expr::aggregate::AggType;
pub use risingwave_pb::expr::agg_call::PbKind as PbAggKind;

use super::{ApplyResult, BoxedRule, FallibleRule};
use crate::TableCatalog;
use crate::catalog::catalog_service::CatalogReadGuard;
use crate::expr::{AggCall, ExprImpl, InputRef, Literal, OrderBy, TableFunctionType};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalProject, LogicalScan, LogicalTableFunction, LogicalUnion,
};
use crate::optimizer::{OptimizerContext, PlanRef};
use crate::utils::{Condition, GroupBy};

/// Transform a special `TableFunction` (with `FILE_SCAN` table function type) into a `LogicalFileScan`
pub struct TableFunctionToInternalBackfillProgressRule {}
impl FallibleRule for TableFunctionToInternalBackfillProgressRule {
    fn apply(&self, plan: PlanRef) -> ApplyResult {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        if logical_table_function.table_function.function_type
            != TableFunctionType::InternalBackfillProgress
        {
            return ApplyResult::NotApplicable;
        }

        let reader = plan.ctx().session_ctx().env().catalog_reader().read_guard();
        // TODO(kwannoel): Make sure it reads from source tables as well.
        let backfilling_tables = get_backfilling_tables(reader);
        let plan = Self::build_plan(plan.ctx(), backfilling_tables)?;
        ApplyResult::Ok(plan)
    }
}

impl TableFunctionToInternalBackfillProgressRule {
    fn build_u32_expr(id: u32) -> ExprImpl {
        ExprImpl::Literal(Box::new(Literal::new(
            Some(ScalarImpl::Int32(id as i32)),
            DataType::Int32,
        )))
    }

    fn build_plan(
        ctx: Rc<OptimizerContext>,
        backfilling_tables: Vec<Arc<TableCatalog>>,
    ) -> anyhow::Result<PlanRef> {
        let mut all_progress = Vec::with_capacity(backfilling_tables.len());
        for table in backfilling_tables {
            let Some(job_id) = table.job_id else {
                bail!("`job_id` column not found in backfill table");
            };
            let Some(row_count_column_index) =
                table.columns.iter().position(|c| c.name() == "row_count")
            else {
                bail!("`row_count` column not found in backfill table");
            };
            let fragment_id = table.fragment_id;
            let table_id = table.id;

            let scan = LogicalScan::create(
                table.name.clone(),
                table,
                vec![],
                ctx.clone(),
                None,
                Default::default(),
            );

            let select_exprs = vec![ExprImpl::AggCall(Box::new(AggCall::new(
                AggType::Builtin(PbAggKind::Sum),
                vec![ExprImpl::InputRef(Box::new(InputRef {
                    index: row_count_column_index,
                    data_type: DataType::Int64,
                }))],
                false,
                OrderBy::any(),
                Condition::true_cond(),
                vec![],
            )?))];
            let group_by = GroupBy::GroupKey(vec![]);
            let (agg, _, _) = LogicalAgg::create(select_exprs, group_by, None, scan.into())?;

            let current_count_per_vnode = ExprImpl::InputRef(Box::new(InputRef {
                index: 0,
                data_type: DataType::Decimal,
            }))
            .cast_explicit(DataType::Int64)?;

            let project = {
                let job_id_expr = Self::build_u32_expr(job_id.table_id);
                let fragment_id_expr = Self::build_u32_expr(fragment_id);
                let table_id_expr = Self::build_u32_expr(table_id.table_id);
                LogicalProject::new(
                    agg,
                    vec![
                        job_id_expr,
                        fragment_id_expr,
                        table_id_expr,
                        current_count_per_vnode,
                    ],
                )
            };
            all_progress.push(project.into());
        }
        Ok(LogicalUnion::new(true, all_progress).into())
    }
}

fn get_backfilling_tables(reader: CatalogReadGuard) -> Vec<Arc<TableCatalog>> {
    reader
        .iter_backfilling_internal_tables()
        .filter(|table| is_backfill_table(&table.name))
        .cloned()
        .collect_vec()
}

impl TableFunctionToInternalBackfillProgressRule {
    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToInternalBackfillProgressRule {})
    }
}
