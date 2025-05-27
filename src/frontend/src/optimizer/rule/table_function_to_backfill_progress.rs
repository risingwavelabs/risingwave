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

use std::sync::Arc;

use anyhow::{Result, anyhow};
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{Field, Schema, internal_table_name_to_parts};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_expr::aggregate::AggType;
pub use risingwave_pb::expr::agg_call::PbKind as PbAggKind;
pub use risingwave_pb::plan_common::JoinType;

use super::{ApplyResult, BoxedRule, FallibleRule};
use crate::TableCatalog;
use crate::catalog::catalog_service::CatalogReadGuard;
use crate::expr::{
    AggCall, ExprImpl, ExprType, FunctionCall, InputRef, Literal, OrderBy, TableFunctionType,
};
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalJoin, LogicalProject, LogicalScan, LogicalTableFunction, LogicalUnion,
    LogicalValues,
};
use crate::utils::{Condition, GroupBy};

/// Transform a special `TableFunction` (with `FILE_SCAN` table function type) into a `LogicalFileScan`
pub struct TableFunctionToBackfillProgressRule {}

impl TableFunctionToBackfillProgressRule {
    fn bind_schema_and_table_name(args: &[ExprImpl]) -> Result<(String, String)> {
        match args {
            [table_name] => {
                let table_name = table_name.to_string_value()?;
                Ok(("public".to_owned(), table_name))
            }
            [schema_name, table_name] => {
                let schema_name = schema_name.to_string_value()?;
                let table_name = table_name.to_string_value()?;
                Ok((schema_name, table_name))
            }
            _ => bail!(
                "expected backfill_progress([schema_name varchar, ] table_name varchar), with literal arguments"
            ),
        }
    }
}

impl FallibleRule for TableFunctionToBackfillProgressRule {
    fn apply(&self, plan: PlanRef) -> ApplyResult {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        if logical_table_function.table_function.function_type
            != TableFunctionType::BackfillProgress
        {
            return ApplyResult::NotApplicable;
        }
        let args = logical_table_function.table_function.args.clone();
        let (schema_name, table_name) = Self::bind_schema_and_table_name(&args)?;

        let fields = vec![
            Field::new("job_id", DataType::Int32),
            Field::new("row_count", DataType::Int64),
        ];

        let backfilling_tables = {
            let reader = plan.ctx().session_ctx().env().catalog_reader().read_guard();
            let db_name = plan.ctx().session_ctx().database();
            // TODO(kwannoel): Make sure it reads from source backfilling tables as well.
            get_backfilling_tables(reader, &db_name, &schema_name, &table_name)?
        };

        // No backfill in progress, just return empty values.
        if backfilling_tables.is_empty() {
            let plan = LogicalValues::new(vec![], Schema::new(fields), plan.ctx().clone());
            return ApplyResult::Ok(plan.into());
        }

        let mut counts = Vec::with_capacity(backfilling_tables.len());
        let mut backfilling_job_ids = vec![];
        for table in backfilling_tables {
            let Some(job_id) = table.job_id else {
                return ApplyResult::Err(
                    anyhow!("`job_id` column not found in backfill table").into(),
                );
            };
            backfilling_job_ids.push(job_id);
            let Some(row_count_column_index) =
                table.columns.iter().position(|c| c.name() == "row_count")
            else {
                return ApplyResult::Err(
                    anyhow!("`row_count` column not found in backfill table").into(),
                );
            };
            let scan = LogicalScan::create(
                table.name.clone(),
                table,
                vec![],
                plan.ctx(),
                None,
                Default::default(),
            );
            let project = {
                let job_id_expr = ExprImpl::Literal(Box::new(Literal::new(
                    Some(ScalarImpl::Int32(job_id.table_id as i32)),
                    DataType::Int32,
                )));
                let row_count_expr = ExprImpl::InputRef(Box::new(InputRef {
                    index: row_count_column_index,
                    data_type: DataType::Int64,
                }));
                LogicalProject::new(scan.into(), vec![job_id_expr, row_count_expr])
            };
            counts.push(project.into());
        }
        let union = LogicalUnion::new(true, counts);
        let select_exprs = {
            let job_id = ExprImpl::InputRef(Box::new(InputRef {
                index: 0,
                data_type: DataType::Int32,
            }));
            let sum_agg = ExprImpl::AggCall(Box::new(AggCall::new(
                AggType::Builtin(PbAggKind::Sum),
                vec![ExprImpl::InputRef(Box::new(InputRef {
                    index: 1,
                    data_type: DataType::Int64,
                }))],
                false,
                OrderBy::any(),
                Condition::true_cond(),
                vec![],
            )?));
            vec![job_id, sum_agg]
        };
        let group_key = GroupBy::GroupKey(vec![ExprImpl::InputRef(Box::new(InputRef {
            index: 0,
            data_type: DataType::Int32,
        }))]);
        let (agg, _rewritten_select_exprs, _rewritten_having_exprs) =
            LogicalAgg::create(select_exprs, group_key, None, union.into())?;
        let current_counts = LogicalProject::new(
            agg,
            vec![
                ExprImpl::InputRef(Box::new(InputRef {
                    index: 0,
                    data_type: DataType::Int32,
                })),
                ExprImpl::InputRef(Box::new(InputRef {
                    index: 1,
                    data_type: DataType::Decimal,
                }))
                .cast_explicit(DataType::Int64)?,
            ],
        );

        let total_counts = {
            let catalog = plan.ctx().session_ctx().env().catalog_reader().read_guard();
            let mut total_counts = vec![];
            for job_id in backfilling_job_ids {
                let total_key_count = if let Some(stats) =
                    catalog.table_stats().table_stats.get(&(job_id.table_id))
                {
                    stats.total_key_count
                } else {
                    return ApplyResult::Err(
                        anyhow!("Table stats not found for table_id: {}", job_id.table_id).into(),
                    );
                };
                let job_id_expr = ExprImpl::Literal(Box::new(Literal::new(
                    Some(ScalarImpl::Int32(job_id.table_id as i32)),
                    DataType::Int32,
                )));
                let total_key_count_expr = ExprImpl::Literal(Box::new(Literal::new(
                    Some(ScalarImpl::Int64(total_key_count as i64)),
                    DataType::Int64,
                )));
                let total_count = LogicalValues::new(
                    vec![vec![job_id_expr, total_key_count_expr]],
                    Schema::new(vec![
                        Field::new("job_id", DataType::Int32),
                        Field::new("total_key_count", DataType::Int64),
                    ]),
                    plan.ctx().clone(),
                );
                total_counts.push(total_count.into());
            }
            LogicalUnion::new(true, total_counts)
        };

        let join = {
            let conjunctions = vec![ExprImpl::FunctionCall(Box::new(FunctionCall::new(
                ExprType::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef {
                        index: 0,
                        data_type: DataType::Int32,
                    })),
                    ExprImpl::InputRef(Box::new(InputRef {
                        index: 2,
                        data_type: DataType::Int32,
                    })),
                ],
            )?))];
            let condition = Condition { conjunctions };
            LogicalJoin::new(
                current_counts.into(),
                total_counts.into(),
                JoinType::Inner,
                condition,
            )
        };

        let project = {
            let op1 = ExprImpl::InputRef(Box::new(InputRef {
                index: 1,
                data_type: DataType::Int64,
            }))
            .cast_implicit(DataType::Decimal)?;
            let op2 = ExprImpl::InputRef(Box::new(InputRef {
                index: 3,
                data_type: DataType::Int64,
            }))
            .cast_implicit(DataType::Decimal)?;
            let div_expr = ExprImpl::FunctionCall(Box::new(FunctionCall::new(
                ExprType::Divide,
                vec![op1, op2],
            )?));
            LogicalProject::new(
                join.into(),
                vec![
                    ExprImpl::InputRef(Box::new(InputRef {
                        index: 0,
                        data_type: DataType::Int32,
                    })),
                    div_expr,
                ],
            )
        };

        ApplyResult::Ok(project.into())
    }
}

/// Gets the internal backfill state tables from the catalog
fn get_backfilling_tables(
    reader: CatalogReadGuard,
    db_name: &str,
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<Arc<TableCatalog>>> {
    let schema = reader.get_schema_by_name(db_name, schema_name)?;
    Ok(schema
        .iter_internal_table()
        .filter(|table| table.is_internal_table())
        .filter(|table| {
            let name = &table.name;
            match internal_table_name_to_parts(name) {
                None => false,
                Some((job_name, _fragment_id, executor_type, _table_id)) => {
                    executor_type == "streamscan" && job_name == table_name
                }
            }
        })
        .cloned()
        .collect_vec())
}

impl TableFunctionToBackfillProgressRule {
    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToBackfillProgressRule {})
    }
}
