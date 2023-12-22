// Copyright 2023 RisingWave Labs
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

use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Row;
use risingwave_common::bail_not_implemented;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{ExplainOptions, ExplainType, Statement};
use thiserror_ext::AsReport;

use super::create_index::gen_create_index_plan;
use super::create_mv::gen_create_mv_plan;
use super::create_sink::gen_sink_plan;
use super::create_table::ColumnIdGenerator;
use super::query::gen_batch_plan_by_statement;
use super::util::SourceSchemaCompatExt;
use super::RwPgResponse;
use crate::handler::create_table::handle_create_table_plan;
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{Convention, Explain};
use crate::optimizer::OptimizerContext;
use crate::scheduler::worker_node_manager::WorkerNodeSelector;
use crate::scheduler::BatchPlanFragmenter;
use crate::stream_fragmenter::build_graph;
use crate::utils::explain_stream_graph;
use crate::OptimizerContextRef;

async fn do_handle_explain(
    context: OptimizerContext,
    stmt: Statement,
    blocks: &mut Vec<String>,
) -> Result<()> {
    // Workaround to avoid `Rc` across `await` point.
    let mut batch_plan_fragmenter = None;

    {
        let session = context.session_ctx().clone();

        let (plan, context) = match stmt {
            // `CREATE TABLE` takes the ownership of the `OptimizerContext` to avoid `Rc` across
            // `await` point. We can only take the reference back from the `PlanRef` if it's
            // successfully planned.
            Statement::CreateTable {
                name,
                columns,
                constraints,
                source_schema,
                source_watermarks,
                append_only,
                cdc_table_info,
                include_column_options,
                ..
            } => {
                let col_id_gen = ColumnIdGenerator::new_initial();

                let source_schema = source_schema.map(|s| s.into_v2_with_warning());

                let (plan, _source, _table, _job_type) = handle_create_table_plan(
                    context,
                    col_id_gen,
                    source_schema,
                    cdc_table_info,
                    name.clone(),
                    columns,
                    constraints,
                    source_watermarks,
                    append_only,
                    include_column_options,
                )
                .await?;
                let context = plan.ctx();
                (Ok(plan), context)
            }

            // For other queries without `await` point, we can keep a copy of reference to the
            // `OptimizerContext` even if the planning fails. This enables us to log the partial
            // traces for better debugging experience.
            _ => {
                let context: OptimizerContextRef = context.into();
                let plan = match stmt {
                    // -- Streaming DDLs --
                    Statement::CreateView {
                        or_replace: false,
                        materialized: true,
                        query,
                        name,
                        columns,
                        emit_mode,
                        ..
                    } => gen_create_mv_plan(
                        &session,
                        context.clone(),
                        *query,
                        name,
                        columns,
                        emit_mode,
                    )
                    .map(|x| x.0),

                    Statement::CreateSink { stmt } => {
                        gen_sink_plan(&session, context.clone(), stmt).map(|plan| plan.sink_plan)
                    }

                    Statement::CreateIndex {
                        name,
                        table_name,
                        columns,
                        include,
                        distributed_by,
                        ..
                    } => gen_create_index_plan(
                        &session,
                        context.clone(),
                        name,
                        table_name,
                        columns,
                        include,
                        distributed_by,
                    )
                    .map(|x| x.0),

                    // -- Batch Queries --
                    Statement::Insert { .. }
                    | Statement::Delete { .. }
                    | Statement::Update { .. }
                    | Statement::Query { .. } => {
                        gen_batch_plan_by_statement(&session, context.clone(), stmt).map(|x| x.plan)
                    }

                    _ => bail_not_implemented!("unsupported statement {:?}", stmt),
                };

                (plan, context)
            }
        };

        let explain_trace = context.is_explain_trace();
        let explain_verbose = context.is_explain_verbose();
        let explain_type = context.explain_type();

        if explain_trace {
            let trace = context.take_trace();
            blocks.extend(trace);
        }

        match explain_type {
            ExplainType::DistSql => {
                if let Ok(plan) = &plan {
                    match plan.convention() {
                        Convention::Logical => unreachable!(),
                        Convention::Batch => {
                            let worker_node_manager_reader = WorkerNodeSelector::new(
                                session.env().worker_node_manager_ref(),
                                session.is_barrier_read(),
                            );
                            batch_plan_fragmenter = Some(BatchPlanFragmenter::new(
                                worker_node_manager_reader,
                                session.env().catalog_reader().clone(),
                                session.config().batch_parallelism().0,
                                plan.clone(),
                            )?);
                        }
                        Convention::Stream => {
                            let graph = build_graph(plan.clone());
                            blocks.push(explain_stream_graph(&graph, explain_verbose));
                        }
                    }
                }
            }
            ExplainType::Physical => {
                // if explain trace is on, the plan has been in the rows
                if !explain_trace && let Ok(plan) = &plan {
                    blocks.push(plan.explain_to_string());
                }
            }
            ExplainType::Logical => {
                // if explain trace is on, the plan has been in the rows
                if !explain_trace {
                    let output = context.take_logical().ok_or_else(|| {
                        ErrorCode::InternalError("Logical plan not found for query".into())
                    })?;
                    blocks.push(output);
                }
            }
        }

        // Throw the error.
        plan?;
    }

    if let Some(fragmenter) = batch_plan_fragmenter {
        let query = fragmenter.generate_complete_query().await?;
        let stage_graph_json = serde_json::to_string_pretty(&query.stage_graph).unwrap();
        blocks.push(stage_graph_json);
    }

    Ok(())
}

pub async fn handle_explain(
    handler_args: HandlerArgs,
    stmt: Statement,
    options: ExplainOptions,
    analyze: bool,
) -> Result<RwPgResponse> {
    if analyze {
        bail_not_implemented!(issue = 4856, "explain analyze");
    }

    let context = OptimizerContext::new(handler_args.clone(), options.clone());

    let mut blocks = Vec::new();
    let result = do_handle_explain(context, stmt, &mut blocks).await;

    if let Err(e) = result {
        if options.trace {
            // If `trace` is on, we include the error in the output with partial traces.
            blocks.push(if options.verbose {
                format!("ERROR: {:?}", e.as_report())
            } else {
                format!("ERROR: {}", e.as_report())
            });
        } else {
            // Else, directly return the error.
            return Err(e);
        }
    }

    let rows = blocks
        .iter()
        .flat_map(|b| b.lines().map(|l| l.to_owned()))
        .map(|l| Row::new(vec![Some(l.into())]))
        .collect_vec();

    Ok(PgResponse::builder(StatementType::EXPLAIN)
        .values(
            rows.into(),
            vec![PgFieldDescriptor::new(
                "QUERY PLAN".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            )],
        )
        .into())
}
