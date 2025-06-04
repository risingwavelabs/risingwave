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

use petgraph::dot::Dot;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_batch::worker_manager::worker_node_manager::WorkerNodeSelector;
use risingwave_common::bail_not_implemented;
use risingwave_common::types::Fields;
use risingwave_sqlparser::ast::{
    ExplainFormat, ExplainOptions, ExplainType, FetchCursorStatement, Statement,
};
use thiserror_ext::AsReport;

use super::create_index::{gen_create_index_plan, resolve_index_schema};
use super::create_mv::gen_create_mv_plan;
use super::create_sink::gen_sink_plan;
use super::query::gen_batch_plan_by_statement;
use super::util::SourceSchemaCompatExt;
use super::{RwPgResponse, RwPgResponseBuilderExt};
use crate::OptimizerContextRef;
use crate::error::{ErrorCode, Result};
use crate::handler::HandlerArgs;
use crate::handler::create_table::handle_create_table_plan;
use crate::optimizer::OptimizerContext;
use crate::optimizer::backfill_order_strategy::explain_backfill_order_in_dot_format;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{Convention, Explain};
use crate::scheduler::BatchPlanFragmenter;
use crate::stream_fragmenter::build_graph;
use crate::utils::{explain_stream_graph, explain_stream_graph_as_dot};

pub async fn do_handle_explain(
    handler_args: HandlerArgs,
    explain_options: ExplainOptions,
    stmt: Statement,
    blocks: &mut Vec<String>,
) -> Result<()> {
    // Workaround to avoid `Rc` across `await` point.
    let mut batch_plan_fragmenter = None;
    let mut batch_plan_fragmenter_fmt = ExplainFormat::Json;

    let session = handler_args.session.clone();

    {
        let (plan, context) = match stmt {
            // `CREATE TABLE` takes the ownership of the `OptimizerContext` to avoid `Rc` across
            // `await` point. We can only take the reference back from the `PlanRef` if it's
            // successfully planned.
            Statement::CreateTable {
                name,
                columns,
                constraints,
                format_encode,
                source_watermarks,
                append_only,
                on_conflict,
                with_version_column,
                cdc_table_info,
                include_column_options,
                wildcard_idx,
                webhook_info,
                ..
            } => {
                let format_encode = format_encode.map(|s| s.into_v2_with_warning());

                let (plan, _source, _table, _job_type) = handle_create_table_plan(
                    handler_args,
                    explain_options,
                    format_encode,
                    cdc_table_info,
                    name.clone(),
                    columns,
                    wildcard_idx,
                    constraints,
                    source_watermarks,
                    append_only,
                    on_conflict,
                    with_version_column.map(|x| x.real_value()),
                    include_column_options,
                    webhook_info,
                    risingwave_common::catalog::Engine::Hummock,
                )
                .await?;
                let context = plan.ctx();
                (Ok(plan), context)
            }
            Statement::CreateSink { stmt } => {
                let plan = gen_sink_plan(handler_args, stmt, Some(explain_options), false)
                    .await
                    .map(|plan| plan.sink_plan)?;
                let context = plan.ctx();
                (Ok(plan), context)
            }

            Statement::FetchCursor {
                stmt: FetchCursorStatement { cursor_name, .. },
            } => {
                let cursor_manager = session.clone().get_cursor_manager();
                let plan = cursor_manager
                    .gen_batch_plan_with_subscription_cursor(
                        &cursor_name.real_value(),
                        handler_args,
                    )
                    .await
                    .map(|x| x.plan)?;
                let context = plan.ctx();
                (Ok(plan), context)
            }

            // For other queries without `await` point, we can keep a copy of reference to the
            // `OptimizerContext` even if the planning fails. This enables us to log the partial
            // traces for better debugging experience.
            _ => {
                let context: OptimizerContextRef =
                    OptimizerContext::new(handler_args, explain_options).into();
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
                    Statement::CreateView {
                        materialized: false,
                        ..
                    } => {
                        return Err(ErrorCode::NotSupported(
                            "EXPLAIN CREATE VIEW".into(),
                            "A created VIEW is just an alias. Instead, use EXPLAIN on the queries which reference the view.".into()
                        ).into());
                    }

                    Statement::CreateSubscription { .. } => {
                        return Err(ErrorCode::NotSupported(
                            "EXPLAIN CREATE SUBSCRIPTION".into(),
                            "A created SUBSCRIPTION only incremental data queries on the table, not supported EXPLAIN".into()
                        ).into());
                    }
                    Statement::CreateIndex {
                        name,
                        table_name,
                        columns,
                        include,
                        distributed_by,
                        ..
                    } => {
                        let (schema_name, table, index_table_name) =
                            resolve_index_schema(&session, name, table_name)?;
                        gen_create_index_plan(
                            &session,
                            context.clone(),
                            schema_name,
                            table,
                            index_table_name,
                            columns,
                            include,
                            distributed_by,
                        )
                    }
                    .map(|x| x.0),

                    // -- Batch Queries --
                    Statement::Insert { .. }
                    | Statement::Delete { .. }
                    | Statement::Update { .. }
                    | Statement::Query { .. } => {
                        gen_batch_plan_by_statement(&session, context, stmt).map(|x| x.plan)
                    }

                    _ => bail_not_implemented!("unsupported statement for EXPLAIN: {stmt}"),
                };

                let plan = plan?;
                let context = plan.ctx().clone();

                (Ok(plan) as Result<_>, context)
            }
        };

        let explain_trace = context.is_explain_trace();
        let explain_verbose = context.is_explain_verbose();
        let explain_backfill = context.is_explain_backfill();
        let explain_type = context.explain_type();
        let explain_format = context.explain_format();

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
                                session.config().timezone().to_owned(),
                                plan.clone(),
                            )?);
                            batch_plan_fragmenter_fmt = if explain_format == ExplainFormat::Dot {
                                ExplainFormat::Dot
                            } else {
                                ExplainFormat::Json
                            }
                        }
                        Convention::Stream => {
                            let graph = build_graph(plan.clone(), None)?;
                            if explain_format == ExplainFormat::Dot {
                                blocks.push(explain_stream_graph_as_dot(&graph, explain_verbose))
                            } else {
                                blocks.push(explain_stream_graph(&graph, explain_verbose));
                            }
                        }
                    }
                }
            }
            ExplainType::Physical => {
                // if explain trace is on, the plan has been in the rows
                if !explain_trace && let Ok(plan) = &plan {
                    match explain_format {
                        ExplainFormat::Text => {
                            blocks.push(plan.explain_to_string());
                        }
                        ExplainFormat::Json => blocks.push(plan.explain_to_json()),
                        ExplainFormat::Xml => blocks.push(plan.explain_to_xml()),
                        ExplainFormat::Yaml => blocks.push(plan.explain_to_yaml()),
                        ExplainFormat::Dot => {
                            if explain_backfill {
                                let dot_formatted_backfill_order =
                                    explain_backfill_order_in_dot_format(
                                        &session,
                                        context.with_options().backfill_order_strategy(),
                                        plan.clone(),
                                    )?;
                                blocks.push(dot_formatted_backfill_order);
                            } else {
                                blocks.push(plan.explain_to_dot());
                            }
                        }
                    }
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
        let stage_graph = if batch_plan_fragmenter_fmt == ExplainFormat::Dot {
            let graph = query.stage_graph.to_petgraph();
            let dot = Dot::new(&graph);
            dot.to_string()
        } else {
            serde_json::to_string_pretty(&query.stage_graph).unwrap()
        };
        blocks.push(stage_graph);
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
        // NOTE(kwannoel): This path is for explain analyze on stream and batch queries.
        // For existing stream jobs, see the handler module `explain_analyze` instead.
        bail_not_implemented!(issue = 4856, "explain analyze");
    }
    if options.trace && options.explain_format == ExplainFormat::Json {
        return Err(ErrorCode::NotSupported(
            "EXPLAIN (TRACE, JSON FORMAT)".to_owned(),
            "Only EXPLAIN (LOGICAL | PHYSICAL, JSON FORMAT) is supported.".to_owned(),
        )
        .into());
    }
    if options.explain_type == ExplainType::DistSql && options.explain_format == ExplainFormat::Json
    {
        return Err(ErrorCode::NotSupported(
            "EXPLAIN (TRACE, JSON FORMAT)".to_owned(),
            "Only EXPLAIN (LOGICAL | PHYSICAL, JSON FORMAT) is supported.".to_owned(),
        )
        .into());
    }

    let mut blocks = Vec::new();
    let result = do_handle_explain(handler_args, options.clone(), stmt, &mut blocks).await;

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

    let rows = blocks.iter().flat_map(|b| b.lines()).map(|l| ExplainRow {
        query_plan: l.into(),
    });

    Ok(PgResponse::builder(StatementType::EXPLAIN)
        .rows(rows)
        .into())
}

#[derive(Fields)]
#[fields(style = "TITLE CASE")]
pub(crate) struct ExplainRow {
    pub query_plan: String,
}
