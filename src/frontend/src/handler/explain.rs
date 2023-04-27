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

use futures::StreamExt;
use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, RowSetResult, StatementType};
use pgwire::types::Row;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{ExplainOptions, ExplainType, Statement};

use super::create_index::gen_create_index_plan;
use super::create_mv::gen_create_mv_plan;
use super::create_sink::gen_sink_plan;
use super::create_table::{
    check_create_table_with_source, gen_create_table_plan, gen_create_table_plan_with_source,
    ColumnIdGenerator,
};
use super::query::gen_batch_plan_by_statement;
use super::RwPgResponse;
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::{Convention, Explain};
use crate::optimizer::OptimizerContext;
use crate::scheduler::BatchPlanFragmenter;
use crate::stream_fragmenter::build_graph;
use crate::utils::explain_stream_graph;
use crate::{OptimizerContextRef, PgResponseStream};

// Statement::CreateTable {
//     name,
//     columns,
//     constraints,
//     source_schema,
//     source_watermarks,
//     append_only,
//     ..
// } => match check_create_table_with_source(&handler_args.with_options, source_schema)?
// {     Some(s) => {
//         gen_create_table_plan_with_source(
//             context,
//             name,
//             columns,
//             constraints,
//             s,
//             source_watermarks,
//             ColumnIdGenerator::new_initial(),
//             append_only,
//         )
//         .await?
//         .0
//     }
//     None => {
//         gen_create_table_plan(
//             context,
//             name,
//             columns,
//             constraints,
//             ColumnIdGenerator::new_initial(),
//             source_watermarks,
//             append_only,
//         )?
//         .0
//     }
// },

async fn do_handle_explain(
    context: OptimizerContext,
    stmt: Statement,
    blocks: &mut Vec<String>,
) -> Result<()> {
    // Hack to avoid `Rc` across `await` point.
    let mut plan_fragmenter = None;

    {
        let context: OptimizerContextRef = context.into();
        let session = context.session_ctx().clone();

        let plan = match stmt {
            Statement::CreateView {
                or_replace: false,
                materialized: true,
                query,
                name,
                columns,
                ..
            } => gen_create_mv_plan(&session, context.clone(), *query, name, columns).map(|x| x.0),

            Statement::CreateSink { stmt } => {
                gen_sink_plan(&session, context.clone(), stmt).map(|x| x.0)
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

            Statement::Insert { .. }
            | Statement::Delete { .. }
            | Statement::Update { .. }
            | Statement::Query { .. } => {
                gen_batch_plan_by_statement(&session, context.clone(), stmt).map(|x| x.plan)
            }

            _ => {
                return Err(ErrorCode::NotImplemented(
                    format!("unsupported statement {:?}", stmt),
                    None.into(),
                )
                .into())
            }
        };

        let explain_trace = context.is_explain_trace();
        let explain_verbose = context.is_explain_verbose();
        let explain_type = context.explain_type();

        if explain_trace {
            let trace = context.take_trace();
            blocks.extend(trace);
        }

        // Throw the error.
        let plan = plan?;

        match explain_type {
            ExplainType::DistSql => match plan.convention() {
                Convention::Logical => unreachable!(),
                Convention::Batch => {
                    plan_fragmenter = Some(BatchPlanFragmenter::new(
                        session.env().worker_node_manager_ref(),
                        session.env().catalog_reader().clone(),
                        session.config().get_batch_parallelism(),
                        plan,
                    )?);
                }
                Convention::Stream => {
                    let graph = build_graph(plan);
                    blocks.push(explain_stream_graph(&graph, explain_verbose));
                }
            },
            ExplainType::Physical => {
                // if explain trace is open, the plan has been in the rows
                if !explain_trace {
                    let output = plan.explain_to_string()?;
                    blocks.push(output);
                }
            }
            ExplainType::Logical => {
                // if explain trace is open, the plan has been in the rows
                if !explain_trace {
                    let output = context.take_logical().ok_or_else(|| {
                        ErrorCode::InternalError("Logical plan not found for query".into())
                    })?;
                    blocks.push(output);
                }
            }
        }
    }

    if let Some(plan_fragmenter) = plan_fragmenter {
        let query = plan_fragmenter.generate_complete_query().await?;
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
        return Err(ErrorCode::NotImplemented("explain analyze".to_string(), 4856.into()).into());
    }

    let context = OptimizerContext::new(handler_args.clone(), options.clone()).into();
    let mut blocks = Vec::new();

    let result = do_handle_explain(context, stmt, &mut blocks).await;

    let mut row_sets = Vec::<RowSetResult>::new();
    for block in blocks {
        let row_set = block
            .lines()
            .map(|l| Row::new(vec![Some(l.to_owned().into())]))
            .collect_vec();
        row_sets.push(Ok(row_set));
    }

    if let Err(e) = result {
        row_sets.push(Err(e.into()));
    }

    let stream = PgResponseStream::Rows(futures::stream::iter(row_sets).boxed());

    Ok(PgResponse::new_for_stream(
        StatementType::EXPLAIN,
        None,
        stream,
        vec![PgFieldDescriptor::new(
            "QUERY PLAN".to_owned(),
            DataType::Varchar.to_oid(),
            DataType::Varchar.type_len(),
        )],
    ))
}
