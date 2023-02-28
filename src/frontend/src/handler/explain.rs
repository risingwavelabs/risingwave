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

use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
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
use super::query::gen_batch_query_plan;
use super::RwPgResponse;
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::{Convention, Explain};
use crate::optimizer::OptimizerContext;
use crate::scheduler::BatchPlanFragmenter;
use crate::stream_fragmenter::build_graph;
use crate::utils::explain_stream_graph;

pub async fn handle_explain(
    handler_args: HandlerArgs,
    stmt: Statement,
    options: ExplainOptions,
    analyze: bool,
) -> Result<RwPgResponse> {
    let context = OptimizerContext::new(handler_args.clone(), options.clone());

    if analyze {
        return Err(ErrorCode::NotImplemented("explain analyze".to_string(), 4856.into()).into());
    }

    let session = context.session_ctx().clone();

    let mut plan_fragmenter = None;
    let mut rows = {
        let plan = match stmt {
            Statement::CreateView {
                or_replace: false,
                materialized: true,
                query,
                name,
                columns,
                ..
            } => gen_create_mv_plan(&session, context.into(), *query, name, columns)?.0,

            Statement::CreateSink { stmt } => gen_sink_plan(&session, context.into(), stmt)?.0,

            Statement::CreateTable {
                name,
                columns,
                constraints,
                source_schema,
                source_watermarks,
                append_only,
                ..
            } => match check_create_table_with_source(&handler_args.with_options, source_schema)? {
                Some(s) => {
                    gen_create_table_plan_with_source(
                        context,
                        name,
                        columns,
                        constraints,
                        s,
                        source_watermarks,
                        ColumnIdGenerator::new_initial(),
                        append_only,
                    )
                    .await?
                    .0
                }
                None => {
                    gen_create_table_plan(
                        context,
                        name,
                        columns,
                        constraints,
                        ColumnIdGenerator::new_initial(),
                        source_watermarks,
                        append_only,
                    )?
                    .0
                }
            },

            Statement::CreateIndex {
                name,
                table_name,
                columns,
                include,
                distributed_by,
                ..
            } => {
                gen_create_index_plan(
                    &session,
                    context.into(),
                    name,
                    table_name,
                    columns,
                    include,
                    distributed_by,
                )?
                .0
            }

            stmt => gen_batch_query_plan(&session, context.into(), stmt)?.0,
        };

        let ctx = plan.plan_base().ctx.clone();
        let explain_trace = ctx.is_explain_trace();
        let explain_verbose = ctx.is_explain_verbose();

        let mut rows = if explain_trace {
            let trace = ctx.take_trace();
            trace
                .iter()
                .flat_map(|s| s.lines())
                .map(|s| Row::new(vec![Some(s.to_string().into())]))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        match options.explain_type {
            ExplainType::DistSql => match plan.convention() {
                Convention::Logical => unreachable!(),
                Convention::Batch => {
                    plan_fragmenter = Some(BatchPlanFragmenter::new(
                        session.env().worker_node_manager_ref(),
                        session.env().catalog_reader().clone(),
                        plan,
                    )?);
                }
                Convention::Stream => {
                    let graph = build_graph(plan);
                    rows.extend(
                        explain_stream_graph(&graph, explain_verbose)?
                            .lines()
                            .map(|s| Row::new(vec![Some(s.to_string().into())])),
                    );
                }
            },
            ExplainType::Physical => {
                // if explain trace is open, the plan has been in the rows
                if !explain_trace {
                    let output = plan.explain_to_string()?;
                    rows.extend(
                        output
                            .lines()
                            .map(|s| Row::new(vec![Some(s.to_string().into())])),
                    );
                }
            }
            ExplainType::Logical => {
                // if explain trace is open, the plan has been in the rows
                if !explain_trace {
                    let output = plan.ctx().take_logical().ok_or_else(|| {
                        ErrorCode::InternalError("Logical plan not found for query".into())
                    })?;
                    rows.extend(
                        output
                            .lines()
                            .map(|s| Row::new(vec![Some(s.to_string().into())])),
                    );
                }
            }
        }
        rows
    };

    if let Some(plan_fragmenter) = plan_fragmenter {
        let query = plan_fragmenter.generate_complete_query().await?;
        let stage_graph_json = serde_json::to_string_pretty(&query.stage_graph).unwrap();
        rows.extend(
            vec![stage_graph_json]
                .iter()
                .flat_map(|s| s.lines())
                .map(|s| Row::new(vec![Some(s.to_string().into())])),
        );
    }

    Ok(PgResponse::new_for_stream(
        StatementType::EXPLAIN,
        None,
        rows.into(),
        vec![PgFieldDescriptor::new(
            "QUERY PLAN".to_owned(),
            DataType::VARCHAR.to_oid(),
            DataType::VARCHAR.type_len(),
        )],
    ))
}
