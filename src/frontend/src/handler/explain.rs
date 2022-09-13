// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::Ordering;

use pgwire::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Row;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::session_config::QueryMode;
use risingwave_sqlparser::ast::{ExplainOptions, ExplainType, Statement};

use super::create_index::gen_create_index_plan;
use super::create_mv::gen_create_mv_plan;
use super::create_sink::gen_sink_plan;
use super::create_table::gen_create_table_plan;
use crate::binder::Binder;
use crate::handler::util::force_local_mode;
use crate::planner::Planner;
use crate::scheduler::BatchPlanFragmenter;
use crate::session::OptimizerContext;
use crate::stream_fragmenter::build_graph;
use crate::utils::explain_stream_graph;

pub(super) fn handle_explain(
    context: OptimizerContext,
    stmt: Statement,
    options: ExplainOptions,
    analyze: bool,
) -> Result<PgResponse> {
    if analyze {
        return Err(ErrorCode::NotImplemented("explain analyze".to_string(), 4856.into()).into());
    }
    if options.explain_type == ExplainType::Logical {
        return Err(ErrorCode::NotImplemented("explain logical".to_string(), 4856.into()).into());
    }

    let session = context.session_ctx.clone();
    context
        .explain_verbose
        .store(options.verbose, Ordering::Release);
    context
        .explain_trace
        .store(options.trace, Ordering::Release);
    // bind, plan, optimize, and serialize here
    let mut planner = Planner::new(context.into());
    // we need to know if the plan is streaming or batch plan,
    let (plan, is_streaming) = match stmt {
        Statement::CreateView {
            or_replace: false,
            materialized: true,
            query,
            name,
            ..
        } => gen_create_mv_plan(&session, planner.ctx(), query, name)?.0,

        Statement::CreateSink { stmt } => (gen_sink_plan(&session, planner.ctx(), stmt)?.0, true),

        Statement::CreateTable {
            name,
            columns,
            constraints,
            ..
        } => (
            gen_create_table_plan(&session, planner.ctx(), name, columns, constraints)?.0,
            true,
        ),

        Statement::CreateIndex {
            name,
            table_name,
            columns,
            include,
            ..
        } => (
            gen_create_index_plan(&session, planner.ctx(), name, table_name, columns, include)?.0,
            true,
        ),

        stmt => {
            let bound = {
                let mut binder = Binder::new(&session);
                binder.bind(stmt)?
            };

            let query_mode = if force_local_mode(&bound) {
                QueryMode::Local
            } else {
                session.config().get_query_mode()
            };
            let logical = planner.plan(bound)?;
            let plan = match query_mode {
                QueryMode::Local => logical.gen_batch_local_plan()?,
                QueryMode::Distributed => logical.gen_batch_distributed_plan()?,
            };

            if options.explain_type == ExplainType::DistSQL {}

            (plan, false)
        }
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

    if options.explain_type == ExplainType::DistSQL {
        if is_streaming {
            let graph = build_graph(plan);
            rows.extend(
                explain_stream_graph(&graph, explain_verbose)?
                    .lines()
                    .map(|s| Row::new(vec![Some(s.to_string().into())])),
            );
        } else {
            let plan_fragmenter = BatchPlanFragmenter::new(
                session.env().worker_node_manager_ref(),
                session.env().catalog_reader().clone(),
            );
            let query = plan_fragmenter.split(plan)?;
            let stage_graph_json = serde_json::to_string_pretty(&query.stage_graph).unwrap();
            rows.extend(
                vec![stage_graph_json]
                    .iter()
                    .flat_map(|s| s.lines())
                    .map(|s| Row::new(vec![Some(s.to_string().into())])),
            );
        }
    } else {
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

    Ok(PgResponse::new(
        StatementType::EXPLAIN,
        rows.len() as i32,
        rows,
        vec![PgFieldDescriptor::new(
            "QUERY PLAN".to_owned(),
            TypeOid::Varchar,
        )],
        true,
    ))
}
