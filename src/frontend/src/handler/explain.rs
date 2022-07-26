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
use risingwave_common::error::Result;
use risingwave_common::session_config::QueryMode;
use risingwave_sqlparser::ast::Statement;

use super::create_index::gen_create_index_plan;
use super::create_mv::gen_create_mv_plan;
use super::create_table::gen_create_table_plan;
use super::util::handle_with_properties;
use crate::binder::Binder;
use crate::planner::Planner;
use crate::session::OptimizerContext;

pub(super) fn handle_explain(
    context: OptimizerContext,
    stmt: Statement,
    verbose: bool,
    trace: bool,
) -> Result<PgResponse> {
    let session = context.session_ctx.clone();
    context.explain_verbose.store(verbose, Ordering::Release);
    context.explain_trace.store(trace, Ordering::Release);
    // bind, plan, optimize, and serialize here
    let mut planner = Planner::new(context.into());
    let plan = match stmt {
        Statement::CreateView {
            or_replace: false,
            materialized: true,
            query,
            name,
            with_options,
            ..
        } => {
            gen_create_mv_plan(
                &session,
                planner.ctx(),
                query,
                name,
                handle_with_properties("explain create_mv", with_options)?,
            )?
            .0
        }

        Statement::CreateTable {
            name,
            columns,
            with_options,
            ..
        } => {
            gen_create_table_plan(
                &session,
                planner.ctx(),
                name,
                columns,
                handle_with_properties("explain create_table", with_options)?,
            )?
            .0
        }

        Statement::CreateIndex {
            name,
            table_name,
            columns,
            ..
        } => gen_create_index_plan(&session, planner.ctx(), name, table_name, columns)?.0,

        stmt => {
            let bound = {
                let mut binder = Binder::new(
                    session.env().catalog_reader().read_guard(),
                    session.database().to_string(),
                );
                binder.bind(stmt)?
            };
            let logical = planner.plan(bound)?;

            let query_mode = session.config().get_query_mode();
            match query_mode {
                QueryMode::Local => logical.gen_batch_local_plan()?,
                QueryMode::Distributed => logical.gen_batch_query_plan()?,
            }
        }
    };

    let ctx = plan.plan_base().ctx.clone();
    let explain_trace = ctx.is_explain_trace();

    let rows = if explain_trace {
        let trace = ctx.take_trace();
        trace
            .iter()
            .flat_map(|s| s.lines())
            .map(|s| Row::new(vec![Some(s.to_string().into())]))
            .collect::<Vec<_>>()
    } else {
        let output = plan.explain_to_string()?;
        output
            .lines()
            .map(|s| Row::new(vec![Some(s.to_string().into())]))
            .collect::<Vec<_>>()
    };

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
