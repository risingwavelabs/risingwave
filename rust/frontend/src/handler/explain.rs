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
//
use std::cell::RefCell;
use std::rc::Rc;

use pgwire::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Row;
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::Statement;

use super::create_mv::gen_create_mv_plan;
use crate::binder::Binder;
use crate::planner::Planner;
use crate::session::QueryContext;

pub(super) fn handle_explain(
    context: QueryContext,
    stmt: Statement,
    _verbose: bool,
) -> Result<PgResponse> {
    let context = Rc::new(RefCell::new(context));
    let session = context.borrow().session_ctx.clone();
    // bind, plan, optimize, and serialize here
    let mut planner = Planner::new(context);

    let plan = match stmt {
        Statement::CreateView {
            or_replace: false,
            materialized: true,
            query,
            ..
        } => gen_create_mv_plan(&session, &mut planner, query.as_ref().clone())?,

        Statement::CreateTable { .. } => todo!(),

        stmt => {
            let bound = {
                let mut binder = Binder::new(
                    session.env().catalog_reader().read_guard(),
                    session.database().to_string(),
                );
                binder.bind(stmt)?
            };
            let logical = planner.plan(bound)?;
            logical.gen_batch_query_plan()
        }
    };

    let output = plan.explain_to_string()?;

    let rows = output
        .lines()
        .map(|s| Row::new(vec![Some(s.into())]))
        .collect::<Vec<_>>();

    Ok(PgResponse::new(
        StatementType::EXPLAIN,
        rows.len() as i32,
        rows,
        vec![PgFieldDescriptor::new(
            "QUERY PLAN".to_owned(),
            TypeOid::Varchar,
        )],
    ))
}
