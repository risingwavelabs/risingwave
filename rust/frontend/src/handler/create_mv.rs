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

use pgwire::pg_response::PgResponse;
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::{ObjectName, Query, Statement};

use crate::binder::Binder;
use crate::planner::Planner;
use crate::session::QueryContext;

pub async fn handle_create_mv(
    context: QueryContext,
    name: ObjectName,
    query: Box<Query>,
) -> Result<PgResponse> {
    let (schema_name, table_name) = Binder::resolve_table_name(name.clone())?;
    let session = context.session_ctx.clone();
    let (_db_id, _schema_id) = session
        .env()
        .catalog_reader()
        .read_guard()
        .check_relation_name(session.database(), &schema_name, &table_name)?;
    let bound = {
        let mut binder = Binder::new(
            session.env().catalog_reader().read_guard(),
            session.database().to_string(),
        );
        binder.bind(Statement::Query(query))?
    };
    let _catalog_writer = session.env().catalog_writer();
    let plan = Planner::new(Rc::new(RefCell::new(context)))
        .plan(bound)?
        .gen_create_mv_plan()
        .to_stream_prost();
    // TODO catalog writer to create mv

    let json_plan = serde_json::to_string(&plan).unwrap();
    tracing::info!(name= ?name, plan = ?json_plan);

    Ok(PgResponse::new(
        pgwire::pg_response::StatementType::CREATE_MATERIALIZED_VIEW,
        0,
        vec![],
        vec![],
    ))
}
