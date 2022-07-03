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

use std::collections::HashMap;

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_sqlparser::ast::{CreateSinkStatement, ObjectName, Query};

use super::util::handle_with_properties;
use crate::binder::{Binder, BoundSetExpr};
use crate::catalog::check_schema_writable;
use crate::optimizer::property::RequiredDist;
use crate::optimizer::PlanRef;
use crate::planner::Planner;
use crate::session::{OptimizerContext, OptimizerContextRef, SessionImpl};

pub fn gen_create_sink_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    query: Box<Query>,
    name: ObjectName,
    properties: HashMap<String, String>,
) -> Result<(PlanRef, ProstTable)> {
    let (schema_name, table_name) = Binder::resolve_table_name(name)?;
    check_schema_writable(&schema_name)?;
    let (database_id, schema_id) = session
        .env()
        .catalog_reader()
        .read_guard()
        .check_relation_name_duplicated(session.database(), &schema_name, &table_name)?;

    let bound = {
        let mut binder = Binder::new(
            session.env().catalog_reader().read_guard(),
            session.database().to_string(),
        );
        binder.bind_query(*query)?
    };

    if let BoundSetExpr::Select(select) = &bound.body {
        // `InputRef`'s alias will be implicitly assigned in `bind_project`.
        // For other expressions, we require the user to explicitly assign an alias.
        if select.aliases.iter().any(Option::is_none) {
            return Err(ErrorCode::BindError(
                "An alias must be specified for an expression".to_string(),
            )
            .into());
        }
    }

    let mut plan_root = Planner::new(context).plan_query(bound)?;
    plan_root.set_required_dist(RequiredDist::Any);
    let materialize = plan_root.gen_create_sink_plan(table_name)?;
    let mut table = materialize.table().to_prost(schema_id, database_id);
    let plan: PlanRef = materialize.into();
    table.owner = session.user_name().to_string();
    table.properties = properties;

    Ok((plan, table))
}

pub async fn handle_create_sink(
    context: OptimizerContext,
    stmt: CreateSinkStatement,
) -> Result<PgResponse> {
    let session = context.session_ctx.clone();

    log::info!("frontend: {:?}", stmt);

    Ok(PgResponse::empty_result(StatementType::CREATE_SINK))
}
