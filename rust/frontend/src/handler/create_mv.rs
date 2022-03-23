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

use itertools::Itertools;
use pgwire::pg_response::PgResponse;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{ObjectName, Query};

use super::create_table::ROWID_NAME;
use crate::binder::{Binder, BoundQuery};
use crate::catalog::ColumnId;
use crate::optimizer::PlanRef;
use crate::planner::Planner;
use crate::session::{QueryContext, SessionImpl};

impl BoundQuery {
    /// Generate create MV's column desc from query.
    pub fn gen_create_mv_column_desc(&self) -> Vec<ColumnDesc> {
        let mut column_descs = vec![ColumnDesc {
            data_type: DataType::Int64,
            column_id: ColumnId::new(0),
            name: ROWID_NAME.to_string(),
        }];

        for (i, (data_type, name)) in self
            .data_types()
            .iter()
            .zip_eq(self.names().iter())
            .enumerate()
        {
            column_descs.push(ColumnDesc {
                data_type: data_type.clone(),
                column_id: ColumnId::new((i + 1) as i32),
                name: name.to_string(),
            });
        }
        column_descs
    }
}

/// Generate create MV plan
pub fn gen_create_mv_plan(
    session: &SessionImpl,
    planner: &mut Planner,
    query: Query,
) -> Result<PlanRef> {
    let bound_query = Binder::new(
        session.env().catalog_reader().read_guard(),
        session.database().to_string(),
    )
    .bind_query(query)?;

    let order = bound_query.order.clone();

    let column_descs = bound_query.gen_create_mv_column_desc();

    let logical = planner.plan_query(bound_query)?;

    let plan =
        logical.gen_create_mv_plan(order, column_descs.iter().map(|x| x.column_id).collect());

    Ok(plan)
}

pub async fn handle_create_mv(
    context: QueryContext,
    name: ObjectName,
    query: Box<Query>,
) -> Result<PgResponse> {
    let session = context.session_ctx.clone();
    let mut planner = Planner::new(Rc::new(RefCell::new(context)));

    let (schema_name, table_name) = Binder::resolve_table_name(name.clone())?;
    let (_db_id, _schema_id) = session
        .env()
        .catalog_reader()
        .read_guard()
        .check_relation_name(session.database(), &schema_name, &table_name)?;

    let plan = gen_create_mv_plan(&session, &mut planner, *query)?.to_stream_prost();

    let json_plan = serde_json::to_string(&plan).unwrap();
    tracing::info!(name= ?name, plan = ?json_plan);

    // TODO catalog writer to create mv
    let _catalog_writer = session.env().catalog_writer();

    Ok(PgResponse::new(
        pgwire::pg_response::StatementType::CREATE_MATERIALIZED_VIEW,
        0,
        vec![],
        vec![],
    ))
}
