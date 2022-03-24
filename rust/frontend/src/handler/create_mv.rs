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

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use itertools::Itertools;
use pgwire::pg_response::PgResponse;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::Result;
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_sqlparser::ast::{ObjectName, Query};

use crate::binder::{Binder, BoundQuery};
use crate::catalog::column_catalog::ColumnCatalog;
use crate::catalog::{ColumnId, TableId};
use crate::optimizer::plan_node::StreamMaterialize;
use crate::optimizer::property::{FieldOrder, Order};
use crate::optimizer::PlanRef;
use crate::planner::Planner;
use crate::session::{QueryContext, SessionImpl};

impl BoundQuery {
    /// Generate create MV's column desc from query.
    pub fn gen_create_mv_column_desc(&self) -> Vec<ColumnDesc> {
        let mut column_descs = vec![];

        for (i, (data_type, name)) in self
            .data_types()
            .iter()
            .zip_eq(self.names().iter())
            .enumerate()
        {
            column_descs.push(ColumnDesc {
                data_type: data_type.clone(),
                column_id: ColumnId::new(i as i32),
                name: name.to_string(),
            });
        }

        column_descs
    }
}

/// Mview information when handling create
pub struct MvInfo {
    pub table_name: String,
    pub database_id: u32,
    pub schema_id: u32,
}

impl MvInfo {
    /// Generate MvInfo when the table name. Note that this cannot be used to actually create an MV.
    pub fn with_name(name: impl Into<String>) -> Self {
        Self {
            table_name: name.into(),
            database_id: u32::MAX,
            schema_id: u32::MAX,
        }
    }
}

/// Generate create MV plan, return plan and mv table info.
pub fn gen_create_mv_plan(
    session: &SessionImpl,
    planner: &mut Planner,
    query: Query,
    info: MvInfo,
) -> Result<(PlanRef, ProstTable)> {
    // For create MV plan, we currently assume column id == column index.
    // If there are anything that would be changed in the future, please carefully revisit this
    // function.

    let bound_query = Binder::new(
        session.env().catalog_reader().read_guard(),
        session.database().to_string(),
    )
    .bind_query(query)?;

    let mut column_orders = bound_query.order.clone();

    let mut column_catalog = bound_query
        .gen_create_mv_column_desc()
        .into_iter()
        .map(|column_desc| ColumnCatalog {
            column_desc,
            is_hidden: false,
        })
        .collect_vec();

    let mut logical = planner.plan_query(bound_query)?;

    let plan = logical.gen_create_mv_plan();

    // Compute pk and column orders that needs to be materialized.

    let pks = plan.pk_indices();
    let ordered_ids: HashSet<usize> = column_orders.iter().map(|x| x.index).collect();

    // For pk columns, start their column id from the last one from catalog.
    let mut column_id = column_catalog.len();

    for pk in pks {
        if !ordered_ids.contains(pk) {
            column_orders.push(FieldOrder::ascending(*pk));
            column_catalog.push(ColumnCatalog {
                column_desc: ColumnDesc {
                    data_type: plan.schema()[*pk].data_type(),
                    column_id: ColumnId::new(column_id as i32),
                    name: format!("_pk_{}", pk),
                },
                is_hidden: true,
            });
            column_id += 1;
        }
    }

    // Add a materialize node upon the original stream plan
    let plan = StreamMaterialize::new(
        plan.ctx(),
        plan,
        column_orders.clone(),
        column_catalog.iter().map(|x| x.column_id()).collect(),
    );

    let plan: PlanRef = plan.into();

    let order = Order::new(column_orders);
    let (pk_column_ids, pk_orders) = order.to_protobuf_id_and_order();

    let table = ProstTable {
        id: TableId::placeholder().table_id(),
        schema_id: info.schema_id,
        database_id: info.database_id,
        name: info.table_name,
        columns: column_catalog
            .iter()
            .map(ColumnCatalog::to_protobuf)
            .collect(),
        // The pk of the corresponding table of MV is order column + upstream pk
        pk_column_ids,
        pk_orders: pk_orders.into_iter().map(|x| x.into()).collect(),
        dependent_relations: vec![],
        optional_associated_source_id: None,
    };

    Ok((plan, table))
}

pub async fn handle_create_mv(
    context: QueryContext,
    name: ObjectName,
    query: Box<Query>,
) -> Result<PgResponse> {
    let session = context.session_ctx.clone();

    let (table, plan) = {
        let mut planner = Planner::new(Rc::new(RefCell::new(context)));

        let (schema_name, table_name) = Binder::resolve_table_name(name.clone())?;
        let (database_id, schema_id) = session
            .env()
            .catalog_reader()
            .read_guard()
            .check_relation_name_duplicated(session.database(), &schema_name, &table_name)?;

        let (plan, table) = gen_create_mv_plan(
            &session,
            &mut planner,
            *query,
            MvInfo {
                schema_id,
                database_id,
                table_name: name.to_string(),
            },
        )?;
        let plan = plan.to_stream_prost();

        (table, plan)
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.create_materialized_view(table, plan).await?;

    Ok(PgResponse::new(
        pgwire::pg_response::StatementType::CREATE_MATERIALIZED_VIEW,
        0,
        vec![],
        vec![],
    ))
}
