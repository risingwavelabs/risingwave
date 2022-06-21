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
use std::rc::Rc;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_sqlparser::ast::{ObjectName, OrderByExpr};

use crate::binder::Binder;
use crate::optimizer::plan_node::{LogicalScan, StreamTableScan};
use crate::optimizer::property::{FieldOrder, Order, RequiredDist};
use crate::optimizer::{PlanRef, PlanRoot};
use crate::session::{OptimizerContext, OptimizerContextRef, SessionImpl};
use crate::stream_fragmenter::StreamFragmenter;

pub(crate) fn gen_create_index_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    index_name: ObjectName,
    table_name: ObjectName,
    columns: Vec<OrderByExpr>,
) -> Result<(PlanRef, ProstTable)> {
    let columns = columns
        .iter()
        .map(|column| {
            if column.asc.is_some() {
                return Err(
                    ErrorCode::NotImplemented("asc not supported".into(), None.into()).into(),
                );
            }

            if column.nulls_first.is_some() {
                return Err(ErrorCode::NotImplemented(
                    "nulls_first not supported".into(),
                    None.into(),
                )
                .into());
            }

            use risingwave_sqlparser::ast::Expr;

            if let Expr::Identifier(ref ident) = column.expr {
                Ok::<_, RwError>(ident)
            } else {
                Err(ErrorCode::NotImplemented(
                    "only identifier is supported for create index".into(),
                    None.into(),
                )
                .into())
            }
        })
        .try_collect::<_, Vec<_>, _>()?;

    let (schema_name, table_name) = Binder::resolve_table_name(table_name)?;
    let catalog_reader = session.env().catalog_reader();
    let table = catalog_reader
        .read_guard()
        .get_table_by_name(session.database(), &schema_name, &table_name)?
        .clone();

    let table_desc = Rc::new(table.table_desc());
    let table_desc_map = table_desc
        .columns
        .iter()
        .enumerate()
        .map(|(x, y)| (y.name.clone(), x))
        .collect::<HashMap<_, _>>();
    let arrange_keys = columns
        .iter()
        .map(|x| {
            let x = x.to_string();
            table_desc_map
                .get(&x)
                .cloned()
                .ok_or_else(|| ErrorCode::ItemNotFound(x).into())
        })
        .try_collect::<_, Vec<_>, RwError>()?;

    // Manually assemble the materialization plan for the index MV.
    let materialize = {
        let mut required_cols = FixedBitSet::with_capacity(table_desc.columns.len());
        required_cols.toggle_range(..);
        required_cols.toggle(0);
        let mut out_names: Vec<String> =
            table_desc.columns.iter().map(|c| c.name.clone()).collect();
        out_names.remove(0);

        let scan_node = StreamTableScan::new(LogicalScan::create(
            table_name,
            false,
            table_desc,
            // indexes are only used by DeltaJoin rule, and we don't need to provide them here.
            vec![],
            context,
        ));

        PlanRoot::new(
            scan_node.into(),
            RequiredDist::AnyShard,
            Order::new(
                arrange_keys
                    .iter()
                    .map(|id| FieldOrder::ascending(*id))
                    .collect(),
            ),
            required_cols,
            out_names,
        )
        .gen_create_index_plan(index_name.to_string(), table.id())?
    };

    let (index_schema_name, index_table_name) = Binder::resolve_table_name(index_name)?;
    let (index_database_id, index_schema_id) = session
        .env()
        .catalog_reader()
        .read_guard()
        .check_relation_name_duplicated(
            session.database(),
            &index_schema_name,
            &index_table_name,
        )?;

    let index_table = materialize
        .table()
        .to_prost(index_schema_id, index_database_id);

    Ok((materialize.into(), index_table))
}

pub async fn handle_create_index(
    context: OptimizerContext,
    name: ObjectName,
    table_name: ObjectName,
    columns: Vec<OrderByExpr>,
) -> Result<PgResponse> {
    let session = context.session_ctx.clone();

    let (graph, table) = {
        let (plan, table) = gen_create_index_plan(
            &session,
            context.into(),
            name.clone(),
            table_name.clone(),
            columns,
        )?;
        let plan = plan.to_stream_prost();
        let graph = StreamFragmenter::build_graph(plan);

        (graph, table)
    };

    log::trace!(
        "name={}, graph=\n{}",
        table_name,
        serde_json::to_string_pretty(&graph).unwrap()
    );

    let catalog_writer = session.env().catalog_writer();
    catalog_writer
        .create_materialized_view(table, graph)
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_TABLE))
}
