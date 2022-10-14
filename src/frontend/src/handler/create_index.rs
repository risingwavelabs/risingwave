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

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{IndexId, TableDesc, TableId, DEFAULT_SCHEMA_NAME};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::catalog::{Index as ProstIndex, Table as ProstTable};
use risingwave_pb::user::grant_privilege::{Action, Object};
use risingwave_sqlparser::ast::{Ident, ObjectName, OrderByExpr};

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::check_schema_writable;
use crate::catalog::root_catalog::SchemaPath;
use crate::expr::{Expr, ExprImpl, InputRef};
use crate::handler::privilege::{check_privileges, ObjectCheckItem};
use crate::optimizer::plan_node::{LogicalProject, LogicalScan, StreamMaterialize};
use crate::optimizer::property::{Distribution, FieldOrder, Order, RequiredDist};
use crate::optimizer::{PlanRef, PlanRoot};
use crate::session::{OptimizerContext, OptimizerContextRef, SessionImpl};
use crate::stream_fragmenter::build_graph;

pub(crate) fn gen_create_index_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    index_name: ObjectName,
    table_name: ObjectName,
    columns: Vec<OrderByExpr>,
    include: Vec<Ident>,
    distributed_by: Vec<Ident>,
) -> Result<(PlanRef, ProstTable, ProstIndex)> {
    let columns = check_columns(columns)?;
    let db_name = session.database();
    let (schema_name, table_name) = Binder::resolve_table_or_source_name(db_name, table_name)?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;
    let schema_path = match schema_name.as_deref() {
        Some(schema_name) => SchemaPath::Name(schema_name),
        None => SchemaPath::Path(&search_path, user_name),
    };
    let index_table_name = Binder::resolve_index_name(index_name)?;

    let catalog_reader = session.env().catalog_reader();
    let (table, schema_name) = {
        let read_guard = catalog_reader.read_guard();
        let (table, schema_name) =
            read_guard.get_table_by_name(db_name, schema_path, &table_name)?;
        (table.clone(), schema_name.to_string())
    };

    if table.is_index {
        return Err(
            ErrorCode::InvalidInputSyntax(format!("\"{}\" is an index", table.name)).into(),
        );
    }

    check_privileges(
        session,
        &vec![ObjectCheckItem::new(
            table.owner,
            Action::Select,
            Object::TableId(table.id.table_id),
        )],
    )?;

    let table_desc = Rc::new(table.table_desc());
    let table_desc_map = table_desc
        .columns
        .iter()
        .enumerate()
        .map(|(x, y)| (y.name.clone(), x))
        .collect::<HashMap<_, _>>();

    let to_column_indices = |ident: &Ident| {
        let x = ident.to_string();
        table_desc_map
            .get(&x)
            .cloned()
            .ok_or_else(|| ErrorCode::ItemNotFound(x).into())
    };

    let mut index_columns = columns
        .iter()
        .map(to_column_indices)
        .try_collect::<_, Vec<_>, RwError>()?;

    let mut include_columns = include
        .iter()
        .map(to_column_indices)
        .try_collect::<_, Vec<_>, RwError>()?;

    let distributed_by_columns = distributed_by
        .iter()
        .map(to_column_indices)
        .try_collect::<_, Vec<_>, RwError>()?;

    // Remove duplicate column of index columns
    let mut set = HashSet::new();
    index_columns = index_columns
        .into_iter()
        .filter(|x| set.insert(*x))
        .collect_vec();

    // Remove include columns are already in index columns
    include_columns = include_columns
        .into_iter()
        .filter(|x| set.insert(*x))
        .collect_vec();

    // Remove duplicate columns of distributed by columns
    let distributed_by_columns = distributed_by_columns.into_iter().unique().collect_vec();
    // Distributed by columns should be a prefix of index columns
    if !index_columns.starts_with(&distributed_by_columns) {
        return Err(ErrorCode::InvalidInputSyntax(
            "Distributed by columns should be a prefix of index columns".to_string(),
        )
        .into());
    }

    // Manually assemble the materialization plan for the index MV.
    let materialize = assemble_materialize(
        table_name,
        table_desc.clone(),
        context,
        index_table_name.clone(),
        &index_columns,
        &include_columns,
        // We use the whole index columns as distributed key by default if users
        // haven't specify the distributed by columns.
        if distributed_by_columns.is_empty() {
            index_columns.len()
        } else {
            distributed_by_columns.len()
        },
    )?;

    let (index_database_id, index_schema_id) = {
        let catalog_reader = session.env().catalog_reader().read_guard();

        let schema = catalog_reader.get_schema_by_name(db_name, &schema_name)?;

        check_schema_writable(&schema_name)?;
        if schema_name != DEFAULT_SCHEMA_NAME {
            check_privileges(
                session,
                &vec![ObjectCheckItem::new(
                    schema.owner(),
                    Action::Create,
                    Object::SchemaId(schema.id()),
                )],
            )?;
        }

        let db_id = catalog_reader.get_database_by_name(db_name)?.id();

        (db_id, schema.id())
    };

    let index_table = materialize.table();
    let mut index_table_prost = index_table.to_prost(index_schema_id, index_database_id);
    index_table_prost.owner = session.user_id();

    let index_prost = ProstIndex {
        id: IndexId::placeholder().index_id,
        schema_id: index_schema_id,
        database_id: index_database_id,
        name: index_table_name,
        owner: index_table_prost.owner,
        index_table_id: TableId::placeholder().table_id,
        primary_table_id: table.id.table_id,
        index_item: build_index_item(index_table.table_desc().into(), table.name(), table_desc)
            .iter()
            .map(InputRef::to_expr_proto)
            .collect_vec(),
    };

    let plan: PlanRef = materialize.into();
    let ctx = plan.ctx();
    let explain_trace = ctx.is_explain_trace();
    if explain_trace {
        ctx.trace("Create Index:");
        ctx.trace(plan.explain_to_string().unwrap());
    }

    Ok((plan, index_table_prost, index_prost))
}

fn build_index_item(
    index_table_desc: Rc<TableDesc>,
    primary_table_name: &str,
    primary_table_desc: Rc<TableDesc>,
) -> Vec<InputRef> {
    let primary_table_desc_map = primary_table_desc
        .columns
        .iter()
        .enumerate()
        .map(|(x, y)| (y.name.clone(), x))
        .collect::<HashMap<_, _>>();

    let primary_table_name_prefix = format!("{}.", primary_table_name);

    index_table_desc
        .columns
        .iter()
        .map(|x| {
            let name = if x.name.starts_with(&primary_table_name_prefix) {
                x.name[primary_table_name_prefix.len()..].to_string()
            } else {
                x.name.clone()
            };

            let column_index = *primary_table_desc_map.get(&name).unwrap();
            InputRef {
                index: column_index,
                data_type: primary_table_desc
                    .columns
                    .get(column_index)
                    .unwrap()
                    .data_type
                    .clone(),
            }
        })
        .collect_vec()
}

/// Note: distributed by columns must be a prefix of index columns, so we just use
/// `distributed_by_columns_len` to represent distributed by columns
fn assemble_materialize(
    table_name: String,
    table_desc: Rc<TableDesc>,
    context: OptimizerContextRef,
    index_name: String,
    index_columns: &[usize],
    include_columns: &[usize],
    distributed_by_columns_len: usize,
) -> Result<StreamMaterialize> {
    // Build logical plan and then call gen_create_index_plan
    // LogicalProject(index_columns, include_columns)
    //   LogicalScan(table_desc)

    let logical_scan = LogicalScan::create(
        table_name,
        false,
        table_desc.clone(),
        // Index table has no indexes.
        vec![],
        context,
    );

    let exprs = index_columns
        .iter()
        .chain(include_columns.iter())
        .map(|&i| {
            ExprImpl::InputRef(
                InputRef::new(i, table_desc.columns.get(i).unwrap().data_type.clone()).into(),
            )
        })
        .collect_vec();

    let logical_project = LogicalProject::create(logical_scan.into(), exprs);
    let mut project_required_cols = FixedBitSet::with_capacity(logical_project.schema().len());
    project_required_cols.toggle_range(0..logical_project.schema().len());

    let out_names: Vec<String> = index_columns
        .iter()
        .chain(include_columns.iter())
        .map(|&i| table_desc.columns.get(i).unwrap().name.clone())
        .collect_vec();

    PlanRoot::new(
        logical_project,
        RequiredDist::PhysicalDist(Distribution::HashShard(
            (0..distributed_by_columns_len).collect(),
        )),
        Order::new(
            (0..index_columns.len())
                .into_iter()
                .map(FieldOrder::ascending)
                .collect(),
        ),
        project_required_cols,
        out_names,
    )
    .gen_create_index_plan(index_name)
}

fn check_columns(columns: Vec<OrderByExpr>) -> Result<Vec<Ident>> {
    columns
        .into_iter()
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

            if let Expr::Identifier(ident) = column.expr {
                Ok::<_, RwError>(ident)
            } else {
                Err(ErrorCode::NotImplemented(
                    "only identifier is supported for create index".into(),
                    None.into(),
                )
                .into())
            }
        })
        .try_collect::<_, Vec<_>, _>()
}

pub async fn handle_create_index(
    context: OptimizerContext,
    if_not_exists: bool,
    name: ObjectName,
    table_name: ObjectName,
    columns: Vec<OrderByExpr>,
    include: Vec<Ident>,
    distributed_by: Vec<Ident>,
) -> Result<RwPgResponse> {
    let session = context.session_ctx.clone();

    let (graph, index_table, index) = {
        {
            // Here is some duplicate code because we need to check name duplicated outside of
            // `gen_xxx_plan` to avoid `explain` reporting the error.
            let db_name = session.database();
            let (schema_name, table_name) =
                Binder::resolve_table_or_source_name(db_name, table_name.clone())?;
            let search_path = session.config().get_search_path();
            let user_name = &session.auth_context().user_name;
            let schema_path = match schema_name.as_deref() {
                Some(schema_name) => SchemaPath::Name(schema_name),
                None => SchemaPath::Path(&search_path, user_name),
            };
            let index_name = Binder::resolve_index_name(name.clone())?;

            let catalog_reader = session.env().catalog_reader().read_guard();
            let (_, schema_name) =
                catalog_reader.get_table_by_name(db_name, schema_path, &table_name)?;

            if let Err(e) =
                catalog_reader.check_relation_name_duplicated(db_name, schema_name, &index_name)
            {
                if if_not_exists {
                    return Ok(PgResponse::empty_result_with_notice(
                        StatementType::CREATE_INDEX,
                        format!(
                            "NOTICE:  relation \"{}\" already exists, skipping",
                            index_name
                        ),
                    ));
                } else {
                    return Err(e);
                }
            }
        }

        let (plan, index_table, index) = gen_create_index_plan(
            &session,
            context.into(),
            name.clone(),
            table_name,
            columns,
            include,
            distributed_by,
        )?;
        let graph = build_graph(plan);

        (graph, index_table, index)
    };

    tracing::trace!(
        "name={}, graph=\n{}",
        name,
        serde_json::to_string_pretty(&graph).unwrap()
    );

    let catalog_writer = session.env().catalog_writer();
    catalog_writer
        .create_index(index, index_table, graph)
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_INDEX))
}
