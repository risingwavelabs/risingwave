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

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{IndexId, TableDesc, TableId};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_pb::catalog::{PbIndex, PbTable};
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_pb::user::grant_privilege::{Action, Object};
use risingwave_sqlparser::ast::{Ident, ObjectName, OrderByExpr};

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::expr::{Expr, ExprImpl, ExprType, InputRef};
use crate::handler::privilege::ObjectCheckItem;
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::{Explain, LogicalProject, LogicalScan, StreamMaterialize};
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::optimizer::{OptimizerContext, OptimizerContextRef, PlanRef, PlanRoot};
use crate::scheduler::streaming_manager::CreatingStreamingJobInfo;
use crate::session::SessionImpl;
use crate::stream_fragmenter::build_graph;

pub(crate) fn gen_create_index_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    index_name: ObjectName,
    table_name: ObjectName,
    columns: Vec<OrderByExpr>,
    include: Vec<Ident>,
    distributed_by: Vec<Ident>,
) -> Result<(PlanRef, PbTable, PbIndex)> {
    let db_name = session.database();
    let (schema_name, table_name) = Binder::resolve_schema_qualified_name(db_name, table_name)?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let index_table_name = Binder::resolve_index_name(index_name)?;

    let catalog_reader = session.env().catalog_reader();
    let (table, schema_name) = {
        let read_guard = catalog_reader.read_guard();
        let (table, schema_name) =
            read_guard.get_table_by_name(db_name, schema_path, &table_name)?;
        (table.clone(), schema_name.to_string())
    };

    if table.is_index() {
        return Err(
            ErrorCode::InvalidInputSyntax(format!("\"{}\" is an index", table.name)).into(),
        );
    }

    session.check_privileges(&[ObjectCheckItem::new(
        table.owner,
        Action::Select,
        Object::TableId(table.id.table_id),
    )])?;

    let mut binder = Binder::new_for_stream(session);
    binder.bind_table(Some(&schema_name), &table_name, None)?;

    let mut index_columns_ordered_expr = vec![];
    let mut include_columns_expr = vec![];
    let mut distributed_columns_expr = vec![];
    for column in columns {
        let order_type = OrderType::from_bools(column.asc, column.nulls_first);
        let expr_impl = binder.bind_expr(column.expr)?;
        match &expr_impl {
            ExprImpl::InputRef(_) => {}
            ExprImpl::FunctionCall(func) => {
                match func.get_expr_type() {
                    // TODO: support more functions after verification
                    ExprType::Lower
                    | ExprType::Upper
                    | ExprType::JsonbAccessInner
                    | ExprType::JsonbAccessStr => {}
                    _ => {
                        return Err(ErrorCode::NotSupported(
                            "this function is not supported for indexes".into(),
                            "use other functions instead".into(),
                        )
                        .into())
                    }
                };
                if !func.inputs().iter().all(|input| {
                    matches!(input, ExprImpl::InputRef(_)) || matches!(input, ExprImpl::Literal(_))
                }) {
                    return Err(ErrorCode::NotSupported(
                        "complex arguments for functions are not supported".into(),
                        "use columns or literals instead".into(),
                    )
                    .into());
                }
            }
            _ => {
                return Err(ErrorCode::NotSupported(
                    "index columns should be columns or functions".into(),
                    "use columns or functions instead".into(),
                )
                .into())
            }
        };
        index_columns_ordered_expr.push((expr_impl, order_type));
    }

    if include.is_empty() {
        // Create index to include all (non-hidden) columns by default.
        include_columns_expr = table
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, column)| !column.is_hidden)
            .map(|(x, column)| {
                ExprImpl::InputRef(InputRef::new(x, column.column_desc.data_type.clone()).into())
            })
            .collect_vec();
    } else {
        for column in include {
            let expr_impl =
                binder.bind_expr(risingwave_sqlparser::ast::Expr::Identifier(column))?;
            include_columns_expr.push(expr_impl);
        }
    };

    for column in distributed_by {
        let expr_impl = binder.bind_expr(risingwave_sqlparser::ast::Expr::Identifier(column))?;
        distributed_columns_expr.push(expr_impl);
    }

    let table_desc = Rc::new(table.table_desc());

    // Remove duplicate column of index columns
    let mut set = HashSet::new();
    index_columns_ordered_expr = index_columns_ordered_expr
        .into_iter()
        .filter(|(expr, _)| match expr {
            ExprImpl::InputRef(input_ref) => set.insert(input_ref.index),
            ExprImpl::FunctionCall(_) => true,
            _ => unreachable!(),
        })
        .collect_vec();

    // Remove include columns are already in index columns
    include_columns_expr = include_columns_expr
        .into_iter()
        .filter(|expr| match expr {
            ExprImpl::InputRef(input_ref) => set.insert(input_ref.index),
            _ => unreachable!(),
        })
        .collect_vec();

    // Remove duplicate columns of distributed by columns
    let mut set = HashSet::new();
    let distributed_columns_expr = distributed_columns_expr
        .into_iter()
        .filter(|expr| match expr {
            ExprImpl::InputRef(input_ref) => set.insert(input_ref.index),
            _ => unreachable!(),
        })
        .collect_vec();
    // Distributed by columns should be a prefix of index columns
    if !index_columns_ordered_expr
        .iter()
        .map(|(expr, _)| expr.clone())
        .collect_vec()
        .starts_with(&distributed_columns_expr)
    {
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
        &index_columns_ordered_expr,
        &include_columns_expr,
        // We use the whole index columns as distributed key by default if users
        // haven't specify the distributed by columns.
        if distributed_columns_expr.is_empty() {
            index_columns_ordered_expr.len()
        } else {
            distributed_columns_expr.len()
        },
    )?;

    let (index_database_id, index_schema_id) =
        session.get_database_and_schema_id_for_create(Some(schema_name))?;

    let index_table = materialize.table();
    let mut index_table_prost = index_table.to_prost(index_schema_id, index_database_id);
    {
        use risingwave_common::constants::hummock::PROPERTIES_RETENTION_SECOND_KEY;
        let retention_second_string_key = PROPERTIES_RETENTION_SECOND_KEY.to_string();

        // Inherit table properties
        table.properties.get(&retention_second_string_key).map(|v| {
            index_table_prost
                .properties
                .insert(retention_second_string_key, v.clone())
        });
    }

    index_table_prost.owner = session.user_id();
    index_table_prost.dependent_relations = vec![table.id.table_id];

    // FIXME: why sqlalchemy need these information?
    let original_columns = index_table
        .columns
        .iter()
        .map(|x| x.column_desc.column_id.get_id())
        .collect();

    let index_item = build_index_item(
        index_table.table_desc().into(),
        table.name(),
        table_desc,
        index_columns_ordered_expr,
    );

    let index_prost = PbIndex {
        id: IndexId::placeholder().index_id,
        schema_id: index_schema_id,
        database_id: index_database_id,
        name: index_table_name,
        owner: index_table_prost.owner,
        index_table_id: TableId::placeholder().table_id,
        primary_table_id: table.id.table_id,
        index_item,
        original_columns,
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
    index_columns: Vec<(ExprImpl, OrderType)>,
) -> Vec<risingwave_pb::expr::ExprNode> {
    let primary_table_desc_map = primary_table_desc
        .columns
        .iter()
        .enumerate()
        .map(|(x, y)| (y.name.clone(), x))
        .collect::<HashMap<_, _>>();

    let primary_table_name_prefix = format!("{}.", primary_table_name);

    let index_columns_len = index_columns.len();
    index_columns
        .into_iter()
        .map(|(expr, _)| expr.to_expr_proto())
        .chain(
            index_table_desc
                .columns
                .iter()
                .skip(index_columns_len)
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
                    .to_expr_proto()
                }),
        )
        .collect_vec()
}

/// Note: distributed by columns must be a prefix of index columns, so we just use
/// `distributed_by_columns_len` to represent distributed by columns
fn assemble_materialize(
    table_name: String,
    table_desc: Rc<TableDesc>,
    context: OptimizerContextRef,
    index_name: String,
    index_columns: &[(ExprImpl, OrderType)],
    include_columns: &[ExprImpl],
    distributed_by_columns_len: usize,
) -> Result<StreamMaterialize> {
    // Build logical plan and then call gen_create_index_plan
    // LogicalProject(index_columns, include_columns)
    //   LogicalScan(table_desc)

    let definition = context.normalized_sql().to_owned();

    let logical_scan = LogicalScan::create(
        table_name,
        false,
        table_desc.clone(),
        // Index table has no indexes.
        vec![],
        context,
        false,
    );

    let exprs = index_columns
        .iter()
        .map(|(expr, _)| expr.clone())
        .chain(include_columns.iter().cloned())
        .collect_vec();

    let logical_project = LogicalProject::create(logical_scan.into(), exprs);
    let mut project_required_cols = FixedBitSet::with_capacity(logical_project.schema().len());
    project_required_cols.toggle_range(0..logical_project.schema().len());

    let mut col_names = HashSet::new();
    let mut count = 0;

    let out_names: Vec<String> = index_columns
        .iter()
        .map(|(expr, _)| match expr {
            ExprImpl::InputRef(input_ref) => table_desc
                .columns
                .get(input_ref.index)
                .unwrap()
                .name
                .clone(),
            ExprImpl::FunctionCall(func) => {
                let func_name = func.get_expr_type().as_str_name().to_string();
                let mut name = func_name.clone();
                while !col_names.insert(name.clone()) {
                    count += 1;
                    name = format!("{}{}", func_name, count);
                }
                name
            }
            _ => unreachable!(),
        })
        .chain(include_columns.iter().map(|expr| {
            match expr {
                ExprImpl::InputRef(input_ref) => table_desc
                    .columns
                    .get(input_ref.index)
                    .unwrap()
                    .name
                    .clone(),
                _ => unreachable!(),
            }
        }))
        .collect_vec();

    PlanRoot::new(
        logical_project,
        RequiredDist::PhysicalDist(Distribution::HashShard(
            (0..distributed_by_columns_len).collect(),
        )),
        Order::new(
            index_columns
                .iter()
                .enumerate()
                .map(|(i, (_, order))| ColumnOrder::new(i, *order))
                .collect(),
        ),
        project_required_cols,
        out_names,
    )
    .gen_index_plan(index_name, definition)
}

pub async fn handle_create_index(
    handler_args: HandlerArgs,
    if_not_exists: bool,
    index_name: ObjectName,
    table_name: ObjectName,
    columns: Vec<OrderByExpr>,
    include: Vec<Ident>,
    distributed_by: Vec<Ident>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();

    let (graph, index_table, index) = {
        {
            if let Err(e) = session.check_relation_name_duplicated(index_name.clone()) {
                if if_not_exists {
                    return Ok(PgResponse::empty_result_with_notice(
                        StatementType::CREATE_INDEX,
                        format!("relation \"{}\" already exists, skipping", index_name),
                    ));
                } else {
                    return Err(e);
                }
            }
        }

        let context = OptimizerContext::from_handler_args(handler_args);
        let (plan, index_table, index) = gen_create_index_plan(
            &session,
            context.into(),
            index_name.clone(),
            table_name,
            columns,
            include,
            distributed_by,
        )?;
        let mut graph = build_graph(plan);
        graph.parallelism = session
            .config()
            .get_streaming_parallelism()
            .map(|parallelism| Parallelism { parallelism });
        (graph, index_table, index)
    };

    tracing::trace!(
        "name={}, graph=\n{}",
        index_name,
        serde_json::to_string_pretty(&graph).unwrap()
    );

    let _job_guard =
        session
            .env()
            .creating_streaming_job_tracker()
            .guard(CreatingStreamingJobInfo::new(
                session.session_id(),
                index.database_id,
                index.schema_id,
                index.name.clone(),
            ));

    let catalog_writer = session.env().catalog_writer();
    catalog_writer
        .create_index(index, index_table, graph)
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_INDEX))
}
