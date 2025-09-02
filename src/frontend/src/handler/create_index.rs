// Copyright 2025 RisingWave Labs
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
use std::num::NonZeroU32;
use std::sync::Arc;

use either::Either;
use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{IndexId, TableId};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_pb::catalog::{
    CreateType, PbFlatIndexConfig, PbIndex, PbIndexColumnProperties, PbStreamJobStatus,
    PbVectorIndexInfo,
};
use risingwave_pb::common::PbDistanceType;
use risingwave_sqlparser::ast;
use risingwave_sqlparser::ast::{Ident, ObjectName, OrderByExpr};

use super::RwPgResponse;
use crate::TableCatalog;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::{DatabaseId, SchemaId};
use crate::error::{ErrorCode, Result};
use crate::expr::{Expr, ExprImpl, ExprRewriter, InputRef};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_expr_rewriter::ConstEvalRewriter;
use crate::optimizer::plan_node::utils::plan_can_use_background_ddl;
use crate::optimizer::plan_node::{
    Explain, LogicalProject, LogicalScan, StreamMaterialize, StreamPlanRef as PlanRef,
};
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::optimizer::{LogicalPlanRoot, OptimizerContext, OptimizerContextRef, PlanRoot};
use crate::scheduler::streaming_manager::CreatingStreamingJobInfo;
use crate::session::SessionImpl;
use crate::stream_fragmenter::{GraphJobType, build_graph};

pub(crate) fn resolve_index_schema(
    session: &SessionImpl,
    index_name: ObjectName,
    table_name: ObjectName,
) -> Result<(String, Arc<TableCatalog>, String)> {
    let db_name = &session.database();
    let (schema_name, table_name) = Binder::resolve_schema_qualified_name(db_name, &table_name)?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let index_table_name = Binder::resolve_index_name(index_name)?;

    let catalog_reader = session.env().catalog_reader();
    let read_guard = catalog_reader.read_guard();
    let (table, schema_name) =
        read_guard.get_created_table_by_name(db_name, schema_path, &table_name)?;
    Ok((schema_name.to_owned(), table.clone(), index_table_name))
}

pub(crate) fn gen_create_index_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    schema_name: String,
    table: Arc<TableCatalog>,
    index_table_name: String,
    method: Option<Ident>,
    columns: Vec<OrderByExpr>,
    include: Vec<Ident>,
    distributed_by: Vec<ast::Expr>,
) -> Result<(PlanRef, TableCatalog, PbIndex)> {
    if table.is_index() {
        return Err(
            ErrorCode::InvalidInputSyntax(format!("\"{}\" is an index", table.name)).into(),
        );
    }

    if !session.is_super_user() && session.user_id() != table.owner {
        return Err(ErrorCode::PermissionDenied(format!(
            "must be owner of table \"{}\"",
            table.name
        ))
        .into());
    }

    let vector_index_config = if let Some(method) = method {
        match method.real_value().to_ascii_lowercase().as_str() {
            "default" => None,
            "flat" => Some(risingwave_pb::catalog::vector_index_info::PbConfig::Flat(
                PbFlatIndexConfig {},
            )),
            _ => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "invalid index method {}",
                    method
                ))
                .into());
            }
        }
    } else {
        None
    };

    let is_vector_index = vector_index_config.is_some();

    if is_vector_index && !table.append_only {
        return Err(ErrorCode::InvalidInputSyntax(format!(
            "\"{}\" is not append-only but vector index must be append-only",
            table.name
        ))
        .into());
    }

    let mut binder = Binder::new_for_stream(session);
    binder.bind_table(Some(&schema_name), &table.name)?;

    let mut index_columns_ordered_expr = vec![];
    let mut include_columns_expr = vec![];
    let mut distributed_columns_expr = vec![];
    if is_vector_index && columns.len() != 1 {
        return Err(ErrorCode::InvalidInputSyntax(format!(
            "vector index must be defined on exactly 1 column, but got {}",
            columns.len()
        ))
        .into());
    }
    let mut dimension = None;
    for column in columns {
        if is_vector_index && (column.asc.is_some() || column.nulls_first.is_some()) {
            return Err(ErrorCode::InvalidInputSyntax(
                "vector index cannot specify order".to_owned(),
            )
            .into());
        }
        let order_type = OrderType::from_bools(column.asc, column.nulls_first);
        let expr_impl = binder.bind_expr(&column.expr)?;
        // Do constant folding and timezone transportation on expressions so that batch queries can match it in the same form.
        let mut const_eval = ConstEvalRewriter { error: None };
        let expr_impl = const_eval.rewrite_expr(expr_impl);
        let expr_impl = context.session_timezone().rewrite_expr(expr_impl);
        match expr_impl {
            ExprImpl::InputRef(_) => {}
            ExprImpl::FunctionCall(_) => {
                if is_vector_index {
                    return Err(ErrorCode::NotSupported(
                        "vector index can not be created on function column".to_owned(),
                        "create index on table column only".to_owned(),
                    )
                    .into());
                }
                if !is_vector_index && expr_impl.is_impure() {
                    return Err(ErrorCode::NotSupported(
                        "this expression is impure".into(),
                        "use a pure expression instead".into(),
                    )
                    .into());
                }
            }
            _ => {
                return Err(ErrorCode::NotSupported(
                    "index columns should be columns or expressions".into(),
                    "use columns or expressions instead".into(),
                )
                .into());
            }
        }
        if is_vector_index {
            match expr_impl.return_type() {
                DataType::Vector(d) => {
                    dimension = Some(d);
                }
                other => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "vector index must be defined on column of Vector type, but got {:?}",
                        other
                    ))
                    .into());
                }
            }
        }
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
                binder.bind_expr(&risingwave_sqlparser::ast::Expr::Identifier(column))?;
            include_columns_expr.push(expr_impl);
        }
    };

    if is_vector_index && !distributed_by.is_empty() {
        return Err(ErrorCode::InvalidInputSyntax(
            "vector index cannot define DISTRIBUTED BY".to_owned(),
        )
        .into());
    }

    for column in distributed_by {
        let expr_impl = binder.bind_expr(&column)?;
        distributed_columns_expr.push(expr_impl);
    }

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
            ExprImpl::FunctionCall(_) => true,
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
            "Distributed by columns should be a prefix of index columns".to_owned(),
        )
        .into());
    }

    let (index_database_id, index_schema_id) =
        session.get_database_and_schema_id_for_create(Some(schema_name))?;

    let (plan, mut index_table) = if let Some(vector_index_config) = vector_index_config {
        assert!(distributed_columns_expr.is_empty());
        assert_eq!(1, index_columns_ordered_expr.len());
        let input = assemble_input(
            table.clone(),
            context.clone(),
            &index_columns_ordered_expr,
            &include_columns_expr,
            RequiredDist::PhysicalDist(Distribution::Single),
        )?;
        let definition = context.normalized_sql().to_owned();
        let retention_seconds = table.retention_seconds.and_then(NonZeroU32::new);

        let distance_type = match context
            .with_options()
            .get("distance_type")
            .ok_or_else(|| {
                ErrorCode::InvalidInputSyntax(
                    "vector index missing `distance_type` in with options".to_owned(),
                )
            })?
            .to_ascii_lowercase()
            .as_str()
        {
            "l1" => PbDistanceType::L1,
            "l2" => PbDistanceType::L2Sqr,
            "inner_product" => PbDistanceType::InnerProduct,
            "cosine" => PbDistanceType::Cosine,
            other => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "unsupported vector index distance type: {}",
                    other
                ))
                .into());
            }
        };

        let vector_index_write = input.gen_vector_index_plan(
            index_table_name.clone(),
            index_database_id,
            index_schema_id,
            definition,
            retention_seconds,
            PbVectorIndexInfo {
                dimension: dimension.expect("should be set for vector index") as _,
                config: Some(vector_index_config),
                distance_type: distance_type as _,
            },
        )?;
        let index_table = vector_index_write.table().clone();
        let plan: PlanRef = vector_index_write.into();
        (plan, index_table)
    } else {
        // Manually assemble the materialization plan for the index MV.
        let materialize = assemble_materialize(
            index_database_id,
            index_schema_id,
            table.clone(),
            context.clone(),
            index_table_name.clone(),
            &index_columns_ordered_expr,
            &include_columns_expr,
            // We use the first index column as distributed key by default if users
            // haven't specified the distributed by columns.
            if distributed_columns_expr.is_empty() {
                1
            } else {
                distributed_columns_expr.len()
            },
        )?;
        let index_table = materialize.table().clone();
        let plan: PlanRef = materialize.into();
        (plan, index_table)
    };

    {
        // Inherit table properties
        index_table.retention_seconds = table.retention_seconds;
    }

    index_table.owner = table.owner;

    let index_columns_len = index_columns_ordered_expr.len() as u32;
    let index_column_properties = index_columns_ordered_expr
        .iter()
        .map(|(_, order)| PbIndexColumnProperties {
            is_desc: order.is_descending(),
            nulls_first: order.nulls_are_first(),
        })
        .collect();
    let index_item = build_index_item(
        &index_table,
        table.name(),
        &table,
        index_columns_ordered_expr,
    );

    let create_type =
        if context.session_ctx().config().background_ddl() && plan_can_use_background_ddl(&plan) {
            CreateType::Background
        } else {
            CreateType::Foreground
        };

    let index_prost = PbIndex {
        id: IndexId::placeholder().index_id,
        schema_id: index_schema_id,
        database_id: index_database_id,
        name: index_table_name,
        owner: index_table.owner,
        index_table_id: TableId::placeholder().table_id,
        primary_table_id: table.id.table_id,
        index_item,
        index_column_properties,
        index_columns_len,
        initialized_at_epoch: None,
        created_at_epoch: None,
        stream_job_status: PbStreamJobStatus::Creating.into(),
        initialized_at_cluster_version: None,
        created_at_cluster_version: None,
        create_type: create_type.into(),
    };

    let ctx = plan.ctx();
    let explain_trace = ctx.is_explain_trace();
    if explain_trace {
        ctx.trace("Create Index:");
        ctx.trace(plan.explain_to_string());
    }

    Ok((plan, index_table, index_prost))
}

fn build_index_item(
    index_table: &TableCatalog,
    primary_table_name: &str,
    primary_table: &TableCatalog,
    index_columns: Vec<(ExprImpl, OrderType)>,
) -> Vec<risingwave_pb::expr::ExprNode> {
    let primary_table_desc_map = primary_table
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
            index_table
                .columns
                .iter()
                .map(|c| &c.column_desc)
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
                        data_type: primary_table
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

fn assemble_input(
    table_catalog: Arc<TableCatalog>,
    context: OptimizerContextRef,
    index_columns: &[(ExprImpl, OrderType)],
    include_columns: &[ExprImpl],
    required_dist: RequiredDist,
) -> Result<LogicalPlanRoot> {
    // Build logical plan and then call gen_create_index_plan
    // LogicalProject(index_columns, include_columns)
    //   LogicalScan(table_desc)

    let logical_scan = LogicalScan::create(table_catalog.clone(), context, None);

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
            ExprImpl::InputRef(input_ref) => table_catalog
                .columns()
                .get(input_ref.index)
                .unwrap()
                .name()
                .to_owned(),
            ExprImpl::FunctionCall(func) => {
                let func_name = func.func_type().as_str_name().to_owned();
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
                ExprImpl::InputRef(input_ref) => table_catalog
                    .columns()
                    .get(input_ref.index)
                    .unwrap()
                    .name()
                    .to_owned(),
                _ => unreachable!(),
            }
        }))
        .collect_vec();

    Ok(PlanRoot::new_with_logical_plan(
        logical_project,
        required_dist,
        Order::new(
            index_columns
                .iter()
                .enumerate()
                .map(|(i, (_, order))| ColumnOrder::new(i, *order))
                .collect(),
        ),
        project_required_cols,
        out_names,
    ))
}

/// Note: distributed by columns must be a prefix of index columns, so we just use
/// `distributed_by_columns_len` to represent distributed by columns
fn assemble_materialize(
    database_id: DatabaseId,
    schema_id: SchemaId,
    table_catalog: Arc<TableCatalog>,
    context: OptimizerContextRef,
    index_name: String,
    index_columns: &[(ExprImpl, OrderType)],
    include_columns: &[ExprImpl],
    distributed_by_columns_len: usize,
) -> Result<StreamMaterialize> {
    let input = assemble_input(
        table_catalog.clone(),
        context.clone(),
        index_columns,
        include_columns,
        // schema of logical_project is such that index columns come first.
        // so we can use distributed_by_columns_len to represent distributed by columns indices.
        RequiredDist::PhysicalDist(Distribution::HashShard(
            (0..distributed_by_columns_len).collect(),
        )),
    )?;

    let definition = context.normalized_sql().to_owned();
    let retention_seconds = table_catalog.retention_seconds.and_then(NonZeroU32::new);

    input.gen_index_plan(
        index_name,
        database_id,
        schema_id,
        definition,
        retention_seconds,
    )
}

pub async fn handle_create_index(
    handler_args: HandlerArgs,
    if_not_exists: bool,
    index_name: ObjectName,
    table_name: ObjectName,
    method: Option<Ident>,
    columns: Vec<OrderByExpr>,
    include: Vec<Ident>,
    distributed_by: Vec<ast::Expr>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();

    let (graph, index_table, index) = {
        let (schema_name, table, index_table_name) =
            resolve_index_schema(&session, index_name, table_name)?;
        let qualified_index_name = ObjectName(vec![
            Ident::from_real_value(&schema_name),
            Ident::from_real_value(&index_table_name),
        ]);
        if let Either::Right(resp) = session.check_relation_name_duplicated(
            qualified_index_name,
            StatementType::CREATE_INDEX,
            if_not_exists,
        )? {
            return Ok(resp);
        }

        let context = OptimizerContext::from_handler_args(handler_args);
        let (plan, index_table, index) = gen_create_index_plan(
            &session,
            context.into(),
            schema_name,
            table,
            index_table_name,
            method,
            columns,
            include,
            distributed_by,
        )?;
        let graph = build_graph(plan, Some(GraphJobType::Index))?;

        (graph, index_table, index)
    };

    tracing::trace!(
        "name={}, graph=\n{}",
        index.name,
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

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .create_index(index, index_table.to_prost(), graph, if_not_exists)
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_INDEX))
}
