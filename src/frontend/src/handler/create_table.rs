// Copyright 2023 Singularity Data
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

use std::collections::HashMap;
use std::rc::Rc;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::catalog::{
    ColumnIndex as ProstColumnIndex, Source as ProstSource, StreamSourceInfo, Table as ProstTable,
};
use risingwave_sqlparser::ast::{
    ColumnDef, ColumnOption, DataType as AstDataType, ObjectName, SourceSchema, TableConstraint,
};

use super::create_source::{check_and_add_timestamp_column, resolve_source_schema};
use super::RwPgResponse;
use crate::binder::{bind_data_type, bind_struct_field};
use crate::catalog::column_catalog::ColumnCatalog;
use crate::catalog::{check_valid_column_name, ColumnId};
use crate::handler::create_source::UPSTREAM_SOURCE_KEY;
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::LogicalSource;
use crate::optimizer::property::{Order, RequiredDist};
use crate::optimizer::{OptimizerContext, OptimizerContextRef, PlanRef, PlanRoot};
use crate::session::SessionImpl;
use crate::stream_fragmenter::build_graph;
use crate::{Binder, WithOptions};

#[derive(PartialEq, Clone, Debug)]
pub enum DmlFlag {
    /// used for `create materialized view / sink / index`
    Disable,
    /// used for `create table`
    All,
    /// used for `create table with (append_only = true)`
    AppendOnly,
}

/// Binds the column schemas declared in CREATE statement into `ColumnDesc`.
/// If a column is marked as `primary key`, its `ColumnId` is also returned.
/// This primary key is not combined with table constraints yet.
pub fn bind_sql_columns(columns: Vec<ColumnDef>) -> Result<(Vec<ColumnDesc>, Option<ColumnId>)> {
    // In `ColumnDef`, pk can contain only one column. So we use `Option` rather than `Vec`.
    let mut pk_column_id = None;

    let column_descs = {
        let mut column_descs = Vec::with_capacity(columns.len());
        for (i, column) in columns.into_iter().enumerate() {
            let column_id = ColumnId::new(i as i32);
            // Destruct to make sure all fields are properly handled rather than ignored.
            // Do NOT use `..` to ignore fields you do not want to deal with.
            // Reject them with a clear NotImplemented error.
            let ColumnDef {
                name,
                data_type,
                collation,
                options,
            } = column;
            let data_type = data_type.ok_or(ErrorCode::InvalidInputSyntax(
                "data type is not specified".into(),
            ))?;
            if let Some(collation) = collation {
                return Err(ErrorCode::NotImplemented(
                    format!("collation \"{}\"", collation),
                    None.into(),
                )
                .into());
            }
            for option_def in options {
                match option_def.option {
                    ColumnOption::Unique { is_primary: true } => {
                        if pk_column_id.is_some() {
                            return Err(ErrorCode::BindError(
                                "multiple primary keys are not allowed".into(),
                            )
                            .into());
                        }
                        pk_column_id = Some(column_id);
                    }
                    _ => {
                        return Err(ErrorCode::NotImplemented(
                            format!("column constraints \"{}\"", option_def),
                            None.into(),
                        )
                        .into())
                    }
                }
            }
            check_valid_column_name(&name.real_value())?;
            let field_descs = if let AstDataType::Struct(fields) = &data_type {
                fields
                    .iter()
                    .map(bind_struct_field)
                    .collect::<Result<Vec<_>>>()?
            } else {
                vec![]
            };
            column_descs.push(ColumnDesc {
                data_type: bind_data_type(&data_type)?,
                column_id,
                name: name.real_value(),
                field_descs,
                type_name: "".to_string(),
            });
        }
        column_descs
    };

    Ok((column_descs, pk_column_id))
}

/// Binds table constraints given the binding results from column definitions.
/// It returns the columns together with `pk_column_ids`, and an optional row id column index if
/// added.
pub fn bind_sql_table_constraints(
    column_descs: Vec<ColumnDesc>,
    pk_column_id_from_columns: Option<ColumnId>,
    constraints: Vec<TableConstraint>,
) -> Result<(Vec<ColumnCatalog>, Vec<ColumnId>, Option<usize>)> {
    let mut pk_column_names = vec![];
    for constraint in constraints {
        match constraint {
            TableConstraint::Unique {
                name: _,
                columns,
                is_primary: true,
            } => {
                if !pk_column_names.is_empty() {
                    return Err(ErrorCode::BindError(
                        "multiple primary keys are not allowed".into(),
                    )
                    .into());
                }
                pk_column_names = columns;
            }
            _ => {
                return Err(ErrorCode::NotImplemented(
                    format!("table constraint \"{}\"", constraint),
                    None.into(),
                )
                .into())
            }
        }
    }
    let mut pk_column_ids = match (pk_column_id_from_columns, pk_column_names.is_empty()) {
        (Some(_), false) => {
            return Err(ErrorCode::BindError("multiple primary keys are not allowed".into()).into())
        }
        (None, true) => {
            // We don't have a pk column now, so we need to add row_id column as the pk column
            // later.
            vec![]
        }
        (Some(cid), true) => vec![cid],
        (None, false) => {
            let name_to_id = column_descs
                .iter()
                .map(|c| (c.name.as_str(), c.column_id))
                .collect::<HashMap<_, _>>();
            pk_column_names
                .iter()
                .map(|ident| {
                    let name = ident.real_value();
                    name_to_id.get(name.as_str()).copied().ok_or_else(|| {
                        ErrorCode::BindError(format!(
                            "column \"{name}\" named in key does not exist"
                        ))
                    })
                })
                .try_collect()?
        }
    };

    let mut columns_catalog = column_descs
        .into_iter()
        .map(|c| {
            // All columns except `_row_id` or starts with `_rw` should be visible.
            let is_hidden = c.name.starts_with("_rw");
            ColumnCatalog {
                column_desc: c,
                is_hidden,
            }
        })
        .collect_vec();

    // Add `_row_id` column if `pk_column_ids` is empty.
    let row_id_index = pk_column_ids.is_empty().then(|| {
        let row_id_index = columns_catalog.len();
        let row_id_column_id = ColumnId::new(row_id_index as i32);
        columns_catalog.push(ColumnCatalog::row_id_column(row_id_column_id));
        pk_column_ids.push(row_id_column_id);
        row_id_index
    });

    if let Some(col) = columns_catalog.iter().map(|c| c.name()).duplicates().next() {
        Err(ErrorCode::InvalidInputSyntax(format!(
            "column \"{col}\" specified more than once"
        )))?;
    }

    Ok((columns_catalog, pk_column_ids, row_id_index))
}

/// `gen_create_table_plan_with_source` generates the plan for creating a table with an external
/// stream source.
pub(crate) async fn gen_create_table_plan_with_source(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    columns: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    source_schema: SourceSchema,
) -> Result<(PlanRef, Option<ProstSource>, ProstTable)> {
    let (mut column_descs, pk_column_id_from_columns) = bind_sql_columns(columns)?;
    let properties = handler_args.with_options.inner().clone();

    check_and_add_timestamp_column(&properties, &mut column_descs, true);

    let (mut columns, pk_column_ids, row_id_index) =
        bind_sql_table_constraints(column_descs, pk_column_id_from_columns, constraints)?;

    let session = handler_args.session.clone();
    let context = OptimizerContext::from_handler_args(handler_args);
    let definition = context.normalized_sql().to_owned();

    let source_info = resolve_source_schema(
        source_schema,
        &mut columns,
        &properties,
        row_id_index,
        &pk_column_ids,
    )
    .await?;

    gen_table_plan_inner(
        &session,
        context.into(),
        table_name,
        columns,
        pk_column_ids,
        row_id_index,
        Some(source_info),
        definition,
    )
}

/// `gen_create_table_plan` generates the plan for creating a table without an external stream
/// source.
pub(crate) fn gen_create_table_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    table_name: ObjectName,
    columns: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
) -> Result<(PlanRef, Option<ProstSource>, ProstTable)> {
    let definition = context.normalized_sql().to_owned();
    let (column_descs, pk_column_id_from_columns) = bind_sql_columns(columns)?;
    gen_create_table_plan_without_bind(
        session,
        context,
        table_name,
        column_descs,
        pk_column_id_from_columns,
        constraints,
        definition,
    )
}

pub(crate) fn gen_create_table_plan_without_bind(
    session: &SessionImpl,
    context: OptimizerContextRef,
    table_name: ObjectName,
    column_descs: Vec<ColumnDesc>,
    pk_column_id_from_columns: Option<ColumnId>,
    constraints: Vec<TableConstraint>,
    definition: String,
) -> Result<(PlanRef, Option<ProstSource>, ProstTable)> {
    let (columns, pk_column_ids, row_id_index) =
        bind_sql_table_constraints(column_descs, pk_column_id_from_columns, constraints)?;

    gen_table_plan_inner(
        session,
        context,
        table_name,
        columns,
        pk_column_ids,
        row_id_index,
        None,
        definition,
    )
}

#[allow(clippy::too_many_arguments)]
fn gen_table_plan_inner(
    session: &SessionImpl,
    context: OptimizerContextRef,
    table_name: ObjectName,
    columns: Vec<ColumnCatalog>,
    pk_column_ids: Vec<ColumnId>,
    row_id_index: Option<usize>,
    source_info: Option<StreamSourceInfo>,
    definition: String,
) -> Result<(PlanRef, Option<ProstSource>, ProstTable)> {
    let db_name = session.database();
    let (schema_name, name) = Binder::resolve_schema_qualified_name(db_name, table_name)?;
    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    let source = source_info.map(|source_info| ProstSource {
        id: 0,
        schema_id,
        database_id,
        name: name.clone(),
        row_id_index: row_id_index.map(|i| ProstColumnIndex { index: i as _ }),
        columns: columns
            .iter()
            .map(|column| column.to_protobuf())
            .collect_vec(),
        pk_column_ids: pk_column_ids.iter().map(Into::into).collect_vec(),
        properties: context.with_options().inner().clone(),
        info: Some(source_info),
        owner: session.user_id(),
    });

    let source_catalog = source.as_ref().map(|source| Rc::new((source).into()));
    let source_node: PlanRef = LogicalSource::new(
        source_catalog,
        columns
            .iter()
            .map(|column| column.column_desc.clone())
            .collect_vec(),
        pk_column_ids,
        row_id_index,
        false,
        context.clone(),
    )
    .into();

    let mut required_cols = FixedBitSet::with_capacity(source_node.schema().len());
    required_cols.toggle_range(..);
    let mut out_names = source_node.schema().names();

    if let Some(row_id_index) = row_id_index {
        required_cols.toggle(row_id_index);
        out_names.remove(row_id_index);
    }

    let mut plan_root = PlanRoot::new(
        source_node,
        RequiredDist::Any,
        Order::any(),
        required_cols,
        out_names,
    );

    // The materialize executor need not handle primary key conflict if the primary key is row id.
    let handle_pk_conflict = row_id_index.is_none();
    let dml_flag = match context.with_options().append_only() {
        true => DmlFlag::AppendOnly,
        false => DmlFlag::All,
    };

    let materialize = plan_root.gen_table_plan(
        name,
        columns,
        definition,
        handle_pk_conflict,
        row_id_index,
        dml_flag,
    )?;

    let mut table = materialize.table().to_prost(schema_id, database_id);

    table.owner = session.user_id();
    Ok((materialize.into(), source, table))
}

/// TODO(Yuanxin): Remove this method after unsupporting `CREATE MATERIALIZED SOURCE`.
pub(crate) fn gen_materialize_plan(
    context: OptimizerContextRef,
    source: ProstSource,
    owner: u32,
) -> Result<(PlanRef, ProstTable)> {
    let materialize = {
        let row_id_index = source.row_id_index.as_ref().map(|index| index.index as _);
        let definition = context.sql().to_owned(); // TODO: use formatted SQL

        // Manually assemble the materialization plan for the table.
        let source_node: PlanRef = LogicalSource::new(
            Some(Rc::new((&source).into())),
            source
                .columns
                .iter()
                .map(|column| column.column_desc.clone().unwrap().into())
                .collect(),
            source
                .pk_column_ids
                .iter()
                .map(|id| ColumnId::new(*id))
                .collect(),
            row_id_index,
            true,
            context,
        )
        .into();

        // row_id_index is Some means that the user has not specified pk, then we will add a hidden
        // column to store pk, and materialize executor do not need to handle pk conflict.
        let handle_pk_conflict = row_id_index.is_none();
        let mut required_cols = FixedBitSet::with_capacity(source_node.schema().len());
        required_cols.toggle_range(..);
        let mut out_names = source_node.schema().names();
        if let Some(row_id_index) = row_id_index {
            required_cols.toggle(row_id_index);
            out_names.remove(row_id_index);
        }

        let mut plan_root = PlanRoot::new(
            source_node,
            RequiredDist::Any,
            Order::any(),
            required_cols,
            out_names,
        );

        plan_root.gen_table_plan(
            source.name.clone(),
            source
                .columns
                .into_iter()
                .map(ColumnCatalog::from)
                .collect(),
            definition,
            handle_pk_conflict,
            row_id_index,
            DmlFlag::Disable,
        )?
    };
    let mut table = materialize
        .table()
        .to_prost(source.schema_id, source.database_id);
    table.owner = owner;
    Ok((materialize.into(), table))
}

pub async fn handle_create_table(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    columns: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    if_not_exists: bool,
    source_schema: Option<SourceSchema>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();

    if let Err(e) = session.check_relation_name_duplicated(table_name.clone()) {
        if if_not_exists {
            return Ok(PgResponse::empty_result_with_notice(
                StatementType::CREATE_TABLE,
                format!("relation \"{}\" already exists, skipping", table_name),
            ));
        } else {
            return Err(e);
        }
    }

    let (graph, source, table) = {
        let (plan, source, table) =
            match check_create_table_with_source(&handler_args.with_options, source_schema)? {
                Some(source_schema) => {
                    gen_create_table_plan_with_source(
                        handler_args,
                        table_name.clone(),
                        columns,
                        constraints,
                        source_schema,
                    )
                    .await?
                }
                None => {
                    let context = OptimizerContext::from_handler_args(handler_args);
                    gen_create_table_plan(
                        &session,
                        context.into(),
                        table_name.clone(),
                        columns,
                        constraints,
                    )?
                }
            };

        let graph = build_graph(plan);

        (graph, source, table)
    };

    tracing::trace!(
        "name={}, graph=\n{}",
        table_name,
        serde_json::to_string_pretty(&graph).unwrap()
    );

    let catalog_writer = session.env().catalog_writer();

    catalog_writer.create_table(source, table, graph).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_TABLE))
}

pub fn check_create_table_with_source(
    with_options: &WithOptions,
    source_schema: Option<SourceSchema>,
) -> Result<Option<SourceSchema>> {
    if with_options.inner().contains_key(UPSTREAM_SOURCE_KEY) {
        source_schema.as_ref().ok_or_else(|| {
            ErrorCode::InvalidInputSyntax(
                "Please specify a source schema using ROW FORMAT".to_owned(),
            )
        })?;
    }
    Ok(source_schema)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::catalog::root_catalog::SchemaPath;
    use crate::catalog::row_id_column_name;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_create_table_handler() {
        let sql = "create table t (v1 smallint, v2 struct<v3 bigint, v4 float, v5 double>) with (appendonly = true);";
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check table exists.
        let (table, _) = catalog_reader
            .get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
            .unwrap();
        assert_eq!(table.name(), "t");

        let columns = table
            .columns
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<HashMap<&str, DataType>>();

        let row_id_col_name = row_id_column_name();
        let expected_columns = maplit::hashmap! {
            row_id_col_name.as_str() => DataType::Int64,
            "v1" => DataType::Int16,
            "v2" => DataType::new_struct(
                vec![DataType::Int64,DataType::Float64,DataType::Float64],
                vec!["v3".to_string(), "v4".to_string(), "v5".to_string()],
            ),
        };

        assert_eq!(columns, expected_columns);
    }

    #[test]
    fn test_bind_primary_key() {
        for (sql, expected) in [
            ("create table t (v1 int, v2 int)", Ok(&[2] as &[_])),
            ("create table t (v1 int primary key, v2 int)", Ok(&[0])),
            ("create table t (v1 int, v2 int primary key)", Ok(&[1])),
            (
                "create table t (v1 int primary key, v2 int primary key)",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int primary key primary key, v2 int)",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int, v2 int, primary key (v1))",
                Ok(&[0]),
            ),
            (
                "create table t (v1 int, primary key (v2), v2 int)",
                Ok(&[1]),
            ),
            (
                "create table t (primary key (v2, v1), v1 int, v2 int)",
                Ok(&[1, 0]),
            ),
            (
                "create table t (v1 int, primary key (v1), v2 int, primary key (v1))",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int primary key, primary key (v1), v2 int)",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int, primary key (V3), v2 int)",
                Err("column \"v3\" named in key does not exist"),
            ),
        ] {
            let mut ast = risingwave_sqlparser::parser::Parser::parse_sql(sql).unwrap();
            let risingwave_sqlparser::ast::Statement::CreateTable {
                    columns,
                    constraints,
                    ..
                } = ast.remove(0) else { panic!("test case should be create table") };
            let actual: Result<_> = (|| {
                let (column_descs, pk_column_id_from_columns) = bind_sql_columns(columns)?;
                let (_, pk_column_ids, _) = bind_sql_table_constraints(
                    column_descs,
                    pk_column_id_from_columns,
                    constraints,
                )?;
                Ok(pk_column_ids)
            })();
            match (expected, actual) {
                (Ok(expected), Ok(actual)) => assert_eq!(
                    expected.iter().copied().map(ColumnId::new).collect_vec(),
                    actual,
                    "sql: {sql}"
                ),
                (Ok(_), Err(actual)) => panic!("sql: {sql}\nunexpected error: {actual:?}"),
                (Err(_), Ok(actual)) => panic!("sql: {sql}\nexpects error but got: {actual:?}"),
                (Err(expected), Err(actual)) => assert!(
                    actual.to_string().contains(expected),
                    "sql: {sql}\nexpected: {expected:?}\nactual: {actual:?}"
                ),
            }
        }
    }
}
