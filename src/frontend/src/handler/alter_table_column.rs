// Copyright 2024 RisingWave Labs
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
use std::sync::Arc;

use anyhow::{anyhow, Context};
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{ColumnCatalog, Engine, ROW_ID_COLUMN_ID};
use risingwave_common::hash::VnodeCount;
use risingwave_common::types::DataType;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::{bail, bail_not_implemented};
use risingwave_connector::sink::catalog::SinkCatalog;
use risingwave_pb::catalog::{Source, Table};
use risingwave_pb::ddl_service::TableJobType;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::{ProjectNode, StreamFragmentGraph};
use risingwave_sqlparser::ast::{
    AlterTableOperation, ColumnDef, ColumnOption, DataType as AstDataType, ExplainOptions, Ident,
    ObjectName, Statement, StructField, TableConstraint,
};
use risingwave_sqlparser::parser::Parser;

use super::create_table::{
    bind_sql_columns, bind_sql_columns_generated_and_default_constraints, bind_sql_pk_names,
    gen_table_plan_inner, generate_stream_graph_for_replace_table, ColumnIdGenerator,
    CreateTableInfo, CreateTableProps,
};
use super::util::SourceSchemaCompatExt;
use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::table_catalog::TableType;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{Expr, ExprImpl, InputRef, Literal};
use crate::handler::create_sink::{fetch_incoming_sinks, insert_merger_to_union_with_project};
use crate::handler::create_table::bind_table_constraints;
use crate::session::SessionImpl;
use crate::{build_graph, Binder, OptimizerContext, TableCatalog};

/// Used in auto schema change process
pub async fn get_new_table_definition_for_cdc_table(
    session: &Arc<SessionImpl>,
    table_name: ObjectName,
    new_columns: &[ColumnCatalog],
) -> Result<(Statement, Arc<TableCatalog>)> {
    let original_catalog = fetch_table_catalog_for_alter(session.as_ref(), &table_name)?;

    // Retrieve the original table definition and parse it to AST.
    let [mut definition]: [_; 1] = Parser::parse_sql(&original_catalog.definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();

    let Statement::CreateTable {
        columns: original_columns,
        format_encode,
        constraints,
        ..
    } = &mut definition
    else {
        panic!("unexpected statement: {:?}", definition);
    };

    assert!(
        format_encode.is_none(),
        "source schema should be None for CDC table"
    );

    if bind_table_constraints(constraints)?.is_empty() {
        // For table created by `create table t (*)` the constraint is empty, we need to
        // retrieve primary key names from original table catalog if available
        let pk_names: Vec<_> = original_catalog
            .pk
            .iter()
            .map(|x| original_catalog.columns[x.column_index].name().to_owned())
            .collect();

        constraints.push(TableConstraint::Unique {
            name: None,
            columns: pk_names.iter().map(Ident::new_unchecked).collect(),
            is_primary: true,
        });
    }

    let orig_column_catalog: HashMap<String, ColumnCatalog> = HashMap::from_iter(
        original_catalog
            .columns()
            .iter()
            .map(|col| (col.name().to_owned(), col.clone())),
    );

    // update the original columns with new version columns
    let mut new_column_defs = vec![];
    for new_col in new_columns {
        // if the column exists in the original catalog, use it to construct the column definition.
        // since we don't support altering the column type right now
        if let Some(original_col) = orig_column_catalog.get(new_col.name()) {
            let ty = to_ast_data_type(original_col.data_type())?;
            new_column_defs.push(ColumnDef::new(original_col.name().into(), ty, None, vec![]));
        } else {
            let ty = to_ast_data_type(new_col.data_type())?;
            new_column_defs.push(ColumnDef::new(new_col.name().into(), ty, None, vec![]));
        }
    }
    *original_columns = new_column_defs;

    Ok((definition, original_catalog))
}

fn to_ast_data_type(ty: &DataType) -> Result<AstDataType> {
    match ty {
        DataType::Boolean => Ok(AstDataType::Boolean),
        DataType::Int16 => Ok(AstDataType::SmallInt),
        DataType::Int32 => Ok(AstDataType::Int),
        DataType::Int64 => Ok(AstDataType::BigInt),
        DataType::Float32 => Ok(AstDataType::Real),
        DataType::Float64 => Ok(AstDataType::Double),
        // TODO: handle precision and scale for decimal
        DataType::Decimal => Ok(AstDataType::Decimal(None, None)),
        DataType::Date => Ok(AstDataType::Date),
        DataType::Varchar => Ok(AstDataType::Varchar),
        DataType::Time => Ok(AstDataType::Time(false)),
        DataType::Timestamp => Ok(AstDataType::Timestamp(false)),
        DataType::Timestamptz => Ok(AstDataType::Timestamp(true)),
        DataType::Interval => Ok(AstDataType::Interval),
        DataType::Jsonb => Ok(AstDataType::Jsonb),
        DataType::Bytea => Ok(AstDataType::Bytea),
        DataType::List(item_ty) => Ok(AstDataType::Array(Box::new(to_ast_data_type(item_ty)?))),
        DataType::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|(name, ty)| {
                    Ok::<StructField, RwError>(StructField {
                        name: name.into(),
                        data_type: to_ast_data_type(ty)?,
                    })
                })
                .try_collect()?;
            Ok(AstDataType::Struct(fields))
        }
        DataType::Serial | DataType::Int256 | DataType::Map(_) => {
            Err(anyhow!("unsupported data type: {:?}", ty).context("to_ast_data_type"))?
        }
    }
}

pub async fn get_replace_table_plan(
    session: &Arc<SessionImpl>,
    table_name: ObjectName,
    new_definition: Statement,
    old_catalog: &Arc<TableCatalog>,
    new_version_columns: Option<Vec<ColumnCatalog>>, // only provided in auto schema change
) -> Result<(
    Option<Source>,
    Table,
    StreamFragmentGraph,
    ColIndexMapping,
    TableJobType,
)> {
    // Create handler args as if we're creating a new table with the altered definition.
    let handler_args = HandlerArgs::new(session.clone(), &new_definition, Arc::from(""))?;
    let col_id_gen = ColumnIdGenerator::new_alter(old_catalog);
    let Statement::CreateTable {
        columns,
        constraints,
        source_watermarks,
        append_only,
        on_conflict,
        with_version_column,
        wildcard_idx,
        cdc_table_info,
        format_encode,
        include_column_options,
        engine,
        ..
    } = new_definition
    else {
        panic!("unexpected statement type: {:?}", new_definition);
    };

    let format_encode = format_encode
        .clone()
        .map(|format_encode| format_encode.into_v2_with_warning());

    let engine = match engine {
        risingwave_sqlparser::ast::Engine::Hummock => Engine::Hummock,
        risingwave_sqlparser::ast::Engine::Iceberg => Engine::Iceberg,
    };

    let (mut graph, table, source, job_type) = generate_stream_graph_for_replace_table(
        session,
        table_name,
        old_catalog,
        format_encode,
        handler_args.clone(),
        col_id_gen,
        columns.clone(),
        wildcard_idx,
        constraints,
        source_watermarks,
        append_only,
        on_conflict,
        with_version_column,
        cdc_table_info,
        new_version_columns,
        include_column_options,
        engine,
    )
    .await?;

    // Calculate the mapping from the original columns to the new columns.
    let col_index_mapping = ColIndexMapping::new(
        old_catalog
            .columns()
            .iter()
            .map(|old_c| {
                table.columns.iter().position(|new_c| {
                    new_c.get_column_desc().unwrap().column_id == old_c.column_id().get_id()
                })
            })
            .collect(),
        table.columns.len(),
    );

    let incoming_sink_ids: HashSet<_> = old_catalog.incoming_sinks.iter().copied().collect();

    let target_columns = table
        .columns
        .iter()
        .map(|col| ColumnCatalog::from(col.clone()))
        .filter(|col| !col.is_rw_timestamp_column())
        .collect_vec();

    for sink in fetch_incoming_sinks(session, &incoming_sink_ids)? {
        hijack_merger_for_target_table(
            &mut graph,
            &target_columns,
            &sink,
            Some(&sink.unique_identity()),
        )?;
    }

    // Set some fields ourselves so that the meta service does not need to maintain them.
    let mut table = table;
    table.incoming_sinks = incoming_sink_ids.iter().copied().collect();
    table.maybe_vnode_count = VnodeCount::set(old_catalog.vnode_count()).to_protobuf();

    Ok((source, table, graph, col_index_mapping, job_type))
}

pub(crate) fn hijack_merger_for_target_table(
    graph: &mut StreamFragmentGraph,
    target_columns: &[ColumnCatalog],
    sink: &SinkCatalog,
    uniq_identify: Option<&str>,
) -> Result<()> {
    let mut sink_columns = sink.original_target_columns.clone();
    if sink_columns.is_empty() {
        // This is due to the fact that the value did not exist in earlier versions,
        // which means no schema changes such as `ADD/DROP COLUMN` have been made to the table.
        // Therefore the columns of the table at this point are `original_target_columns`.
        // This value of sink will be filled on the meta.
        sink_columns = target_columns.to_vec();
    }

    let mut i = 0;
    let mut j = 0;
    let mut exprs = Vec::new();

    while j < target_columns.len() {
        if i < sink_columns.len() && sink_columns[i].data_type() == target_columns[j].data_type() {
            exprs.push(ExprImpl::InputRef(Box::new(InputRef {
                data_type: sink_columns[i].data_type().clone(),
                index: i,
            })));

            i += 1;
            j += 1;
        } else {
            exprs.push(ExprImpl::Literal(Box::new(Literal::new(
                None,
                target_columns[j].data_type().clone(),
            ))));

            j += 1;
        }
    }

    let pb_project = PbNodeBody::Project(Box::new(ProjectNode {
        select_list: exprs.iter().map(|expr| expr.to_expr_proto()).collect(),
        ..Default::default()
    }));

    for fragment in graph.fragments.values_mut() {
        if let Some(node) = &mut fragment.node {
            insert_merger_to_union_with_project(node, &pb_project, uniq_identify);
        }
    }

    Ok(())
}

/// Handle `ALTER TABLE [ADD|DROP] COLUMN` statements. The `operation` must be either `AddColumn` or
/// `DropColumn`.
pub async fn handle_alter_table_column(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    operation: AlterTableOperation,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let (original_table, _associated_source) =
        fetch_table_source_for_alter(session.as_ref(), &table_name)?;

    if !original_table.incoming_sinks.is_empty() && original_table.has_generated_column() {
        return Err(RwError::from(ErrorCode::BindError(
            "Alter a table with incoming sink and generated column has not been implemented."
                .to_owned(),
        )));
    }

    if !original_table.incoming_sinks.is_empty()
        && matches!(operation, AlterTableOperation::DropColumn { .. })
    {
        return Err(ErrorCode::InvalidInputSyntax(
            "dropping columns in target table of sinks is not supported".to_owned(),
        ))?;
    }

    let fail_if_schema_is_inferred = || {
        // TODO: check if the schema is inferred from `associated_source`.
        Result::Ok(())
    };

    let mut column_catalogs = original_table.columns().to_vec();
    let pk_column_ids = original_table.pk_column_ids();

    match operation {
        AlterTableOperation::AddColumn {
            column_def: added_column,
        } => {
            // If the schema is inferred, do not allow adding columns.
            fail_if_schema_is_inferred()?;

            // Duplicated names can actually be checked by `StreamMaterialize`. We do here for
            // better error reporting.
            let added_column_name = added_column.name.real_value();
            if column_catalogs
                .iter()
                .any(|c| c.name() == added_column_name)
            {
                Err(ErrorCode::InvalidInputSyntax(format!(
                    "column \"{added_column_name}\" of table \"{table_name}\" already exists"
                )))?
            }

            if (added_column.options.iter())
                .any(|x| matches!(x.option, ColumnOption::GeneratedColumns(_)))
            {
                Err(ErrorCode::InvalidInputSyntax(
                    "alter table add generated columns is not supported".to_owned(),
                ))?
            }

            let added_column = vec![added_column];

            let added_pk = !bind_sql_pk_names(&added_column, Vec::new())?.is_empty();
            if added_pk {
                Err(ErrorCode::InvalidInputSyntax(
                    "cannot add a primary key column".to_owned(),
                ))?
            }

            let mut added_column_catalog = bind_sql_columns(&added_column)?;

            bind_sql_columns_generated_and_default_constraints(
                &session,
                table_name.real_value(),
                &mut added_column_catalog,
                added_column,
            )?;

            column_catalogs.extend(added_column_catalog);
        }

        AlterTableOperation::DropColumn {
            column_name,
            if_exists,
            cascade,
        } => {
            if cascade {
                bail_not_implemented!(issue = 6903, "drop column cascade");
            }

            // Check if the column to drop is referenced by any generated columns.
            for column in original_table.columns() {
                if let Some(expr) = column.generated_expr() {
                    let expr = ExprImpl::from_expr_proto(expr)?;
                    let refs = expr.collect_input_refs(original_table.columns().len());
                    for idx in refs.ones() {
                        let refed_column = &original_table.columns()[idx];
                        if refed_column.name() == column_name.real_value() {
                            bail!(format!(
                                "failed to drop column \"{}\" because it's referenced by a generated column \"{}\"",
                                column_name,
                                column.name()
                            ))
                        }
                    }
                }
            }

            // Locate the column by name and remove it.
            let column_name = column_name.real_value();
            let removed_column = column_catalogs
                .extract_if(|c| c.name() == column_name)
                .at_most_one()
                .ok()
                .expect("should not be multiple columns with the same name");

            let Some(removed_column) = removed_column else {
                if if_exists {
                    return Ok(PgResponse::builder(StatementType::ALTER_TABLE)
                        .notice(format!(
                            "column \"{}\" does not exist, skipping",
                            column_name
                        ))
                        .into());
                } else {
                    Err(ErrorCode::InvalidInputSyntax(format!(
                        "column \"{}\" of table \"{}\" does not exist",
                        column_name, table_name
                    )))?
                }
            };

            if !removed_column.can_drop() {
                bail!("cannot drop a hidden or system column");
            }

            // If the schema is inferred, only allow dropping generated columns.
            if !removed_column.is_generated() {
                fail_if_schema_is_inferred()?;
            }

            if pk_column_ids.contains(&removed_column.column_id()) {
                Err(ErrorCode::InvalidInputSyntax(
                    "cannot drop a primary key column".to_owned(),
                ))?
            }
        }

        _ => unreachable!(),
    };

    column_catalogs.retain(|c| !c.is_rw_timestamp_column());

    let (source, table, graph, col_index_mapping, job_type) =
        get_replace_table_plan_2(&session, table_name, column_catalogs, &original_table).await?;

    let catalog_writer = session.catalog_writer()?;

    catalog_writer
        .replace_table(source, table, graph, col_index_mapping, job_type)
        .await?;
    Ok(PgResponse::empty_result(StatementType::ALTER_TABLE))
}

pub async fn get_replace_table_plan_2(
    session: &Arc<SessionImpl>,
    table_name: ObjectName,
    mut new_columns: Vec<ColumnCatalog>,
    old_catalog: &Arc<TableCatalog>,
) -> Result<(
    Option<Source>,
    Table,
    StreamFragmentGraph,
    ColIndexMapping,
    TableJobType,
)> {
    // Create handler args as if we're creating a new table with the altered definition.
    let handler_args = HandlerArgs::new(session.clone(), &Statement::Abort, Arc::from(""))?;
    let mut col_id_gen = ColumnIdGenerator::new_alter(old_catalog);

    for new_column in &mut new_columns {
        new_column.column_desc.column_id = col_id_gen.generate(&*new_column)?;
    }

    let context = OptimizerContext::new(handler_args, ExplainOptions::default());

    let db_name = session.database();
    let (schema_name, name) = Binder::resolve_schema_qualified_name(db_name, table_name)?;

    let row_id_index = new_columns
        .iter()
        .position(|c| c.column_id() == ROW_ID_COLUMN_ID);

    if old_catalog.has_associated_source() || old_catalog.cdc_table_id.is_some() {
        bail_not_implemented!("new replace table not supported for table with source");
    }

    let props = CreateTableProps {
        definition: "".to_owned(), // TODO: no more definition!
        append_only: old_catalog.append_only,
        on_conflict: old_catalog.conflict_behavior.into(),
        with_version_column: old_catalog
            .version_column_index
            .map(|i| old_catalog.columns()[i].name().to_owned()),
        webhook_info: old_catalog.webhook_info.clone(),
        engine: old_catalog.engine,
    };

    let info = CreateTableInfo {
        columns: new_columns,
        pk_column_ids: old_catalog.pk_column_ids(),
        row_id_index,
        watermark_descs: vec![], // TODO: this is not persisted in the catalog!
        source_catalog: None,
        version: col_id_gen.into_version(),
    };

    let (plan, table) = gen_table_plan_inner(context.into(), schema_name, name, info, props)?;

    let mut graph = build_graph(plan)?;

    // Fill the original table ID.
    let table = Table {
        id: old_catalog.id().table_id(),
        ..table
    };

    // Calculate the mapping from the original columns to the new columns.
    let col_index_mapping = ColIndexMapping::new(
        old_catalog
            .columns()
            .iter()
            .map(|old_c| {
                table.columns.iter().position(|new_c| {
                    new_c.get_column_desc().unwrap().column_id == old_c.column_id().get_id()
                })
            })
            .collect(),
        table.columns.len(),
    );

    let incoming_sink_ids: HashSet<_> = old_catalog.incoming_sinks.iter().copied().collect();

    let target_columns = table
        .columns
        .iter()
        .map(|col| ColumnCatalog::from(col.clone()))
        .filter(|col| !col.is_rw_timestamp_column())
        .collect_vec();

    for sink in fetch_incoming_sinks(&session, &incoming_sink_ids)? {
        hijack_merger_for_target_table(
            &mut graph,
            &target_columns,
            &sink,
            Some(&sink.unique_identity()),
        )?;
    }

    // Set some fields ourselves so that the meta service does not need to maintain them.
    let mut table = table;
    table.incoming_sinks = incoming_sink_ids.iter().copied().collect();
    table.maybe_vnode_count = VnodeCount::set(old_catalog.vnode_count()).to_protobuf();

    Ok((None, table, graph, col_index_mapping, TableJobType::General))
}

pub fn fetch_table_catalog_for_alter(
    session: &SessionImpl,
    table_name: &ObjectName,
) -> Result<Arc<TableCatalog>> {
    let (table, _source) = fetch_table_source_for_alter(session, table_name)?;
    Ok(table)
}

pub fn fetch_table_source_for_alter(
    session: &SessionImpl,
    table_name: &ObjectName,
) -> Result<(Arc<TableCatalog>, Option<Arc<SourceCatalog>>)> {
    let db_name = session.database();
    let (schema_name, real_table_name) =
        Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let reader = session.env().catalog_reader().read_guard();

    let table = {
        let (table, schema_name) =
            reader.get_created_table_by_name(db_name, schema_path, &real_table_name)?;

        match table.table_type() {
            TableType::Table => {}

            _ => Err(ErrorCode::InvalidInputSyntax(format!(
                "\"{table_name}\" is not a table or cannot be altered"
            )))?,
        }

        session.check_privilege_for_drop_alter(schema_name, &**table)?;

        table.clone()
    };

    let source = if let Some(associated_source_id) = table.associated_source_id() {
        let (source, schema_name) =
            reader.get_source_by_name(db_name, schema_path, &real_table_name)?;

        assert_eq!(
            source.id,
            associated_source_id.table_id(),
            "associated source id mismatches"
        );

        session.check_privilege_for_drop_alter(schema_name, &**source)?;

        Some(source.clone())
    } else {
        None
    };

    Ok((table, source))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, ROWID_PREFIX};
    use risingwave_common::types::DataType;

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_add_column_handler() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        let sql = "create table t (i int, r real);";
        frontend.run_sql(sql).await.unwrap();

        let get_table = || {
            let catalog_reader = session.env().catalog_reader().read_guard();
            catalog_reader
                .get_created_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
                .unwrap()
                .0
                .clone()
        };

        let table = get_table();

        let columns: HashMap<_, _> = table
            .columns
            .iter()
            .map(|col| (col.name(), (col.data_type().clone(), col.column_id())))
            .collect();

        // Alter the table.
        let sql = "alter table t add column s text;";
        frontend.run_sql(sql).await.unwrap();

        let altered_table = get_table();

        let altered_columns: HashMap<_, _> = altered_table
            .columns
            .iter()
            .map(|col| (col.name(), (col.data_type().clone(), col.column_id())))
            .collect();

        // Check the new column.
        assert_eq!(columns.len() + 1, altered_columns.len());
        assert_eq!(altered_columns["s"].0, DataType::Varchar);

        // Check the old columns and IDs are not changed.
        assert_eq!(columns["i"], altered_columns["i"]);
        assert_eq!(columns["r"], altered_columns["r"]);
        assert_eq!(columns[ROWID_PREFIX], altered_columns[ROWID_PREFIX]);

        // Check the version is updated.
        assert_eq!(
            table.version.as_ref().unwrap().version_id + 1,
            altered_table.version.as_ref().unwrap().version_id
        );
        assert_eq!(
            table.version.as_ref().unwrap().next_column_id.next(),
            altered_table.version.as_ref().unwrap().next_column_id
        );
    }
}
