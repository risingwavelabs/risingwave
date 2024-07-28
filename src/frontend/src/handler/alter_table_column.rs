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

use std::sync::Arc;

use anyhow::{anyhow, Context};
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::types::DataType;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::{bail, bail_not_implemented};
use risingwave_pb::catalog::{Source, Table};
use risingwave_pb::ddl_service::TableJobType;
use risingwave_pb::stream_plan::StreamFragmentGraph;
use risingwave_sqlparser::ast::{
    AlterTableOperation, ColumnDef, ColumnOption, ConnectorSchema, DataType as AstDataType, Encode,
    ObjectName, Statement, StructField,
};
use risingwave_sqlparser::parser::Parser;

use super::create_source::get_json_schema_location;
use super::create_table::{generate_stream_graph_for_table, ColumnIdGenerator};
use super::util::SourceSchemaCompatExt;
use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::ExprImpl;
use crate::session::SessionImpl;
use crate::{Binder, TableCatalog, WithOptions};

pub async fn replace_table_with_definition(
    session: &Arc<SessionImpl>,
    table_name: ObjectName,
    definition: Statement,
    original_catalog: &Arc<TableCatalog>,
    source_schema: Option<ConnectorSchema>,
) -> Result<()> {
    let (source, table, graph, col_index_mapping, job_type) = get_replace_table_plan(
        session,
        table_name,
        definition,
        original_catalog,
        source_schema,
    )
    .await?;

    let catalog_writer = session.catalog_writer()?;

    catalog_writer
        .replace_table(source, table, graph, col_index_mapping, job_type)
        .await?;
    Ok(())
}

pub async fn get_new_table_definition_for_cdc_table(
    session: &Arc<SessionImpl>,
    table_name: ObjectName,
    new_columns: Vec<ColumnCatalog>,
) -> Result<(Statement, Arc<TableCatalog>)> {
    let original_catalog = fetch_table_catalog_for_alter(session.as_ref(), &table_name)?;

    // Retrieve the original table definition and parse it to AST.
    let [mut definition]: [_; 1] = Parser::parse_sql(&original_catalog.definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();
    let Statement::CreateTable {
        columns: original_columns,
        source_schema,
        ..
    } = &mut definition
    else {
        panic!("unexpected statement: {:?}", definition);
    };

    assert!(
        source_schema.is_none(),
        "source schema should be None for CDC table"
    );
    if original_columns.is_empty() {
        Err(ErrorCode::NotSupported(
            "alter a table with empty column definitions".to_string(),
            "Please recreate the table with column definitions.".to_string(),
        ))?
    }

    // since the DDL is committed on upstream, so we can safely replace the original columns with new columns
    let mut new_column_defs = vec![];
    for col in new_columns.into_iter() {
        let ty = to_ast_data_type(col.data_type())?;
        new_column_defs.push(ColumnDef::new(col.name().into(), ty, None, vec![]));
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
        _ => Err(anyhow!("unsupported data type: {:?}", ty).context("to_ast_data_type"))?,
    }
}

pub async fn get_replace_table_plan(
    session: &Arc<SessionImpl>,
    table_name: ObjectName,
    definition: Statement,
    original_catalog: &Arc<TableCatalog>,
    source_schema: Option<ConnectorSchema>,
) -> Result<(
    Option<Source>,
    Table,
    StreamFragmentGraph,
    ColIndexMapping,
    TableJobType,
)> {
    // Create handler args as if we're creating a new table with the altered definition.
    let handler_args = HandlerArgs::new(session.clone(), &definition, Arc::from(""))?;
    let col_id_gen = ColumnIdGenerator::new_alter(original_catalog);
    let Statement::CreateTable {
        columns,
        constraints,
        source_watermarks,
        append_only,
        on_conflict,
        with_version_column,
        wildcard_idx,
        cdc_table_info,
        ..
    } = definition
    else {
        panic!("unexpected statement type: {:?}", definition);
    };

    let (graph, table, source, job_type) = generate_stream_graph_for_table(
        session,
        table_name,
        original_catalog,
        source_schema,
        handler_args,
        col_id_gen,
        columns,
        wildcard_idx,
        constraints,
        source_watermarks,
        append_only,
        on_conflict,
        with_version_column,
        cdc_table_info,
    )
    .await?;

    // Calculate the mapping from the original columns to the new columns.
    let col_index_mapping = ColIndexMapping::new(
        original_catalog
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

    Ok((source, table, graph, col_index_mapping, job_type))
}

/// Handle `ALTER TABLE [ADD|DROP] COLUMN` statements. The `operation` must be either `AddColumn` or
/// `DropColumn`.
pub async fn handle_alter_table_column(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    operation: AlterTableOperation,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let original_catalog = fetch_table_catalog_for_alter(session.as_ref(), &table_name)?;

    if !original_catalog.incoming_sinks.is_empty() {
        bail_not_implemented!("alter table with incoming sinks");
    }

    // Retrieve the original table definition and parse it to AST.
    let [mut definition]: [_; 1] = Parser::parse_sql(&original_catalog.definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();
    let Statement::CreateTable {
        columns,
        source_schema,
        ..
    } = &mut definition
    else {
        panic!("unexpected statement: {:?}", definition);
    };
    let source_schema = source_schema
        .clone()
        .map(|source_schema| source_schema.into_v2_with_warning());

    let fail_if_has_schema_registry = || {
        if let Some(source_schema) = &source_schema
            && schema_has_schema_registry(source_schema)
        {
            Err(ErrorCode::NotSupported(
                "alter table with schema registry".to_string(),
                "try `ALTER TABLE .. FORMAT .. ENCODE .. (...)` instead".to_string(),
            ))
        } else {
            Ok(())
        }
    };

    if columns.is_empty() {
        Err(ErrorCode::NotSupported(
            "alter a table with empty column definitions".to_string(),
            "Please recreate the table with column definitions.".to_string(),
        ))?
    }

    match operation {
        AlterTableOperation::AddColumn {
            column_def: new_column,
        } => {
            fail_if_has_schema_registry()?;

            // Duplicated names can actually be checked by `StreamMaterialize`. We do here for
            // better error reporting.
            let new_column_name = new_column.name.real_value();
            if columns
                .iter()
                .any(|c| c.name.real_value() == new_column_name)
            {
                Err(ErrorCode::InvalidInputSyntax(format!(
                    "column \"{new_column_name}\" of table \"{table_name}\" already exists"
                )))?
            }

            if new_column
                .options
                .iter()
                .any(|x| matches!(x.option, ColumnOption::GeneratedColumns(_)))
            {
                Err(ErrorCode::InvalidInputSyntax(
                    "alter table add generated columns is not supported".to_string(),
                ))?
            }

            // Add the new column to the table definition if it is not created by `create table (*)` syntax.
            columns.push(new_column);
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
            for column in original_catalog.columns() {
                if column_name.real_value() == column.name() && !column.is_generated() {
                    fail_if_has_schema_registry()?;
                }

                if let Some(expr) = column.generated_expr() {
                    let expr = ExprImpl::from_expr_proto(expr)?;
                    let refs = expr.collect_input_refs(original_catalog.columns().len());
                    for idx in refs.ones() {
                        let refed_column = &original_catalog.columns()[idx];
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
            let removed_column = columns
                .extract_if(|c| c.name.real_value() == column_name)
                .at_most_one()
                .ok()
                .unwrap();

            if removed_column.is_some() {
                // PASS
            } else if if_exists {
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
        }

        _ => unreachable!(),
    };

    replace_table_with_definition(
        &session,
        table_name,
        definition,
        &original_catalog,
        source_schema,
    )
    .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_TABLE))
}

pub fn schema_has_schema_registry(schema: &ConnectorSchema) -> bool {
    match schema.row_encode {
        Encode::Avro | Encode::Protobuf => true,
        Encode::Json => {
            let mut options = WithOptions::try_from(schema.row_options()).unwrap();
            matches!(get_json_schema_location(options.inner_mut()), Ok(Some(_)))
        }
        _ => false,
    }
}

pub fn fetch_table_catalog_for_alter(
    session: &SessionImpl,
    table_name: &ObjectName,
) -> Result<Arc<TableCatalog>> {
    let db_name = session.database();
    let (schema_name, real_table_name) =
        Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let original_catalog = {
        let reader = session.env().catalog_reader().read_guard();
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

    Ok(original_catalog)
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
