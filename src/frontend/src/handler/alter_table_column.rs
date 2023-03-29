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

use anyhow::Context;
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::catalog::Table;
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_pb::stream_plan::StreamFragmentGraph;
use risingwave_sqlparser::ast::{AlterTableOperation, ColumnOption, ObjectName, Statement};
use risingwave_sqlparser::parser::Parser;

use super::create_table::{gen_create_table_plan, ColumnIdGenerator};
use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::{build_graph, Binder, OptimizerContext, TableCatalog};

/// Handle `ALTER TABLE [ADD|DROP] COLUMN` statements. The `operation` must be either `AddColumn` or
/// `DropColumn`.
pub async fn handle_alter_table_column(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    operation: AlterTableOperation,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, real_table_name) =
        Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let original_catalog = {
        let reader = session.env().catalog_reader().read_guard();
        let (table, schema_name) =
            reader.get_table_by_name(db_name, schema_path, &real_table_name)?;

        match table.table_type() {
            // Do not allow altering a table with a connector. It should be done passively according
            // to the messages from the connector.
            TableType::Table if table.has_associated_source() => {
                Err(ErrorCode::InvalidInputSyntax(format!(
                    "cannot alter table \"{table_name}\" because it has a connector"
                )))?
            }
            TableType::Table => {}

            _ => Err(ErrorCode::InvalidInputSyntax(format!(
                "\"{table_name}\" is not a table or cannot be altered"
            )))?,
        }

        session.check_privilege_for_drop_alter(schema_name, &**table)?;

        table.clone()
    };

    // TODO(yuhao): alter table with generated columns.
    if original_catalog.has_generated_column() {
        return Err(RwError::from(ErrorCode::BindError(
            "Alter a table with generated column has not been implemented.".to_string(),
        )));
    }

    // Retrieve the original table definition and parse it to AST.
    let [mut definition]: [_; 1] = Parser::parse_sql(&original_catalog.definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();
    let Statement::CreateTable { columns, .. } = &mut definition else {
        panic!("unexpected statement: {:?}", definition);
    };

    match operation {
        AlterTableOperation::AddColumn {
            column_def: new_column,
        } => {
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
                Err(ErrorCode::InvalidInputSyntax(format!(
                    "alter table add generated columns is not supported"
                )))?
            }

            // Add the new column to the table definition.
            columns.push(new_column);
        }

        AlterTableOperation::DropColumn {
            column_name,
            if_exists,
            cascade,
        } => {
            if cascade {
                Err(ErrorCode::NotImplemented(
                    "drop column cascade".to_owned(),
                    6903.into(),
                ))?
            }

            // Locate the column by name and remove it.
            let column_name = column_name.real_value();
            let removed_column = columns
                .drain_filter(|c| c.name.real_value() == column_name)
                .at_most_one()
                .ok()
                .unwrap();

            if removed_column.is_some() {
                // PASS
            } else if if_exists {
                return Ok(PgResponse::empty_result_with_notice(
                    StatementType::ALTER_TABLE,
                    format!("column \"{}\" does not exist, skipping", column_name),
                ));
            } else {
                Err(ErrorCode::InvalidInputSyntax(format!(
                    "column \"{}\" of table \"{}\" does not exist",
                    column_name, table_name
                )))?
            }
        }

        _ => unreachable!(),
    }

    // Create handler args as if we're creating a new table with the altered definition.
    let handler_args = HandlerArgs::new(session.clone(), &definition, "")?;
    let col_id_gen = ColumnIdGenerator::new_alter(&original_catalog);
    let Statement::CreateTable { columns, constraints, source_watermarks, append_only, .. } = definition else {
        panic!("unexpected statement type: {:?}", definition);
    };

    let (graph, table) = {
        let context = OptimizerContext::from_handler_args(handler_args);
        let (plan, source, table) = gen_create_table_plan(
            context,
            table_name,
            columns,
            constraints,
            col_id_gen,
            source_watermarks,
            append_only,
        )?;

        // We should already have rejected the case where the table has a connector.
        assert!(source.is_none());

        // TODO: avoid this backward conversion.
        if TableCatalog::from(&table).pk_column_ids() != original_catalog.pk_column_ids() {
            Err(ErrorCode::InvalidInputSyntax(
                "alter primary key of table is not supported".to_owned(),
            ))?
        }

        let graph = StreamFragmentGraph {
            parallelism: session
                .config()
                .get_streaming_parallelism()
                .map(|parallelism| Parallelism { parallelism }),
            ..build_graph(plan)
        };

        // Fill the original table ID.
        let table = Table {
            id: original_catalog.id().table_id(),
            ..table
        };

        (graph, table)
    };

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
    );

    let catalog_writer = session.env().catalog_writer();

    catalog_writer
        .replace_table(table, graph, col_index_mapping)
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_TABLE))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::{
        row_id_column_name, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME,
    };
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
                .get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
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
        assert_eq!(
            columns[row_id_column_name().as_str()],
            altered_columns[row_id_column_name().as_str()]
        );

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
