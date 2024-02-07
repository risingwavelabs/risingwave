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

use anyhow::Context;
use pgwire::pg_response::StatementType;
use risingwave_common::bail_not_implemented;
use risingwave_common::types::DataType;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_sqlparser::ast::{ConnectorSchema, ObjectName, Statement};
use risingwave_sqlparser::parser::Parser;

use super::alter_source_with_sr::alter_definition_format_encode;
use super::alter_table_column::{fetch_table_catalog_for_alter, schema_has_schema_registry};
use super::{HandlerArgs, RwPgResponse};
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::alter_source_with_sr::{
    check_format_encode, fetch_source_catalog_with_db_schema_id,
};
use crate::handler::create_table::{generate_stream_graph_for_table, ColumnIdGenerator};

pub async fn handle_alter_table_with_sr(
    handler_args: HandlerArgs,
    name: ObjectName,
    connector_schema: ConnectorSchema,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let original_table = fetch_table_catalog_for_alter(session.as_ref(), &name)?;
    let (original_source, _, _) = fetch_source_catalog_with_db_schema_id(session.as_ref(), &name)?;

    if !original_table.incoming_sinks.is_empty() {
        bail_not_implemented!("alter table with incoming sinks");
    }

    // TODO(yuhao): alter table with generated columns.
    if original_table.has_generated_column() {
        return Err(RwError::from(ErrorCode::BindError(
            "Alter a table with generated column has not been implemented.".to_string(),
        )));
    }

    check_format_encode(&original_source, &connector_schema)?;

    if !schema_has_schema_registry(&connector_schema) {
        return Err(ErrorCode::NotSupported(
            "altering a table without schema registry".to_string(),
            "try `ALTER TABLE .. ADD COLUMN ...` instead".to_string(),
        )
        .into());
    }

    let definition = alter_definition_format_encode(
        &original_table.definition,
        connector_schema.row_options.clone(),
    )?;

    let [definition]: [_; 1] = Parser::parse_sql(&definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();

    // Create handler args as if we're creating a new table with the altered definition.
    let handler_args = HandlerArgs::new(session.clone(), &definition, Arc::from(""))?;
    let col_id_gen = ColumnIdGenerator::new_alter(&original_table);
    let Statement::CreateTable {
        columns,
        constraints,
        source_watermarks,
        append_only,
        wildcard_idx,
        ..
    } = definition
    else {
        panic!("unexpected statement type: {:?}", definition);
    };

    let (graph, table, source) = generate_stream_graph_for_table(
        &session,
        name,
        &original_table,
        Some(connector_schema),
        handler_args,
        col_id_gen,
        columns,
        wildcard_idx,
        constraints,
        source_watermarks,
        append_only,
    )
    .await?;

    // Calculate the mapping from the original columns to the new columns.
    let col_index_mapping = ColIndexMapping::new(
        original_table
            .columns()
            .iter()
            .map(|old_c| {
                table.columns.iter().position(|new_c| {
                    new_c.get_column_desc().unwrap().column_id == old_c.column_id().get_id()
                        && DataType::from(
                            new_c
                                .get_column_desc()
                                .unwrap()
                                .column_type
                                .as_ref()
                                .unwrap(),
                        ) == *old_c.data_type()
                })
            })
            .collect(),
        table.columns.len(),
    );

    let catalog_writer = session.catalog_writer()?;

    catalog_writer
        .replace_table(source, table, graph, col_index_mapping)
        .await?;

    Ok(RwPgResponse::empty_result(StatementType::ALTER_TABLE))
}

#[cfg(test)]
pub mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_alter_table_with_sr_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE TABLE t
            WITH (
                connector = 'kafka',
                topic = 'test-topic',
                properties.bootstrap.server = 'localhost:29092'
            )
            FORMAT PLAIN ENCODE PROTOBUF (
                message = '.test.TestRecord',
                schema.location = 'file://{}'
            )"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        frontend.run_sql(sql).await.unwrap();

        let sql = format!(
            r#"ALTER TABLE t FORMAT UPSERT ENCODE PROTOBUF (
                message = '.test.TestRecord',
                schema.location = 'file://{}'
            )"#,
            proto_file.path().to_str().unwrap()
        );
        assert!(frontend
            .run_sql(sql)
            .await
            .unwrap_err()
            .to_string()
            .contains("the original definition is FORMAT Plain ENCODE Protobuf"));

        let sql = format!(
            r#"ALTER TABLE t FORMAT PLAIN ENCODE PROTOBUF (
                message = '.test.TestRecordExt',
                schema.location = 'file://{}'
            )"#,
            proto_file.path().to_str().unwrap()
        );
        frontend.run_sql(sql).await.unwrap();

        let altered_table = session
            .env()
            .catalog_reader()
            .read_guard()
            .get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
            .unwrap()
            .0
            .clone();

        let altered_sql = format!(
            r#"CREATE TABLE t () WITH (connector = 'kafka', topic = 'test-topic', properties.bootstrap.server = 'localhost:29092') FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecordExt', schema.location = 'file://{}')"#,
            proto_file.path().to_str().unwrap()
        );
        assert_eq!(altered_sql, altered_table.definition);
    }
}
