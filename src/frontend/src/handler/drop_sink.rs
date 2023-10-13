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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::Result;
use risingwave_pb::ddl_service::ReplaceTablePlan;
use risingwave_sqlparser::ast::ObjectName;

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::handler::create_sink::regenerate_table;
use crate::handler::HandlerArgs;

pub async fn handle_drop_sink(
    handler_args: HandlerArgs,
    sink_name: ObjectName,
    if_exists: bool,
    cascade: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, sink_name) = Binder::resolve_schema_qualified_name(db_name, sink_name)?;
    let search_path = session.config().search_path();
    let user_name = &session.auth_context().user_name;
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let sink = {
        let catalog_reader = session.env().catalog_reader().read_guard();
        let (sink, schema_name) =
            match catalog_reader.get_sink_by_name(db_name, schema_path, &sink_name) {
                Ok((sink, schema)) => (sink.clone(), schema),
                Err(e) => {
                    return if if_exists {
                        Ok(RwPgResponse::builder(StatementType::DROP_SINK)
                            .notice(format!("sink \"{}\" does not exist, skipping", sink_name))
                            .into())
                    } else {
                        Err(e.into())
                    }
                }
            };

        session.check_privilege_for_drop_alter(schema_name, &*sink)?;

        sink
    };

    let sink_id = sink.id;

    let mut affected_table_change = None;
    if let Some(target_table_name) = &sink.sink_into_name {
        use anyhow::Context;
        use risingwave_common::error::{ErrorCode, RwError};
        use risingwave_common::util::column_index_mapping::ColIndexMapping;
        use risingwave_sqlparser::ast::Statement;
        use risingwave_sqlparser::parser::Parser;

        use super::create_table::ColumnIdGenerator;
        use crate::catalog::table_catalog::TableType;

        let table_name = ObjectName::from_test_str(target_table_name);

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
        let Statement::CreateTable { source_schema, .. } = &mut definition else {
            panic!("unexpected statement: {:?}", definition);
        };
        let source_schema = source_schema
            .clone()
            .map(|source_schema| source_schema.into_source_schema_v2().0);

        // Create handler args as if we're creating a new table with the altered definition.
        let handler_args = HandlerArgs::new(session.clone(), &definition, "")?;
        let col_id_gen = ColumnIdGenerator::new_alter(&original_catalog);
        let Statement::CreateTable {
            columns,
            constraints,
            source_watermarks,
            append_only,
            ..
        } = definition
        else {
            panic!("unexpected statement type: {:?}", definition);
        };

        let (graph, table, source, _) = regenerate_table(
            &session,
            table_name,
            &original_catalog,
            source_schema,
            handler_args,
            col_id_gen,
            columns,
            constraints,
            source_watermarks,
            append_only,
            0,
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

        affected_table_change = Some(ReplaceTablePlan {
            source,
            table: Some(table),
            fragment_graph: Some(graph),
            table_col_index_mapping: Some(col_index_mapping.to_protobuf()),
        });
    }

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .drop_sink(sink_id.sink_id, cascade, affected_table_change)
        .await?;

    Ok(PgResponse::empty_result(StatementType::DROP_SINK))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_drop_sink_handler() {
        let sql_create_table = "create table t (v1 smallint primary key);";
        let sql_create_mv = "create materialized view mv as select v1 from t;";
        let sql_create_sink = "create sink snk from mv with( connector = 'kafka')";
        let sql_drop_sink = "drop sink snk;";
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql_create_table).await.unwrap();
        frontend.run_sql(sql_create_mv).await.unwrap();
        frontend.run_sql(sql_create_sink).await.unwrap();
        frontend.run_sql(sql_drop_sink).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        let sink = catalog_reader.get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "snk");
        assert!(sink.is_err());
    }
}
