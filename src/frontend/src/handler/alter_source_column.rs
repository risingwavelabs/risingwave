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
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_connector::source::{SourceEncode, SourceStruct};
use risingwave_source::source_desc::extract_source_struct;
use risingwave_sqlparser::ast::{AlterSourceOperation, ObjectName};

use super::create_table::bind_sql_columns;
use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::Binder;

// Note for future drop column:
// 1. Dependencies of generated columns

// TODO:
// 1. change the definition of source

/// Handle `ALTER TABLE [ADD] COLUMN` statements.
pub async fn handle_alter_source_column(
    handler_args: HandlerArgs,
    source_name: ObjectName,
    operation: AlterSourceOperation,
) -> Result<RwPgResponse> {
    // Get original definition
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, real_source_name) =
        Binder::resolve_schema_qualified_name(db_name, source_name.clone())?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let original_catalog = {
        let reader = session.env().catalog_reader().read_guard();
        let (source, schema_name) =
            reader.get_source_by_name(db_name, schema_path, &real_source_name)?;

        session.check_privilege_for_drop_alter(schema_name, &**source)?;

        source.clone()
    };

    // Currently only allow source without schema registry
    let SourceStruct { encode, .. } = extract_source_struct(&original_catalog.info)?;
    match encode {
        SourceEncode::Avro | SourceEncode::Protobuf => {
            return Err(RwError::from(ErrorCode::NotImplemented(
                "Alter source with schema registry".into(),
                None.into(),
            )));
        }
        SourceEncode::Invalid | SourceEncode::Native => {
            return Err(RwError::from(ErrorCode::NotSupported(
                format!("Alter source with encode {:?}", encode),
                "Alter source with encode JSON | BYTES | CSV".into(),
            )));
        }
        _ => {}
    }

    let columns = &original_catalog.columns;
    let diff_column = match operation {
        AlterSourceOperation::AddColumn { column_def } => {
            let new_column_name = column_def.name.real_value();
            if columns
                .iter()
                .any(|c| c.column_desc.name.to_lowercase() == new_column_name)
            {
                Err(ErrorCode::InvalidInputSyntax(format!(
                    "column \"{new_column_name}\" of table \"{source_name}\" already exists"
                )))?
            }
            let mut bound_column = bind_sql_columns(&[column_def])?.remove(0);
            bound_column.column_desc.column_id = columns
                .iter()
                .fold(ColumnId::new(i32::MIN), |a, b| a.max(b.column_id()))
                .next();
            bound_column
        }
        _ => unreachable!(),
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_source_column(original_catalog.id, diff_column.to_protobuf())
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_SOURCE))
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use risingwave_common::types::DataType;

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_alter_source_column_handler() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        let sql = r#"create source s (v1 int) with (
            connector = 'kafka',
            topic = 'abc',
            properties.bootstrap.server = 'localhost:29092',
          ) FORMAT PLAIN ENCODE JSON;"#;

        frontend.run_sql(sql).await.unwrap();

        let get_source = || {
            let catalog_reader = session.env().catalog_reader().read_guard();
            catalog_reader
                .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "s")
                .unwrap()
                .0
                .clone()
        };

        let source = get_source();
        let columns: HashMap<_, _> = source
            .columns
            .iter()
            .map(|col| (col.name(), (col.data_type().clone(), col.column_id())))
            .collect();

        let sql = "alter source s add column v2 varchar;";
        frontend.run_sql(sql).await.unwrap();

        let altered_source = get_source();

        let altered_columns: HashMap<_, _> = altered_source
            .columns
            .iter()
            .map(|col| (col.name(), (col.data_type().clone(), col.column_id())))
            .collect();

        // Check the new column.
        assert_eq!(columns.len() + 1, altered_columns.len());
        assert_eq!(altered_columns["v2"].0, DataType::Varchar);

        // Check the old columns and IDs are not changed.
        assert_eq!(columns["v1"], altered_columns["v1"]);
    }
}
