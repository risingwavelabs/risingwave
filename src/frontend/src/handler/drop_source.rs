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

use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::ErrorCode::PermissionDenied;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_sqlparser::ast::ObjectName;

use super::privilege::check_super_user;
use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::session::OptimizerContext;

pub async fn handle_drop_source(
    context: OptimizerContext,
    name: ObjectName,
) -> Result<RwPgResponse> {
    let session = context.session_ctx;
    let db_name = session.database();
    let (schema_name, source_name) = Binder::resolve_table_or_source_name(db_name, name)?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = match schema_name.as_deref() {
        Some(schema_name) => SchemaPath::Name(schema_name),
        None => SchemaPath::Path(&search_path, user_name),
    };

    let (source_id, table_id, index_ids) = {
        let catalog_reader = session.env().catalog_reader().read_guard();
        let (source, schema_name) =
            catalog_reader.get_source_by_name(db_name, schema_path, &source_name)?;

        let schema_catalog = catalog_reader
            .get_schema_by_name(db_name, schema_name)
            .unwrap();
        let schema_owner = schema_catalog.owner();
        if session.user_id() != source.owner
            && session.user_id() != schema_owner
            && !check_super_user(&session)
        {
            return Err(PermissionDenied("Do not have the privilege".to_string()).into());
        }

        if source.is_table() {
            return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                "Use `DROP TABLE` to drop a table.".to_owned(),
            )));
        }

        let table_id = catalog_reader
            .get_table_by_name(db_name, SchemaPath::Name(schema_name), &source_name)
            .map(|(table, _)| table.id())
            .ok();

        let index_ids = table_id.map(|table_id| {
            schema_catalog
                .get_indexes_by_table_id(&table_id)
                .iter()
                .map(|index| index.id)
                .collect_vec()
        });

        (source.id, table_id, index_ids)
    };

    let catalog_writer = session.env().catalog_writer();
    if let Some(table_id) = table_id {
        // Dropping a materialized source.
        catalog_writer
            .drop_materialized_source(source_id, table_id, index_ids.unwrap())
            .await?;
    } else {
        catalog_writer.drop_source(source_id).await?;
    }

    Ok(PgResponse::empty_result(StatementType::DROP_SOURCE))
}

#[cfg(test)]
mod tests {
    use crate::test_utils::LocalFrontend;

    async fn test_drop_source(materialized: bool, error_str: &str) {
        let frontend = LocalFrontend::new(Default::default()).await;

        let materialized = if materialized { "MATERIALIZED " } else { "" };
        let sql = format!("CREATE {}SOURCE s ROW FORMAT JSON", materialized);
        frontend.run_sql(sql).await.unwrap();

        assert_eq!(
            error_str,
            frontend
                .run_sql("DROP TABLE s")
                .await
                .unwrap_err()
                .to_string()
        );

        assert_eq!(
            error_str,
            frontend
                .run_sql("DROP MATERIALIZED VIEW s")
                .await
                .unwrap_err()
                .to_string()
        );

        frontend.run_sql("DROP SOURCE s").await.unwrap();
    }

    #[tokio::test]
    async fn test_drop_normal_source() {
        test_drop_source(false, "Catalog error: table not found: s").await;
    }

    #[tokio::test]
    async fn test_drop_materialized_source() {
        test_drop_source(
            true,
            "Invalid input syntax: Use `DROP SOURCE` to drop a source.",
        )
        .await;
    }

    #[tokio::test]
    async fn test_drop_table_using_drop_source() {
        let frontend = LocalFrontend::new(Default::default()).await;

        frontend.run_sql("CREATE TABLE s").await.unwrap();

        assert_eq!(
            "Invalid input syntax: Use `DROP TABLE` to drop a table.".to_string(),
            frontend
                .run_sql("DROP SOURCE s")
                .await
                .unwrap_err()
                .to_string()
        );
    }
}
