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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::DEFAULT_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::ObjectName;

use crate::binder::Binder;
use crate::catalog::CatalogError;
use crate::session::OptimizerContext;

pub async fn handle_create_database(
    context: OptimizerContext,
    database_name: ObjectName,
    if_not_exist: bool,
) -> Result<PgResponse> {
    let session = context.session_ctx;
    let database_name = Binder::resolve_database_name(database_name)?;

    {
        let catalog_reader = session.env().catalog_reader();
        let reader = catalog_reader.read_guard();
        if reader.get_database_by_name(&database_name).is_ok() {
            // If `if_not_exist` is true, not return error.
            return if if_not_exist {
                Ok(PgResponse::empty_result_with_notice(
                    StatementType::CREATE_DATABASE,
                    format!("database {} exists, skipping", database_name),
                ))
            } else {
                Err(CatalogError::Duplicated("database", database_name).into())
            };
        }
    }

    let catalog_writer = session.env().catalog_writer();
    let owner: String = session.user_name().to_string();
    catalog_writer.create_database(&database_name, owner).await?;

    // Default create dev schema.
    let db_id = {
        let catalog_reader = session.env().catalog_reader();
        let reader = catalog_reader.read_guard();
        reader.get_database_by_name(&database_name)?.id()
    };
    catalog_writer
        .create_schema(db_id, DEFAULT_SCHEMA_NAME)
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_DATABASE))
}

#[cfg(test)]
mod tests {
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_create_database() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader();

        frontend.run_sql("CREATE DATABASE database").await.unwrap();

        let database = catalog_reader
            .read_guard()
            .get_database_by_name("database")
            .ok()
            .cloned();
        assert!(database.is_some());
    }
}
