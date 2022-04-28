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
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::{AstOption, DropMode, ObjectName};

use crate::binder::Binder;
use crate::catalog::CatalogError;
use crate::session::OptimizerContext;

pub async fn handle_drop_database(
    context: OptimizerContext,
    database_name: ObjectName,
    if_exist: bool,
    mode: AstOption<DropMode>,
) -> Result<PgResponse> {
    let session = context.session_ctx;
    let catalog_reader = session.env().catalog_reader();
    let database_name = Binder::resolve_database_name(database_name)?;

    let database = {
        let reader = catalog_reader.read_guard();
        match reader.get_database_by_name(&database_name) {
            Ok(db) => db.clone(),
            Err(err) => {
                // If `if_exist` is true, not return error.
                return if if_exist {
                    Ok(PgResponse::empty_result(StatementType::DROP_DATABASE))
                } else {
                    Err(err)
                };
            }
        }
    };
    let database_id = {
        // If the mode is `Restrict` or `None`, the `database` need to be empty.
        if AstOption::Some(DropMode::Restrict) == mode || AstOption::None == mode {
            if !database.is_empty() {
                return Err(CatalogError::NotEmpty("database", database_name).into());
            }
            database.id()
        } else {
            todo!();
        }
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.drop_database(database_id).await?;
    Ok(PgResponse::empty_result(StatementType::DROP_DATABASE))
}

#[cfg(test)]
mod tests {
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_drop_database() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader();

        frontend.run_sql("CREATE DATABASE database").await.unwrap();

        frontend
            .run_sql("CREATE SCHEMA database.schema")
            .await
            .unwrap();

        assert!(frontend.run_sql("DROP DATABASE database").await.is_err());

        frontend
            .run_sql("DROP SCHEMA database.schema")
            .await
            .unwrap();

        frontend.run_sql("DROP DATABASE database").await.unwrap();

        let database = catalog_reader
            .read_guard()
            .get_database_by_name("database")
            .ok()
            .cloned();
        assert!(database.is_none());
    }
}
