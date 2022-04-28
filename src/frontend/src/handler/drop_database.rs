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
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{AstOption, DropMode, Ident};

use crate::session::OptimizerContext;

pub async fn handle_drop_database(
    context: OptimizerContext,
    database_name: Ident,
    if_exist: bool,
    mode: AstOption<DropMode>,
) -> Result<PgResponse> {
    let session = context.session_ctx;
    let catalog_reader = session.env().catalog_reader();

    let database = {
        let reader = catalog_reader.read_guard();
        match reader.get_database_by_name(&database_name.value) {
            Ok(db) => db.clone(),
            Err(err) => {
                return if if_exist {
                    Ok(PgResponse::empty_result(StatementType::DROP_DATABASE))
                } else {
                    Err(err)
                }
            }
        }
    };
    let database_id = {
        if AstOption::Some(DropMode::Restrict) == mode || AstOption::None == mode {
            if !database.is_empty() {
                return Err(ErrorCode::InternalError(
                    "Please drop schemas in this database before drop it".to_string(),
                )
                .into());
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

        frontend.run_sql("CREATE DATABASE t1").await.unwrap();

        frontend.run_sql("CREATE SCHEMA t1.s1").await.unwrap();

        frontend.run_sql("DROP DATABASE t1").await.unwrap();

        assert_eq!(1, 2);
    }
}
