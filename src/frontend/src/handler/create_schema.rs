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
use risingwave_common::catalog::RESERVED_PG_SCHEMA_PREFIX;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::user::grant_privilege::{Action, Object};
use risingwave_sqlparser::ast::ObjectName;

use super::privilege::check_privileges;
use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::CatalogError;
use crate::handler::privilege::ObjectCheckItem;
use crate::session::OptimizerContext;

pub async fn handle_create_schema(
    context: OptimizerContext,
    schema_name: ObjectName,
    if_not_exist: bool,
) -> Result<RwPgResponse> {
    let session = context.session_ctx;
    let (database_name, schema_name) =
        Binder::resolve_schema_name(session.database(), schema_name)?;

    if schema_name.starts_with(RESERVED_PG_SCHEMA_PREFIX) {
        return Err(ErrorCode::ProtocolError(format!(
            "unacceptable schema name \"{}\", The prefix \"{}\" is reserved for system schemas",
            schema_name, RESERVED_PG_SCHEMA_PREFIX
        ))
        .into());
    }

    let (db_id, db_owner) = {
        let catalog_reader = session.env().catalog_reader();
        let reader = catalog_reader.read_guard();
        if reader
            .get_schema_by_name(&database_name, &schema_name)
            .is_ok()
        {
            // If `if_not_exist` is true, not return error.
            return if if_not_exist {
                Ok(PgResponse::empty_result_with_notice(
                    StatementType::CREATE_SCHEMA,
                    format!("schema {} exists, skipping", schema_name),
                ))
            } else {
                Err(CatalogError::Duplicated("schema", schema_name).into())
            };
        }
        let db = reader.get_database_by_name(&database_name)?;
        (db.id(), db.owner())
    };

    check_privileges(
        &session,
        &vec![ObjectCheckItem::new(
            db_owner,
            Action::Create,
            Object::DatabaseId(db_id),
        )],
    )?;

    let catalog_writer = session.env().catalog_writer();
    catalog_writer
        .create_schema(db_id, &schema_name, session.user_id())
        .await?;
    Ok(PgResponse::empty_result(StatementType::CREATE_SCHEMA))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::DEFAULT_DATABASE_NAME;

    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_create_schema() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader();

        frontend.run_sql("CREATE SCHEMA schema").await.unwrap();

        let schema = catalog_reader
            .read_guard()
            .get_schema_by_name(DEFAULT_DATABASE_NAME, "schema")
            .ok()
            .cloned();
        assert!(schema.is_some());
    }
}
