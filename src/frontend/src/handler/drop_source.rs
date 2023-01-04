// Copyright 2023 Singularity Data
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
use risingwave_common::error::ErrorCode::{self, PermissionDenied};
use risingwave_common::error::{Result, RwError};
use risingwave_sqlparser::ast::ObjectName;

use super::privilege::check_super_user;
use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::handler::HandlerArgs;

pub async fn handle_drop_source(
    handler_args: HandlerArgs,
    name: ObjectName,
    if_exists: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, source_name) = Binder::resolve_schema_qualified_name(db_name, name)?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let (source_id, table_id) = {
        let catalog_reader = session.env().catalog_reader().read_guard();

        // TODO(Yuanxin): This should be removed after unsupporting `CREATE MATERIALIZED SOURCE`.
        let table_id = if let Ok((table, _)) =
            catalog_reader.get_table_by_name(db_name, schema_path, &source_name)
        {
            if table.is_table() && table.associated_source_id().is_none() {
                return Err(RwError::from(ErrorCode::InvalidInputSyntax(
                    "Use `DROP TABLE` to drop a table.".to_owned(),
                )));
            }
            Some(table.id)
        } else {
            None
        };

        let (source, schema_name) =
            match catalog_reader.get_source_by_name(db_name, schema_path, &source_name) {
                Ok((s, schema)) => (s.clone(), schema),
                Err(e) => {
                    return if if_exists {
                        Ok(RwPgResponse::empty_result_with_notice(
                            StatementType::DROP_SOURCE,
                            format!("source \"{}\" does not exist, skipping", source_name),
                        ))
                    } else {
                        Err(e.into())
                    }
                }
            };

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

        (source.id, table_id)
    };

    let catalog_writer = session.env().catalog_writer();
    if let Some(table_id) = table_id {
        // Dropping a materialized source.
        // TODO(Yuanxin): This should be removed after unsupporting `CREATE MATERIALIZED SOURCE`.
        catalog_writer.drop_table(Some(source_id), table_id).await?;
    } else {
        catalog_writer.drop_source(source_id).await?;
    }

    Ok(PgResponse::empty_result(StatementType::DROP_SOURCE))
}
