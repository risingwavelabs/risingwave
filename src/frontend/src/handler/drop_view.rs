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
use risingwave_common::error::ErrorCode::PermissionDenied;
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::ObjectName;

use super::privilege::check_super_user;
use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::session::OptimizerContext;

pub async fn handle_drop_view(
    context: OptimizerContext,
    table_name: ObjectName,
    if_exists: bool,
) -> Result<RwPgResponse> {
    let session = context.session_ctx;
    let db_name = session.database();
    let (schema_name, table_name) = Binder::resolve_schema_qualified_name(db_name, table_name)?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let view_id = {
        let reader = session.env().catalog_reader().read_guard();
        let (view, schema_name) =
            match reader.get_view_by_name(session.database(), schema_path, &table_name) {
                Ok((t, s)) => (t, s),
                Err(e) => {
                    return if if_exists {
                        Ok(RwPgResponse::empty_result_with_notice(
                            StatementType::DROP_MATERIALIZED_VIEW,
                            format!("view \"{}\" does not exist, skipping", table_name),
                        ))
                    } else {
                        Err(e)
                    }
                }
            };

        let schema_catalog = reader
            .get_schema_by_name(session.database(), schema_name)
            .unwrap();
        let schema_owner = schema_catalog.owner();
        if session.user_id() != view.owner
            && session.user_id() != schema_owner
            && !check_super_user(&session)
        {
            return Err(PermissionDenied("Do not have the privilege".to_string()).into());
        }

        view.id
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.drop_view(view_id).await?;

    Ok(PgResponse::empty_result(StatementType::DROP_VIEW))
}
