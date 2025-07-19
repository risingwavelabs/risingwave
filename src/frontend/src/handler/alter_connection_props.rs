// Copyright 2025 RisingWave Labs
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

use super::RwPgResponse;
use crate::catalog::root_catalog::SchemaPath;
use crate::error::{ErrorCode, Result};
use crate::handler::{HandlerArgs, ObjectName, SqlOption, StatementType};
use crate::utils::resolve_connection_ref_and_secret_ref;
use crate::{Binder, WithOptions};

pub async fn handle_alter_connection_connector_props(
    handler_args: HandlerArgs,
    connection_name: ObjectName,
    alter_props: Vec<SqlOption>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, real_connection_name) =
        Binder::resolve_schema_qualified_name(db_name, connection_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let connection_id = {
        let reader = session.env().catalog_reader().read_guard();
        let (connection, schema_name) =
            reader.get_connection_by_name(db_name, schema_path, &real_connection_name)?;

        session.check_privilege_for_drop_alter(schema_name, &**connection)?;

        tracing::info!(
            "handle_alter_connection_connector_props: connection_name: {}, connection_id: {}",
            real_connection_name,
            connection.id
        );

        connection.id
    };

    let meta_client = session.env().meta_client();
    let (resolved_with_options, _, connector_conn_ref) = resolve_connection_ref_and_secret_ref(
        WithOptions::try_from(alter_props.as_ref() as &[SqlOption])?,
        &session,
        None,
    )?;
    let (changed_props, changed_secret_refs) = resolved_with_options.into_parts();

    if connector_conn_ref.is_some() {
        return Err(ErrorCode::InvalidInputSyntax(
            "ALTER CONNECTION CONNECTOR does not support nested CONNECTION references".to_owned(),
        )
        .into());
    }

    meta_client
        .alter_connection_connector_props(connection_id, changed_props, changed_secret_refs)
        .await?;

    Ok(RwPgResponse::empty_result(StatementType::ALTER_CONNECTION))
}
