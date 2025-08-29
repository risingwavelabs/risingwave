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
use crate::catalog::catalog_service::CatalogReadGuard;
use crate::catalog::root_catalog::SchemaPath;
use crate::error::{ErrorCode, Result};
use crate::handler::{HandlerArgs, ObjectName, SqlOption, StatementType};
use crate::session::SessionImpl;
use crate::utils::resolve_connection_ref_and_secret_ref;
use crate::{Binder, WithOptions};

pub async fn handle_alter_table_connector_props(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    alter_props: Vec<SqlOption>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, real_table_name) =
        Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let source_id = {
        let reader = session.env().catalog_reader().read_guard();
        // let database_id = reader.get_database_by_name(db_name)?.id();
        let (table, schema_name) =
            reader.get_any_table_by_name(db_name, schema_path, &real_table_name)?;
        let Some(associate_source_id) = table.associated_source_id else {
            return Err(ErrorCode::InvalidInputSyntax(
                "Only table with connector can use ALTER TABLE CONNECTOR syntax.".to_owned(),
            )
            .into());
        };

        session.check_privilege_for_drop_alter(schema_name, &**table)?;
        let source_catalog =
            reader.get_source_by_id_2(&db_name, &schema_path, &associate_source_id.table_id)?;

        ensure_alter_props_not_set_by_connection(
            &reader,
            db_name,
            source_catalog.connection_id,
            &alter_props,
        )?;

        tracing::info!(
            "handle_alter_table_connector_props: table_name: {}, table id: {}, source_id: {}",
            real_table_name,
            table.id,
            associate_source_id.table_id
        );

        associate_source_id.table_id
    };

    handle_alter_source_props_inner(&session, alter_props, source_id).await?;

    Ok(RwPgResponse::empty_result(StatementType::ALTER_TABLE))
}

async fn handle_alter_source_props_inner(
    session: &SessionImpl,
    alter_props: Vec<SqlOption>,
    source_id: u32,
) -> Result<()> {
    let meta_client = session.env().meta_client();
    let (resolved_with_options, connector_conn_ref) = resolve_connection_ref_and_secret_ref(
        WithOptions::try_from(alter_props.as_ref() as &[SqlOption])?,
        session,
        None,
    )?;
    let (changed_props, changed_secret_refs) = resolved_with_options.into_parts();
    if connector_conn_ref.is_some() {
        return Err(ErrorCode::InvalidInputSyntax(
            "ALTER SOURCE CONNECTOR does not support CONNECTION".to_owned(),
        )
        .into());
    }

    meta_client
        .alter_source_connector_props(
            source_id,
            changed_props,
            changed_secret_refs,
            connector_conn_ref, // always None, keep the interface for future extension
        )
        .await?;
    Ok(())
}

pub async fn handle_alter_source_connector_props(
    handler_args: HandlerArgs,
    source_name: ObjectName,
    alter_props: Vec<SqlOption>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, real_source_name) =
        Binder::resolve_schema_qualified_name(db_name, source_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let source_id = {
        let reader = session.env().catalog_reader().read_guard();
        let (source, schema_name) =
            reader.get_source_by_name(db_name, schema_path, &real_source_name)?;

        // For `CREATE TABLE WITH (connector = '...')`, users should call `ALTER TABLE` instead.
        if source.associated_table_id.is_some() {
            return Err(ErrorCode::InvalidInputSyntax(
                "Use `ALTER TABLE` to alter a table with connector.".to_owned(),
            )
            .into());
        }

        session.check_privilege_for_drop_alter(schema_name, &**source)?;

        ensure_alter_props_not_set_by_connection(
            &reader,
            db_name,
            source.connection_id,
            &alter_props,
        )?;

        source.id
    };

    handle_alter_source_props_inner(&session, alter_props, source_id).await?;

    Ok(RwPgResponse::empty_result(StatementType::ALTER_SOURCE))
}

/// Validates that the properties being altered don't conflict with properties set by a CONNECTION.
pub(crate) fn ensure_alter_props_not_set_by_connection(
    _reader: &CatalogReadGuard,
    _db_name: &str,
    connection_id: Option<u32>,
    _alter_props: &[SqlOption],
) -> Result<()> {
    if let Some(_conn_id) = connection_id {
        return Err(
            ErrorCode::InvalidInputSyntax("alter connection is not supported".to_owned()).into(),
        );
    }
    Ok(())
}
