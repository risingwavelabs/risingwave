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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::acl::AclMode;
use risingwave_common::catalog::is_system_schema;
use risingwave_common::session_config::RuntimeParameters;
use risingwave_pb::ddl_service::alter_name_request;
use risingwave_pb::user::grant_privilege;
use risingwave_sqlparser::ast::ObjectName;

use super::{HandlerArgs, RwPgResponse};
use crate::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::error::{ErrorCode, Result};

pub async fn handle_rename_table(
    handler_args: HandlerArgs,
    table_type: TableType,
    table_name: ObjectName,
    new_table_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, real_table_name) =
        Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
    let new_table_name = Binder::resolve_table_name(new_table_name)?;
    let search_path = session.running_sql_runtime_parameters(RuntimeParameters::search_path);
    let user_name = &session.user_name();

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let table_id = {
        let reader = session.env().catalog_reader().read_guard();
        let (table, schema_name) =
            reader.get_created_table_by_name(db_name, schema_path, &real_table_name)?;
        if table_type != table.table_type {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "\"{table_name}\" is not a {}",
                table_type.to_prost().as_str_name()
            ))
            .into());
        }

        session.check_privilege_for_drop_alter(schema_name, &**table)?;
        table.id
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_name(
            alter_name_request::Object::TableId(table_id.table_id),
            &new_table_name,
        )
        .await?;

    let stmt_type = match table_type {
        TableType::Table => StatementType::ALTER_TABLE,
        TableType::MaterializedView => StatementType::ALTER_MATERIALIZED_VIEW,
        _ => unreachable!(),
    };
    Ok(PgResponse::empty_result(stmt_type))
}

pub async fn handle_rename_index(
    handler_args: HandlerArgs,
    index_name: ObjectName,
    new_index_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, real_index_name) =
        Binder::resolve_schema_qualified_name(db_name, index_name.clone())?;
    let new_index_name = Binder::resolve_index_name(new_index_name)?;
    let search_path = session.running_sql_runtime_parameters(RuntimeParameters::search_path);
    let user_name = &session.user_name();

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let index_id = {
        let reader = session.env().catalog_reader().read_guard();
        let (index, schema_name) =
            reader.get_index_by_name(db_name, schema_path, &real_index_name)?;
        session.check_privilege_for_drop_alter(schema_name, &**index)?;
        index.id
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_name(
            alter_name_request::Object::IndexId(index_id.index_id),
            &new_index_name,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_INDEX))
}

pub async fn handle_rename_view(
    handler_args: HandlerArgs,
    view_name: ObjectName,
    new_view_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, real_view_name) =
        Binder::resolve_schema_qualified_name(db_name, view_name.clone())?;
    let new_view_name = Binder::resolve_view_name(new_view_name)?;
    let search_path = session.running_sql_runtime_parameters(RuntimeParameters::search_path);
    let user_name = &session.user_name();

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let view_id = {
        let reader = session.env().catalog_reader().read_guard();
        let (view, schema_name) = reader.get_view_by_name(db_name, schema_path, &real_view_name)?;
        session.check_privilege_for_drop_alter(schema_name, &**view)?;
        view.id
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_name(alter_name_request::Object::ViewId(view_id), &new_view_name)
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_VIEW))
}

pub async fn handle_rename_sink(
    handler_args: HandlerArgs,
    sink_name: ObjectName,
    new_sink_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, real_sink_name) =
        Binder::resolve_schema_qualified_name(db_name, sink_name.clone())?;
    let new_sink_name = Binder::resolve_sink_name(new_sink_name)?;
    let search_path = session.running_sql_runtime_parameters(RuntimeParameters::search_path);
    let user_name = &session.user_name();

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let sink_id = {
        let reader = session.env().catalog_reader().read_guard();
        let (sink, schema_name) = reader.get_sink_by_name(db_name, schema_path, &real_sink_name)?;
        session.check_privilege_for_drop_alter(schema_name, &**sink)?;
        sink.id
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_name(
            alter_name_request::Object::SinkId(sink_id.sink_id),
            &new_sink_name,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_SINK))
}

pub async fn handle_rename_subscription(
    handler_args: HandlerArgs,
    subscription_name: ObjectName,
    new_subscription_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, real_subscription_name) =
        Binder::resolve_schema_qualified_name(db_name, subscription_name.clone())?;
    let new_subscription_name = Binder::resolve_subscription_name(new_subscription_name)?;
    let search_path = session.running_sql_runtime_parameters(RuntimeParameters::search_path);
    let user_name = &session.user_name();

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let subscription_id = {
        let reader = session.env().catalog_reader().read_guard();
        let (subscription, schema_name) =
            reader.get_subscription_by_name(db_name, schema_path, &real_subscription_name)?;
        session.check_privilege_for_drop_alter(schema_name, &**subscription)?;
        subscription.id
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_name(
            alter_name_request::Object::SubscriptionId(subscription_id.subscription_id),
            &new_subscription_name,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_SUBSCRIPTION))
}

pub async fn handle_rename_source(
    handler_args: HandlerArgs,
    source_name: ObjectName,
    new_source_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, real_source_name) =
        Binder::resolve_schema_qualified_name(db_name, source_name.clone())?;
    let new_source_name = Binder::resolve_source_name(new_source_name)?;
    let search_path = session.running_sql_runtime_parameters(RuntimeParameters::search_path);
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
        source.id
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_name(
            alter_name_request::Object::SourceId(source_id),
            &new_source_name,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_SOURCE))
}

pub async fn handle_rename_schema(
    handler_args: HandlerArgs,
    schema_name: ObjectName,
    new_schema_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let schema_name = Binder::resolve_schema_name(schema_name)?;
    let new_schema_name = Binder::resolve_schema_name(new_schema_name)?;

    let schema_id = {
        let user_reader = session.env().user_info_reader().read_guard();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema = catalog_reader.get_schema_by_name(db_name, &schema_name)?;
        let db_id = catalog_reader.get_database_by_name(db_name)?.id();

        // The current one should not be system schema.
        if is_system_schema(&schema.name()) {
            return Err(ErrorCode::ProtocolError(format!(
                "permission denied to rename on \"{}\", System catalog modifications are currently disallowed.",
                schema_name
            )).into());
        }

        // The user should be super user or owner to alter the schema.
        session.check_privilege_for_drop_alter_db_schema(schema)?;

        // To rename a schema you must also have the CREATE privilege for the database.
        if let Some(user) = user_reader.get_user_by_name(&session.user_name()) {
            if !user.is_super
                && !user.has_privilege(&grant_privilege::Object::DatabaseId(db_id), AclMode::Create)
            {
                return Err(ErrorCode::PermissionDenied(
                    "Do not have create privilege on the current database".to_owned(),
                )
                .into());
            }
        } else {
            return Err(ErrorCode::PermissionDenied("Session user is invalid".to_owned()).into());
        }

        schema.id()
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_name(
            alter_name_request::Object::SchemaId(schema_id),
            &new_schema_name,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_SCHEMA))
}

pub async fn handle_rename_database(
    handler_args: HandlerArgs,
    database_name: ObjectName,
    new_database_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let database_name = Binder::resolve_database_name(database_name)?;
    let new_database_name = Binder::resolve_database_name(new_database_name)?;

    let database_id = {
        let user_reader = session.env().user_info_reader().read_guard();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let database = catalog_reader.get_database_by_name(&database_name)?;

        // The user should be super user or owner to alter the database.
        session.check_privilege_for_drop_alter_db_schema(database)?;

        // Non-superuser owners must also have the CREATEDB privilege.
        if let Some(user) = user_reader.get_user_by_name(&session.user_name()) {
            if !user.is_super && !user.can_create_db {
                return Err(ErrorCode::PermissionDenied(
                    "Non-superuser owners must also have the CREATEDB privilege".to_owned(),
                )
                .into());
            }
        } else {
            return Err(ErrorCode::PermissionDenied("Session user is invalid".to_owned()).into());
        }

        // The current database cannot be renamed.
        if database_name == session.database() {
            return Err(ErrorCode::PermissionDenied(
                "Current database cannot be renamed".to_owned(),
            )
            .into());
        }

        database.id()
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_name(
            alter_name_request::Object::DatabaseId(database_id),
            &new_database_name,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_DATABASE))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_alter_table_name_handler() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        let sql = "create table t (i int, r real);";
        frontend.run_sql(sql).await.unwrap();

        let table_id = {
            let catalog_reader = session.env().catalog_reader().read_guard();
            catalog_reader
                .get_created_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
                .unwrap()
                .0
                .id
        };

        // Alter table name.
        let sql = "alter table t rename to t1;";
        frontend.run_sql(sql).await.unwrap();

        let catalog_reader = session.env().catalog_reader().read_guard();
        let altered_table_name = catalog_reader
            .get_any_table_by_id(&table_id)
            .unwrap()
            .name()
            .to_owned();
        assert_eq!(altered_table_name, "t1");
    }
}
