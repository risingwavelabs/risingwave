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
use risingwave_common::session_config::RuntimeParameters;
use risingwave_pb::user::PbGrantPrivilege;
use risingwave_pb::user::grant_privilege::{ActionWithGrantOption, PbObject};
use risingwave_sqlparser::ast::{GrantObjects, Privileges, Statement};

use super::RwPgResponse;
use crate::bind_data_type;
use crate::binder::Binder;
use crate::catalog::CatalogError;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::error::{ErrorCode, Result};
use crate::handler::HandlerArgs;
use crate::session::SessionImpl;
use crate::user::user_privilege::{
    available_privilege_actions, check_privilege_type, get_prost_action,
};

fn make_prost_privilege(
    session: &SessionImpl,
    privileges: Privileges,
    objects: GrantObjects,
) -> Result<Vec<PbGrantPrivilege>> {
    check_privilege_type(&privileges, &objects)?;

    let catalog_reader = session.env().catalog_reader();
    let reader = catalog_reader.read_guard();
    let actions = match privileges {
        Privileges::All { .. } => available_privilege_actions(&objects)?,
        Privileges::Actions(actions) => actions
            .into_iter()
            .map(|action| get_prost_action(&action))
            .collect(),
    };
    let mut grant_objs = vec![];
    match objects {
        GrantObjects::Databases(databases) => {
            for db in databases {
                let database_name = Binder::resolve_database_name(db)?;
                let database = reader.get_database_by_name(&database_name)?;
                grant_objs.push(PbObject::DatabaseId(database.id()));
            }
        }
        GrantObjects::Schemas(schemas) => {
            for schema in schemas {
                let schema_name = Binder::resolve_schema_name(schema)?;
                let schema = reader.get_schema_by_name(&session.database(), &schema_name)?;
                grant_objs.push(PbObject::SchemaId(schema.id()));
            }
        }
        GrantObjects::Mviews(tables) => {
            let db_name = &session.database();
            let search_path =
                session.running_sql_runtime_parameters(RuntimeParameters::search_path);
            let user_name = &session.user_name();

            for name in tables {
                let (schema_name, table_name) =
                    Binder::resolve_schema_qualified_name(db_name, name)?;
                let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

                let (table, _) =
                    reader.get_created_table_by_name(db_name, schema_path, &table_name)?;
                match table.table_type() {
                    TableType::MaterializedView => {}
                    _ => {
                        return Err(ErrorCode::InvalidInputSyntax(format!(
                            "{table_name} is not a materialized view",
                        ))
                        .into());
                    }
                }
                grant_objs.push(PbObject::TableId(table.id().table_id));
            }
        }
        GrantObjects::Tables(tables) => {
            let db_name = &session.database();
            let search_path =
                session.running_sql_runtime_parameters(RuntimeParameters::search_path);
            let user_name = &session.user_name();

            for name in tables {
                let (schema_name, table_name) =
                    Binder::resolve_schema_qualified_name(db_name, name)?;
                let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

                match reader.get_created_table_by_name(db_name, schema_path, &table_name) {
                    Ok((table, _)) => {
                        match table.table_type() {
                            TableType::Table => {
                                grant_objs.push(PbObject::TableId(table.id().table_id));
                                continue;
                            }
                            _ => {
                                return Err(ErrorCode::InvalidInputSyntax(format!(
                                    "{table_name} is not a table",
                                ))
                                .into());
                            }
                        };
                    }
                    Err(CatalogError::NotFound("table", _)) => {
                        let (view, _) = reader
                            .get_view_by_name(db_name, schema_path, &table_name)
                            .map_err(|_| CatalogError::NotFound("table", table_name))?;
                        grant_objs.push(PbObject::ViewId(view.id));
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
        }
        GrantObjects::Sources(sources) => {
            let db_name = &session.database();
            let search_path =
                session.running_sql_runtime_parameters(RuntimeParameters::search_path);
            let user_name = &session.user_name();

            for name in sources {
                let (schema_name, source_name) =
                    Binder::resolve_schema_qualified_name(db_name, name)?;
                let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

                let (source, _) = reader.get_source_by_name(db_name, schema_path, &source_name)?;
                grant_objs.push(PbObject::SourceId(source.id));
            }
        }
        GrantObjects::Sinks(sinks) => {
            let db_name = &session.database();
            let search_path =
                session.running_sql_runtime_parameters(RuntimeParameters::search_path);
            let user_name = &session.user_name();

            for name in sinks {
                let (schema_name, sink_name) =
                    Binder::resolve_schema_qualified_name(db_name, name)?;
                let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

                let (sink, _) = reader.get_sink_by_name(db_name, schema_path, &sink_name)?;
                grant_objs.push(PbObject::SinkId(sink.id.sink_id));
            }
        }
        GrantObjects::Views(views) => {
            let db_name = &session.database();
            let search_path =
                session.running_sql_runtime_parameters(RuntimeParameters::search_path);
            let user_name = &session.user_name();

            for name in views {
                let (schema_name, view_name) =
                    Binder::resolve_schema_qualified_name(db_name, name)?;
                let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

                let (view, _) = reader.get_view_by_name(db_name, schema_path, &view_name)?;
                grant_objs.push(PbObject::ViewId(view.id));
            }
        }
        GrantObjects::Connections(conns) => {
            let db_name = &session.database();
            let search_path =
                session.running_sql_runtime_parameters(RuntimeParameters::search_path);
            let user_name = &session.user_name();

            for name in conns {
                let (schema_name, conn_name) =
                    Binder::resolve_schema_qualified_name(db_name, name)?;
                let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

                let (conn, _) = reader.get_connection_by_name(db_name, schema_path, &conn_name)?;
                grant_objs.push(PbObject::ConnectionId(conn.id));
            }
        }
        GrantObjects::Subscriptions(subscriptions) => {
            let db_name = &session.database();
            let search_path =
                session.running_sql_runtime_parameters(RuntimeParameters::search_path);
            let user_name = &session.user_name();

            for name in subscriptions {
                let (schema_name, sub_name) = Binder::resolve_schema_qualified_name(db_name, name)?;
                let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

                let (sub, _) = reader.get_subscription_by_name(db_name, schema_path, &sub_name)?;
                grant_objs.push(PbObject::SubscriptionId(sub.id.subscription_id));
            }
        }
        GrantObjects::Functions(func_descs) => {
            let db_name = &session.database();
            let search_path =
                session.running_sql_runtime_parameters(RuntimeParameters::search_path);
            let user_name = &session.user_name();

            for func_desc in func_descs {
                let (schema_name, func_name) =
                    Binder::resolve_schema_qualified_name(db_name, func_desc.name)?;
                let arg_types = match func_desc.args {
                    Some(args) => {
                        let mut arg_types = vec![];
                        for arg in args {
                            arg_types.push(bind_data_type(&arg.data_type)?);
                        }
                        Some(arg_types)
                    }
                    None => None,
                };
                let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

                let (func, _) = match arg_types {
                    Some(arg_types) => reader.get_function_by_name_args(
                        db_name,
                        schema_path,
                        &func_name,
                        &arg_types,
                    )?,
                    None => {
                        let (functions, schema_name) =
                            reader.get_functions_by_name(db_name, schema_path, &func_name)?;
                        if functions.len() > 1 {
                            return Err(ErrorCode::CatalogError(format!(
                                "function name {func_name:?} is not unique\nHINT: Specify the argument list to select the function unambiguously."
                            ).into()).into());
                        }
                        (
                            functions.into_iter().next().expect("no functions"),
                            schema_name,
                        )
                    }
                };
                grant_objs.push(PbObject::FunctionId(func.id.function_id()));
            }
        }
        GrantObjects::Secrets(secrets) => {
            let db_name = &session.database();
            let search_path =
                session.running_sql_runtime_parameters(RuntimeParameters::search_path);
            let user_name = &session.user_name();

            for name in secrets {
                let (schema_name, secret_name) =
                    Binder::resolve_schema_qualified_name(db_name, name)?;
                let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

                let (secret, _) = reader.get_secret_by_name(db_name, schema_path, &secret_name)?;
                grant_objs.push(PbObject::SecretId(secret.id.secret_id()));
            }
        }
        GrantObjects::AllSourcesInSchema { schemas } => {
            for schema in schemas {
                let schema_name = Binder::resolve_schema_name(schema)?;
                let schema = reader.get_schema_by_name(&session.database(), &schema_name)?;
                schema.iter_source().for_each(|source| {
                    grant_objs.push(PbObject::SourceId(source.id));
                });
            }
        }
        GrantObjects::AllMviewsInSchema { schemas } => {
            for schema in schemas {
                let schema_name = Binder::resolve_schema_name(schema)?;
                let schema = reader.get_schema_by_name(&session.database(), &schema_name)?;
                schema.iter_all_mvs().for_each(|mview| {
                    grant_objs.push(PbObject::TableId(mview.id().table_id));
                });
            }
        }
        GrantObjects::AllTablesInSchema { schemas } => {
            for schema in schemas {
                let schema_name = Binder::resolve_schema_name(schema)?;
                let schema = reader.get_schema_by_name(&session.database(), &schema_name)?;
                schema.iter_user_table().for_each(|table| {
                    grant_objs.push(PbObject::TableId(table.id().table_id));
                });
            }
        }
        GrantObjects::AllSinksInSchema { schemas } => {
            for schema in schemas {
                let schema_name = Binder::resolve_schema_name(schema)?;
                let schema = reader.get_schema_by_name(&session.database(), &schema_name)?;
                schema.iter_sink().for_each(|sink| {
                    grant_objs.push(PbObject::SinkId(sink.id.sink_id));
                });
            }
        }
        GrantObjects::AllViewsInSchema { schemas } => {
            for schema in schemas {
                let schema_name = Binder::resolve_schema_name(schema)?;
                let schema = reader.get_schema_by_name(&session.database(), &schema_name)?;
                schema.iter_view().for_each(|view| {
                    grant_objs.push(PbObject::ViewId(view.id));
                });
            }
        }
        GrantObjects::AllFunctionsInSchema { schemas } => {
            for schema in schemas {
                let schema_name = Binder::resolve_schema_name(schema)?;
                let schema = reader.get_schema_by_name(&session.database(), &schema_name)?;
                schema.iter_function().for_each(|func| {
                    grant_objs.push(PbObject::FunctionId(func.id.function_id()));
                });
            }
        }
        GrantObjects::AllSecretsInSchema { schemas } => {
            for schema in schemas {
                let schema_name = Binder::resolve_schema_name(schema)?;
                let schema = reader.get_schema_by_name(&session.database(), &schema_name)?;
                schema.iter_secret().for_each(|secret| {
                    grant_objs.push(PbObject::SecretId(secret.id.secret_id()));
                });
            }
        }
        GrantObjects::AllSubscriptionsInSchema { schemas } => {
            for schema in schemas {
                let schema_name = Binder::resolve_schema_name(schema)?;
                let schema = reader.get_schema_by_name(&session.database(), &schema_name)?;
                schema.iter_subscription().for_each(|sub| {
                    grant_objs.push(PbObject::SubscriptionId(sub.id.subscription_id));
                });
            }
        }
        GrantObjects::AllConnectionsInSchema { schemas } => {
            for schema in schemas {
                let schema_name = Binder::resolve_schema_name(schema)?;
                let schema = reader.get_schema_by_name(&session.database(), &schema_name)?;
                schema.iter_connections().for_each(|conn| {
                    grant_objs.push(PbObject::ConnectionId(conn.id));
                });
            }
        }
        o => {
            return Err(ErrorCode::BindError(format!(
                "GRANT statement does not support object type: {:?}",
                o
            ))
            .into());
        }
    };
    let action_with_opts = actions
        .into_iter()
        .map(|action| ActionWithGrantOption {
            action: action as i32,
            granted_by: session.user_id(),
            ..Default::default()
        })
        .collect::<Vec<_>>();

    let mut prost_privileges = vec![];
    for objs in grant_objs {
        prost_privileges.push(PbGrantPrivilege {
            action_with_opts: action_with_opts.clone(),
            object: Some(objs),
        });
    }
    Ok(prost_privileges)
}

pub async fn handle_grant_privilege(
    handler_args: HandlerArgs,
    stmt: Statement,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let Statement::Grant {
        privileges,
        objects,
        grantees,
        with_grant_option,
        granted_by,
    } = stmt
    else {
        return Err(ErrorCode::BindError("Invalid grant statement".to_owned()).into());
    };
    let mut users = vec![];
    {
        let user_reader = session.env().user_info_reader();
        let reader = user_reader.read_guard();
        for grantee in grantees {
            if let Some(user) = reader.get_user_by_name(&grantee.real_value()) {
                users.push(user.id);
            } else {
                return Err(ErrorCode::BindError("Grantee does not exist".to_owned()).into());
            }
        }
        if let Some(granted_by) = &granted_by {
            // We remark that the user name is always case-sensitive.
            if reader.get_user_by_name(&granted_by.real_value()).is_none() {
                return Err(ErrorCode::BindError("Grantor does not exist".to_owned()).into());
            }
        }
    };

    let privileges = make_prost_privilege(&session, privileges, objects)?;
    let user_info_writer = session.user_info_writer()?;
    user_info_writer
        .grant_privilege(users, privileges, with_grant_option, session.user_id())
        .await?;
    Ok(PgResponse::empty_result(StatementType::GRANT_PRIVILEGE))
}

pub async fn handle_revoke_privilege(
    handler_args: HandlerArgs,
    stmt: Statement,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let Statement::Revoke {
        privileges,
        objects,
        grantees,
        granted_by,
        revoke_grant_option,
        cascade,
    } = stmt
    else {
        return Err(ErrorCode::BindError("Invalid revoke statement".to_owned()).into());
    };
    let mut users = vec![];
    let mut granted_by_id = None;
    {
        let user_reader = session.env().user_info_reader();
        let reader = user_reader.read_guard();
        for grantee in grantees {
            if let Some(user) = reader.get_user_by_name(&grantee.real_value()) {
                users.push(user.id);
            } else {
                return Err(ErrorCode::BindError("Grantee does not exist".to_owned()).into());
            }
        }
        if let Some(granted_by) = &granted_by {
            if let Some(user) = reader.get_user_by_name(&granted_by.real_value()) {
                granted_by_id = Some(user.id);
            } else {
                return Err(ErrorCode::BindError("Grantor does not exist".to_owned()).into());
            }
        }
    };
    let privileges = make_prost_privilege(&session, privileges, objects)?;
    let user_info_writer = session.user_info_writer()?;
    user_info_writer
        .revoke_privilege(
            users,
            privileges,
            granted_by_id.unwrap_or(session.user_id()),
            session.user_id(),
            revoke_grant_option,
            cascade,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::REVOKE_PRIVILEGE))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::DEFAULT_SUPER_USER_ID;
    use risingwave_pb::user::grant_privilege::Action;

    use super::*;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_grant_privilege() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        frontend
            .run_sql("CREATE USER user WITH SUPERUSER PASSWORD 'password'")
            .await
            .unwrap();
        frontend
            .run_sql("CREATE USER user1 WITH PASSWORD 'password1'")
            .await
            .unwrap();
        frontend.run_sql("CREATE DATABASE db1").await.unwrap();
        frontend
            .run_sql("GRANT ALL ON DATABASE db1 TO user1 WITH GRANT OPTION GRANTED BY user")
            .await
            .unwrap();

        let (session_database_id, database_id) = {
            let catalog_reader = session.env().catalog_reader();
            let reader = catalog_reader.read_guard();
            (
                reader
                    .get_database_by_name(&session.database())
                    .unwrap()
                    .id(),
                reader.get_database_by_name("db1").unwrap().id(),
            )
        };

        {
            let user_reader = session.env().user_info_reader();
            let reader = user_reader.read_guard();
            let user_info = reader.get_user_by_name("user1").unwrap();
            assert_eq!(
                user_info.grant_privileges,
                vec![
                    PbGrantPrivilege {
                        action_with_opts: vec![ActionWithGrantOption {
                            action: Action::Connect as i32,
                            with_grant_option: true,
                            granted_by: session.user_id(),
                        }],
                        object: Some(PbObject::DatabaseId(session_database_id)),
                    },
                    PbGrantPrivilege {
                        action_with_opts: vec![
                            ActionWithGrantOption {
                                action: Action::Create as i32,
                                with_grant_option: true,
                                granted_by: DEFAULT_SUPER_USER_ID,
                            },
                            ActionWithGrantOption {
                                action: Action::Connect as i32,
                                with_grant_option: true,
                                granted_by: DEFAULT_SUPER_USER_ID,
                            }
                        ],
                        object: Some(PbObject::DatabaseId(database_id)),
                    }
                ]
            );
        }

        frontend
            .run_sql("REVOKE GRANT OPTION FOR ALL ON DATABASE db1 from user1 GRANTED BY user")
            .await
            .unwrap();
        {
            let user_reader = session.env().user_info_reader();
            let reader = user_reader.read_guard();
            let user_info = reader.get_user_by_name("user1").unwrap();
            assert!(
                user_info
                    .grant_privileges
                    .iter()
                    .filter(|gp| gp.object == Some(PbObject::DatabaseId(database_id)))
                    .all(|p| p.action_with_opts.iter().all(|ao| !ao.with_grant_option))
            );
        }

        frontend
            .run_sql("REVOKE ALL ON DATABASE db1 from user1 GRANTED BY user")
            .await
            .unwrap();
        {
            let user_reader = session.env().user_info_reader();
            let reader = user_reader.read_guard();
            let user_info = reader.get_user_by_name("user1").unwrap();
            assert_eq!(
                user_info.grant_privileges,
                vec![PbGrantPrivilege {
                    action_with_opts: vec![ActionWithGrantOption {
                        action: Action::Connect as i32,
                        with_grant_option: true,
                        granted_by: session.user_id(),
                    }],
                    object: Some(PbObject::DatabaseId(session_database_id)),
                }]
            );
        }
        frontend.run_sql("DROP USER user1").await.unwrap();
    }
}
