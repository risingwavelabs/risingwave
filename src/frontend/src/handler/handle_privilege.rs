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
use risingwave_pb::user::grant_privilege::{ActionWithGrantOption, Object as ProstObject};
use risingwave_pb::user::GrantPrivilege as ProstPrivilege;
use risingwave_sqlparser::ast::{GrantObjects, Privileges, Statement};

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::session::{OptimizerContext, SessionImpl};
use crate::user::user_privilege::{
    available_privilege_actions, check_privilege_type, get_prost_action,
};

fn make_prost_privilege(
    session: &SessionImpl,
    privileges: Privileges,
    objects: GrantObjects,
) -> Result<Vec<ProstPrivilege>> {
    check_privilege_type(&privileges, &objects)?;

    let catalog_reader = session.env().catalog_reader();
    let reader = catalog_reader.read_guard();
    let actions = match privileges {
        Privileges::All { .. } => available_privilege_actions(&objects)?,
        Privileges::Actions(actions) => actions,
    };
    let mut grant_objs = vec![];
    match objects {
        GrantObjects::Databases(databases) => {
            for db in databases {
                let database_name = Binder::resolve_database_name(db)?;
                let database = reader.get_database_by_name(&database_name)?;
                grant_objs.push(ProstObject::DatabaseId(database.id()));
            }
        }
        GrantObjects::Schemas(schemas) => {
            for schema in schemas {
                let schema_name = Binder::resolve_schema_name(schema)?;
                let schema = reader.get_schema_by_name(session.database(), &schema_name)?;
                grant_objs.push(ProstObject::SchemaId(schema.id()));
            }
        }
        GrantObjects::Mviews(tables) => {
            let db_name = session.database();
            let search_path = session.config().get_search_path();
            let user_name = &session.auth_context().user_name;

            for name in tables {
                let (schema_name, table_name) = Binder::resolve_table_name(db_name, name)?;
                let schema_path = match schema_name.as_deref() {
                    Some(schema_name) => SchemaPath::Name(schema_name),
                    None => SchemaPath::Path(&search_path, user_name),
                };

                let (table, _) = reader.get_table_by_name(db_name, schema_path, &table_name)?;
                grant_objs.push(ProstObject::TableId(table.id().table_id));
            }
        }
        GrantObjects::Sources(sources) => {
            let db_name = session.database();
            let search_path = session.config().get_search_path();
            let user_name = &session.auth_context().user_name;

            for name in sources {
                let (schema_name, source_name) = Binder::resolve_table_name(db_name, name)?;
                let schema_path = match schema_name.as_deref() {
                    Some(schema_name) => SchemaPath::Name(schema_name),
                    None => SchemaPath::Path(&search_path, user_name),
                };

                let (source, _) = reader.get_source_by_name(db_name, schema_path, &source_name)?;
                grant_objs.push(ProstObject::SourceId(source.id));
            }
        }
        GrantObjects::AllSourcesInSchema { schemas } => {
            for schema in schemas {
                let schema_name = Binder::resolve_schema_name(schema)?;
                let schema = reader.get_schema_by_name(session.database(), &schema_name)?;
                grant_objs.push(ProstObject::AllSourcesSchemaId(schema.id()));
            }
        }
        GrantObjects::AllMviewsInSchema { schemas } => {
            for schema in schemas {
                let schema_name = Binder::resolve_schema_name(schema)?;
                let schema = reader.get_schema_by_name(session.database(), &schema_name)?;
                grant_objs.push(ProstObject::AllTablesSchemaId(schema.id()));
            }
        }
        _ => {
            return Err(ErrorCode::BindError(
                "GRANT statement does not support this object type".to_string(),
            )
            .into());
        }
    };
    let action_with_opts = actions
        .iter()
        .map(|action| {
            let prost_action = get_prost_action(action);
            ActionWithGrantOption {
                action: prost_action as i32,
                granted_by: session.user_id(),
                ..Default::default()
            }
        })
        .collect::<Vec<_>>();

    let mut prost_privileges = vec![];
    for objs in grant_objs {
        prost_privileges.push(ProstPrivilege {
            action_with_opts: action_with_opts.clone(),
            object: Some(objs),
        });
    }
    Ok(prost_privileges)
}

pub async fn handle_grant_privilege(
    context: OptimizerContext,
    stmt: Statement,
) -> Result<RwPgResponse> {
    let session = context.session_ctx;
    let Statement::Grant {
        privileges,
        objects,
        grantees,
        with_grant_option,
        granted_by,
    } = stmt else { return Err(ErrorCode::BindError("Invalid grant statement".to_string()).into()); };
    let mut users = vec![];
    {
        let user_reader = session.env().user_info_reader();
        let reader = user_reader.read_guard();
        for grantee in grantees {
            if let Some(user) = reader.get_user_by_name(&grantee.value) {
                users.push(user.id);
            } else {
                return Err(ErrorCode::BindError("Grantee does not exist".to_string()).into());
            }
        }
        if let Some(granted_by) = &granted_by {
            // We remark that the user name is always case-sensitive.
            if reader.get_user_by_name(&granted_by.value).is_none() {
                return Err(ErrorCode::BindError("Grantor does not exist".to_string()).into());
            }
        }
    };

    let privileges = make_prost_privilege(&session, privileges, objects)?;
    let user_info_writer = session.env().user_info_writer();
    user_info_writer
        .grant_privilege(users, privileges, with_grant_option, session.user_id())
        .await?;
    Ok(PgResponse::empty_result(StatementType::GRANT_PRIVILEGE))
}

pub async fn handle_revoke_privilege(
    context: OptimizerContext,
    stmt: Statement,
) -> Result<RwPgResponse> {
    let session = context.session_ctx;
    let Statement::Revoke {
        privileges,
        objects,
        grantees,
        granted_by,
        revoke_grant_option,
        cascade,
    } = stmt else { return Err(ErrorCode::BindError("Invalid revoke statement".to_string()).into()); };
    let mut users = vec![];
    let mut granted_by_id = None;
    {
        let user_reader = session.env().user_info_reader();
        let reader = user_reader.read_guard();
        for grantee in grantees {
            if let Some(user) = reader.get_user_by_name(&grantee.value) {
                users.push(user.id);
            } else {
                return Err(ErrorCode::BindError("Grantee does not exist".to_string()).into());
            }
        }
        if let Some(granted_by) = &granted_by {
            if let Some(user) = reader.get_user_by_name(&granted_by.value) {
                granted_by_id = Some(user.id);
            } else {
                return Err(ErrorCode::BindError("Grantor does not exist".to_string()).into());
            }
        }
    };
    let privileges = make_prost_privilege(&session, privileges, objects)?;
    let user_info_writer = session.env().user_info_writer();
    user_info_writer
        .revoke_privilege(
            users,
            privileges,
            granted_by_id,
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

        let database_id = {
            let catalog_reader = session.env().catalog_reader();
            let reader = catalog_reader.read_guard();
            reader.get_database_by_name("db1").unwrap().id()
        };

        {
            let user_reader = session.env().user_info_reader();
            let reader = user_reader.read_guard();
            let user_info = reader.get_user_by_name("user1").unwrap();
            assert_eq!(
                user_info.grant_privileges,
                vec![ProstPrivilege {
                    action_with_opts: vec![
                        ActionWithGrantOption {
                            action: Action::Connect as i32,
                            with_grant_option: true,
                            granted_by: DEFAULT_SUPER_USER_ID,
                        },
                        ActionWithGrantOption {
                            action: Action::Create as i32,
                            with_grant_option: true,
                            granted_by: DEFAULT_SUPER_USER_ID,
                        }
                    ],
                    object: Some(ProstObject::DatabaseId(database_id)),
                }]
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
            assert!(user_info
                .grant_privileges
                .iter()
                .all(|p| p.action_with_opts.iter().all(|ao| !ao.with_grant_option)));
        }

        frontend
            .run_sql("REVOKE ALL ON DATABASE db1 from user1 GRANTED BY user")
            .await
            .unwrap();
        {
            let user_reader = session.env().user_info_reader();
            let reader = user_reader.read_guard();
            let user_info = reader.get_user_by_name("user1").unwrap();
            assert!(user_info.grant_privileges.is_empty());
        }
    }
}
