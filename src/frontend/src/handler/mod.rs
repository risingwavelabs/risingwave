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

use std::sync::Arc;

use pgwire::pg_response::PgResponse;
use pgwire::pg_response::StatementType::{ABORT, START_TRANSACTION};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{DropStatement, ObjectType, Statement};

use self::util::handle_with_properties;
use crate::session::{OptimizerContext, SessionImpl};

pub mod alter_user;
mod create_database;
pub mod create_index;
pub mod create_mv;
mod create_schema;
pub mod create_sink;
pub mod create_source;
pub mod create_table;
pub mod create_user;
mod describe;
pub mod dml;
mod drop_database;
mod drop_index;
pub mod drop_mv;
mod drop_schema;
pub mod drop_sink;
pub mod drop_source;
pub mod drop_table;
pub mod drop_user;
mod explain;
mod flush;
pub mod handle_privilege;
pub mod privilege;
pub mod query;
mod show;
pub mod util;
pub mod variable;

pub async fn handle(
    session: Arc<SessionImpl>,
    stmt: Statement,
    sql: &str,
    format: bool,
) -> Result<PgResponse> {
    let mut context = OptimizerContext::new(session.clone(), Arc::from(sql));
    match stmt {
        Statement::Explain {
            statement,
            describe_alias: _,
            analyze,
            options,
        } => {
            match statement.as_ref() {
                Statement::CreateTable { with_options, .. }
                | Statement::CreateView { with_options, .. } => {
                    context.with_properties =
                        handle_with_properties("explain create_table", with_options.clone())?;
                }

                _ => {}
            }

            explain::handle_explain(context, *statement, options, analyze)
        }
        Statement::CreateSource {
            is_materialized,
            stmt,
        } => create_source::handle_create_source(context, is_materialized, stmt).await,
        Statement::CreateSink { stmt } => create_sink::handle_create_sink(context, stmt).await,
        Statement::CreateTable {
            name,
            columns,
            with_options,
            constraints,
            ..
        } => {
            context.with_properties = handle_with_properties("handle_create_table", with_options)?;
            create_table::handle_create_table(context, name, columns, constraints).await
        }
        Statement::CreateDatabase {
            db_name,
            if_not_exists,
            ..
        } => create_database::handle_create_database(context, db_name, if_not_exists).await,
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
            ..
        } => create_schema::handle_create_schema(context, schema_name, if_not_exists).await,
        Statement::CreateUser(stmt) => create_user::handle_create_user(context, stmt).await,
        Statement::AlterUser(stmt) => alter_user::handle_alter_user(context, stmt).await,
        Statement::Grant { .. } => handle_privilege::handle_grant_privilege(context, stmt).await,
        Statement::Revoke { .. } => handle_privilege::handle_revoke_privilege(context, stmt).await,
        Statement::Describe { name } => describe::handle_describe(context, name),
        Statement::ShowObjects(show_object) => show::handle_show_object(context, show_object),
        Statement::Drop(DropStatement {
            object_type,
            object_name,
            if_exists,
            drop_mode,
        }) => match object_type {
            ObjectType::Table => drop_table::handle_drop_table(context, object_name).await,
            ObjectType::MaterializedView => drop_mv::handle_drop_mv(context, object_name).await,
            ObjectType::Index => drop_index::handle_drop_index(context, object_name).await,
            ObjectType::Source => drop_source::handle_drop_source(context, object_name).await,
            ObjectType::Sink => drop_sink::handle_drop_sink(context, object_name).await,
            ObjectType::Database => {
                drop_database::handle_drop_database(
                    context,
                    object_name,
                    if_exists,
                    drop_mode.into(),
                )
                .await
            }
            ObjectType::Schema => {
                drop_schema::handle_drop_schema(context, object_name, if_exists, drop_mode.into())
                    .await
            }
            ObjectType::User => {
                drop_user::handle_drop_user(context, object_name, if_exists, drop_mode.into()).await
            }
            _ => Err(
                ErrorCode::InvalidInputSyntax(format!("DROP {} is unsupported", object_type))
                    .into(),
            ),
        },
        Statement::Query(_) => query::handle_query(context, stmt, format).await,
        Statement::Insert { .. } | Statement::Delete { .. } | Statement::Update { .. } => {
            query::handle_query(context, stmt, false).await
        }
        Statement::CreateView {
            materialized: true,
            or_replace: false,
            name,
            query,
            with_options,
            ..
        } => {
            context.with_properties = handle_with_properties("handle_create_mv", with_options)?;
            create_mv::handle_create_mv(context, name, query).await
        }
        Statement::Flush => flush::handle_flush(context).await,
        Statement::SetVariable {
            local: _,
            variable,
            value,
        } => variable::handle_set(context, variable, value),
        Statement::ShowVariable { variable } => variable::handle_show(context, variable),
        Statement::CreateIndex {
            name,
            table_name,
            columns,
            include,
            unique,
            if_not_exists,
        } => {
            if unique {
                return Err(
                    ErrorCode::NotImplemented("create unique index".into(), None.into()).into(),
                );
            }
            if if_not_exists {
                return Err(ErrorCode::NotImplemented(
                    "create if_not_exists index".into(),
                    None.into(),
                )
                .into());
            }
            create_index::handle_create_index(context, name, table_name, columns.to_vec(), include)
                .await
        }
        // Ignore `StartTransaction` and `Abort` temporarily.Its not final implementation.
        // 1. Fully support transaction is too hard and gives few benefits to us.
        // 2. Some client e.g. psycopg2 will use this statement.
        // TODO: Track issues #2595 #2541
        Statement::StartTransaction { .. } => Ok(PgResponse::empty_result_with_notice(
            START_TRANSACTION,
            "Ignored temporarily.See detail in issue#2541".to_string(),
        )),
        Statement::Abort { .. } => Ok(PgResponse::empty_result_with_notice(
            ABORT,
            "Ignored temporarily.See detail in issue#2541".to_string(),
        )),
        _ => {
            Err(ErrorCode::NotImplemented(format!("Unhandled ast: {:?}", stmt), None.into()).into())
        }
    }
}
