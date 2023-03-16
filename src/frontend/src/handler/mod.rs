// Copyright 2023 RisingWave Labs
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

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt};
use pgwire::pg_response::StatementType::{
    ABORT, BEGIN, COMMIT, ROLLBACK, SET_TRANSACTION, START_TRANSACTION,
};
use pgwire::pg_response::{PgResponse, RowSetResult};
use pgwire::pg_server::BoxedError;
use pgwire::types::{Format, Row};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::*;

use self::util::DataChunkToRowSetAdapter;
use crate::scheduler::{DistributedQueryStream, LocalQueryStream};
use crate::session::SessionImpl;
use crate::utils::WithOptions;

mod alter_system;
mod alter_table_column;
pub mod alter_user;
mod create_database;
pub mod create_function;
pub mod create_index;
pub mod create_mv;
pub mod create_schema;
pub mod create_sink;
pub mod create_source;
pub mod create_table;
pub mod create_table_as;
pub mod create_user;
pub mod create_view;
mod describe;
mod drop_database;
pub mod drop_function;
mod drop_index;
pub mod drop_mv;
mod drop_schema;
pub mod drop_sink;
pub mod drop_source;
pub mod drop_table;
pub mod drop_user;
mod drop_view;
pub mod explain;
mod flush;
pub mod handle_privilege;
pub mod privilege;
pub mod query;
mod show;
pub mod util;
pub mod variable;

/// The [`PgResponse`] used by Risingwave.
pub type RwPgResponse = PgResponse<PgResponseStream>;

pub enum PgResponseStream {
    LocalQuery(DataChunkToRowSetAdapter<LocalQueryStream>),
    DistributedQuery(DataChunkToRowSetAdapter<DistributedQueryStream>),
    Rows(BoxStream<'static, RowSetResult>),
}

impl Stream for PgResponseStream {
    type Item = std::result::Result<Vec<Row>, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            PgResponseStream::LocalQuery(inner) => inner.poll_next_unpin(cx),
            PgResponseStream::DistributedQuery(inner) => inner.poll_next_unpin(cx),
            PgResponseStream::Rows(inner) => inner.poll_next_unpin(cx),
        }
    }
}

impl From<Vec<Row>> for PgResponseStream {
    fn from(rows: Vec<Row>) -> Self {
        Self::Rows(stream::iter(vec![Ok(rows)]).boxed())
    }
}

#[derive(Clone)]
pub struct HandlerArgs {
    pub session: Arc<SessionImpl>,
    pub sql: String,
    pub normalized_sql: String,
    pub with_options: WithOptions,
}

impl HandlerArgs {
    pub fn new(session: Arc<SessionImpl>, stmt: &Statement, sql: &str) -> Result<Self> {
        Ok(Self {
            session,
            sql: sql.into(),
            with_options: WithOptions::try_from(stmt)?,
            normalized_sql: Self::normalize_sql(stmt),
        })
    }

    /// Get normalized SQL from the statement.
    ///
    /// - Generally, the normalized SQL is the unparsed (and formatted) result of the statement.
    /// - For `CREATE` statements, the clauses like `OR REPLACE` and `IF NOT EXISTS` are removed to
    ///   make it suitable for the `SHOW CREATE` statements.
    fn normalize_sql(stmt: &Statement) -> String {
        let mut stmt = stmt.clone();
        match &mut stmt {
            Statement::CreateView { or_replace, .. } => {
                *or_replace = false;
            }
            Statement::CreateTable {
                or_replace,
                if_not_exists,
                ..
            } => {
                *or_replace = false;
                *if_not_exists = false;
            }
            Statement::CreateIndex { if_not_exists, .. } => {
                *if_not_exists = false;
            }
            Statement::CreateSource {
                stmt: CreateSourceStatement { if_not_exists, .. },
                ..
            } => {
                *if_not_exists = false;
            }
            Statement::CreateSink {
                stmt: CreateSinkStatement { if_not_exists, .. },
            } => {
                *if_not_exists = false;
            }
            _ => {}
        }
        stmt.to_string()
    }
}

pub async fn handle(
    session: Arc<SessionImpl>,
    stmt: Statement,
    sql: &str,
    formats: Vec<Format>,
) -> Result<RwPgResponse> {
    session.clear_cancel_query_flag();
    let handler_args = HandlerArgs::new(session, &stmt, sql)?;
    match stmt {
        Statement::Explain {
            statement,
            analyze,
            options,
        } => explain::handle_explain(handler_args, *statement, options, analyze).await,
        Statement::CreateSource { stmt } => {
            create_source::handle_create_source(handler_args, stmt).await
        }
        Statement::CreateSink { stmt } => create_sink::handle_create_sink(handler_args, stmt).await,
        Statement::CreateFunction {
            or_replace,
            temporary,
            name,
            args,
            returns,
            params,
        } => {
            create_function::handle_create_function(
                handler_args,
                or_replace,
                temporary,
                name,
                args,
                returns,
                params,
            )
            .await
        }
        Statement::CreateTable {
            name,
            columns,
            constraints,
            query,
            with_options: _, // It is put in OptimizerContext
            // Not supported things
            or_replace,
            temporary,
            if_not_exists,
            source_schema,
            source_watermarks,
            append_only,
        } => {
            if or_replace {
                return Err(ErrorCode::NotImplemented(
                    "CREATE OR REPLACE TABLE".to_string(),
                    None.into(),
                )
                .into());
            }
            if temporary {
                return Err(ErrorCode::NotImplemented(
                    "CREATE TEMPORARY TABLE".to_string(),
                    None.into(),
                )
                .into());
            }
            if let Some(query) = query {
                return create_table_as::handle_create_as(
                    handler_args,
                    name,
                    if_not_exists,
                    query,
                    columns,
                    append_only,
                )
                .await;
            }
            create_table::handle_create_table(
                handler_args,
                name,
                columns,
                constraints,
                if_not_exists,
                source_schema,
                source_watermarks,
                append_only,
            )
            .await
        }
        Statement::CreateDatabase {
            db_name,
            if_not_exists,
        } => create_database::handle_create_database(handler_args, db_name, if_not_exists).await,
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
        } => create_schema::handle_create_schema(handler_args, schema_name, if_not_exists).await,
        Statement::CreateUser(stmt) => create_user::handle_create_user(handler_args, stmt).await,
        Statement::AlterUser(stmt) => alter_user::handle_alter_user(handler_args, stmt).await,
        Statement::Grant { .. } => {
            handle_privilege::handle_grant_privilege(handler_args, stmt).await
        }
        Statement::Revoke { .. } => {
            handle_privilege::handle_revoke_privilege(handler_args, stmt).await
        }
        Statement::Describe { name } => describe::handle_describe(handler_args, name),
        Statement::ShowObjects(show_object) => show::handle_show_object(handler_args, show_object),
        Statement::ShowCreateObject { create_type, name } => {
            show::handle_show_create_object(handler_args, create_type, name)
        }
        Statement::Drop(DropStatement {
            object_type,
            object_name,
            if_exists,
            drop_mode,
        }) => match object_type {
            ObjectType::Table => {
                drop_table::handle_drop_table(handler_args, object_name, if_exists).await
            }
            ObjectType::MaterializedView => {
                drop_mv::handle_drop_mv(handler_args, object_name, if_exists).await
            }
            ObjectType::Index => {
                drop_index::handle_drop_index(handler_args, object_name, if_exists).await
            }
            ObjectType::Source => {
                drop_source::handle_drop_source(handler_args, object_name, if_exists).await
            }
            ObjectType::Sink => {
                drop_sink::handle_drop_sink(handler_args, object_name, if_exists).await
            }
            ObjectType::Database => {
                drop_database::handle_drop_database(
                    handler_args,
                    object_name,
                    if_exists,
                    drop_mode.into(),
                )
                .await
            }
            ObjectType::Schema => {
                drop_schema::handle_drop_schema(
                    handler_args,
                    object_name,
                    if_exists,
                    drop_mode.into(),
                )
                .await
            }
            ObjectType::User => {
                drop_user::handle_drop_user(handler_args, object_name, if_exists, drop_mode.into())
                    .await
            }
            ObjectType::View => {
                drop_view::handle_drop_view(handler_args, object_name, if_exists).await
            }
        },
        Statement::DropFunction {
            if_exists,
            func_desc,
            option,
        } => drop_function::handle_drop_function(handler_args, if_exists, func_desc, option).await,
        Statement::Query(_)
        | Statement::Insert { .. }
        | Statement::Delete { .. }
        | Statement::Update { .. } => query::handle_query(handler_args, stmt, formats).await,
        Statement::CreateView {
            materialized,
            name,
            columns,
            query,

            with_options: _, // It is put in OptimizerContext
            or_replace,      // not supported
            emit_mode,
        } => {
            if or_replace {
                return Err(ErrorCode::NotImplemented(
                    "CREATE OR REPLACE VIEW".to_string(),
                    None.into(),
                )
                .into());
            }
            if emit_mode == Some(EmitMode::OnWindowClose) {
                return Err(ErrorCode::NotImplemented(
                    "CREATE MATERIALIZED VIEW EMIT ON WINDOW CLOSE".to_string(),
                    None.into(),
                )
                .into());
            }
            if materialized {
                create_mv::handle_create_mv(handler_args, name, *query, columns).await
            } else {
                create_view::handle_create_view(handler_args, name, columns, *query).await
            }
        }
        Statement::Flush => flush::handle_flush(handler_args).await,
        Statement::SetVariable {
            local: _,
            variable,
            value,
        } => variable::handle_set(handler_args, variable, value),
        Statement::ShowVariable { variable } => variable::handle_show(handler_args, variable).await,
        Statement::CreateIndex {
            name,
            table_name,
            columns,
            include,
            distributed_by,
            unique,
            if_not_exists,
        } => {
            if unique {
                return Err(
                    ErrorCode::NotImplemented("create unique index".into(), None.into()).into(),
                );
            }

            create_index::handle_create_index(
                handler_args,
                if_not_exists,
                name,
                table_name,
                columns.to_vec(),
                include,
                distributed_by,
            )
            .await
        }
        Statement::AlterTable {
            name,
            operation:
                operation @ (AlterTableOperation::AddColumn { .. }
                | AlterTableOperation::DropColumn { .. }),
        } => alter_table_column::handle_alter_table_column(handler_args, name, operation).await,
        Statement::AlterSystem { param, value } => {
            alter_system::handle_alter_system(handler_args, param, value).await
        }
        // Ignore `StartTransaction` and `BEGIN`,`Abort`,`Rollback`,`Commit`temporarily.Its not
        // final implementation.
        // 1. Fully support transaction is too hard and gives few benefits to us.
        // 2. Some client e.g. psycopg2 will use this statement.
        // TODO: Track issues #2595 #2541
        Statement::StartTransaction { .. } => Ok(PgResponse::empty_result_with_notice(
            START_TRANSACTION,
            "Ignored temporarily. See detail in issue#2541".to_string(),
        )),
        Statement::BEGIN { .. } => Ok(PgResponse::empty_result_with_notice(
            BEGIN,
            "Ignored temporarily. See detail in issue#2541".to_string(),
        )),
        Statement::Abort { .. } => Ok(PgResponse::empty_result_with_notice(
            ABORT,
            "Ignored temporarily. See detail in issue#2541".to_string(),
        )),
        Statement::Commit { .. } => Ok(PgResponse::empty_result_with_notice(
            COMMIT,
            "Ignored temporarily. See detail in issue#2541".to_string(),
        )),
        Statement::Rollback { .. } => Ok(PgResponse::empty_result_with_notice(
            ROLLBACK,
            "Ignored temporarily. See detail in issue#2541".to_string(),
        )),
        Statement::SetTransaction { .. } => Ok(PgResponse::empty_result_with_notice(
            SET_TRANSACTION,
            "Ignored temporarily. See detail in issue#2541".to_string(),
        )),
        _ => Err(
            ErrorCode::NotImplemented(format!("Unhandled statement: {}", stmt), None.into()).into(),
        ),
    }
}
