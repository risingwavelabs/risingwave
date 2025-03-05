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

use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use bytes::Bytes;
use pgwire::types::Format;
use risingwave_common::bail_not_implemented;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{CreateSink, Query, Statement};

use super::query::BoundResult;
use super::{HandlerArgs, RwPgResponse, fetch_cursor, handle, query};
use crate::error::Result;
use crate::session::SessionImpl;

/// Except for Query,Insert,Delete,Update statement, we store other statement as `PureStatement`.
/// We separate them because `PureStatement` don't have query and parameters (except
/// create-table-as, create-view-as, create-sink-as), so we don't need to do extra work(infer and
/// bind parameter) for them.
/// For create-table-as, create-view-as, create-sink-as with query parameters, we can't
/// support them. If we find that there are parameter in their query, we return a error otherwise we
/// store them as `PureStatement`.
#[derive(Clone)]
pub enum PrepareStatement {
    Empty,
    Prepared(PreparedResult),
    PureStatement(Statement),
}

#[derive(Clone)]
pub struct PreparedResult {
    pub statement: Statement,
    pub bound_result: BoundResult,
}

/// Partial was a concept in the PostgreSQL protocol.
///
/// In the extended-query protocol, execution of SQL commands is divided into multiple steps.
/// The state retained between steps is represented by two types of objects: prepared statements
/// and portals. A prepared statement represents the result of parsing and semantic analysis of a
/// textual query string. A prepared statement is not in itself ready to execute, because it might
/// lack specific values for parameters.
/// A portal represents a ready-to-execute or already-partially-executed statement,
/// with any missing parameter values filled in.
///
/// Reference: <https://www.postgresql.org/docs/current/protocol-overview.html#PROTOCOL-QUERY-CONCEPTS>
#[expect(clippy::enum_variant_names)]
#[derive(Clone)]
pub enum Portal {
    Empty,
    Portal(PortalResult),
    PureStatement(Statement),
}

impl std::fmt::Display for Portal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            Portal::Empty => write!(f, "Empty"),
            Portal::Portal(portal) => portal.fmt(f),
            Portal::PureStatement(stmt) => write!(f, "{}", stmt),
        }
    }
}

/// See the docs of [`Portal`].
#[derive(Clone)]
pub struct PortalResult {
    pub statement: Statement,
    pub bound_result: BoundResult,
    pub result_formats: Vec<Format>,
}

impl std::fmt::Display for PortalResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}, params = {:?}",
            self.statement, self.bound_result.parsed_params
        )
    }
}

pub async fn handle_parse(
    session: Arc<SessionImpl>,
    statement: Statement,
    specific_param_types: Vec<Option<DataType>>,
) -> Result<PrepareStatement> {
    session.clear_cancel_query_flag();
    let sql: Arc<str> = Arc::from(statement.to_string());
    let handler_args = HandlerArgs::new(session, &statement, sql)?;
    match &statement {
        Statement::Query(_)
        | Statement::Insert { .. }
        | Statement::Delete { .. }
        | Statement::Update { .. } => {
            query::handle_parse(handler_args, statement, specific_param_types)
        }
        Statement::FetchCursor { .. } => {
            fetch_cursor::handle_parse(handler_args, statement, specific_param_types).await
        }
        Statement::DeclareCursor { .. } => {
            query::handle_parse(handler_args, statement, specific_param_types)
        }
        Statement::CreateView {
            query,
            materialized,
            ..
        } => {
            if *materialized {
                return query::handle_parse(handler_args, statement, specific_param_types);
            }
            if have_parameter_in_query(query) {
                bail_not_implemented!("CREATE VIEW with parameters");
            }
            Ok(PrepareStatement::PureStatement(statement))
        }
        Statement::CreateTable { query, .. } => {
            if let Some(query) = query
                && have_parameter_in_query(query)
            {
                bail_not_implemented!("CREATE TABLE AS SELECT with parameters");
            } else {
                Ok(PrepareStatement::PureStatement(statement))
            }
        }
        Statement::CreateSink { stmt } => {
            if let CreateSink::AsQuery(query) = &stmt.sink_from
                && have_parameter_in_query(query)
            {
                bail_not_implemented!("CREATE SINK AS SELECT with parameters");
            } else {
                Ok(PrepareStatement::PureStatement(statement))
            }
        }
        _ => Ok(PrepareStatement::PureStatement(statement)),
    }
}

pub fn handle_bind(
    prepare_statement: PrepareStatement,
    params: Vec<Option<Bytes>>,
    param_formats: Vec<Format>,
    result_formats: Vec<Format>,
) -> Result<Portal> {
    match prepare_statement {
        PrepareStatement::Empty => Ok(Portal::Empty),
        PrepareStatement::Prepared(prepared_result) => {
            let PreparedResult {
                bound_result,
                statement,
            } = prepared_result;
            let BoundResult {
                stmt_type,
                must_dist,
                bound,
                param_types,
                dependent_relations,
                dependent_udfs,
                ..
            } = bound_result;

            let (new_bound, parsed_params) = bound.bind_parameter(params, param_formats)?;
            let new_bound_result = BoundResult {
                stmt_type,
                must_dist,
                param_types,
                parsed_params: Some(parsed_params),
                dependent_relations,
                dependent_udfs,
                bound: new_bound,
            };
            Ok(Portal::Portal(PortalResult {
                bound_result: new_bound_result,
                result_formats,
                statement,
            }))
        }
        PrepareStatement::PureStatement(stmt) => {
            // Jdbc might send set statements in a prepare statement, so params could be not empty.
            Ok(Portal::PureStatement(stmt))
        }
    }
}

pub async fn handle_execute(session: Arc<SessionImpl>, portal: Portal) -> Result<RwPgResponse> {
    match portal {
        Portal::Empty => Ok(RwPgResponse::empty_result(
            pgwire::pg_response::StatementType::EMPTY,
        )),
        Portal::Portal(portal) => {
            session.clear_cancel_query_flag();
            let _guard = session.txn_begin_implicit(); // TODO(bugen): is this behavior correct?
            let sql: Arc<str> = Arc::from(portal.statement.to_string());
            let handler_args = HandlerArgs::new(session, &portal.statement, sql)?;
            if let Statement::FetchCursor { .. } = &portal.statement {
                fetch_cursor::handle_fetch_cursor_execute(handler_args, portal).await
            } else {
                query::handle_execute(handler_args, portal).await
            }
        }
        Portal::PureStatement(stmt) => {
            let sql: Arc<str> = Arc::from(stmt.to_string());
            handle(session, stmt, sql, vec![]).await
        }
    }
}

/// A quick way to check if a query contains parameters.
fn have_parameter_in_query(query: &Query) -> bool {
    query.to_string().contains("$1")
}
