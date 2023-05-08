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

use std::sync::Arc;

use bytes::Bytes;
use pgwire::types::Format;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{CreateSink, Query, Statement};

use super::query::BoundResult;
use super::{handle, query, HandlerArgs, RwPgResponse};
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
    Prepared(PreparedResult),
    PureStatement(Statement),
}

#[derive(Clone)]
pub struct PreparedResult {
    pub statement: Statement,
    pub bound_result: BoundResult,
}

#[derive(Clone)]
pub enum Portal {
    Portal(PortalResult),
    PureStatement(Statement),
}

#[derive(Clone)]
pub struct PortalResult {
    pub statement: Statement,
    pub bound_result: BoundResult,
    pub result_formats: Vec<Format>,
}

pub fn handle_parse(
    session: Arc<SessionImpl>,
    statement: Statement,
    specific_param_types: Vec<DataType>,
) -> Result<PrepareStatement> {
    session.clear_cancel_query_flag();
    let str_sql = statement.to_string();
    let handler_args = HandlerArgs::new(session, &statement, &str_sql)?;
    match &statement {
        Statement::Query(_)
        | Statement::Insert { .. }
        | Statement::Delete { .. }
        | Statement::Update { .. } => {
            query::handle_parse(handler_args, statement, specific_param_types)
        }
        Statement::CreateView { query, .. } => {
            if have_parameter_in_query(query) {
                return Err(ErrorCode::NotImplemented(
                    "CREATE VIEW with parameters".to_string(),
                    None.into(),
                )
                .into());
            }
            Ok(PrepareStatement::PureStatement(statement))
        }
        Statement::CreateTable { query, .. } => {
            if let Some(query) = query && have_parameter_in_query(query) {
                Err(ErrorCode::NotImplemented(
                    "CREATE TABLE AS SELECT with parameters".to_string(),
                    None.into(),
                )
                .into())
            } else {
                Ok(PrepareStatement::PureStatement(statement))
            }
        }
        Statement::CreateSink { stmt } => {
            if let CreateSink::AsQuery(query) = &stmt.sink_from && have_parameter_in_query(query) {
                Err(ErrorCode::NotImplemented(
                    "CREATE SINK AS SELECT with parameters".to_string(),
                    None.into(),
                )
                .into())
            } else {
                Ok(PrepareStatement::PureStatement(statement))
            }
        }
        _ => Ok(PrepareStatement::PureStatement(statement)),
    }
}

pub fn handle_bind(
    prepare_statement: PrepareStatement,
    params: Vec<Bytes>,
    param_formats: Vec<Format>,
    result_formats: Vec<Format>,
) -> Result<Portal> {
    match prepare_statement {
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
            } = bound_result;

            let new_bound = bound.bind_parameter(params, param_formats)?;
            let new_bound_result = BoundResult {
                stmt_type,
                must_dist,
                param_types,
                dependent_relations,
                bound: new_bound,
            };
            Ok(Portal::Portal(PortalResult {
                bound_result: new_bound_result,
                result_formats,
                statement,
            }))
        }
        PrepareStatement::PureStatement(stmt) => {
            assert!(
                params.is_empty(),
                "params should be empty for pure statement"
            );
            Ok(Portal::PureStatement(stmt))
        }
    }
}

pub async fn handle_execute(session: Arc<SessionImpl>, portal: Portal) -> Result<RwPgResponse> {
    match portal {
        Portal::Portal(portal) => {
            session.clear_cancel_query_flag();
            let str_sql = portal.statement.to_string();
            let handler_args = HandlerArgs::new(session, &portal.statement, &str_sql)?;
            match &portal.statement {
                Statement::Query(_)
                | Statement::Insert { .. }
                | Statement::Delete { .. }
                | Statement::Update { .. } => query::handle_execute(handler_args, portal).await,
                _ => unreachable!(),
            }
        }
        Portal::PureStatement(stmt) => {
            let sql = stmt.to_string();
            handle(session, stmt, &sql, vec![]).await
        }
    }
}

/// A quick way to check if a query contains parameters.
fn have_parameter_in_query(query: &Query) -> bool {
    query.to_string().contains("$1")
}
