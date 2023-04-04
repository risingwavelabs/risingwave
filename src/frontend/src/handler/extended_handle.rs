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
use risingwave_sqlparser::ast::{Query, Statement};

use super::{handle, query, HandlerArgs, RwPgResponse};
use crate::binder::BoundStatement;
use crate::session::SessionImpl;

#[derive(Clone)]
pub enum PrepareStatement {
    Prepared(PreparedResult),
    PureStatement(Statement),
}

#[derive(Clone)]
pub struct PreparedResult {
    pub statement: Statement,
    pub bound_statement: BoundStatement,
    pub param_types: Vec<DataType>,
}

#[derive(Clone)]
pub enum Portal {
    Portal(PortalResult),
    PureStatement(Statement),
}

#[derive(Clone)]
pub struct PortalResult {
    pub statement: Statement,
    pub bound_statement: BoundStatement,
    pub result_formats: Vec<Format>,
}

pub fn handle_parse(
    session: Arc<SessionImpl>,
    stmt: Statement,
    specific_param_types: Vec<DataType>,
) -> Result<PrepareStatement> {
    session.clear_cancel_query_flag();
    let str_sql = stmt.to_string();
    let handler_args = HandlerArgs::new(session, &stmt, &str_sql)?;
    match &stmt {
        Statement::Query(_)
        | Statement::Insert { .. }
        | Statement::Delete { .. }
        | Statement::Update { .. } => query::handle_parse(handler_args, stmt, specific_param_types),
        Statement::CreateView {
            query,
            ..
        } => {
            if have_parameter_in_query(query) {
                return Err(ErrorCode::NotImplemented(
                    "CREATE VIEW with parameters".to_string(),
                    None.into(),
                )
                .into());
            }
            Ok(PrepareStatement::PureStatement(stmt))
        }
        Statement::CreateTable {
            query,
            ..
        } => {
            if let Some(query) = query && have_parameter_in_query(query) {
                Err(ErrorCode::NotImplemented(
                    "CREATE TABLE AS SELECT with parameters".to_string(),
                    None.into(),
                ).into())
            } else {
                Ok(PrepareStatement::PureStatement(stmt))
            }
        }
        _ => Ok(PrepareStatement::PureStatement(stmt)),
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
                statement,
                bound_statement,
                ..
            } = prepared_result;
            let bound_statement = bound_statement.bind_parameter(params, param_formats)?;
            Ok(Portal::Portal(PortalResult {
                statement,
                bound_statement,
                result_formats,
            }))
        }
        PrepareStatement::PureStatement(stmt) => Ok(Portal::PureStatement(stmt)),
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
