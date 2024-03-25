// Copyright 2024 RisingWave Labs
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
use pgwire::types::Format;
use risingwave_sqlparser::ast::{ObjectName, Query, Statement};

use super::query::{gen_batch_plan_by_statement, gen_batch_plan_fragmenter};
use super::RwPgResponse;
use crate::error::Result;
use crate::handler::HandlerArgs;
use crate::session::cursor::Cursor;
use crate::OptimizerContext;

pub async fn handle_declare_cursor(
    handler_args: HandlerArgs,
    _binary: bool,
    cursor_name: ObjectName,
    query: Query,
    formats: Vec<Format>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    if !session.is_in_transaction() {
        return Err(crate::error::ErrorCode::InternalError(
            "DECLARE CURSOR can only be used in transaction blocks".to_string(),
        )
        .into());
    }
    let plan_fragmenter_result = {
        let context = OptimizerContext::from_handler_args(handler_args);
        let plan_result = gen_batch_plan_by_statement(
            &session,
            context.into(),
            Statement::Query(Box::new(query.clone())),
        )?;
        gen_batch_plan_fragmenter(&session, plan_result)?
    };

    let cursor = Cursor::new(plan_fragmenter_result, formats, session.clone());
    // cursor.forward_all(session.clone()).await?;
    session.add_cursor(cursor_name, cursor).await?;
    Ok(PgResponse::empty_result(StatementType::DECLARE_CURSOR))
}

pub async fn handle_cursor_fetch(
    handler_args: HandlerArgs,
    cursor_name: ObjectName,
    count: Option<i32>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let rows = session.curosr_next(&cursor_name, count).await?;
    let pg_descs = session.pg_descs(&cursor_name).await?;
    Ok(PgResponse::builder(StatementType::CURSOR_FETCH)
        .values(rows.into(), pg_descs)
        .into())
}

pub async fn handle_close_cursor(
    handler_args: HandlerArgs,
    cursor_name: Option<ObjectName>,
) -> Result<RwPgResponse> {
    if let Some(name) = cursor_name {
        handler_args.session.drop_cursor(name).await?;
    } else {
        handler_args.session.drop_all_cursors().await;
    }
    Ok(PgResponse::empty_result(StatementType::CLOSE_CURSOR))
}
