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
use risingwave_sqlparser::ast::{DeclareCursorStatement, Ident, ObjectName, Statement};

use super::query::handle_query;
use super::util::gen_query_from_table_name;
use super::{HandlerArgs, RwPgResponse};
use crate::error::Result;
use crate::session::cursor_manager::Cursor;
use crate::Binder;

pub async fn handle_declare_cursor(
    handle_args: HandlerArgs,
    stmt: DeclareCursorStatement,
    formats: Vec<Format>,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();
    let db_name = session.database();
    let (schema_name, cursor_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.cursor_name.clone())?;

    let cursor_from_subscription_name = stmt.cursor_from.0.last().unwrap().real_value().clone();
    let start_rw_timestamp = stmt
        .rw_timestamp
        .0
        .map(|ident| ident.real_value().parse::<u64>().unwrap())
        .unwrap_or_else(|| 0);
    let is_snapshot = start_rw_timestamp == 0;
    let subscription =
        session.get_subscription_by_name(schema_name, &cursor_from_subscription_name)?;
    let retention_seconds = subscription.get_retention_seconds()?;
    if is_snapshot {
    } else {
        todo!()
    }
    let subscription_from_table_name = ObjectName(vec![Ident::from(
        subscription.subscription_from_name.as_ref(),
    )]);
    let query_stmt = Statement::Query(Box::new(gen_query_from_table_name(
        subscription_from_table_name,
    )?));

    let res = handle_query(handle_args, query_stmt, formats).await?;
    let cursor = Cursor::new(
        cursor_name.clone(),
        res,
        start_rw_timestamp,
        true,
        stmt.cursor_from.clone(),
    )
    .await?;
    session
        .get_cursor_manager()
        .lock()
        .await
        .add_cursor(cursor)?;

    Ok(PgResponse::empty_result(StatementType::DECLARE_CURSOR))
}
