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

use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::{Format, Row};
use risingwave_sqlparser::ast::FetchCursorStatement;

use super::query::handle_query;
use super::util::gen_query_from_logstore_ge_rw_timestamp;
use super::{HandlerArgs, RwPgResponse};
use crate::error::{ErrorCode, Result};
use crate::session::cursor_manager::{Cursor, CursorRowValue};
use crate::{Binder, PgResponseStream};

pub async fn handle_fetch_cursor(
    handle_args: HandlerArgs,
    stmt: FetchCursorStatement,
    formats: Vec<Format>,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();
    let db_name = session.database();
    let (_, cursor_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.cursor_name.clone())?;
    let cursor_manager = session.get_cursor_manager();
    let mut cursor_manager = cursor_manager.lock().await;
    match cursor_manager
        .get_row_with_cursor(cursor_name.clone())
        .await?
    {
        CursorRowValue::Row((row, pg_descs)) => {
            return Ok(build_fetch_cursor_response(vec![row], pg_descs));
        }
        CursorRowValue::QueryWithNextRwTimestamp(rw_timestamp, subscription_name) => {
            let query_stmt =
                gen_query_from_logstore_ge_rw_timestamp(subscription_name.clone(), rw_timestamp)?;
            let res = handle_query(handle_args, query_stmt, formats).await?;

            let cursor = Cursor::new(
                cursor_name.clone(),
                res,
                rw_timestamp,
                false,
                true,
                subscription_name.clone(),
            )
            .await?;
            cursor_manager.update_cursor(cursor)?;
        }
        CursorRowValue::QueryWithStartRwTimestamp(rw_timestamp, subscription_name) => {
            let query_stmt = gen_query_from_logstore_ge_rw_timestamp(
                subscription_name.clone(),
                rw_timestamp + 1,
            )?;
            let res = handle_query(handle_args, query_stmt, formats).await?;

            let cursor = Cursor::new(
                cursor_name.clone(),
                res,
                rw_timestamp,
                false,
                false,
                subscription_name.clone(),
            )
            .await?;
            cursor_manager.update_cursor(cursor)?;
        }
    };

    match cursor_manager.get_row_with_cursor(cursor_name).await? {
        CursorRowValue::Row((row, pg_descs)) => {
            Ok(build_fetch_cursor_response(vec![row], pg_descs))
        }
        CursorRowValue::QueryWithStartRwTimestamp(_, _) => {
            Ok(build_fetch_cursor_response(vec![], vec![]))
        }
        CursorRowValue::QueryWithNextRwTimestamp(_, _) => Err(ErrorCode::InternalError(
            "Fetch cursor, one must get a row or null".to_string(),
        )
        .into()),
    }
}

fn build_fetch_cursor_response(rows: Vec<Row>, pg_descs: Vec<PgFieldDescriptor>) -> RwPgResponse {
    PgResponse::builder(StatementType::FETCH)
        .row_cnt_opt(Some(rows.len() as i32))
        .values(PgResponseStream::from(rows), pg_descs)
        .into()
}
