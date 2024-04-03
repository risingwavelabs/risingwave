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
use pgwire::types::Row;
use risingwave_sqlparser::ast::{FetchCursorStatement, ObjectName, Statement};

use super::declare_cursor::create_stream_for_cursor;
use super::util::gen_query_from_logstore_ge_rw_timestamp;
use super::RwPgResponse;
use crate::error::{ErrorCode, Result};
use crate::handler::HandlerArgs;
use crate::session::cursor_manager::CursorRowValue;
use crate::{Binder, PgResponseStream};

pub async fn handle_fetch_cursor(
    handle_args: HandlerArgs,
    stmt: FetchCursorStatement,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();
    let db_name = session.database();
    let (schema_name, cursor_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.cursor_name.clone())?;

    let cursor_manager = session.get_cursor_manager();

    let (cursors, pg_descs) = cursor_manager
        .get_row_with_cursor(cursor_name.clone(), stmt.count)
        .await?;
    let (rows, next_query_parameter) = covert_cursor_row_values(cursors);

    match next_query_parameter {
        // Try fetch data after update cursor
        // todo! support fetch count with subscription cursor
        Some((rw_timestamp, subscription_name, need_check_timestamp)) => {
            let subscription = session.get_subscription_by_name(
                schema_name,
                &subscription_name.0.last().unwrap().real_value().clone(),
            )?;
            let query_stmt = Statement::Query(Box::new(gen_query_from_logstore_ge_rw_timestamp(
                &subscription.get_log_store_name()?,
                rw_timestamp,
            )));
            let (row_stream, pg_descs) = create_stream_for_cursor(handle_args, query_stmt).await?;
            cursor_manager
                .update_subscription_cursor(
                    cursor_name.clone(),
                    row_stream,
                    pg_descs,
                    rw_timestamp,
                    false,
                    need_check_timestamp,
                    subscription_name.clone(),
                    subscription.get_retention_seconds()?,
                )
                .await?;
            let (cursors, pg_descs) = cursor_manager
                .get_row_with_cursor(cursor_name.clone(), stmt.count)
                .await?;
            let (rows, next_query_parameter) = covert_cursor_row_values(cursors);
            if let Some((_, _, true)) = next_query_parameter {
                Err(ErrorCode::InternalError(
                    "Fetch cursor, one must get a row or null".to_string(),
                )
                .into())
            } else {
                Ok(build_fetch_cursor_response(rows, pg_descs))
            }
        }
        None => Ok(build_fetch_cursor_response(rows, pg_descs)),
    }
}

fn covert_cursor_row_values(
    cursors: Vec<CursorRowValue>,
) -> (Vec<Row>, Option<(i64, ObjectName, bool)>) {
    let mut rows = Vec::with_capacity(cursors.len());
    for cursor_row_value in cursors {
        match cursor_row_value {
            CursorRowValue::Row(row) => rows.push(row),
            CursorRowValue::QueryWithNextRwTimestamp(rw_timestamp, subscription_name) => {
                return (rows, Some((rw_timestamp, subscription_name, true)))
            }
            CursorRowValue::QueryWithStartRwTimestamp(rw_timestamp, subscription_name) => {
                return (rows, Some((rw_timestamp + 1, subscription_name, false)))
            }
        }
    }
    (rows, None)
}

fn build_fetch_cursor_response(rows: Vec<Row>, pg_descs: Vec<PgFieldDescriptor>) -> RwPgResponse {
    PgResponse::builder(StatementType::FETCH_CURSOR)
        .row_cnt_opt(Some(rows.len() as i32))
        .values(PgResponseStream::from(rows), pg_descs)
        .into()
}
