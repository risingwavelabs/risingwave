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
use risingwave_common::util::epoch::Epoch;
use risingwave_sqlparser::ast::{DeclareCursorStatement, Ident, ObjectName, Statement};

use super::query::handle_query;
use super::util::{
    convert_epoch_to_logstore_i64, convert_unix_millis_to_logstore_i64,
    gen_query_from_logstore_ge_rw_timestamp, gen_query_from_table_name,
};
use super::{HandlerArgs, RwPgResponse};
use crate::error::{ErrorCode, Result};
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
    let subscription =
        session.get_subscription_by_name(schema_name, &cursor_from_subscription_name)?;
    // Start the first query of cursor, which includes querying the table and querying the subscription's logstore
    let (start_rw_timestamp, res, is_snapshot) = match stmt.rw_timestamp {
        risingwave_sqlparser::ast::Since::TimestampMsNum(start_rw_timestamp) => {
            check_cursor_unix_millis(start_rw_timestamp, subscription.get_retention_seconds()?)?;
            let start_rw_timestamp = convert_unix_millis_to_logstore_i64(start_rw_timestamp);
            let query_stmt = Statement::Query(Box::new(gen_query_from_logstore_ge_rw_timestamp(
                &subscription.get_log_store_name()?,
                start_rw_timestamp,
            )));
            let res = handle_query(handle_args, query_stmt, formats).await?;
            (start_rw_timestamp, res, false)
        }
        risingwave_sqlparser::ast::Since::ProcessTime => {
            let start_rw_timestamp = convert_epoch_to_logstore_i64(Epoch::now().0);
            let query_stmt = Statement::Query(Box::new(gen_query_from_logstore_ge_rw_timestamp(
                &subscription.get_log_store_name()?,
                start_rw_timestamp,
            )));
            let res = handle_query(handle_args, query_stmt, formats).await?;
            (start_rw_timestamp, res, false)
        }
        risingwave_sqlparser::ast::Since::WithSnapshot => {
            let subscription_from_table_name = ObjectName(vec![Ident::from(
                subscription.subscription_from_name.as_ref(),
            )]);
            let query_stmt = Statement::Query(Box::new(gen_query_from_table_name(
                subscription_from_table_name,
            )));
            let res = handle_query(handle_args, query_stmt, formats).await?;
            let pinned_epoch = session
                .get_pinned_snapshot()
                .ok_or_else(|| {
                    ErrorCode::InternalError("Fetch Cursor can't find snapshot epoch".to_string())
                })?
                .epoch_with_frontend_pinned()
                .ok_or_else(|| {
                    ErrorCode::InternalError(
                        "Fetch Cursor can't support setting an epoch".to_string(),
                    )
                })?
                .0;
            (convert_epoch_to_logstore_i64(pinned_epoch), res, true)
        }
    };
    // Create cursor based on the response
    let cursor_manager = session.get_cursor_manager();
    let mut cursor_manager = cursor_manager.lock().await;
    cursor_manager
        .add_cursor(
            cursor_name.clone(),
            res,
            start_rw_timestamp,
            is_snapshot,
            false,
            stmt.cursor_from.clone(),
            subscription.get_retention_seconds()?,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::DECLARE_CURSOR))
}

fn check_cursor_unix_millis(unix_millis: u64, retention_seconds: u64) -> Result<()> {
    let now = Epoch::now().as_unix_millis();
    let min_unix_millis = now - retention_seconds * 1000;
    if unix_millis > now {
        return Err(ErrorCode::InternalError(format!(
            "rw_timestamp is too large, need to be less than the current unix_millis {:?}",
            now
        ))
        .into());
    }
    if unix_millis < min_unix_millis {
        return Err(ErrorCode::InternalError(
            format!("rw_timestamp is too small, need to be large than the current unix_millis {:?} - subscription's retention time {:?}",now,retention_seconds * 1000)).into());
    }
    Ok(())
}
