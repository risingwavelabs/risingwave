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

use pgwire::pg_response::StatementType;
use risingwave_common::bail_not_implemented;
use risingwave_common::types::Fields;
use risingwave_sqlparser::ast::{TransactionAccessMode, TransactionMode, Value};

use super::{HandlerArgs, RwPgResponse, RwPgResponseBuilderExt};
use crate::error::Result;
use crate::session::transaction::AccessMode;

macro_rules! not_impl {
    ($body:expr) => {
        bail_not_implemented!(issue = 10376, "{}", $body)
    };
}

#[expect(clippy::unused_async)]
pub async fn handle_begin(
    handler_args: HandlerArgs,
    stmt_type: StatementType,
    modes: Vec<TransactionMode>,
) -> Result<RwPgResponse> {
    let HandlerArgs { session, .. } = handler_args;

    let mut builder = RwPgResponse::builder(stmt_type);

    let access_mode = {
        let mut access_mode = None;
        for mode in modes {
            match mode {
                TransactionMode::AccessMode(mode) => {
                    let _ = access_mode.replace(mode);
                }
                TransactionMode::IsolationLevel(_) => {
                    // Note: This is for compatibility with some external drivers (like postgres_fdw) that
                    // always start a transaction with an Isolation Level.
                    const MESSAGE: &str = "\
                        Transaction with given Isolation Level is not supported yet.\n\
                        For compatibility, this statement will proceed with RepeatableRead.";
                    builder = builder.notice(MESSAGE);
                }
            }
        }

        match access_mode {
            Some(TransactionAccessMode::ReadOnly) => AccessMode::ReadOnly,
            Some(TransactionAccessMode::ReadWrite) | None => {
                // Note: This is for compatibility with some external drivers (like psycopg2) that
                // issue `BEGIN` implicitly for users. Not actually starting a transaction is okay
                // since `COMMIT` and `ROLLBACK` are no-ops (except for warnings) when there is no
                // active transaction.
                const MESSAGE: &str = "\
                    Read-write transaction is not supported yet. Please specify `READ ONLY` to start a read-only transaction.\n\
                    For compatibility, this statement will still succeed but no transaction is actually started.";
                builder = builder.notice(MESSAGE);
                return Ok(builder.into());
            }
        }
    };

    session.txn_begin_explicit(access_mode);
    Ok(builder.into())
}

pub async fn handle_commit(
    handler_args: HandlerArgs,
    stmt_type: StatementType,
    chain: bool,
) -> Result<RwPgResponse> {
    let HandlerArgs { session, .. } = handler_args;

    if chain {
        not_impl!("COMMIT AND CHAIN");
    }

    session.txn_commit_explicit();
    session.get_cursor_manager().remove_all_query_cursor().await;

    Ok(RwPgResponse::empty_result(stmt_type))
}

pub async fn handle_rollback(
    handler_args: HandlerArgs,
    stmt_type: StatementType,
    chain: bool,
) -> Result<RwPgResponse> {
    let HandlerArgs { session, .. } = handler_args;

    if chain {
        not_impl!("ROLLBACK AND CHAIN");
    }

    session.txn_rollback_explicit();
    session.get_cursor_manager().remove_all_query_cursor().await;

    Ok(RwPgResponse::empty_result(stmt_type))
}

#[expect(clippy::unused_async)]
pub async fn handle_set(
    _handler_args: HandlerArgs,
    _modes: Vec<TransactionMode>,
    _snapshot: Option<Value>,
    _session: bool,
) -> Result<RwPgResponse> {
    const MESSAGE: &str = "\
        `SET TRANSACTION` is not supported yet.\n\
        For compatibility, this statement will still succeed but no changes are actually made.";

    Ok(RwPgResponse::builder(StatementType::SET_TRANSACTION)
        .notice(MESSAGE)
        .into())
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowVariableRow {
    name: String,
}

pub fn handle_show_isolation_level(handler_args: HandlerArgs) -> Result<RwPgResponse> {
    let config_reader = handler_args.session.config();

    let rows = [ShowVariableRow {
        name: config_reader.get("transaction_isolation")?,
    }];

    Ok(RwPgResponse::builder(StatementType::SHOW_VARIABLE)
        .rows(rows)
        .into())
}
