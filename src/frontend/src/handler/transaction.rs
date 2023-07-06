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
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{TransactionAccessMode, TransactionMode};

use super::{HandlerArgs, RwPgResponse};
use crate::session::transaction::AccessMode;

macro_rules! not_impl {
    ($body:expr) => {
        Err(ErrorCode::NotImplemented($body.into(), 10736.into()))
    };
}

#[expect(clippy::unused_async)]
pub async fn handle_begin(
    handler_args: HandlerArgs,
    stmt_type: StatementType,
    modes: Vec<TransactionMode>,
) -> Result<RwPgResponse> {
    let HandlerArgs { session, .. } = handler_args;

    let access_mode = {
        let mut access_mode = None;
        for mode in modes {
            match mode {
                TransactionMode::AccessMode(mode) => {
                    let _ = access_mode.replace(mode);
                }
                TransactionMode::IsolationLevel(_) => not_impl!("ISOLATION LEVEL")?,
            }
        }

        match access_mode {
            Some(TransactionAccessMode::ReadOnly) => AccessMode::ReadOnly,
            Some(TransactionAccessMode::ReadWrite) => not_impl!("READ WRITE")?,
            None => {
                session.notice_to_user("access mode is not specified, using default READ ONLY");
                AccessMode::ReadOnly
            }
        }
    };

    session.txn_begin_explicit(access_mode);

    // TODO: pgwire integration of the transaction state
    Ok(RwPgResponse::empty_result(stmt_type))
}

#[expect(clippy::unused_async)]
pub async fn handle_commit(
    handler_args: HandlerArgs,
    stmt_type: StatementType,
    chain: bool,
) -> Result<RwPgResponse> {
    let HandlerArgs { session, .. } = handler_args;

    if chain {
        not_impl!("COMMIT AND CHAIN")?;
    }

    session.txn_commit_explicit();

    // TODO: pgwire integration of the transaction state
    Ok(RwPgResponse::empty_result(stmt_type))
}

#[expect(clippy::unused_async)]
pub async fn handle_rollback(
    handler_args: HandlerArgs,
    stmt_type: StatementType,
    chain: bool,
) -> Result<RwPgResponse> {
    let HandlerArgs { session, .. } = handler_args;

    if chain {
        not_impl!("ROLLBACK AND CHAIN")?;
    }

    session.txn_rollback_explicit();

    // TODO: pgwire integration of the transaction state
    Ok(RwPgResponse::empty_result(stmt_type))
}
