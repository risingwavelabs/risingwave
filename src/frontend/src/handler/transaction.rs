use pgwire::pg_response::StatementType;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{TransactionAccessMode, TransactionMode};

use super::{HandlerArgs, RwPgResponse};
use crate::session::transaction::AccessMode;

macro_rules! not_impl {
    ($body:expr) => {
        Err(ErrorCode::NotImplemented($body.into(), None.into()))
    };
}

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
                session.notice_to_user("Access mode is not specified, using default READ ONLY");
                AccessMode::ReadOnly
            }
        }
    };

    session.begin_explicit(access_mode);

    Ok(RwPgResponse::empty_result(stmt_type).into())
}

pub async fn handle_commit(
    handler_args: HandlerArgs,
    stmt_type: StatementType,
    chain: bool,
) -> Result<RwPgResponse> {
    let HandlerArgs { session, .. } = handler_args;

    if chain {
        not_impl!("COMMIT AND CHAIN")?;
    }

    session.end_explicit();

    Ok(RwPgResponse::empty_result(stmt_type).into())
}
