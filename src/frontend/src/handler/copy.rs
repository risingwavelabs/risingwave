use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{CopyRelation, CopyTarget, Statement};

use super::query::handle_query;
use super::{HandlerArgs, RwPgResponse};

pub async fn handle_copy(
    handler_args: HandlerArgs,
    relation: CopyRelation,
    to: bool,
    target: CopyTarget,
) -> Result<RwPgResponse> {
    if !to {
        return Err(
            ErrorCode::InvalidInputSyntax("COPY FROM is unsupported yet".to_string()).into(),
        );
    }
    handle_copy_to(handler_args, relation, target).await
}

async fn handle_copy_to(
    handler_args: HandlerArgs,
    relation: CopyRelation,
    target: CopyTarget,
) -> Result<RwPgResponse> {
    match target {
        CopyTarget::Stdin => {
            return Err(ErrorCode::InvalidInputSyntax(
                "stdin is not a valid COPY TO target".to_string(),
            )
            .into());
        }
        CopyTarget::Stdout => {}
    };
    match relation {
        CopyRelation::Table { .. } => Err(ErrorCode::InvalidInputSyntax(
            "copying a table is unsupported yet".to_string(),
        )
        .into()),
        CopyRelation::Query(query) => {
            handle_query(handler_args, Statement::Query(query), vec![]).await
        }
    }
}
