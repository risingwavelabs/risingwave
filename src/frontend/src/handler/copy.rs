use pgwire::pg_response::PgResponse;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{CopyRelation, CopyTarget, Statement};

use super::query::handle_query;
use crate::session::OptimizerContext;

pub async fn handle_copy(
    context: OptimizerContext,
    relation: CopyRelation,
    to: bool,
    target: CopyTarget,
) -> Result<PgResponse> {
    if !to {
        return Err(
            ErrorCode::InvalidInputSyntax("COPY FROM is unsupported yet".to_string()).into(),
        );
    }
    handle_copy_to(context, relation, target).await
}

async fn handle_copy_to(
    context: OptimizerContext,
    relation: CopyRelation,
    target: CopyTarget,
) -> Result<PgResponse> {
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
        CopyRelation::Query(query) => handle_query(context, Statement::Query(query), false).await,
    }
}
