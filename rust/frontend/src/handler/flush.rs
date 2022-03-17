use pgwire::pg_response::PgResponse;
use risingwave_common::error::Result;

use crate::session::QueryContext;

pub(super) async fn handle_flush(context: QueryContext) -> Result<PgResponse> {
    let client = context.session_ctx.env().meta_client();
    client.flush().await?;

    Ok(PgResponse::new(
        pgwire::pg_response::StatementType::FLUSH,
        0,
        vec![],
        vec![],
    ))
}
