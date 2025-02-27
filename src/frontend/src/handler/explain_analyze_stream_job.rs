use crate::error::Result;
use crate::handler::{HandlerArgs, RwPgResponse};

pub async fn handle_explain_analyze_stream_job(
    handler_args: HandlerArgs,
    job_id: u32,
) -> Result<RwPgResponse> {
    todo!()
}
