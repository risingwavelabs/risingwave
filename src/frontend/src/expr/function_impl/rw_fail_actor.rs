use risingwave_expr::{function, ExprError, Result};

use super::context::META_CLIENT;

#[function("rw_fail_actor(varchar) -> int8", volatile)]
async fn rw_fail_actor(identity_contains: &str) -> Result<i64> {
    let meta_client = META_CLIENT::try_with(|c| c.clone())?;

    meta_client
        .fail_actor(identity_contains)
        .await
        .map_err(|e| ExprError::Internal(e.into()))?;

    Ok(233)
}
