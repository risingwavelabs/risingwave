// Copyright 2025 RisingWave Labs
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

use std::fmt::Write;
use std::sync::Arc;

use risingwave_expr::{ExprError, Result, capture_context, function};
use risingwave_pb::meta::RecoveryStatus;

use super::context::META_CLIENT;
use crate::meta_client::FrontendMetaClient;

#[function("rw_recovery_status() -> varchar", volatile)]
async fn rw_recovery_status(writer: &mut impl Write) -> Result<()> {
    writer
        .write_str(
            rw_recovery_status_impl_captured()
                .await?
                .as_str_name()
                .strip_prefix("STATUS_")
                .unwrap(),
        )
        .unwrap();
    Ok(())
}

#[function("pg_is_in_recovery() -> boolean", volatile)]
async fn pg_is_in_recovery() -> Result<bool> {
    let status = rw_recovery_status_impl_captured().await?;
    Ok(status != RecoveryStatus::StatusRunning)
}

#[capture_context(META_CLIENT)]
async fn rw_recovery_status_impl(
    meta_client: &Arc<dyn FrontendMetaClient>,
) -> Result<RecoveryStatus> {
    meta_client
        .get_cluster_recovery_status()
        .await
        .map_err(|e| ExprError::Internal(e.into()))
}
