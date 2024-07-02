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

use std::sync::Arc;

use risingwave_expr::{capture_context, function, Result};

use super::context::META_CLIENT;
use crate::meta_client::FrontendMetaClient;

#[function("pg_is_in_recovery() -> boolean", volatile)]
async fn pg_is_in_recovery() -> Result<bool> {
    pg_is_in_recovery_impl_captured().await
}

#[capture_context(META_CLIENT)]
async fn pg_is_in_recovery_impl(meta_client: &Arc<dyn FrontendMetaClient>) -> Result<bool> {
    Ok(meta_client
        .check_cluster_in_recovery()
        .await
        .unwrap_or(true))
}
