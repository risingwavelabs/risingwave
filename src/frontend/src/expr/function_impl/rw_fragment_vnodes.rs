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

use std::sync::Arc;

use risingwave_common::types::JsonbVal;
use risingwave_expr::{ExprError, Result, capture_context, function};
use serde_json::json;

use super::context::META_CLIENT;
use crate::meta_client::FrontendMetaClient;

#[function("rw_fragment_vnodes(int4) -> jsonb", volatile)]
async fn rw_fragment_vnodes(fragment_id: i32) -> Result<JsonbVal> {
    rw_fragment_vnodes_impl_captured(fragment_id).await
}

#[capture_context(META_CLIENT)]
async fn rw_fragment_vnodes_impl(
    meta_client: &Arc<dyn FrontendMetaClient>,
    fragment_id: i32,
) -> Result<JsonbVal> {
    let actors = meta_client
        .get_fragment_vnodes(fragment_id as u32)
        .await
        .map_err(|e| ExprError::Internal(e.into()))?;

    let result: serde_json::Map<String, serde_json::Value> = actors
        .into_iter()
        .map(|(actor_id, vnode_indices)| (actor_id.to_string(), json!(vnode_indices)))
        .collect();

    Ok(json!(result).into())
}
