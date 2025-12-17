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

use risingwave_expr::{Result, capture_context, function};

use super::context::META_CLIENT;
use crate::meta_client::FrontendMetaClient;

#[function("rw_cluster_id() -> varchar", volatile)]
async fn rw_cluster_id(writer: &mut impl std::fmt::Write) -> Result<()> {
    writer
        .write_str(&rw_cluster_id_impl_captured().await?)
        .unwrap();
    Ok(())
}

#[capture_context(META_CLIENT)]
async fn rw_cluster_id_impl(meta_client: &Arc<dyn FrontendMetaClient>) -> Result<String> {
    Ok(meta_client.cluster_id().to_owned())
}
