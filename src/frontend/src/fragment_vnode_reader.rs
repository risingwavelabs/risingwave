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

use anyhow::{Context, Result};
use async_trait::async_trait;
use risingwave_common::fragment_vnode::{
    FragmentVNodeEntry, FragmentVNodeInfo, FragmentVNodeReader,
};

use crate::meta_client::FrontendMetaClient;

/// Default implementation of [`FragmentVNodeReader`] that issues RPCs via the frontend meta client.
pub struct FragmentVNodeReaderImpl {
    meta_client: Arc<dyn FrontendMetaClient>,
}

impl FragmentVNodeReaderImpl {
    pub fn new(meta_client: Arc<dyn FrontendMetaClient>) -> Self {
        Self { meta_client }
    }
}

#[async_trait]
impl FragmentVNodeReader for FragmentVNodeReaderImpl {
    async fn get_fragment_vnodes(&self, fragment_id: u32) -> Result<FragmentVNodeInfo> {
        let response = self
            .meta_client
            .get_fragment_vnodes(fragment_id)
            .await
            .with_context(|| format!("failed to fetch fragment {fragment_id} vnode mapping"))?;

        let actors = response
            .actors
            .into_iter()
            .map(|actor| FragmentVNodeEntry {
                actor_id: actor.actor_id,
                worker_id: actor.worker_id,
                vnodes: actor
                    .vnode_indices
                    .into_iter()
                    .map(|idx| idx as i16)
                    .collect(),
            })
            .collect();

        Ok(FragmentVNodeInfo {
            fragment_id: response.fragment_id,
            vnode_count: response.vnode_count as usize,
            actors,
        })
    }
}
