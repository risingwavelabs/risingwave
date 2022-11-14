// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use risingwave_pb::hummock::HummockSnapshot;
use risingwave_pb::meta::list_table_fragments_response::TableFragmentInfo;
use risingwave_rpc_client::error::Result;
use risingwave_rpc_client::{HummockMetaClient, MetaClient};

/// A wrapper around the `MetaClient` that only provides a minor set of meta rpc.
/// Most of the rpc to meta are delegated by other separate structs like `CatalogWriter`,
/// `WorkerNodeManager`, etc. So frontend rarely needs to call `MetaClient` directly.
/// Hence instead of to mock all rpc of `MetaClient` in tests, we aggregate those "direct" rpc
/// in this trait so that the mocking can be simplified.
#[async_trait::async_trait]
pub trait FrontendMetaClient: Send + Sync {
    async fn pin_snapshot(&self) -> Result<HummockSnapshot>;

    async fn get_epoch(&self) -> Result<HummockSnapshot>;

    async fn flush(&self, checkpoint: bool) -> Result<HummockSnapshot>;

    async fn list_table_fragments(
        &self,
        table_ids: &[u32],
    ) -> Result<HashMap<u32, TableFragmentInfo>>;

    async fn unpin_snapshot(&self) -> Result<()>;

    async fn unpin_snapshot_before(&self, epoch: u64) -> Result<()>;
}

pub struct FrontendMetaClientImpl(pub MetaClient);

#[async_trait::async_trait]
impl FrontendMetaClient for FrontendMetaClientImpl {
    async fn pin_snapshot(&self) -> Result<HummockSnapshot> {
        self.0.pin_snapshot().await
    }

    async fn get_epoch(&self) -> Result<HummockSnapshot> {
        self.0.get_epoch().await
    }

    async fn flush(&self, checkpoint: bool) -> Result<HummockSnapshot> {
        self.0.flush(checkpoint).await
    }

    async fn list_table_fragments(
        &self,
        table_ids: &[u32],
    ) -> Result<HashMap<u32, TableFragmentInfo>> {
        self.0.list_table_fragments(table_ids).await
    }

    async fn unpin_snapshot(&self) -> Result<()> {
        self.0.unpin_snapshot().await
    }

    async fn unpin_snapshot_before(&self, epoch: u64) -> Result<()> {
        self.0.unpin_snapshot_before(epoch).await
    }
}
