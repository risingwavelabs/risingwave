// Copyright 2026 RisingWave Labs
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

use std::future::Future;
use std::iter;
use std::pin::Pin;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::Shared;
use moka::sync::Cache;
use risingwave_common::id::TableId;
use risingwave_hummock_sdk::change_log::TableChangeLogs;
use risingwave_rpc_client::HummockMetaClient;

use crate::hummock::{HummockError, HummockResult};

type InflightResult = Shared<Pin<Box<dyn Future<Output = HummockResult<TableChangeLogs>> + Send>>>;

#[derive(Eq, Hash, PartialEq)]
struct CacheKey {
    table_id: TableId,
    epoch_range: (u64, u64),
    include_epoch_only: bool,
}

/// A naive cache to reduce number of RPC sent to meta node.
pub struct TableChangeLogManager {
    cache: Cache<CacheKey, InflightResult>,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
}

impl TableChangeLogManager {
    pub fn new(capacity: u64, hummock_meta_client: Arc<dyn HummockMetaClient>) -> Self {
        let cache = Cache::builder().max_capacity(capacity).build();
        Self {
            cache,
            hummock_meta_client,
        }
    }

    async fn get_or_insert(
        &self,
        table_id: TableId,
        epoch_range: (u64, u64),
        include_epoch_only: bool,
        fetch: impl Future<Output = HummockResult<TableChangeLogs>> + Send + 'static,
    ) -> HummockResult<TableChangeLogs> {
        self.cache
            .entry(CacheKey {
                table_id,
                epoch_range,
                include_epoch_only,
            })
            .or_insert_with_if(
                || fetch.boxed().shared(),
                |inflight| {
                    if let Some(result) = inflight.peek() {
                        return result.is_err();
                    }
                    false
                },
            )
            .value()
            .clone()
            .await
    }

    /// `epoch_range` start and end are both inclusive.
    pub async fn fetch_table_change_logs(
        &self,
        table_id: TableId,
        epoch_range: (u64, u64),
        include_epoch_only: bool,
    ) -> HummockResult<TableChangeLogs> {
        let hummock_meta_client = self.hummock_meta_client.clone();
        self.get_or_insert(table_id, epoch_range, include_epoch_only, async move {
            hummock_meta_client
                .get_table_change_logs(
                    include_epoch_only,
                    Some(epoch_range.0),
                    Some(epoch_range.1),
                    Some(iter::once(table_id).collect()),
                    false,
                    None,
                )
                .await
                .map_err(HummockError::meta_error)
        })
        .await
    }
}
