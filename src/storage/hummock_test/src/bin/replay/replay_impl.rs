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

use std::ops::Bound;

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_hummock_trace::{ReplayIter, Replayable, Result, TraceError};
use risingwave_meta::manager::NotificationManagerRef;
use risingwave_meta::storage::MemStore;
use risingwave_pb::meta::subscribe_response::{Info, Operation as RespOperation};
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::{ReadOptions, SyncResult, WriteOptions};
use risingwave_storage::{StateStore, StateStoreIter};

pub(crate) struct HummockReplayIter<I: StateStoreIter<Item = (Bytes, Bytes)>>(I);

impl<I: StateStoreIter<Item = (Bytes, Bytes)> + Send + Sync> HummockReplayIter<I> {
    fn new(iter: I) -> Self {
        Self(iter)
    }
}

#[async_trait::async_trait]
impl<I: StateStoreIter<Item = (Bytes, Bytes)> + Send + Sync> ReplayIter for HummockReplayIter<I> {
    async fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        let key_value: Option<(Bytes, Bytes)> = self.0.next().await.unwrap();
        key_value.map(|(key, value)| (key.to_vec(), value.to_vec()))
    }
}

pub(crate) struct HummockInterface {
    store: HummockStorage,
    notifier: NotificationManagerRef<MemStore>,
}

impl HummockInterface {
    pub(crate) fn new(store: HummockStorage, notifier: NotificationManagerRef<MemStore>) -> Self {
        Self { store, notifier }
    }
}

#[async_trait::async_trait]
impl Replayable for HummockInterface {
    async fn get(
        &self,
        key: Vec<u8>,
        check_bloom_filter: bool,
        epoch: u64,
        table_id: u32,
        retention_seconds: Option<u32>,
    ) -> Result<Option<Vec<u8>>> {
        let value = self
            .store
            .get(
                &key,
                check_bloom_filter,
                ReadOptions {
                    epoch,
                    table_id: TableId { table_id },
                    retention_seconds,
                },
            )
            .await
            .map_err(|_| TraceError::GetFailed)?;

        Ok(value.map(|b| b.to_vec()))
    }

    async fn ingest(
        &self,
        mut kv_pairs: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        epoch: u64,
        table_id: u32,
    ) -> Result<usize> {
        let kv_pairs = kv_pairs
            .drain(..)
            .map(|(key, value)| {
                (
                    Bytes::from(key),
                    StorageValue {
                        user_value: value.map(Bytes::from),
                    },
                )
            })
            .collect();
        let size = self
            .store
            .ingest_batch(
                kv_pairs,
                WriteOptions {
                    epoch,
                    table_id: TableId { table_id },
                },
            )
            .await
            .map_err(|_| TraceError::IngestFailed)?;
        Ok(size)
    }

    async fn iter(
        &self,
        prefix_hint: Option<Vec<u8>>,
        left_bound: Bound<Vec<u8>>,
        right_bound: Bound<Vec<u8>>,
        epoch: u64,
        table_id: u32,
        retention_seconds: Option<u32>,
    ) -> Result<Box<dyn ReplayIter>> {
        let iter = self
            .store
            .iter(
                prefix_hint,
                (left_bound.clone(), right_bound.clone()),
                ReadOptions {
                    epoch,
                    table_id: TableId { table_id },
                    retention_seconds,
                },
            )
            .await
            .map_err(|e| TraceError::IterFailed(format!("{e}")))?;

        let iter = HummockReplayIter::new(iter);
        Ok(Box::new(iter))
    }

    async fn sync(&self, id: u64) -> Result<usize> {
        let result: SyncResult = self
            .store
            .sync(id)
            .await
            .map_err(|_| TraceError::SyncFailed)?;
        Ok(result.sync_size)
    }

    async fn seal_epoch(&self, epoch_id: u64, is_checkpoint: bool) {
        self.store.seal_epoch(epoch_id, is_checkpoint);
    }

    async fn notify_hummock(&self, info: Info, op: RespOperation) -> Result<u64> {
        let prev_version_id = match &info {
            Info::HummockVersionDeltas(deltas) => deltas.version_deltas.last().map(|d| d.prev_id),
            _ => None,
        };

        let version = self.notifier.notify_hummock(op, info).await;

        // wait till version updated
        if let Some(prev_version_id) = prev_version_id {
            self.store.wait_version_update(prev_version_id).await;
        }
        Ok(version)
    }
}
