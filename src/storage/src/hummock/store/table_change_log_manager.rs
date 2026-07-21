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
use risingwave_hummock_sdk::change_log::{TableChangeLog, TableChangeLogs};
use risingwave_rpc_client::HummockMetaClient;

use crate::hummock::{HummockError, HummockResult};
use crate::monitor::HummockStateStoreMetrics;

type InflightResult = Shared<Pin<Box<dyn Future<Output = HummockResult<TableChangeLogs>> + Send>>>;

#[derive(Clone, Eq, Hash, PartialEq)]
struct CacheKey {
    table_id: TableId,
    epoch_range: (u64, u64),
    include_epoch_only: bool,
    limit: Option<u32>,
}

/// A naive cache to reduce number of RPC sent to meta node.
pub struct TableChangeLogManager {
    cache: Cache<CacheKey, InflightResult>,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    metrics: Arc<HummockStateStoreMetrics>,
}

impl TableChangeLogManager {
    pub fn new(
        capacity: u64,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        metrics: Arc<HummockStateStoreMetrics>,
    ) -> Self {
        let cache = Cache::builder().max_capacity(capacity).build();
        Self {
            cache,
            hummock_meta_client,
            metrics,
        }
    }

    async fn get_or_insert(
        &self,
        table_id: TableId,
        epoch_range: (u64, u64),
        include_epoch_only: bool,
        limit: Option<u32>,
        fetch: impl Future<Output = HummockResult<TableChangeLogs>> + Send + 'static,
    ) -> HummockResult<TableChangeLogs> {
        let entry = self
            .cache
            .entry(CacheKey {
                table_id,
                epoch_range,
                include_epoch_only,
                limit,
            })
            .or_insert_with_if(
                || fetch.boxed().shared(),
                |inflight| {
                    if let Some(result) = inflight.peek() {
                        return result.is_err();
                    }
                    false
                },
            );
        if entry.is_fresh() {
            self.metrics.table_change_log_cache_miss.inc();
        } else {
            self.metrics.table_change_log_cache_hit.inc();
        }
        entry.value().clone().await
    }

    fn filter_table_change_logs(
        table_change_logs: TableChangeLogs,
        table_id: TableId,
        epoch_range: (u64, u64),
    ) -> TableChangeLogs {
        table_change_logs
            .get(&table_id)
            .map(|change_log| {
                (
                    table_id,
                    TableChangeLog::new(change_log.filter_epoch(epoch_range).cloned()),
                )
            })
            .into_iter()
            .collect()
    }

    fn table_change_logs_cover_epoch_range(
        table_change_logs: &TableChangeLogs,
        table_id: TableId,
        epoch_range: (u64, u64),
    ) -> bool {
        table_change_logs
            .get(&table_id)
            .and_then(|change_log| change_log.filter_epoch(epoch_range).last())
            .is_some_and(|change_log| change_log.epochs().any(|epoch| epoch == epoch_range.1))
    }

    async fn get_cached_covering_range(
        &self,
        table_id: TableId,
        epoch_range: (u64, u64),
        include_epoch_only: bool,
        limit: Option<u32>,
    ) -> Option<HummockResult<TableChangeLogs>> {
        if limit.is_some() {
            return None;
        }

        let candidates = self
            .cache
            .iter()
            .filter_map(|entry| {
                let (key, inflight) = entry;
                if key.table_id == table_id
                    && key.epoch_range.0 <= epoch_range.0
                    && key.include_epoch_only == include_epoch_only
                    && (key.limit.is_some() || epoch_range.1 <= key.epoch_range.1)
                {
                    if let Some(result) = inflight.peek()
                        && result.is_err()
                    {
                        return None;
                    }
                    Some((key.clone(), inflight.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for (key, inflight) in candidates {
            let result = inflight.await;
            if key.limit.is_none()
                || result.as_ref().is_ok_and(|table_change_logs| {
                    Self::table_change_logs_cover_epoch_range(
                        table_change_logs,
                        table_id,
                        epoch_range,
                    )
                })
            {
                self.metrics.table_change_log_cache_hit.inc();
                return Some(result.map(|table_change_logs| {
                    Self::filter_table_change_logs(table_change_logs, table_id, epoch_range)
                }));
            }
        }
        None
    }

    /// Fetches table change logs for the given `table_id` and `epoch_range`.
    ///
    /// - If the end value of `epoch_range` is not `u64::MAX`, attempts to retrieve logs from the cache; if not cached, fetches via an RPC to the meta node and stores the result in the cache.
    /// - If the end value of `epoch_range` is `u64::MAX`, always fetches table change logs directly from the meta node (bypassing the cache).
    ///
    /// Both the start and end values of `epoch_range` are inclusive.
    ///
    /// IMPORTANT: The caller must guarantee that the current max committed epoch is at least as large as the end of the provided `epoch_range`, if it's not `u64::MAX`.
    /// Otherwise, the cache may serve outdated results: as new epochs are committed beyond the current maximum, subsequent RPC calls for the same `epoch_range`
    /// could retrieve different or additional change logs that were not present in the previously cached result. For example, if you request logs for
    /// `epoch_range = (1, N)` when the current max committed epoch is `M < N`, committing epochs `M+1` through `N` would make the cache inconsistent with reality;
    /// future fetches for `(1, N)` could return new or updated information absent from the previous cache entry.
    pub async fn fetch_table_change_logs(
        &self,
        table_id: TableId,
        epoch_range: (u64, u64),
        include_epoch_only: bool,
        limit: Option<u32>,
    ) -> HummockResult<TableChangeLogs> {
        let _timer = self.metrics.table_change_log_fetch_latency.start_timer();
        if epoch_range.1 != u64::MAX
            && let Some(result) = self
                .get_cached_covering_range(table_id, epoch_range, include_epoch_only, limit)
                .await
        {
            return result;
        }

        let hummock_meta_client = self.hummock_meta_client.clone();
        let fetch = async move {
            hummock_meta_client
                .get_table_change_logs(
                    include_epoch_only,
                    Some(epoch_range.0),
                    Some(epoch_range.1),
                    Some(iter::once(table_id).collect()),
                    false,
                    limit,
                )
                .await
                .map_err(HummockError::meta_error)
        };
        if epoch_range.1 == u64::MAX && limit.is_none() {
            return fetch.await;
        }
        self.get_or_insert(table_id, epoch_range, include_epoch_only, limit, fetch)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use futures::stream::BoxStream;
    use risingwave_hummock_sdk::change_log::{EpochNewChangeLog, TableChangeLog};
    use risingwave_hummock_sdk::version::HummockVersion;
    use risingwave_hummock_sdk::{
        CompactionGroupId, HummockEpoch, HummockVersionId, ObjectIdRange, SyncResult,
    };
    use risingwave_pb::hummock::{PbHummockVersion, SubscribeCompactionEventRequest};
    use risingwave_pb::iceberg_compaction::SubscribeIcebergCompactionEventRequest;
    use risingwave_pb::id::{HummockSstableId, JobId};
    use risingwave_rpc_client::error::Result;
    use risingwave_rpc_client::{
        CompactionEventItem, HummockMetaClientChangeLogInfo, IcebergCompactionEventItem,
    };
    use tokio::sync::mpsc::UnboundedSender;

    use super::*;

    struct TestHummockMetaClient {
        table_id: TableId,
        fetch_count: AtomicUsize,
        requested_ranges: Mutex<Vec<(Option<u64>, Option<u64>)>>,
    }

    impl TestHummockMetaClient {
        fn new(table_id: TableId) -> Self {
            Self {
                table_id,
                fetch_count: AtomicUsize::new(0),
                requested_ranges: Mutex::new(vec![]),
            }
        }
    }

    #[async_trait::async_trait]
    impl HummockMetaClient for TestHummockMetaClient {
        async fn unpin_version_before(
            &self,
            _unpin_version_before: HummockVersionId,
        ) -> Result<()> {
            unimplemented!()
        }

        async fn get_current_version(&self) -> Result<HummockVersion> {
            unimplemented!()
        }

        async fn get_new_object_ids(&self, _number: u32) -> Result<ObjectIdRange> {
            unimplemented!()
        }

        async fn commit_epoch_with_change_log(
            &self,
            _epoch: HummockEpoch,
            _sync_result: SyncResult,
            _change_log_info: Option<HummockMetaClientChangeLogInfo>,
        ) -> Result<()> {
            unimplemented!()
        }

        async fn trigger_manual_compaction(
            &self,
            _compaction_group_id: CompactionGroupId,
            _table_id: JobId,
            _level: u32,
            _target_level: Option<u32>,
            _sst_ids: Vec<HummockSstableId>,
            _exclusive: bool,
        ) -> Result<bool> {
            unimplemented!()
        }

        async fn trigger_full_gc(
            &self,
            _sst_retention_time_sec: u64,
            _prefix: Option<String>,
        ) -> Result<()> {
            unimplemented!()
        }

        async fn subscribe_compaction_event(
            &self,
        ) -> Result<(
            UnboundedSender<SubscribeCompactionEventRequest>,
            BoxStream<'static, CompactionEventItem>,
        )> {
            unimplemented!()
        }

        async fn get_version_by_epoch(
            &self,
            _epoch: HummockEpoch,
            _table_id: risingwave_pb::id::TableId,
        ) -> Result<PbHummockVersion> {
            unimplemented!()
        }

        async fn subscribe_iceberg_compaction_event(
            &self,
        ) -> Result<(
            UnboundedSender<SubscribeIcebergCompactionEventRequest>,
            BoxStream<'static, IcebergCompactionEventItem>,
        )> {
            unimplemented!()
        }

        async fn get_table_change_logs(
            &self,
            _epoch_only: bool,
            start_epoch_inclusive: Option<u64>,
            end_epoch_inclusive: Option<u64>,
            _table_ids: Option<HashSet<TableId>>,
            _exclude_empty: bool,
            limit: Option<u32>,
        ) -> Result<TableChangeLogs> {
            self.fetch_count.fetch_add(1, Ordering::Relaxed);
            self.requested_ranges
                .lock()
                .unwrap()
                .push((start_epoch_inclusive, end_epoch_inclusive));

            let logs = (start_epoch_inclusive.unwrap()..=end_epoch_inclusive.unwrap())
                .take(limit.map(|limit| limit as usize).unwrap_or(usize::MAX))
                .map(|checkpoint_epoch| EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    non_checkpoint_epochs: vec![],
                    checkpoint_epoch,
                });
            Ok(TableChangeLogs::from_iter([(
                self.table_id,
                TableChangeLog::new(logs),
            )]))
        }
    }

    #[tokio::test]
    async fn test_fetch_uses_cached_covering_range() {
        let table_id = TableId::new(1);
        let meta_client = Arc::new(TestHummockMetaClient::new(table_id));
        let manager = TableChangeLogManager::new(
            10,
            meta_client.clone(),
            Arc::new(HummockStateStoreMetrics::unused()),
        );

        manager
            .fetch_table_change_logs(table_id, (1, u64::MAX), false, Some(3))
            .await
            .unwrap();
        let table_change_logs = manager
            .fetch_table_change_logs(table_id, (2, 2), false, None)
            .await
            .unwrap();

        assert_eq!(meta_client.fetch_count.load(Ordering::Relaxed), 1);
        assert_eq!(
            meta_client.requested_ranges.lock().unwrap().as_slice(),
            &[(Some(1), Some(u64::MAX))]
        );

        let epochs = table_change_logs[&table_id].epochs().collect::<Vec<_>>();
        assert_eq!(epochs, vec![2]);
    }

    #[tokio::test]
    async fn test_fetch_ignores_limited_cache_when_not_covering_range() {
        let table_id = TableId::new(1);
        let meta_client = Arc::new(TestHummockMetaClient::new(table_id));
        let manager = TableChangeLogManager::new(
            10,
            meta_client.clone(),
            Arc::new(HummockStateStoreMetrics::unused()),
        );

        manager
            .fetch_table_change_logs(table_id, (1, u64::MAX), false, Some(1))
            .await
            .unwrap();
        let table_change_logs = manager
            .fetch_table_change_logs(table_id, (2, 2), false, None)
            .await
            .unwrap();

        assert_eq!(meta_client.fetch_count.load(Ordering::Relaxed), 2);
        assert_eq!(
            meta_client.requested_ranges.lock().unwrap().as_slice(),
            &[(Some(1), Some(u64::MAX)), (Some(2), Some(2))]
        );

        let epochs = table_change_logs[&table_id].epochs().collect::<Vec<_>>();
        assert_eq!(epochs, vec![2]);
    }
}
