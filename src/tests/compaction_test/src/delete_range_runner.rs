// Copyright 2023 RisingWave Labs
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

use std::collections::HashMap;
use std::future::Future;
use std::ops::{Bound, RangeBounds};
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use risingwave_common::cache::CachePriority;
use risingwave_common::catalog::hummock::PROPERTIES_RETENTION_SECOND_KEY;
use risingwave_common::catalog::TableId;
use risingwave_common::config::{
    extract_storage_memory_config, load_config, RwConfig, NO_OVERRIDE,
};
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_test::get_notification_client_for_test;
use risingwave_meta::hummock::compaction::compaction_config::CompactionConfigBuilder;
use risingwave_meta::hummock::test_utils::setup_compute_env_with_config;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_pb::catalog::PbTable;
use risingwave_pb::hummock::{CompactionConfig, CompactionGroupInfo};
use risingwave_pb::meta::SystemParams;
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::filter_key_extractor::{
    FilterKeyExtractorImpl, FilterKeyExtractorManager, FullKeyFilterKeyExtractor,
};
use risingwave_storage::hummock::compactor::{CompactionExecutor, CompactorContext};
use risingwave_storage::hummock::sstable_store::SstableStoreRef;
use risingwave_storage::hummock::utils::cmp_delete_range_left_bounds;
use risingwave_storage::hummock::{
    CachePolicy, HummockStorage, MemoryLimiter, SstableObjectIdManager, SstableStore, TieredCache,
};
use risingwave_storage::monitor::{CompactorMetrics, HummockStateStoreMetrics};
use risingwave_storage::opts::StorageOpts;
use risingwave_storage::store::{LocalStateStore, NewLocalOptions, PrefetchOptions, ReadOptions};
use risingwave_storage::StateStore;

use crate::CompactionTestOpts;

pub fn start_delete_range(opts: CompactionTestOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        tracing::info!("Compaction delete-range test start with options {:?}", opts);
        let prefix = opts.state_store.strip_prefix("hummock+");
        match prefix {
            Some(s) => {
                assert!(
                    s.starts_with("s3://") || s.starts_with("minio://"),
                    "Only support S3 and MinIO object store"
                );
            }
            None => {
                panic!("Invalid state store");
            }
        }
        let ret = compaction_test_main(opts).await;

        match ret {
            Ok(_) => {
                tracing::info!("Compaction delete-range test Success");
            }
            Err(e) => {
                panic!("Compaction delete-range test Fail: {}", e);
            }
        }
    })
}
pub async fn compaction_test_main(opts: CompactionTestOpts) -> anyhow::Result<()> {
    let config = load_config(&opts.config_path, NO_OVERRIDE);
    let compaction_config = CompactionConfigBuilder::new().build();
    compaction_test(compaction_config, config, &opts.state_store, 1000000, 800).await
}

async fn compaction_test(
    compaction_config: CompactionConfig,
    config: RwConfig,
    state_store_type: &str,
    test_range: u64,
    test_count: u64,
) -> anyhow::Result<()> {
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env_with_config(8080, compaction_config.clone()).await;
    let meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));

    let delete_key_table = PbTable {
        id: 1,
        schema_id: 1,
        database_id: 1,
        name: "delete-key-table".to_string(),
        columns: vec![],
        pk: vec![],
        dependent_relations: vec![],
        distribution_key: vec![],
        stream_key: vec![],
        owner: 0,
        properties: HashMap::<String, String>::from([(
            PROPERTIES_RETENTION_SECOND_KEY.to_string(),
            0.to_string(),
        )]),
        fragment_id: 0,
        dml_fragment_id: None,
        vnode_col_index: None,
        value_indices: vec![],
        definition: "".to_string(),
        handle_pk_conflict_behavior: 0,
        read_prefix_len_hint: 0,
        optional_associated_source_id: None,
        table_type: 0,
        append_only: false,
        row_id_index: None,
        version: None,
        watermark_indices: vec![],
        dist_key_in_pk: vec![],
    };
    let mut delete_range_table = delete_key_table.clone();
    delete_range_table.id = 2;
    delete_range_table.name = "delete-range-table".to_string();
    let group1 = CompactionGroupInfo {
        id: StaticCompactionGroupId::StateDefault as _,
        parent_id: 0,
        member_table_ids: vec![1],
        compaction_config: Some(compaction_config.clone()),
    };
    let group2 = CompactionGroupInfo {
        id: StaticCompactionGroupId::MaterializedView as _,
        parent_id: 0,
        member_table_ids: vec![2],
        compaction_config: Some(compaction_config.clone()),
    };
    hummock_manager_ref
        .init_metadata_for_version_replay(
            vec![delete_key_table, delete_range_table],
            vec![group1, group2],
        )
        .await?;

    let system_params = SystemParams {
        sstable_size_mb: Some(256),
        block_size_kb: Some(1024),
        bloom_false_positive: Some(0.001),
        data_directory: Some("hummock_001".to_string()),
        backup_storage_url: Some("memory".to_string()),
        backup_storage_directory: Some("backup".to_string()),
        ..Default::default()
    }
    .into();
    let storage_memory_config = extract_storage_memory_config(&config);
    let storage_opts = Arc::new(StorageOpts::from((
        &config,
        &system_params,
        &storage_memory_config,
    )));
    let state_store_metrics = Arc::new(HummockStateStoreMetrics::unused());
    let compactor_metrics = Arc::new(CompactorMetrics::unused());
    let object_store_metrics = Arc::new(ObjectStoreMetrics::unused());
    let remote_object_store = parse_remote_object_store(
        state_store_type.strip_prefix("hummock+").unwrap(),
        object_store_metrics.clone(),
        "Hummock",
    )
    .await;
    let sstable_store = Arc::new(SstableStore::new(
        Arc::new(remote_object_store),
        system_params.data_directory().to_string(),
        storage_memory_config.block_cache_capacity_mb * (1 << 20),
        storage_memory_config.meta_cache_capacity_mb * (1 << 20),
        0,
        TieredCache::none(),
    ));

    let store = HummockStorage::new(
        storage_opts.clone(),
        sstable_store.clone(),
        meta_client.clone(),
        get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node),
        Arc::new(FilterKeyExtractorManager::default()),
        state_store_metrics.clone(),
        compactor_metrics.clone(),
    )
    .await?;
    let sstable_object_id_manager = store.sstable_object_id_manager().clone();
    let filter_key_extractor_manager = store.filter_key_extractor_manager().clone();
    filter_key_extractor_manager.update(
        1,
        Arc::new(FilterKeyExtractorImpl::FullKey(
            FullKeyFilterKeyExtractor {},
        )),
    );
    filter_key_extractor_manager.update(
        2,
        Arc::new(FilterKeyExtractorImpl::FullKey(
            FullKeyFilterKeyExtractor {},
        )),
    );

    let (compactor_thrd, compactor_shutdown_tx) = run_compactor_thread(
        storage_opts,
        sstable_store,
        meta_client.clone(),
        filter_key_extractor_manager,
        sstable_object_id_manager,
        compactor_metrics,
    );
    run_compare_result(&store, meta_client.clone(), test_range, test_count)
        .await
        .unwrap();
    let version = store.get_pinned_version().version();
    let remote_version = meta_client.get_current_version().await.unwrap();
    println!(
        "version-{}, remote version-{}",
        version.id, remote_version.id
    );
    for (group, levels) in &version.levels {
        let l0 = levels.l0.as_ref().unwrap();
        let sz = levels
            .levels
            .iter()
            .map(|level| level.total_file_size)
            .sum::<u64>();
        let count = levels
            .levels
            .iter()
            .map(|level| level.table_infos.len())
            .sum::<usize>();
        println!(
            "group-{}: base: {} {} , l0 sz: {}, count: {}",
            group,
            sz,
            count,
            l0.total_file_size,
            l0.sub_levels
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>()
        );
    }
    compactor_shutdown_tx.send(()).unwrap();
    compactor_thrd.await.unwrap();
    Ok(())
}

async fn run_compare_result(
    hummock: &HummockStorage,
    meta_client: Arc<MockHummockMetaClient>,
    test_range: u64,
    test_count: u64,
) -> Result<(), String> {
    let init_epoch = hummock.get_pinned_version().max_committed_epoch() + 1;
    let mut normal = NormalState::new(hummock, 1, init_epoch).await;
    let mut delete_range = DeleteRangeState::new(hummock, 2, init_epoch).await;
    const RANGE_BASE: u64 = 400;
    let range_mod = test_range / RANGE_BASE;

    let mut rng = StdRng::seed_from_u64(10097);
    let mut overlap_ranges = vec![];
    for epoch_idx in 0..test_count {
        let epoch = init_epoch + epoch_idx;
        for idx in 0..1000 {
            let op = rng.next_u32() % 50;
            let key_number = rng.next_u64() % test_range;
            if op == 0 {
                let end_key = key_number + (rng.next_u64() % range_mod) + 1;
                overlap_ranges.push((key_number, end_key, epoch, idx));
                let start_key = format!("{:010}", key_number);
                let end_key = format!("{:010}", end_key);
                normal
                    .delete_range(start_key.as_bytes(), end_key.as_bytes())
                    .await;
                delete_range
                    .delete_range(start_key.as_bytes(), end_key.as_bytes())
                    .await;
            } else if op < 5 {
                let key = format!("{:010}", key_number);
                let a = normal.get(key.as_bytes()).await;
                let b = delete_range.get(key.as_bytes()).await;
                assert!(
                    a.eq(&b),
                    "query {} {:?} vs {:?} in epoch-{}",
                    key_number,
                    a.map(|raw| String::from_utf8(raw.to_vec()).unwrap()),
                    b.map(|raw| String::from_utf8(raw.to_vec()).unwrap()),
                    epoch,
                );
            } else if op < 10 {
                let end_key = key_number + (rng.next_u64() % range_mod) + 1;
                let start_key = format!("{:010}", key_number);
                let end_key = format!("{:010}", end_key);
                let ret1 = normal.scan(start_key.as_bytes(), end_key.as_bytes()).await;
                let ret2 = delete_range
                    .scan(start_key.as_bytes(), end_key.as_bytes())
                    .await;
                assert_eq!(ret1, ret2);
            } else {
                let overlap = overlap_ranges
                    .iter()
                    .any(|(left, right, _, _)| *left <= key_number && key_number < *right);
                if overlap {
                    continue;
                }
                let key = format!("{:010}", key_number);
                let val = format!("val-{:010}-{:016}-{:016}", idx, key_number, epoch);
                normal.insert(key.as_bytes(), val.as_bytes());
                delete_range.insert(key.as_bytes(), val.as_bytes());
            }
        }
        let next_epoch = epoch + 1;
        normal.commit(next_epoch).await?;
        delete_range.commit(next_epoch).await?;
        // let checkpoint = epoch % 10 == 0;
        let ret = hummock.seal_and_sync_epoch(epoch).await.unwrap();
        meta_client
            .commit_epoch(epoch, ret.uncommitted_ssts)
            .await
            .map_err(|e| format!("{:?}", e))?;
        if epoch % 200 == 0 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    Ok(())
}

struct NormalState {
    storage: <HummockStorage as StateStore>::Local,
    table_id: TableId,
}

struct DeleteRangeState {
    inner: NormalState,
    delete_ranges: Vec<(Bound<Bytes>, Bound<Bytes>)>,
}

impl DeleteRangeState {
    async fn new(hummock: &HummockStorage, table_id: u32, epoch: u64) -> Self {
        Self {
            inner: NormalState::new(hummock, table_id, epoch).await,
            delete_ranges: vec![],
        }
    }
}

#[async_trait::async_trait]
trait CheckState {
    async fn delete_range(&mut self, left: &[u8], right: &[u8]);
    async fn get(&self, key: &[u8]) -> Option<Bytes>;
    async fn scan(&self, left: &[u8], right: &[u8]) -> Vec<(Bytes, Bytes)>;
    fn insert(&mut self, key: &[u8], val: &[u8]);
    async fn commit(&mut self, epoch: u64) -> Result<(), String>;
}

impl NormalState {
    async fn new(hummock: &HummockStorage, table_id: u32, epoch: u64) -> Self {
        let table_id = TableId::new(table_id);
        let mut storage = hummock.new_local(NewLocalOptions::for_test(table_id)).await;
        storage.init(epoch);
        Self { storage, table_id }
    }

    async fn commit_impl(
        &mut self,
        delete_ranges: Vec<(Bound<Bytes>, Bound<Bytes>)>,
        next_epoch: u64,
    ) -> Result<(), String> {
        self.storage
            .flush(delete_ranges)
            .await
            .map_err(|e| format!("{:?}", e))?;
        self.storage.seal_current_epoch(next_epoch);
        Ok(())
    }

    async fn get_impl(&self, key: &[u8], ignore_range_tombstone: bool) -> Option<Bytes> {
        self.storage
            .get(
                Bytes::copy_from_slice(key),
                ReadOptions {
                    prefix_hint: None,
                    ignore_range_tombstone,
                    retention_seconds: None,
                    table_id: self.table_id,
                    read_version_from_backup: false,
                    prefetch_options: Default::default(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                },
            )
            .await
            .unwrap()
    }

    async fn scan_impl(
        &self,
        left: &[u8],
        right: &[u8],
        ignore_range_tombstone: bool,
    ) -> Vec<(Bytes, Bytes)> {
        let mut iter = pin!(self
            .storage
            .iter(
                (
                    Bound::Included(Bytes::copy_from_slice(left)),
                    Bound::Excluded(Bytes::copy_from_slice(right)),
                ),
                ReadOptions {
                    prefix_hint: None,
                    ignore_range_tombstone,
                    retention_seconds: None,
                    table_id: self.table_id,
                    read_version_from_backup: false,
                    prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                },
            )
            .await
            .unwrap(),);
        let mut ret = vec![];
        while let Some(item) = iter.next().await {
            let (full_key, val) = item.unwrap();
            let tkey = full_key.user_key.table_key.0.clone();
            ret.push((tkey, val));
        }
        ret
    }
}

#[async_trait::async_trait]
impl CheckState for NormalState {
    async fn delete_range(&mut self, left: &[u8], right: &[u8]) {
        let mut iter = Box::pin(
            self.storage
                .iter(
                    (
                        Bound::Included(Bytes::copy_from_slice(left)),
                        Bound::Excluded(Bytes::copy_from_slice(right)),
                    ),
                    ReadOptions {
                        prefix_hint: None,
                        ignore_range_tombstone: true,
                        retention_seconds: None,
                        table_id: self.table_id,
                        read_version_from_backup: false,
                        prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                        cache_policy: CachePolicy::Fill(CachePriority::High),
                    },
                )
                .await
                .unwrap(),
        );
        let mut delete_item = Vec::new();
        while let Some(item) = iter.next().await {
            let (full_key, value) = item.unwrap();
            delete_item.push((full_key.user_key.table_key.0, value));
        }
        drop(iter);
        for (key, value) in delete_item {
            self.storage.delete(key, value).unwrap();
        }
    }

    fn insert(&mut self, key: &[u8], val: &[u8]) {
        self.storage
            .insert(Bytes::from(key.to_vec()), Bytes::copy_from_slice(val), None)
            .unwrap();
    }

    async fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.get_impl(key, true).await
    }

    async fn scan(&self, left: &[u8], right: &[u8]) -> Vec<(Bytes, Bytes)> {
        self.scan_impl(left, right, true).await
    }

    async fn commit(&mut self, next_epoch: u64) -> Result<(), String> {
        self.commit_impl(vec![], next_epoch).await
    }
}

#[async_trait::async_trait]
impl CheckState for DeleteRangeState {
    async fn delete_range(&mut self, left: &[u8], right: &[u8]) {
        self.delete_ranges.push((
            Bound::Included(Bytes::copy_from_slice(left)),
            Bound::Excluded(Bytes::copy_from_slice(right)),
        ));
    }

    async fn get(&self, key: &[u8]) -> Option<Bytes> {
        for delete_range in &self.delete_ranges {
            if delete_range.contains(key) {
                return None;
            }
        }
        self.inner.get_impl(key, false).await
    }

    async fn scan(&self, left: &[u8], right: &[u8]) -> Vec<(Bytes, Bytes)> {
        let mut ret = self.inner.scan_impl(left, right, false).await;
        ret.retain(|(key, _)| {
            for delete_range in &self.delete_ranges {
                if delete_range.contains(key) {
                    return false;
                }
            }
            true
        });
        ret
    }

    fn insert(&mut self, key: &[u8], val: &[u8]) {
        self.inner.insert(key, val);
    }

    async fn commit(&mut self, next_epoch: u64) -> Result<(), String> {
        let mut delete_ranges = std::mem::take(&mut self.delete_ranges);
        delete_ranges.sort_by(|a, b| cmp_delete_range_left_bounds(a.0.as_ref(), b.0.as_ref()));
        self.inner.commit_impl(delete_ranges, next_epoch).await
    }
}

fn run_compactor_thread(
    storage_opts: Arc<StorageOpts>,
    sstable_store: SstableStoreRef,
    meta_client: Arc<MockHummockMetaClient>,
    filter_key_extractor_manager: Arc<FilterKeyExtractorManager>,
    sstable_object_id_manager: Arc<SstableObjectIdManager>,
    compactor_metrics: Arc<CompactorMetrics>,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let compactor_context = Arc::new(CompactorContext {
        storage_opts,
        hummock_meta_client: meta_client.clone(),
        sstable_store,
        compactor_metrics,
        is_share_buffer_compact: false,
        compaction_executor: Arc::new(CompactionExecutor::new(None)),
        filter_key_extractor_manager,
        output_memory_limiter: MemoryLimiter::unlimit(),
        sstable_object_id_manager,
        task_progress_manager: Default::default(),
        await_tree_reg: None,
    });
    risingwave_storage::hummock::compactor::Compactor::start_compactor(
        compactor_context,
        meta_client,
    )
}

#[cfg(test)]
mod tests {

    use risingwave_common::config::RwConfig;
    use risingwave_meta::hummock::compaction::compaction_config::CompactionConfigBuilder;

    use super::compaction_test;

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_small_data() {
        let config = RwConfig::default();
        let mut compaction_config = CompactionConfigBuilder::new().build();
        compaction_config.max_sub_compaction = 1;
        compaction_config.level0_tier_compact_file_number = 2;
        compaction_config.max_bytes_for_level_base = 512 * 1024;
        compaction_config.sub_level_max_compaction_bytes = 256 * 1024;
        compaction_test(compaction_config, config, "hummock+memory", 10000, 60)
            .await
            .unwrap();
    }
}
