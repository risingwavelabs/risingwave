use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use itertools::Itertools;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use risingwave_common::catalog::TableId;
use risingwave_common::config::{load_config, StorageConfig};
use risingwave_hummock_sdk::compact::CompactorRuntimeConfig;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManager;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_hummock_test::get_test_notification_client;
use risingwave_meta::hummock::compaction::compaction_config::CompactionConfigBuilder;
use risingwave_meta::hummock::test_utils::setup_compute_env_with_config;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_pb::hummock::CompactionGroup;
use risingwave_storage::hummock::compactor::{CompactionExecutor, CompactorContext, Context};
use risingwave_storage::hummock::sstable_store::SstableStoreRef;
use risingwave_storage::hummock::store::state_store::LocalHummockStorage;
use risingwave_storage::hummock::{
    CompactorSstableStore, HummockStorage, MemoryLimiter, SstableIdManager, SstableStore,
    TieredCache,
};
use risingwave_storage::monitor::StateStoreMetrics;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::{ReadOptions, StateStoreRead, StateStoreWrite, WriteOptions};
use risingwave_storage::{StateStore, StateStoreIter};

use crate::{CompactionTestOpts, TestToolConfig};

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
                tracing::info!("Success");
            }
            Err(e) => {
                tracing::error!("Failure {}", e);
            }
        }
    })
}

pub async fn compaction_test_main(opts: CompactionTestOpts) -> anyhow::Result<()> {
    let config = CompactionConfigBuilder::new().build();
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env_with_config(8080, config).await;
    let meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));

    // Wait for meta starts
    tokio::time::sleep(Duration::from_secs(1)).await;
    tracing::info!("Started embedded Meta");

    let delete_key_table = ProstTable {
        id: 1,
        schema_id: 1,
        database_id: 1,
        name: "delete-key-table".to_string(),
        columns: vec![],
        pk: vec![],
        dependent_relations: vec![],
        is_index: false,
        distribution_key: vec![],
        stream_key: vec![],
        appendonly: false,
        owner: 0,
        properties: Default::default(),
        fragment_id: 0,
        vnode_col_idx: None,
        value_indices: vec![],
        definition: "".to_string(),
        handle_pk_conflict: false,
        optional_associated_source_id: None,
    };
    let mut delete_range_table = delete_key_table.clone();
    delete_range_table.id = 2;
    delete_range_table.name = "delete-range-table".to_string();
    let group1 = CompactionGroup {
        id: 0,
        parent_id: 0,
        member_table_ids: vec![1],
        compaction_config: None,
        table_id_to_options: Default::default(),
    };
    let group2 = CompactionGroup {
        id: 0,
        parent_id: 0,
        member_table_ids: vec![2],
        compaction_config: None,
        table_id_to_options: Default::default(),
    };
    hummock_manager_ref
        .init_metadata_for_replay(
            vec![delete_key_table, delete_range_table],
            vec![group1, group2],
        )
        .await?;
    let mut config: TestToolConfig = load_config(&opts.config_path).unwrap();
    config.storage.enable_state_store_v1 = false;
    let config = Arc::new(config.storage.clone());

    let state_store_metrics = Arc::new(StateStoreMetrics::unused());
    let object_store_metrics = Arc::new(ObjectStoreMetrics::unused());
    let remote_object_store = parse_remote_object_store(
        opts.state_store.strip_prefix("hummock+").unwrap(),
        object_store_metrics.clone(),
        config.object_store_use_batch_delete,
    )
    .await;
    let sstable_store = Arc::new(SstableStore::new(
        Arc::new(remote_object_store),
        config.data_directory.to_string(),
        config.block_cache_capacity_mb * (1 << 20),
        config.meta_cache_capacity_mb * (1 << 20),
        TieredCache::none(),
    ));

    let store = HummockStorage::new(
        config.clone(),
        sstable_store.clone(),
        meta_client.clone(),
        get_test_notification_client(env, hummock_manager_ref.clone(), worker_node),
        state_store_metrics.clone(),
    )
    .await
    .unwrap();

    let (compactor_thrd, compactor_shutdown_tx) =
        run_compactor_thread(config, sstable_store, meta_client, state_store_metrics);
    run_compare_result(&store, 100000, 100000).await.unwrap();
    compactor_shutdown_tx.send(()).unwrap();
    compactor_thrd.await.unwrap();
    Ok(())
}

async fn run_compare_result(
    hummock: &HummockStorage,
    test_range: u64,
    test_count: u64,
) -> Result<(), String> {
    let storage = hummock.new_local(TableId::new(1)).await;
    let mut normal = NormalState::new(storage, 0);
    let storage = hummock.new_local(TableId::new(2)).await;
    let mut delete_range = DeleteRangeState::new(storage, 0);
    const RANGE_BASE: u64 = 100;
    let range_mod = test_range / RANGE_BASE;

    let mut rng = StdRng::seed_from_u64(10097);
    for epoch in 1..test_count {
        for _ in 0..100 {
            let op = rng.next_u32() % 20;
            let start_key = rng.next_u64() % test_range;
            if op == 0 {
                let end_key = start_key + (rng.next_u64() % range_mod) * RANGE_BASE;
                let start_key = format!("{:06}", start_key);
                let end_key = format!("{:06}", end_key);
                normal
                    .delete_range(start_key.as_bytes(), end_key.as_bytes())
                    .await;
                delete_range
                    .delete_range(start_key.as_bytes(), end_key.as_bytes())
                    .await;
            } else if op < 5 {
                let key = format!("{:06}", start_key);
                let a = normal.get(key.as_bytes()).await;
                let b = delete_range.get(key.as_bytes()).await;
                assert!(a.eq(&b));
            } else {
                let key = format!("{:06}", start_key);
                let val = epoch.to_le_bytes();
                normal.insert(key.as_bytes(), &val);
                delete_range.insert(key.as_bytes(), &val);
            }
        }
        normal.commit(epoch).await?;
        delete_range.commit(epoch).await?;
        let checkpoint = epoch % 10 == 0;
        hummock.seal_epoch(epoch, checkpoint);
        hummock
            .try_wait_epoch(HummockReadEpoch::Current(epoch))
            .await
            .unwrap();
    }
    Ok(())
}

struct NormalState {
    storage: LocalHummockStorage,
    cache: BTreeMap<Vec<u8>, StorageValue>,
    epoch: u64,
}

struct DeleteRangeState {
    inner: NormalState,
    delete_ranges: Vec<(Bytes, Bytes)>,
}

impl DeleteRangeState {
    fn new(storage: LocalHummockStorage, epoch: u64) -> Self {
        Self {
            inner: NormalState::new(storage, epoch),
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
    fn new(storage: LocalHummockStorage, epoch: u64) -> Self {
        Self {
            cache: BTreeMap::default(),
            storage,
            epoch,
        }
    }

    async fn commit_impl(
        &mut self,
        delete_ranges: Vec<(Bytes, Bytes)>,
        epoch: u64,
    ) -> Result<(), String> {
        let data = std::mem::take(&mut self.cache)
            .into_iter()
            .map(|(key, val)| (Bytes::from(key), val))
            .collect_vec();
        self.storage
            .ingest_batch(
                data,
                delete_ranges,
                WriteOptions {
                    epoch,
                    table_id: TableId::new(1),
                },
            )
            .await
            .map_err(|e| format!("{:?}", e))?;
        self.epoch = epoch;
        Ok(())
    }
}

#[async_trait::async_trait]
impl CheckState for NormalState {
    async fn delete_range(&mut self, left: &[u8], right: &[u8]) {
        self.cache
            .retain(|key, _| key.as_slice().lt(left) || key.as_slice().ge(right));
        let mut iter = self
            .storage
            .iter(
                (
                    Bound::Included(left.to_vec()),
                    Bound::Excluded(right.to_vec()),
                ),
                self.epoch,
                ReadOptions {
                    prefix_hint: None,
                    ignore_range_tombstone: true,
                    check_bloom_filter: false,
                    retention_seconds: None,
                    table_id: Default::default(),
                },
            )
            .await
            .unwrap();
        while let Some((full_key, _)) = iter.next().await.unwrap() {
            self.cache
                .insert(full_key.user_key.encode(), StorageValue::new_delete());
        }
    }

    fn insert(&mut self, key: &[u8], val: &[u8]) {
        if self.cache.contains_key(key) {
            return;
        }
        self.cache
            .insert(key.to_vec(), StorageValue::new_put(val.to_vec()));
    }

    async fn get(&self, key: &[u8]) -> Option<Bytes> {
        if let Some(val) = self.cache.get(key) {
            return val.user_value.clone();
        }
        self.storage
            .get(
                key,
                self.epoch,
                ReadOptions {
                    prefix_hint: None,
                    ignore_range_tombstone: true,
                    check_bloom_filter: true,
                    retention_seconds: None,
                    table_id: Default::default(),
                },
            )
            .await
            .unwrap()
    }

    async fn scan(&self, left: &[u8], right: &[u8]) -> Vec<(Bytes, Bytes)> {
        let mut iter = self
            .storage
            .iter(
                (
                    Bound::Included(left.to_vec()),
                    Bound::Excluded(right.to_vec()),
                ),
                self.epoch,
                ReadOptions {
                    prefix_hint: None,
                    ignore_range_tombstone: true,
                    check_bloom_filter: false,
                    retention_seconds: None,
                    table_id: Default::default(),
                },
            )
            .await
            .unwrap();
        let mut ret = vec![];
        while let Some((full_key, val)) = iter.next().await.unwrap() {
            let ukey = full_key.user_key.encode();
            if let Some(cache_val) = self.cache.get(&ukey) {
                if cache_val.user_value.is_some() {
                    ret.push((
                        Bytes::from(full_key.user_key.encode()),
                        cache_val.user_value.clone().unwrap(),
                    ));
                } else {
                    continue;
                }
            }
            ret.push((Bytes::from(ukey), val));
        }
        for (key, val) in self.cache.range((
            Bound::Included(left.to_vec()),
            Bound::Excluded(right.to_vec()),
        )) {
            if let Some(uval) = val.user_value.as_ref() {
                ret.push((Bytes::from(key.clone()), uval.clone()));
            }
        }
        ret.sort_by(|a, b| a.0.cmp(&b.0));
        ret
    }

    async fn commit(&mut self, epoch: u64) -> Result<(), String> {
        self.commit_impl(vec![], epoch).await
    }
}

#[async_trait::async_trait]
impl CheckState for DeleteRangeState {
    async fn delete_range(&mut self, left: &[u8], right: &[u8]) {
        self.delete_ranges
            .push((Bytes::copy_from_slice(left), Bytes::copy_from_slice(right)));
    }

    async fn get(&self, key: &[u8]) -> Option<Bytes> {
        for (left, right) in &self.delete_ranges {
            if left.as_ref().lt(key) && right.as_ref().gt(key) {
                return None;
            }
        }
        self.inner.get(key).await
    }

    async fn scan(&self, left: &[u8], right: &[u8]) -> Vec<(Bytes, Bytes)> {
        let mut ret = self.inner.scan(left, right).await;
        ret.retain(|(key, _)| {
            for (left, right) in &self.delete_ranges {
                if left.as_ref().lt(key) && right.as_ref().gt(key) {
                    return false;
                }
            }
            true
        });
        ret
    }

    fn insert(&mut self, key: &[u8], val: &[u8]) {
        for (left, right) in &self.delete_ranges {
            if left.as_ref().lt(key) && right.as_ref().gt(key) {
                return;
            }
        }
        self.inner.insert(key, val);
    }

    async fn commit(&mut self, epoch: u64) -> Result<(), String> {
        let delete_ranges = std::mem::take(&mut self.delete_ranges);
        self.inner.commit_impl(delete_ranges, epoch).await
    }
}

fn run_compactor_thread(
    config: Arc<StorageConfig>,
    sstable_store: SstableStoreRef,
    meta_client: Arc<MockHummockMetaClient>,
    state_store_metrics: Arc<StateStoreMetrics>,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let filter_key_extractor_manager = Arc::new(FilterKeyExtractorManager::default());
    let compact_sstable_store = Arc::new(CompactorSstableStore::new(
        sstable_store.clone(),
        MemoryLimiter::unlimit(),
    ));

    let sstable_id_manager = Arc::new(SstableIdManager::new(meta_client.clone(), 10));
    let context = Arc::new(Context {
        options: config,
        hummock_meta_client: meta_client.clone(),
        sstable_store,
        stats: state_store_metrics,
        is_share_buffer_compact: false,
        compaction_executor: Arc::new(CompactionExecutor::new(None)),
        filter_key_extractor_manager,
        read_memory_limiter: MemoryLimiter::unlimit(),
        sstable_id_manager,
        task_progress_manager: Default::default(),
    });
    let compactor_context = Arc::new(CompactorContext::with_config(
        context,
        compact_sstable_store,
        CompactorRuntimeConfig {
            max_concurrent_task_number: 4,
        },
    ));
    risingwave_storage::hummock::compactor::Compactor::start_compactor(
        compactor_context,
        meta_client,
    )
}
