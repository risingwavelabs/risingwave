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

//! Fixed-state, one-sided `HashJoin` replay. See `hash_join_replay/README.md`.
use std::collections::{BTreeMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use bytes::Bytes;
use foyer::{CacheBuilder, Hint, HybridCacheBuilder};
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use prometheus::TextEncoder;
use prost::Message as _;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::{ColumnDesc, ColumnId, ConflictBehavior, Field, Schema, TableId};
use risingwave_common::config::{MetricLevel, RwConfig, extract_storage_memory_config};
use risingwave_common::hash::{Key128, VirtualNode};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::system_param::system_params_for_test;
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::{EpochPair, test_epoch};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_common::util::value_encoding::{BasicSerializer, ValueRowSerializer};
use risingwave_expr::aggregate::AggCall;
use risingwave_hummock_sdk::key::prefixed_range_with_vnode;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{HummockReadEpoch, HummockSstableObjectId};
use risingwave_hummock_test::test_utils::{HummockTestEnv, prepare_hummock_test_env_with_store};
use risingwave_object_store::object::build_remote_object_store;
use risingwave_pb::catalog::PbTable;
use risingwave_pb::hummock::PbHummockVersion;
use risingwave_pb::stream_plan::PbAggNodeVersion;
use risingwave_storage::hummock::none::NoneRecentFilter;
use risingwave_storage::hummock::{
    Block, CachePolicy, HummockStorage, SstableBlockIndex, SstableMetaCacheValue, SstableStore,
    SstableStoreConfig, SstableStoreRef,
};
use risingwave_storage::monitor::{
    ObjectStoreMetrics, StoreLocalStatistic, global_hummock_state_store_metrics,
};
use risingwave_storage::opts::StorageOpts;
use risingwave_storage::row_serde::row_serde_util::serialize_pk_with_vnode;
use risingwave_storage::store::{
    InitOptions, LocalStateStore, NewLocalOptions, NewReadSnapshotOptions, ReadOptions, StateStore,
    StateStoreGet, StateStoreIter, StateStoreRead, StateStoreWriteEpochControl,
};
use risingwave_stream::common::table::state_table::StateTable;
use risingwave_stream::common::table::test_utils::gen_pbtable;
use risingwave_stream::executor::aggregate::{
    AggExecutorArgs, AggStateStorage, HashAggExecutor, HashAggExecutorExtraArgs,
};
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::executor::test_utils::{MockSource, with_streaming_config};
use risingwave_stream::executor::{
    ActorContext, ActorContextRef, Executor, ExecutorInfo, HashJoinExecutor, JoinParams, JoinType,
    MaterializeExecutor, MemoryEncoding, Message,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::runtime::{Builder, Runtime};

#[path = "hash_join_replay/distributed.rs"]
mod distributed;

risingwave_expr_impl::enable!();

#[derive(Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProbeOrder {
    #[default]
    Stride,
    Sequential,
    Shuffled,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProbePattern {
    #[default]
    InsertOnly,
    InsertDelete,
    DeleteOnly,
}

impl ProbePattern {
    fn operations(&self) -> &'static [Op] {
        match self {
            Self::InsertOnly => &[Op::Insert],
            Self::InsertDelete => &[Op::Insert, Op::Delete],
            Self::DeleteOnly => &[Op::Delete],
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// Production TOML controls used by both store construction paths.
    #[serde(default)]
    rw_config: Option<String>,
    #[serde(default)]
    workload: distributed::Workload,
    #[serde(default = "default_probe_rounds")]
    group_rows: usize,
    #[serde(default = "distributed::default_vnode_count")]
    vnode_count: usize,
    #[serde(default)]
    point_reads: bool,
    #[serde(default)]
    downstream: bool,
    #[serde(default)]
    disable_sanity_check: bool,
    #[serde(default = "default_probe_rounds")]
    fixture_epochs: usize,
    keys: usize,
    rows_per_key: usize,
    value_bytes: usize,
    probe_chunk_size: usize,
    #[serde(default = "default_probe_rounds")]
    probe_rounds: usize,
    #[serde(default)]
    probe_order: ProbeOrder,
    #[serde(default)]
    probe_pattern: ProbePattern,
    warm_every_nth_block: Option<usize>,
    repeats: usize,
    #[serde(default)]
    trace: bool,
    #[serde(default = "default_scheduler_arms")]
    schedulers: Vec<bool>,
}

fn default_scheduler_arms() -> Vec<bool> {
    vec![false, true]
}

fn default_probe_rounds() -> usize {
    1
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

fn types() -> Vec<DataType> {
    vec![DataType::Int64, DataType::Int64, DataType::Varchar]
}

fn catalog(id: u32, config: &Config) -> PbTable {
    if config.workload != distributed::Workload::LegacyJoin {
        return distributed::catalog(id, config);
    }
    let columns = (if id < 4 {
        types()
    } else {
        vec![DataType::Int64; 3]
    })
    .into_iter()
    .enumerate()
    .map(|(i, ty)| ColumnDesc::unnamed(ColumnId::new(i as i32), ty))
    .collect();
    let mut table = gen_pbtable(
        TableId::new(id),
        columns,
        vec![OrderType::ascending(); 2],
        vec![0, 1],
        match id {
            0..=3 => 1,
            4 | 5 => 2,
            _ => 0,
        },
    );
    // Match the planner: full group-key filter prefix and aggregate-only values.
    if id == 4 {
        table.value_indices = vec![2];
    }
    table
}

fn table_ids(config: &Config) -> HashSet<TableId> {
    (0..if config.downstream { 6 } else { 4 })
        .map(TableId::new)
        .collect()
}

// Stable bytes with enough entropy that compression does not erase block geometry.
fn payload(key: usize, seq: usize, len: usize) -> String {
    let mut state = ((key as u64) << 32) ^ seq as u64 ^ 0x9e3779b97f4a7c15;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (b'!' + (state % 90) as u8) as char
        })
        .collect()
}

// Keep resource budgets explicit while using production geometry and cache policy defaults.
fn storage_options(replay: &Config) -> StorageOpts {
    let mut config = replay
        .rw_config
        .as_ref()
        .map_or_else(RwConfig::default, |path| {
            risingwave_common::config::load_config(path, risingwave_common::config::NoOverride)
        });
    config
        .storage
        .cache
        .block_cache_capacity_mb
        .get_or_insert(64);
    config
        .storage
        .cache
        .meta_cache_capacity_mb
        .get_or_insert(64);
    // CN memory/config.rs selects one shard below MIN_BUFFER_SIZE_PER_SHARD (256 MiB).
    // The common test/default helper still returns shard bits, so set the CN result explicitly.
    config.storage.cache.block_cache_shard_num = Some(1);
    config.storage.cache.meta_cache_shard_num = Some(1);
    config.storage.shared_buffer_capacity_mb = Some(64);
    config.storage.shared_buffer_min_batch_flush_size_mb = 16;
    config.storage.share_buffer_compaction_worker_threads_number = 1;
    config.storage.share_buffer_upload_concurrency = 1;
    let memory = extract_storage_memory_config(&config);
    StorageOpts::from((&config, &system_params_for_test().into(), &memory))
}

#[expect(clippy::borrowed_box)]
async fn store(url: &str, scheduler: bool, config: &Config) -> SstableStoreRef {
    let opts = storage_options(config);
    let object = build_remote_object_store(
        url,
        Arc::new(ObjectStoreMetrics::unused()),
        "Hummock",
        Arc::new(opts.object_store_config.clone()),
    )
    .await;
    let object = if scheduler {
        object.with_read_coalescing()
    } else {
        object
    };
    // Match StateStoreImpl's weights and policy; the generic mock store weighs entries as 1.
    let meta_cache = HybridCacheBuilder::new()
        .memory(opts.meta_cache_capacity_mb << 20)
        .with_shards(opts.meta_cache_shard_num)
        .with_eviction_config(opts.meta_cache_eviction_config.clone())
        .with_weighter(
            |_: &HummockSstableObjectId, value: &SstableMetaCacheValue| {
                std::mem::size_of::<HummockSstableObjectId>()
                    + value.estimated_meta_cache_memory_weight()
            },
        )
        .storage()
        .build()
        .await
        .unwrap();
    let block_cache = HybridCacheBuilder::new()
        .memory(opts.block_cache_capacity_mb << 20)
        .with_shards(opts.block_cache_shard_num)
        .with_eviction_config(opts.block_cache_eviction_config.clone())
        .with_weighter(|_: &SstableBlockIndex, value: &Box<Block>| {
            std::mem::size_of::<SstableBlockIndex>() + value.estimated_memory_weight()
        })
        .storage()
        .build()
        .await
        .unwrap();
    Arc::new(SstableStore::new(SstableStoreConfig {
        store: Arc::new(object),
        path: opts.data_directory,
        prefetch_buffer_capacity: opts.prefetch_buffer_capacity_mb << 20,
        max_prefetch_block_number: opts.max_prefetch_block_number,
        blockwise_prefetch: opts.blockwise_prefetch,
        read_batch: opts.read_batch,
        recent_filter: Arc::new(NoneRecentFilter::default().into()),
        state_store_metrics: Arc::new(global_hummock_state_store_metrics(MetricLevel::Debug)),
        use_new_object_prefix_strategy: true,
        skip_bloom_filter_in_serde: opts.sst_skip_bloom_filter_in_serde,
        meta_cache,
        block_cache,
        vector_meta_cache: CacheBuilder::new(1).build(),
        vector_block_cache: CacheBuilder::new(1).build(),
    }))
}

async fn environment(store: SstableStoreRef, config: &Config) -> HummockTestEnv {
    let mut opts = storage_options(config);
    if config.downstream {
        opts.shared_buffer_capacity_mb = 688;
    }
    let env = prepare_hummock_test_env_with_store(store, opts).await;
    for id in 0..if config.downstream { 6 } else { 4 } {
        env.register_table(catalog(id, config)).await;
    }
    env
}

async fn make_fixture(url: &str, config: &Config) -> HummockVersion {
    if config.workload != distributed::Workload::LegacyJoin {
        return Box::pin(distributed::make_fixture(url, config)).await;
    }
    let store = store(url, false, config).await;
    assert!(
        store
            .store()
            .list("", None, Some(1))
            .await
            .unwrap()
            .next()
            .await
            .is_none(),
        "creating a fixture requires an empty dedicated bucket"
    );
    let env = environment(store, config).await;
    env.storage.start_epoch(test_epoch(1), table_ids(config));
    let mut build =
        StateTable::from_table_catalog(&catalog(2, config), env.storage.clone(), None).await;
    build
        .init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
        .await
        .unwrap();
    let mut downstream_state = Vec::new();
    if config.downstream {
        for id in [0, 4, 5] {
            let mut table =
                StateTable::from_table_catalog(&catalog(id, config), env.storage.clone(), None)
                    .await;
            table
                .init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
                .await
                .unwrap();
            downstream_state.push(table);
        }
    }
    for layer in 0..config.fixture_epochs {
        for key in 0..config.keys {
            if config.downstream && layer == 0 {
                downstream_state[0].insert(OwnedRow::new(vec![
                    Some((key as i64).into()),
                    Some((key as i64).into()),
                    Some("probe".into()),
                ]));
            }
            // Interleave keys across committed epochs to retain overlapping key ranges.
            for seq in (layer..config.rows_per_key).step_by(config.fixture_epochs) {
                build.insert(OwnedRow::new(vec![
                    Some((key as i64).into()),
                    Some((seq as i64).into()),
                    Some(payload(key, seq, config.value_bytes).into()),
                ]));
                for table in downstream_state.iter_mut().skip(1) {
                    table.insert(OwnedRow::new(vec![
                        Some((key as i64).into()),
                        Some((seq as i64).into()),
                        Some(1_i64.into()),
                    ]));
                }
                if (seq / config.fixture_epochs + 1).is_multiple_of(256) {
                    for table in &mut downstream_state {
                        table.try_flush().await.unwrap();
                    }
                    build.try_flush().await.unwrap();
                }
            }
            build.try_flush().await.unwrap();
        }
        let next_epoch = test_epoch((layer + 2) as u64);
        env.storage.start_epoch(next_epoch, table_ids(config));
        build
            .commit_assert_no_update_vnode_bitmap(EpochPair::new_test_epoch(next_epoch))
            .await
            .unwrap();
        for table in &mut downstream_state {
            table
                .commit_assert_no_update_vnode_bitmap(EpochPair::new_test_epoch(next_epoch))
                .await
                .unwrap();
        }
        env.commit_epoch(test_epoch((layer + 1) as u64)).await;
    }
    (*env.storage.get_pinned_version()).clone()
}

fn sstables(version: &HummockVersion) -> Vec<SstableInfo> {
    let mut result: Vec<_> = version
        .levels
        .values()
        .flat_map(|levels| levels.l0.sub_levels.iter().chain(levels.levels.iter()))
        .flat_map(|level| level.table_infos.iter().cloned())
        .collect();
    result.sort_by_key(|sst| sst.sst_id);
    assert!(!result.is_empty(), "fixture must contain committed SSTs");
    result
}

async fn manifest(url: &str, version: &HummockVersion, config: &Config) -> serde_json::Value {
    let store = store(url, false, config).await;
    let mut files = Vec::new();
    for info in sstables(version) {
        let bytes = store
            .store()
            .read(
                &store.get_object_data_path(risingwave_hummock_sdk::HummockObjectId::Sstable(
                    info.object_id,
                )),
                ..,
            )
            .await
            .unwrap();
        let mut hash = DefaultHasher::new();
        bytes.hash(&mut hash);
        let table = store
            .sstable(&info, &mut StoreLocalStatistic::default())
            .await
            .unwrap();
        files.push(json!({"object_id": info.object_id.as_raw_id(), "bytes": bytes.len(),
            "content_fingerprint": format!("{:016x}", hash.finish()),
            "blocks": table.meta.block_metas.iter().map(|b| (b.offset, b.len, b.uncompressed_size)).collect::<Vec<_>>() }));
    }
    json!(files)
}

fn metrics() -> String {
    TextEncoder::new()
        .encode_to_string(&GLOBAL_METRICS_REGISTRY.gather())
        .unwrap()
}

fn counter(raw: &str, metric: &str) -> f64 {
    raw.lines()
        .filter(|line| {
            line.starts_with(&format!("{metric}{{")) || line.starts_with(&format!("{metric} "))
        })
        .map(|line| {
            line.split_whitespace()
                .last()
                .unwrap()
                .parse::<f64>()
                .unwrap()
        })
        .sum()
}

// A scalar read diagnostic for flush sanity checks: no prefix hint and no writes.
async fn replay_points(
    url: &str,
    config: &Config,
    version: &HummockVersion,
    scheduler: bool,
    dest: &Path,
) {
    let store = store(url, scheduler, config).await;
    let env = environment(store.clone(), config).await;
    env.storage.update_version_and_wait(version.clone()).await;
    let epoch = test_epoch((config.fixture_epochs + 1) as u64);
    env.storage.start_epoch(epoch, table_ids(config));
    let table =
        StateTable::from_table_catalog(&catalog(2, config), env.storage.clone(), None).await;
    let mut local = env
        .storage
        .new_local(NewLocalOptions::for_test(TableId::new(2)))
        .await;
    local
        .init(InitOptions::new(EpochPair::new_test_epoch(epoch)))
        .await
        .unwrap();
    let reader = local.new_flushed_snapshot_reader();
    let mut queries = Vec::new();
    for key in 0..config.keys {
        for seq in 0..config.rows_per_key {
            let pk = OwnedRow::new(vec![Some((key as i64).into()), Some((seq as i64).into())]);
            queries.push((
                serialize_pk_with_vnode(&pk, table.pk_serde(), table.compute_vnode_by_pk(&pk)),
                key,
                seq,
            ));
        }
    }
    // Fixed shuffle makes request order independent of the two binaries and object layout.
    let mut random = 0x9e3779b97f4a7c15_u64;
    for i in (1..queries.len()).rev() {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        queries.swap(i, random as usize % (i + 1));
    }
    if let Some(step) = config.warm_every_nth_block {
        for info in sstables(version) {
            let mut stats = StoreLocalStatistic::default();
            let table = store.sstable(&info, &mut stats).await.unwrap();
            for block in (0..table.block_count()).filter(|i| (i + 1).is_multiple_of(step)) {
                drop(
                    store
                        .get(&table, block, CachePolicy::Fill(Hint::Normal), &mut stats)
                        .await
                        .unwrap(),
                );
            }
            stats.discard();
        }
    }
    StoreLocalStatistic::flush_all();
    let before = metrics();
    std::fs::write(dest.join("before.prom"), &before).unwrap();
    let mut values = Vec::with_capacity(queries.len() * config.probe_rounds);
    let started = Instant::now();
    for _ in 0..config.probe_rounds {
        for (key, _, _) in &queries {
            values.push(
                reader
                    .on_key_value(
                        key.clone(),
                        ReadOptions {
                            cache_policy: CachePolicy::Fill(Hint::Normal),
                            ..Default::default()
                        },
                        |_, value| Ok(Bytes::copy_from_slice(value)),
                    )
                    .await
                    .unwrap(),
            );
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    StoreLocalStatistic::flush_all();
    let after = metrics();
    std::fs::write(dest.join("after.prom"), &after).unwrap();
    assert_eq!(&*env.storage.get_pinned_version(), version);
    let delta = |name| counter(&after, name) - counter(&before, name);
    assert_eq!(delta("object_store_write_bytes"), 0.0);
    assert_eq!(delta("object_store_failure_count"), 0.0);
    assert_eq!(delta("object_store_request_retry_count"), 0.0);
    for (value, (_, key, seq)) in values.iter().zip_eq(
        queries
            .iter()
            .cycle()
            .take(queries.len() * config.probe_rounds),
    ) {
        let expected = BasicSerializer.serialize(OwnedRow::new(vec![
            Some((*key as i64).into()),
            Some((*seq as i64).into()),
            Some(payload(*key, *seq, config.value_bytes).into()),
        ]));
        assert_eq!(
            value.as_ref().expect("missing fixture row").as_ref(),
            expected.as_slice()
        );
    }
    let physical = |raw: &str| {
        raw.lines()
            .filter(|line| {
                line.starts_with("object_store_operation_latency_count{")
                    && line.contains("type=\"read\"")
            })
            .map(|line| {
                line.split_whitespace()
                    .last()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
            })
            .sum::<u64>()
    };
    if config.warm_every_nth_block == Some(1) {
        assert_eq!(
            physical(&after) - physical(&before),
            0,
            "hot fixture exceeds cache"
        );
        assert_eq!(delta("object_store_read_bytes"), 0.0);
    }
    assert_eq!(
        delta("object_store_read_requests"),
        delta("object_store_read_request_duration_seconds_count")
    );
    let mut deltas = BTreeMap::new();
    for name in [
        "object_store_read_bytes",
        "object_store_read_requests",
        "object_store_read_scheduler_group_size_count",
        "object_store_read_scheduler_group_size_sum",
        "state_store_iter_merge_sstable_counts_sum",
        "state_store_sst_store_block_request_counts",
    ] {
        deltas.insert(name, delta(name));
    }
    let result = json!({"workload": "scalar_get_no_prefix_hint", "scheduler": scheduler,
        "rows": values.len(), "elapsed_s": elapsed, "physical_reads": physical(&after) - physical(&before),
        "metrics": deltas, "fixture_epochs": config.fixture_epochs,
        "sst_count": sstables(version).len(), "version_id": version.id.to_string()});
    std::fs::write(
        dest.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    println!("{result}");
}

async fn replay(
    url: &str,
    config: &Config,
    version: &HummockVersion,
    scheduler: bool,
    dest: &Path,
    actor: ActorContextRef,
) {
    if config.workload != distributed::Workload::LegacyJoin {
        Box::pin(distributed::replay(
            url, config, version, scheduler, dest, actor,
        ))
        .await;
        return;
    }
    let store = store(url, scheduler, config).await;
    let env = environment(store.clone(), config).await;
    assert!(env.storage.get_pinned_version().id < version.id);
    // Read-only fixture metadata is authoritative for this probe epoch. No commit is issued
    // against the fresh test meta manager, whose tables exist only to construct the local stores.
    env.storage.update_version_and_wait(version.clone()).await;
    assert_eq!(&*env.storage.get_pinned_version(), version);
    env.storage.start_epoch(test_epoch(2), table_ids(config));

    let mut state = Vec::new();
    for id in 0..4 {
        state.push(
            StateTable::from_table_catalog(&catalog(id, config), env.storage.clone(), None).await,
        );
    }
    let [left, degree_left, right, degree_right]: [_; 4] = state.try_into().ok().unwrap();
    let (mut tx_l, source_l) = MockSource::channel();
    let (mut tx_r, source_r) = MockSource::channel();
    let schema = Schema::new(types().into_iter().map(Field::unnamed).collect());
    let source_l = source_l
        .stop_on_finish(false)
        .into_executor(schema.clone(), vec![0, 1]);
    let source_r = source_r
        .stop_on_finish(false)
        .into_executor(schema, vec![0, 1]);
    let output_schema = Schema::new(
        types()
            .into_iter()
            .chain(types())
            .map(Field::unnamed)
            .collect(),
    );
    let chunk_size = actor.config.developer.chunk_size;
    let amplification_threshold = actor.config.developer.high_join_amplification_threshold;
    let info = ExecutorInfo::for_test(output_schema, vec![0, 1, 3, 4], "HashJoinReplay".into(), 0);
    let executor = HashJoinExecutor::<Key128, HummockStorage, {JoinType::Inner}, MemoryEncoding>::new_with_cache_size(
        actor.clone(), info.clone(),
        source_l, source_r, JoinParams::new(vec![0], vec![1]), JoinParams::new(vec![0], vec![1]),
        vec![false], (0..6).collect(), None, vec![], left, degree_left, right, degree_right,
        Arc::new(AtomicU64::new(0)), false, Arc::new(StreamingMetrics::unused()), chunk_size, amplification_threshold,
        None, vec![]);
    let mut executor: Executor = (info, executor).into();
    if config.downstream {
        let schema = Schema::new(vec![Field::unnamed(DataType::Int64); 3]);
        let info = ExecutorInfo::for_test(schema, vec![0, 1], "ReplayHashAgg".into(), 1);
        let aggregate = HashAggExecutor::<Key128, HummockStorage>::new(AggExecutorArgs {
            version: PbAggNodeVersion::LATEST,
            input: executor,
            actor_ctx: actor.clone(),
            info: info.clone(),
            extreme_cache_size: actor.config.developer.unsafe_extreme_cache_size,
            agg_calls: vec![AggCall::from_pretty("(count:int8)")],
            row_count_index: 0,
            storages: vec![AggStateStorage::Value],
            intermediate_state_table: StateTable::from_table_catalog(
                &catalog(4, config),
                env.storage.clone(),
                None,
            )
            .await,
            distinct_dedup_tables: Default::default(),
            watermark_epoch: Arc::new(AtomicU64::new(0)),
            extra: HashAggExecutorExtraArgs {
                group_key_indices: vec![3, 4],
                chunk_size,
                max_dirty_groups_heap_size: actor
                    .config
                    .developer
                    .hash_agg_max_dirty_groups_heap_size,
                emit_on_window_close: false,
            },
        })
        .unwrap();
        let aggregate: Executor = (info.clone(), aggregate).into();
        // The helper uses a zero read-prefix hint internally. NoCheck only writes/flushes
        // (sanity gets have no hint); SST filters use registered catalog(5, config)'s real PK prefix.
        let materialize = MaterializeExecutor::for_test(
            aggregate,
            env.storage.clone(),
            TableId::new(5),
            (0..2)
                .map(|i| ColumnOrder::new(i, OrderType::ascending()))
                .collect(),
            (0..3).map(ColumnId::new).collect(),
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::NoCheck,
        )
        .await;
        executor = (info, materialize).into();
    }
    let mut output = executor.execute();
    tx_l.push_barrier_with_prev_epoch_for_test(test_epoch(2), test_epoch(1), false);
    tx_r.push_barrier_with_prev_epoch_for_test(test_epoch(2), test_epoch(1), false);
    assert!(matches!(
        output.next().await.unwrap().unwrap(),
        Message::Barrier(_)
    ));
    // Finish initialization. Both inputs are now pending; no wall-clock scheduling is needed.
    assert!(output.next().now_or_never().is_none());
    if let Some(step) = config.warm_every_nth_block {
        for info in sstables(version) {
            let table = store
                .sstable(&info, &mut StoreLocalStatistic::default())
                .await
                .unwrap();
            for block in (0..table.block_count()).filter(|i| (i + 1).is_multiple_of(step)) {
                drop(
                    store
                        .get(
                            &table,
                            block,
                            CachePolicy::Fill(Hint::Normal),
                            &mut StoreLocalStatistic::default(),
                        )
                        .await
                        .unwrap(),
                );
            }
        }
    }
    // Only the left input has ready chunks. The production barrier aligner remains unchanged.
    let mut key_order: Vec<_> = (0..config.keys).collect();
    match config.probe_order {
        ProbeOrder::Stride => {
            for (i, key) in key_order.iter_mut().enumerate() {
                *key = (i * 5) % config.keys;
            }
        }
        ProbeOrder::Sequential => {}
        ProbeOrder::Shuffled => {
            // Fixed Fisher-Yates permutation: input locality is reproducible across binaries.
            let mut state = 0x9e3779b97f4a7c15_u64;
            for i in (1..key_order.len()).rev() {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                key_order.swap(i, (state % (i as u64 + 1)) as usize);
            }
        }
    }
    let operations = config.probe_pattern.operations();
    let mut probes = Vec::new();
    for round in 0..config.probe_rounds {
        // Finish inserting all dimension keys before deleting the same rows.
        for &op in operations {
            for &key in &key_order {
                let probe_id = round * config.keys + key;
                probes.push((
                    op,
                    OwnedRow::new(vec![
                        Some((key as i64).into()),
                        Some((probe_id as i64).into()),
                        Some("probe".into()),
                    ]),
                ));
            }
        }
    }
    let chunks: Vec<_> = probes
        .chunks(config.probe_chunk_size)
        .map(|rows| StreamChunk::from_rows(rows, &types()))
        .collect();
    StoreLocalStatistic::flush_all();
    let before = metrics();
    std::fs::write(dest.join("before.prom"), &before).unwrap();
    tracing::trace!(target: "state_store_read_batch", "replay measurement started");
    let start = Instant::now();
    for chunk in chunks {
        tx_l.push_chunk(chunk);
    }
    if config.downstream {
        // Seal local state, but do not sync/commit to fresh meta or create a new SST layout.
        env.storage.start_epoch(test_epoch(3), table_ids(config));
        tx_l.push_barrier_with_prev_epoch_for_test(test_epoch(3), test_epoch(2), false);
        tx_r.push_barrier_with_prev_epoch_for_test(test_epoch(3), test_epoch(2), false);
    } else {
        // Join-only replay intentionally excludes epoch flush work.
        drop(tx_l);
        drop(tx_r);
    }
    let expected = config.keys * config.probe_rounds * config.rows_per_key * operations.len();
    let mut rows = Vec::with_capacity(expected);
    let mut final_barriers = 0;
    while let Some(message) = output.next().await {
        match message.unwrap() {
            Message::Chunk(chunk) => {
                for (op, row) in chunk.rows() {
                    rows.push((op, row.into_owned_row()));
                    assert!(rows.len() <= expected, "extra output");
                }
            }
            Message::Barrier(barrier) if config.downstream => {
                assert_eq!(barrier.epoch.curr, test_epoch(3));
                final_barriers += 1;
                // Complete post-barrier work while retaining all local read versions.
                assert!(output.next().now_or_never().is_none());
                break;
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }
    assert_eq!(rows.len(), expected, "unexpected output count");
    assert_eq!(final_barriers, usize::from(config.downstream));
    let elapsed = start.elapsed().as_secs_f64();
    tracing::trace!(target: "state_store_read_batch", "replay measurement finished");
    StoreLocalStatistic::flush_all();
    let after = metrics();
    std::fs::write(dest.join("after.prom"), &after).unwrap();
    assert_eq!(&*env.storage.get_pinned_version(), version);
    let delta = |name| counter(&after, name) - counter(&before, name);
    assert_eq!(
        delta("object_store_write_bytes"),
        0.0,
        "measured epoch must not upload SSTs"
    );
    assert_eq!(delta("object_store_failure_count"), 0.0);
    assert_eq!(delta("object_store_request_retry_count"), 0.0);
    assert_eq!(
        delta("object_store_read_requests"),
        delta("object_store_read_request_duration_seconds_count"),
        "unfinished reads"
    );

    let mut seen = vec![false; expected];
    for (op, row) in &rows {
        if config.downstream {
            let key = row.datum_at(0).unwrap().into_int64() as usize;
            let seq = row.datum_at(1).unwrap().into_int64() as usize;
            assert_eq!(*op, Op::Delete);
            assert!(key < config.keys && seq < config.rows_per_key);
            assert_eq!(
                row,
                &OwnedRow::new(vec![
                    Some((key as i64).into()),
                    Some((seq as i64).into()),
                    Some(1_i64.into())
                ])
            );
            assert!(!std::mem::replace(
                &mut seen[key * config.rows_per_key + seq],
                true
            ));
            continue;
        }
        let key = row.datum_at(0).unwrap().into_int64() as usize;
        let seq = row.datum_at(4).unwrap().into_int64() as usize;
        let probe_id = row.datum_at(1).unwrap().into_int64() as usize;
        assert!(probe_id < config.keys * config.probe_rounds);
        assert_eq!(key, probe_id % config.keys);
        assert!(key < config.keys && seq < config.rows_per_key);
        let phase = match op {
            Op::Insert => 0,
            Op::Delete if matches!(config.probe_pattern, ProbePattern::InsertDelete) => 1,
            other => panic!("unexpected output operation: {other:?}"),
        };
        let insert_index = probe_id * operations.len() * config.rows_per_key + seq;
        if phase == 1 {
            assert!(
                seen[insert_index],
                "delete preceded its corresponding insert"
            );
        }
        assert!(!std::mem::replace(
            &mut seen[insert_index + phase * config.rows_per_key],
            true
        ));
        assert_eq!(
            row,
            &OwnedRow::new(vec![
                Some((key as i64).into()),
                Some((probe_id as i64).into()),
                Some("probe".into()),
                Some((key as i64).into()),
                Some((seq as i64).into()),
                Some(payload(key, seq, config.value_bytes).into())
            ])
        );
    }
    assert!(seen.into_iter().all(|x| x));
    let mut deltas = BTreeMap::new();
    for name in [
        "object_store_read_bytes",
        "object_store_read_requests",
        "object_store_read_scheduler_group_size_count",
        "object_store_read_scheduler_group_size_sum",
        "object_store_read_scheduler_queue_duration_seconds_sum",
        "object_store_read_scheduler_queue_duration_seconds_count",
        "object_store_read_request_duration_seconds_sum",
    ] {
        deltas.insert(name, delta(name));
    }
    // before/after operation latency has other operation labels; isolate physical reads.
    let physical = |raw: &str| {
        raw.lines()
            .filter(|line| {
                line.starts_with("object_store_operation_latency_count{")
                    && line.contains("type=\"read\"")
            })
            .map(|line| {
                line.split_whitespace()
                    .last()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
            })
            .sum::<u64>()
    };
    let block_cache_entries = store.block_cache().memory().entries();
    let block_cache_weight_bytes = store.block_cache().memory().usage();
    let sanity_check_enabled = cfg!(debug_assertions) && !config.disable_sanity_check;
    let mut downstream_metrics = BTreeMap::new();
    if config.downstream {
        for id in [0, 4, 5] {
            let label = format!("table_id=\"{id}\"");
            let filtered = |raw: &str| {
                raw.lines()
                    .filter(|line| line.contains(&label))
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            let before_table = filtered(&before);
            let after_table = filtered(&after);
            let writes = counter(&after_table, "state_store_write_batch_tuple_counts")
                - counter(&before_table, "state_store_write_batch_tuple_counts");
            let expected_writes = if id == 0 { config.keys } else { expected };
            assert_eq!(
                writes, expected_writes as f64,
                "table {id} was not fully flushed"
            );
            let mut counters = BTreeMap::new();
            for metric in [
                "state_store_write_batch_tuple_counts",
                "state_store_iter_merge_sstable_counts_sum",
                "state_store_sst_store_block_request_counts",
                "state_store_sst_store_meta_request_counts",
            ] {
                counters.insert(
                    metric,
                    counter(&after_table, metric) - counter(&before_table, metric),
                );
            }
            let gets = |raw: &str| {
                counter(
                    &raw.lines()
                        .filter(|line| line.contains("type=\"committed-overlapping-get\""))
                        .collect::<Vec<_>>()
                        .join("\n"),
                    "state_store_iter_merge_sstable_counts_count",
                )
            };
            let gets = gets(&after_table) - gets(&before_table);
            // Count restoration reads separately from debug-only flush validation.
            let expected_gets = match id {
                0 => config.keys * usize::from(sanity_check_enabled),
                4 => expected * (1 + usize::from(sanity_check_enabled)),
                5 => expected * usize::from(sanity_check_enabled),
                _ => unreachable!(),
            };
            assert_eq!(
                gets, expected_gets as f64,
                "unexpected point reads for table {id}"
            );
            counters.insert("point_gets", gets);
            downstream_metrics.insert(id, counters);
            // Outside timing: verify deletes reached local Hummock state, not only output.
            let reader = env
                .storage
                .new_read_snapshot(
                    HummockReadEpoch::NoWait(test_epoch(2)),
                    NewReadSnapshotOptions {
                        table_id: TableId::new(id),
                        table_option: Default::default(),
                    },
                )
                .await
                .unwrap();
            let mut iter = reader
                .iter(
                    prefixed_range_with_vnode(
                        (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                        VirtualNode::ZERO,
                    ),
                    ReadOptions::default(),
                )
                .await
                .unwrap();
            assert!(
                iter.try_next().await.unwrap().is_none(),
                "table {id} retains rows after delete flush"
            );
        }
    }
    let result = json!({"sanity_check_enabled": sanity_check_enabled, "downstream": config.downstream, "final_barriers": final_barriers, "downstream_metrics": downstream_metrics, "output_chunk_size": chunk_size, "scheduler": scheduler, "rows": rows.len(), "elapsed_s": elapsed,
        "physical_reads": physical(&after) - physical(&before), "metrics": deltas,
        "block_cache_entries": block_cache_entries,
        "block_cache_weight_bytes": block_cache_weight_bytes,
        "block_cache_capacity_bytes": store.block_cache().memory().capacity()});
    std::fs::write(
        dest.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    println!("{result}");
    // No meta commit changes the fixture. Dropping the runtime cancels all test tasks.
    drop(output);
}

fn main() {
    let config_path = std::env::args()
        .skip(1)
        .find(|arg| !arg.starts_with('-'))
        .expect("pass config.json");
    let config_path = Path::new(&config_path);
    let dir = config_path.parent().unwrap();
    let config: Config = serde_json::from_slice(&std::fs::read(config_path).unwrap()).unwrap();
    assert!((1..=4).contains(&config.probe_rounds));
    assert!((1..=8).contains(&config.fixture_epochs));
    assert!(config.fixture_epochs <= config.rows_per_key);
    assert!(config.point_reads || config.fixture_epochs == 1);
    assert!(
        !config.downstream
            || (!config.point_reads
                && config.probe_rounds == 1
                && matches!(config.probe_pattern, ProbePattern::DeleteOnly))
    );
    assert!(config.downstream || !matches!(config.probe_pattern, ProbePattern::DeleteOnly));
    assert!((1..=32768).contains(&config.keys));
    if matches!(config.probe_order, ProbeOrder::Stride) {
        assert!(
            config.keys.is_power_of_two(),
            "stride order requires power-of-two keys"
        );
    }
    assert!((1..=200000).contains(&config.rows_per_key) && config.value_bytes <= 1024);
    assert!(config.keys * config.rows_per_key * (config.value_bytes + 64) <= 768 << 20);
    let output_rows = config.keys
        * config.rows_per_key
        * config.probe_rounds
        * config.probe_pattern.operations().len();
    assert!(output_rows <= 8_000_000);
    assert!(output_rows * (config.value_bytes + 64) <= 1 << 30);
    assert!(config.probe_chunk_size > 0 && (1..=20).contains(&config.repeats));
    assert!(config.warm_every_nth_block.is_none_or(|n| n > 0));
    assert!(!config.schedulers.is_empty() && config.schedulers.len() <= 2);
    assert!(config.schedulers.len() != 2 || config.schedulers[0] != config.schedulers[1]);
    let url = std::env::var("RW_HASH_JOIN_REPLAY_URL")
        .expect("set RW_HASH_JOIN_REPLAY_URL to a dedicated MinIO bucket");
    assert!(
        url.starts_with("minio://"),
        "this benchmark requires real MinIO reads"
    );
    // Install a subscriber even when trace diagnostics are off, as the CN does.
    // Without one, tracing's log compatibility path can evaluate disabled span fields.
    tracing_subscriber::fmt()
        .with_env_filter(if config.trace {
            "state_store_read_batch=trace"
        } else {
            "info"
        })
        .json()
        .with_writer(std::io::stderr)
        .init();
    let mut opts = storage_options(&config);
    if config.downstream {
        opts.shared_buffer_capacity_mb = 688;
    }
    let mut spec = json!({"format_version": 3, "fixture_epochs": config.fixture_epochs, "sstable_size_mb": opts.sstable_size_mb, "block_size_kb": opts.block_size_kb, "keys": config.keys, "rows_per_key": config.rows_per_key, "value_bytes": config.value_bytes, "tables": (0..if config.downstream { 6 } else { 4 }).map(|id| catalog(id, &config).encode_to_vec()).collect::<Vec<_>>()});
    if config.downstream {
        spec["format_version"] = json!(4);
        spec["downstream"] = json!(true);
    }
    if config.workload != distributed::Workload::LegacyJoin {
        assert!(config.downstream && !config.point_reads);
        assert!(config.group_rows > 0);
        assert!((config.keys * config.rows_per_key).is_multiple_of(config.group_rows));
        assert!((config.keys * config.rows_per_key / config.group_rows).is_power_of_two());
        assert_eq!(
            config.vnode_count, 256,
            "this replay actor owns the configured test vnode space"
        );
        if config.workload == distributed::Workload::JoinRegroup {
            assert!(
                config.keys * config.rows_per_key / config.group_rows >= config.rows_per_key,
                "join primary keys must remain unique within a prefix"
            );
        }
        spec["format_version"] = json!(5);
        spec["workload"] = json!(config.workload);
        spec["group_rows"] = json!(config.group_rows);
        spec["vnode_count"] = json!(config.vnode_count);
    }
    let version_path = dir.join("fixture.pb");
    let version = if version_path.exists() {
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.join("fixture-spec.json")).unwrap()).unwrap();
        assert_eq!(
            saved, spec,
            "use a new fixture directory when changing data"
        );
        HummockVersion::from(
            PbHummockVersion::decode(std::fs::read(&version_path).unwrap().as_slice()).unwrap(),
        )
    } else {
        let version = runtime().block_on(make_fixture(&url, &config));
        let pb: PbHummockVersion = (&version).into();
        std::fs::write(&version_path, pb.encode_to_vec()).unwrap();
        std::fs::write(
            dir.join("fixture-spec.json"),
            serde_json::to_vec_pretty(&spec).unwrap(),
        )
        .unwrap();
        version
    };
    let files = runtime().block_on(manifest(&url, &version, &config));
    let manifest_path = dir.join("fixture-manifest.json");
    if manifest_path.exists() {
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        assert_eq!(saved, files, "SST bytes or block layout changed");
    } else {
        std::fs::write(&manifest_path, serde_json::to_vec_pretty(&files).unwrap()).unwrap();
    }
    if risingwave_common::util::env_var::env_var_is_true("RW_REPLAY_PREPARE_ONLY") {
        if config.workload != distributed::Workload::LegacyJoin {
            let layout = runtime().block_on(distributed::layout(&url, &version, &config));
            std::fs::write(
                dir.join("fixture-layout.json"),
                serde_json::to_vec_pretty(&layout).unwrap(),
            )
            .unwrap();
        }
        return;
    }
    let runs = dir.join(format!(
        "results-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&runs).unwrap();
    std::fs::copy(config_path, runs.join("config.json")).unwrap();
    println!("results={}", runs.display());
    std::fs::write(
        runs.join("effective-storage-options.txt"),
        format!("{opts:#?}"),
    )
    .unwrap();
    // Fixture construction keeps the build's normal checks. One config applies to all
    // replay runtimes in this process; the existing storage switch is intentionally one-way.
    if config.disable_sanity_check {
        risingwave_storage::hummock::utils::disable_sanity_check();
    }
    for repeat in 0..config.repeats {
        let mut arms = config.schedulers.clone();
        if !repeat.is_multiple_of(2) {
            arms.reverse();
        }
        for scheduler in arms {
            let dest = runs.join(format!("run-{repeat}-scheduler-{scheduler}"));
            std::fs::create_dir(&dest)
                .expect("preserve existing measurements; choose a new output directory");
            runtime().block_on(async {
                tokio::time::timeout(
                    std::time::Duration::from_secs(900),
                    async {
                        if config.point_reads {
                            Box::pin(replay_points(&url, &config, &version, scheduler, &dest))
                                .await;
                        } else {
                            let actor = ActorContext::for_test(123);
                            Box::pin(with_streaming_config(
                                actor.config.clone(),
                                replay(&url, &config, &version, scheduler, &dest, actor),
                            ))
                            .await;
                        }
                    }
                    .boxed(),
                )
                .await
                .unwrap();
            });
        }
    }
    assert_eq!(
        runtime().block_on(manifest(&url, &version, &config)),
        files,
        "fixture changed during replay"
    );
}
