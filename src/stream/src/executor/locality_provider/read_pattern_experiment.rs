// Copyright 2026 RisingWave Labs
// SPDX-License-Identifier: Apache-2.0

// Actual Locality and HashJoin executors connected through the local exchange
// permit channel. Inputs and barriers are supplied by this harness; no SQL plan,
// meta barrier loop, remote exchange, MinIO or full Q18 query is simulated.
use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use foyer::{CacheBuilder, HybridCacheBuilder};
use prometheus::Registry;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, TableId};
use risingwave_common::config::storage::{
    BlockwisePrefetchConfig, ReadBatchConfig, ReadSchedulerConfig,
};
use risingwave_common::config::{MetricLevel, StreamingConfig};
use risingwave_common::hash::Key64;
use risingwave_common::util::epoch::{EpochPair, test_epoch};
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_test::test_utils::{HummockTestEnv, prepare_hummock_test_env_with_store};
use risingwave_object_store::object::{
    InMemObjectStore, ObjectStore, ObjectStoreConfig, ObjectStoreImpl,
};
use risingwave_storage::hummock::none::NoneRecentFilter;
use risingwave_storage::hummock::test_utils::default_opts_for_test;
use risingwave_storage::hummock::{
    HummockStorage, SstableStore, SstableStoreConfig, SstableStoreRef,
};
use risingwave_storage::monitor::{HummockStateStoreMetrics, ObjectStoreMetrics};
use tokio::sync::{Barrier as Gate, oneshot};

use super::*;
use crate::common::table::test_utils::gen_pbtable_with_dist_key;
use crate::executor::exchange::permit;
use crate::executor::test_utils::{MockSource, with_streaming_config};
use crate::executor::{
    DispatcherMessageBatch, HashJoinExecutor, JoinParams, JoinType, MemoryEncoding,
};
use crate::task::LocalBarrierManager;

const ACTORS: usize = 4;
const VNODES: usize = 16;
fn is_semi() -> bool {
    std::env::var("RW_CHAIN_JOIN").is_ok_and(|s| s == "semi")
        || std::env::var("RW_CHAIN_JOIN").is_err()
}
fn repeats() -> usize {
    if is_semi() { 4 } else { 1 }
}
const CHUNK: usize = 256;
const IDS: [u32; 6] = [30, 31, 32, 33, 34, 35];
type Catalog = risingwave_pb::catalog::Table;

fn data_types() -> Vec<DataType> {
    vec![DataType::Int64, DataType::Int64, DataType::Varchar]
}
fn catalog(id: u32) -> Catalog {
    let types = if id == 35 {
        if is_semi() {
            vec![
                VirtualNode::RW_TYPE,
                DataType::Int64,
                DataType::Int64,
                DataType::Boolean,
                DataType::Int64,
            ]
        } else {
            vec![
                VirtualNode::RW_TYPE,
                DataType::Int64,
                DataType::Boolean,
                DataType::Int64,
            ]
        }
    } else if id == 31 || id == 33 {
        vec![DataType::Int64, DataType::Int64, DataType::Int64]
    } else {
        data_types()
    };
    let pk = if id == 35 || (is_semi() && id == 32) || (!is_semi() && (id == 30 || id == 34)) {
        vec![0]
    } else {
        vec![0, 1]
    };
    let mut t = gen_pbtable_with_dist_key(
        TableId::new(id),
        types
            .into_iter()
            .enumerate()
            .map(|(i, t)| ColumnDesc::unnamed(ColumnId::new(i as i32), t))
            .collect(),
        vec![OrderType::ascending(); pk.len()],
        pk,
        1,
        vec![0],
    );
    t.maybe_vnode_count = Some(VNODES as u32);
    if id == 35 {
        t.vnode_col_index = Some(0);
    }
    t
}
fn bitmap(actor: Option<usize>) -> Arc<Bitmap> {
    Arc::new(
        (0..VNODES)
            .map(|v| actor.is_none_or(|a| v % ACTORS == a))
            .collect(),
    )
}
fn payload(key: usize, row: usize) -> String {
    let mut x = ((key * 131 + row + 1) as u64).wrapping_mul(0x9e3779b97f4a7c15);
    (0..512)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            char::from(33 + (x % 90) as u8)
        })
        .collect()
}
fn row(k: usize, r: usize) -> OwnedRow {
    OwnedRow::new(vec![
        Some((k as i64).into()),
        Some((r as i64).into()),
        Some(payload(k, r).into()),
    ])
}
async fn table(env: &HummockTestEnv, id: u32, actor: Option<usize>) -> StateTable<HummockStorage> {
    StateTable::from_table_catalog(&catalog(id), env.storage.clone(), Some(bitmap(actor))).await
}

async fn make_store(
    registry: &Registry,
) -> (SstableStoreRef, InMemObjectStore, Arc<ObjectStoreMetrics>) {
    let metrics = Arc::new(ObjectStoreMetrics::new(registry));
    let backend = InMemObjectStore::for_test();
    let object = Arc::new(
        ObjectStoreImpl::InMem(backend.clone().monitored(
            metrics.clone(),
            Arc::new(ObjectStoreConfig {
                read_scheduler: ReadSchedulerConfig {
                    use_dedicated_io_runtime: true,
                    dedicated_io_runtime_workers: 2.try_into().unwrap(),
                    max_admitted_groups: 128.try_into().unwrap(),
                    max_inflight_reads: 32.try_into().unwrap(),
                    max_batch_requests: 16.try_into().unwrap(),
                    max_read_bytes: (1024 * 1024).try_into().unwrap(),
                    max_gap_bytes: 0,
                    reuse_inflight_reads: true,
                    ..Default::default()
                },
                ..Default::default()
            }),
        ))
        .with_read_coalescing(),
    );
    let meta_cache = HybridCacheBuilder::new()
        .memory(64 << 20)
        .with_shards(2)
        .with_weighter(
            |_: &risingwave_hummock_sdk::HummockSstableObjectId,
             v: &risingwave_storage::hummock::SstableMetaCacheValue| {
                v.estimated_meta_cache_memory_weight()
            },
        )
        .storage()
        .build()
        .await
        .unwrap();
    let cache_mb = std::env::var("RW_CHAIN_CACHE_MB").map_or(64usize, |s| s.parse().unwrap());
    let block_cache = HybridCacheBuilder::new()
        .memory(cache_mb << 20)
        .with_shards(2)
        .with_weighter(
            |_: &risingwave_storage::hummock::SstableBlockIndex,
             v: &Box<risingwave_storage::hummock::Block>| v.estimated_memory_weight(),
        )
        .storage()
        .build()
        .await
        .unwrap();
    let store = Arc::new(SstableStore::new(SstableStoreConfig {
        store: object,
        path: "test".to_owned(),
        prefetch_buffer_capacity: 64 << 20,
        max_prefetch_block_number: 16,
        blockwise_prefetch: BlockwisePrefetchConfig {
            enabled: true,
            locality_backfill_enabled: true,
            snapshot_backfill_enabled: true,
            max_blocks: 16.try_into().unwrap(),
            max_bytes: (1024 * 1024).try_into().unwrap(),
            ..Default::default()
        },
        read_batch: ReadBatchConfig {
            enabled: true,
            meta_reuse: true,
            ..Default::default()
        },
        recent_filter: Arc::new(NoneRecentFilter::default().into()),
        state_store_metrics: Arc::new(HummockStateStoreMetrics::new(registry, MetricLevel::Info)),
        use_new_object_prefix_strategy: true,
        skip_bloom_filter_in_serde: false,
        meta_cache,
        block_cache,
        vector_meta_cache: CacheBuilder::new(64 << 20).build(),
        vector_block_cache: CacheBuilder::new(64 << 20).build(),
    }));
    assert!(!store.block_cache().is_hybrid());
    (store, backend, metrics)
}

// Production permit accounting is retained; meta-provided barrier mutations are
// unnecessary at the receiving Join in this fixed graph.
struct LocalInput(permit::Receiver);
impl Execute for LocalInput {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.stream().boxed()
    }
}
impl LocalInput {
    #[try_stream(ok=Message,error=StreamExecutorError)]
    async fn stream(mut self: Box<Self>) {
        while let Some(m) = self.0.recv().await {
            match m {
                DispatcherMessageBatch::Chunk(c) => yield Message::Chunk(c),
                DispatcherMessageBatch::Watermark(w) => yield Message::Watermark(w),
                DispatcherMessageBatch::BarrierBatch(bs) => {
                    for b in bs {
                        yield Message::Barrier(Barrier {
                            epoch: b.epoch,
                            mutation: None,
                            kind: b.kind,
                            tracing_context: b.tracing_context,
                        });
                    }
                }
            }
        }
    }
}

async fn sample<const T: u8>(
    keys: usize,
    fanout: usize,
    delay_us: u64,
    capacity: usize,
    streaming: &tokio::runtime::Runtime,
) {
    let registry = Registry::new();
    let (store, backend, object_metrics) = make_store(&registry).await;
    let env = prepare_hummock_test_env_with_store(store.clone(), default_opts_for_test()).await;
    for id in IDS {
        env.register_table(catalog(id)).await;
    }
    let all_ids = IDS.into_iter().map(TableId::new).collect::<HashSet<_>>();
    env.storage.start_epoch(test_epoch(1), all_ids.clone());
    let mut seeded = Vec::new();
    let mut actor_rows = [0; ACTORS];
    let mut actor_keys: Vec<HashSet<usize>> = (0..ACTORS).map(|_| HashSet::new()).collect();
    for id in IDS {
        let mut t = table(&env, id, None).await;
        t.init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
            .await
            .unwrap();
        if id == 34 || id == 32 || id == 33 {
            for k in 0..keys {
                let count = if id == 34 { repeats() } else { fanout };
                for r in 0..count {
                    let data = if id == 33 {
                        OwnedRow::new(vec![
                            Some((k as i64).into()),
                            Some((r as i64).into()),
                            Some(0_i64.into()),
                        ])
                    } else {
                        row(k, r)
                    };
                    if id == 34 {
                        let actor = t
                            .compute_vnode_by_pk((&data).project(if is_semi() {
                                &[0, 1][..]
                            } else {
                                &[0][..]
                            }))
                            .to_index()
                            % ACTORS;
                        actor_rows[actor] += 1;
                        actor_keys[actor].insert(k);
                    }
                    t.insert(data);
                }
            }
        }
        seeded.push(t);
    }
    env.storage.start_epoch(test_epoch(2), all_ids.clone());
    for t in &mut seeded {
        t.commit_for_test(EpochPair::new(test_epoch(2), test_epoch(1)))
            .await
            .unwrap();
    }
    env.commit_epoch(test_epoch(1)).await;
    drop(seeded);
    let version = env.manager.get_current_version().await;
    println!(
        "CHAIN_SEED keys={keys} fanout={fanout} version={:?} actor_rows={actor_rows:?}",
        version.id
    );
    let mut seed_ssts = Vec::new();
    for levels in version.levels.values() {
        for level in levels.l0.sub_levels.iter().chain(levels.levels.iter()) {
            for sst in &level.table_infos {
                seed_ssts.push((sst.object_id, sst.table_ids.clone(), sst.file_size));
            }
        }
    }
    seed_ssts.sort();
    println!("CHAIN_SSTS {}", serde_json::to_string(&seed_ssts).unwrap());
    store.clear_block_cache().await.unwrap();
    backend.set_read_delay_for_test(Duration::from_micros(delay_us));
    env.storage.start_epoch(test_epoch(3), all_ids.clone());
    let metrics = Arc::new(StreamingMetrics::new(&registry, MetricLevel::Info));
    let gate = Arc::new(Gate::new(ACTORS * 2 + 1));
    let physical = object_metrics.operation_size.with_label_values(&["read"]);
    let before_count = physical.get_sample_count();
    let before_bytes = physical.get_sample_sum();
    let mut joins = Vec::new();
    let mut producers = Vec::new();
    let mut controls = Vec::new();
    let mut done = Vec::new();
    let mut holds = Vec::new();
    let mut retained_metrics = Vec::new();
    for (a, expected) in actor_rows.into_iter().enumerate() {
        for (side, table_id) in [("left", "30"), ("right", "32")] {
            let actor = (200 + a).to_string();
            retained_metrics.push(
                metrics
                    .join_lookup_total_count
                    .with_guarded_label_values(&[&actor, side, table_id, "0"]),
            );
            retained_metrics.push(
                metrics
                    .join_lookup_miss_count
                    .with_guarded_label_values(&[&actor, side, table_id, "0"]),
            );
        }
        let schema = Schema::new(data_types().into_iter().map(Field::unnamed).collect());
        let (mut tx_l, source) = MockSource::channel();
        let provider = LocalityProviderExecutor::new(
            source.into_executor(schema.clone(), if is_semi() { vec![0, 1] } else { vec![0] }),
            vec![0],
            table(&env, 34, Some(a)).await,
            table(&env, 35, Some(a)).await,
            None,
            SortBufferSettings {
                enabled: false,
                activate_threshold: 0,
            },
            schema.clone(),
            CreateMviewProgressReporter::new(
                LocalBarrierManager::for_test(),
                22.into(),
                (100 + a as u32).into(),
            ),
            metrics.clone(),
            CHUNK,
            22.into(),
            RateLimit::Disabled,
        );
        let (tx, rx) =
            permit::channel_with_metrics(capacity, 256, 1, permit::ChannelMetrics::for_test());
        let g = gate.clone();
        producers.push(streaming.spawn(with_streaming_config(
            Arc::new(StreamingConfig::default()),
            async move {
                let mut stream = provider.boxed().execute();
                let mut started: Option<Instant> = None;
                let mut emitted = 0;
                let mut scan_us = 0.;
                let mut blocked_us = 0.;
                while let Some(m) = stream.try_next().await.unwrap() {
                    let start = matches!(&m,Message::Barrier(b) if b.epoch.curr==test_epoch(3));
                    let terminal = matches!(&m,Message::Barrier(b) if b.epoch.curr==test_epoch(4));
                    let out = match m {
                        Message::Chunk(c) => {
                            emitted += c.cardinality();
                            if emitted == expected {
                                scan_us = started.unwrap().elapsed().as_secs_f64() * 1e6;
                            }
                            DispatcherMessageBatch::Chunk(c)
                        }
                        Message::Barrier(b) => {
                            DispatcherMessageBatch::BarrierBatch(vec![b.into_dispatcher()])
                        }
                        Message::Watermark(w) => DispatcherMessageBatch::Watermark(w),
                    };
                    let t = Instant::now();
                    if tx.send(out).await.is_err() {
                        break;
                    }
                    if !terminal {
                        blocked_us += t.elapsed().as_secs_f64() * 1e6;
                    }
                    if start {
                        g.wait().await;
                        started = Some(Instant::now());
                    }
                    if terminal {
                        break;
                    }
                }
                assert_eq!(emitted, expected);
                (scan_us, blocked_us)
            },
        )));
        let input = Executor::new(
            ExecutorInfo::for_test(
                schema.clone(),
                if is_semi() { vec![0, 1] } else { vec![0] },
                "LocalPermitExchange".to_owned(),
                0,
            ),
            Box::new(LocalInput(rx)),
        );
        let (mut tx_r, right) = MockSource::channel();
        let output_schema = if T == JoinType::LeftSemi {
            schema.clone()
        } else {
            Schema::new([schema.fields.clone(), schema.fields.clone()].concat())
        };
        let len = output_schema.len();
        let join = HashJoinExecutor::<Key64, HummockStorage, T, MemoryEncoding>::new(
            ActorContext::for_test(200 + a as u32),
            ExecutorInfo::for_test(
                output_schema,
                if is_semi() { vec![0, 1] } else { vec![0, 4] },
                "PatternJoin".to_owned(),
                0,
            ),
            input,
            right.into_executor(schema, if is_semi() { vec![0] } else { vec![0, 1] }),
            JoinParams::new(vec![0], if is_semi() { vec![1] } else { vec![] }),
            JoinParams::new(vec![0], if is_semi() { vec![] } else { vec![1] }),
            vec![false],
            (0..len).collect(),
            None,
            vec![],
            table(&env, 30, Some(a)).await,
            table(&env, 31, Some(a)).await,
            table(&env, 32, Some(a)).await,
            table(&env, 33, Some(a)).await,
            Arc::new(AtomicU64::new(0)),
            false,
            metrics.clone(),
            CHUNK,
            2048,
            vec![],
        );
        let (done_tx, done_rx) = oneshot::channel();
        let (hold_tx, hold_rx) = oneshot::channel();
        done.push(done_rx);
        holds.push(hold_tx);
        let g = gate.clone();
        let expected_keys = actor_keys[a].clone();
        let expected_repeats = repeats();
        joins.push(streaming.spawn(with_streaming_config(
            Arc::new(StreamingConfig::default()),
            async move {
                let mut stream = join.boxed().execute();
                let mut started: Option<Instant> = None;
                let mut count = 0;
                let mut first_us = 0.;
                let mut waits = Vec::new();
                let mut seen = HashSet::new();
                let expected_out = expected * if T == JoinType::Inner { fanout } else { 1 };
                loop {
                    let polled = Instant::now();
                    let msg = stream.try_next().await.unwrap().expect("join ended early");
                    match msg {
                        Message::Barrier(b) if b.epoch.curr == test_epoch(3) => {
                            g.wait().await;
                            started = Some(Instant::now());
                        }
                        Message::Chunk(c) => {
                            let now = started.unwrap().elapsed().as_secs_f64() * 1e6;
                            if count == 0 {
                                first_us = now;
                            }
                            waits.push(polled.elapsed().as_secs_f64() * 1e6);
                            for (op, r) in c.rows() {
                                assert_eq!(op, Op::Insert);
                                let k = r.datum_at(0).unwrap().into_int64();
                                let rid = r.datum_at(1).unwrap().into_int64();
                                assert!(
                                    k >= 0
                                        && (k as usize) < keys
                                        && rid >= 0
                                        && (rid as usize) < expected_repeats
                                );
                                assert!(expected_keys.contains(&(k as usize)));
                                let right_id = if T == JoinType::Inner {
                                    r.datum_at(4).unwrap().into_int64()
                                } else {
                                    0
                                };
                                assert!(seen.insert((k, rid, right_id)), "duplicate output");
                                if T == JoinType::Inner {
                                    assert_eq!(r.datum_at(0), r.datum_at(3));
                                    let right_id = r.datum_at(4).unwrap().into_int64();
                                    assert!(right_id >= 0 && (right_id as usize) < fanout);
                                }
                            }
                            count += c.cardinality();
                            if count == expected_out {
                                done_tx.send((first_us, now, count, waits)).unwrap();
                                let _ = hold_rx.await;
                                loop {
                                    match stream.try_next().await.unwrap().unwrap() {
                                        Message::Barrier(b) if b.epoch.curr == test_epoch(4) => {
                                            break;
                                        }
                                        other => panic!(
                                            "unexpected message after expected rows: {other:?}"
                                        ),
                                    }
                                }
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            },
        )));
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
        tx_l.send_barrier(Barrier::new_test_barrier(test_epoch(3)).with_mutation(
            Mutation::StartFragmentBackfill {
                fragment_ids: [22.into()].into(),
            },
        ));
        tx_r.push_barrier(test_epoch(3), false);
        controls.push((tx_l, tx_r));
    }
    gate.wait().await;
    let started = Instant::now();
    let mut results = Vec::new();
    for rx in done {
        results.push(
            tokio::time::timeout(Duration::from_secs(120), rx)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let total_us = started.elapsed().as_secs_f64() * 1e6;
    let gets = physical.get_sample_count() - before_count;
    let bytes = physical.get_sample_sum() - before_bytes;
    let read_families = registry
        .gather()
        .into_iter()
        .filter(|f| f.name().contains("block") || f.name().contains("read"))
        .collect::<Vec<_>>();
    println!(
        "CHAIN_READ_METRICS {}",
        serde_json::to_string(
            &prometheus::TextEncoder::new()
                .encode_to_string(&read_families)
                .unwrap()
        )
        .unwrap()
    );
    env.storage.start_epoch(test_epoch(4), all_ids);
    for (left, right) in &mut controls {
        left.push_barrier(test_epoch(4), false);
        right.push_barrier(test_epoch(4), false);
    }
    for tx in holds {
        let _ = tx.send(());
    }
    let mut scan = Vec::new();
    for p in producers {
        scan.push(p.await.unwrap());
    }
    for t in joins {
        t.await.unwrap();
    }
    let join_families = registry
        .gather()
        .into_iter()
        .filter(|f| f.name().contains("join_lookup"))
        .collect::<Vec<_>>();
    println!(
        "CHAIN_JOIN_METRICS {}",
        serde_json::to_string(
            &prometheus::TextEncoder::new()
                .encode_to_string(&join_families)
                .unwrap()
        )
        .unwrap()
    );
    println!(
        "CHAIN_RESULT {}",
        serde_json::json!({"join":if T==JoinType::Inner {"inner"}else{"semi"},"keys":keys,"fanout":fanout,"delay_us":delay_us,"exchange_permits":capacity,"cache_mb":std::env::var("RW_CHAIN_CACHE_MB").unwrap_or_else(|_|"64".to_owned()),
        "get":gets,"bytes":bytes,"total_us":total_us,"actors":results,"scan":scan})
    );
    drop(controls);
}

#[test]
#[ignore = "explicit real Locality/exchange/HashJoin read-pattern experiment"]
fn locality_join_read_pattern() {
    let main = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    let streaming = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    let keys = std::env::var("RW_CHAIN_KEYS").map_or(16384, |s| s.parse().unwrap());
    let delay = std::env::var("RW_CHAIN_DELAY_US").map_or(2000, |s| s.parse().unwrap());
    let capacity = std::env::var("RW_CHAIN_PERMITS").map_or(2048, |s| s.parse().unwrap());
    main.block_on(async {
        if std::env::var("RW_CHAIN_JOIN").is_ok_and(|s| s == "inner") {
            sample::<{ JoinType::Inner }>(keys, 3, delay, capacity, &streaming).await;
        } else {
            sample::<{ JoinType::LeftSemi }>(keys, 1, delay, capacity, &streaming).await;
        }
    });
}
