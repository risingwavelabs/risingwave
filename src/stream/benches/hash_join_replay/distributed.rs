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

//! Single actor owning all vnodes: isolate state access after distribution, not network exchange.
use std::collections::BTreeSet;
use std::sync::Mutex;

use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::Key64;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_stream::common::StateTableColumnMapping;
use risingwave_stream::executor::{BoxedMessageStream, Execute};
use risingwave_stream::task::LocalBarrierManager;

use super::*;

#[derive(Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Workload {
    #[default]
    LegacyJoin,
    HashAgg,
    JoinRegroup,
}

#[derive(Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ReplayStage {
    #[default]
    Pipeline,
    HashAgg,
    Materialize,
}

#[derive(Default)]
struct StageTiming {
    chunk_us: Vec<f64>,
    windows: Vec<ReadWindowSample>,
    final_barrier: Option<Instant>,
}

#[derive(Serialize)]
struct ReadWindowSample {
    elapsed_s: f64,
    chunks: usize,
    rows: usize,
    get_count: u64,
    read_bytes: u64,
}

struct TimedInput {
    input: Executor,
    timing: Arc<Mutex<StageTiming>>,
    final_epoch: u64,
}

impl Execute for TimedInput {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        let Self {
            input,
            timing,
            final_epoch,
        } = *self;
        let object_metrics = ObjectStoreMetrics::unused();
        let get_count = object_metrics
            .operation_latency
            .with_label_values(&["Minio", "read"]);
        let mut previous_chunk = None;
        let mut first_chunk = None;
        let mut last_window_s = 0.0;
        let mut rows = 0;
        // An isolated operator consumes its current chunk before pulling the next input.
        input
            .execute()
            .inspect(move |message| {
                let now = Instant::now();
                let final_barrier = matches!(message, Ok(Message::Barrier(barrier)) if barrier.epoch.curr == final_epoch);
                if let Some((start, cardinality)) = previous_chunk.take() {
                    let mut timing = timing.lock().unwrap();
                    rows += cardinality;
                    timing.chunk_us.push(now.duration_since(start).as_secs_f64() * 1e6);
                    let elapsed_s = now.duration_since(first_chunk.unwrap()).as_secs_f64();
                    if final_barrier || elapsed_s - last_window_s >= 1.0 {
                        let chunks = timing.chunk_us.len();
                        timing.windows.push(ReadWindowSample {
                            elapsed_s,
                            chunks,
                            rows,
                            get_count: get_count.get_sample_count(),
                            read_bytes: object_metrics.read_bytes.get(),
                        });
                        last_window_s = elapsed_s;
                    }
                }
                if let Ok(Message::Chunk(chunk)) = message {
                    if first_chunk.is_none() {
                        first_chunk = Some(now);
                        timing.lock().unwrap().windows.push(ReadWindowSample {
                            elapsed_s: 0.0,
                            chunks: 0,
                            rows: 0,
                            get_count: get_count.get_sample_count(),
                            read_bytes: object_metrics.read_bytes.get(),
                        });
                    }
                    previous_chunk = Some((now, chunk.cardinality()));
                }
                if final_barrier {
                    timing.lock().unwrap().final_barrier = Some(now);
                }
            })
            .boxed()
    }
}

pub(super) fn default_vnode_count() -> usize {
    256
}

fn vnodes(config: &Config) -> Option<Arc<Bitmap>> {
    Some(Arc::new(Bitmap::ones(config.vnode_count)))
}

fn groups(config: &Config) -> usize {
    config.keys * config.rows_per_key / config.group_rows
}

// An odd multiplier permutes a power-of-two key space. Modulo the smaller group space,
// each group occurs exactly group_rows times, independently of the join prefix.
fn group_id(row: usize, config: &Config) -> usize {
    row.wrapping_mul(0x9e37_79b1) & (groups(config) - 1)
}

pub(super) fn catalog(id: u32, config: &Config) -> PbTable {
    let types = if id < 4 {
        types()
    } else {
        vec![DataType::Int64; if id == 4 && config.hash_agg_min { 3 } else { 2 }]
    };
    let pk = if id < 4 || id == 6 {
        vec![0, 1]
    } else {
        vec![0]
    };
    let mut table = gen_pbtable(
        TableId::new(id),
        types
            .into_iter()
            .enumerate()
            .map(|(i, ty)| ColumnDesc::unnamed(ColumnId::new(i as i32), ty))
            .collect(),
        vec![OrderType::ascending(); pk.len()],
        pk.clone(),
        1,
    );
    table.distribution_key = vec![0];
    table.stream_key = pk.into_iter().map(|i| i as i32).collect();
    table.maybe_vnode_count = Some(config.vnode_count as u32);
    if id == 4 {
        table.value_indices = if config.hash_agg_min {
            vec![1, 2]
        } else {
            vec![1]
        };
    }
    table
}

pub(super) async fn make_fixture(url: &str, config: &Config) -> HummockVersion {
    let store = store(url, false, storage_options(config)).await;
    assert!(
        store
            .store()
            .list("", None, Some(1))
            .await
            .unwrap()
            .next()
            .await
            .is_none()
    );
    let env = environment(store, config).await;
    env.storage.start_epoch(test_epoch(1), table_ids(config));
    let mut states = Vec::new();
    for id in 0..table_count(config) {
        let mut state = StateTable::from_table_catalog(
            &catalog(id, config),
            env.storage.clone(),
            vnodes(config),
        )
        .await;
        state
            .init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
            .await
            .unwrap();
        states.push(state);
    }
    if config.workload == Workload::JoinRegroup {
        for key in 0..config.keys {
            states[0].insert(OwnedRow::new(vec![
                Some((key as i64).into()),
                Some((key as i64).into()),
                Some("probe".into()),
            ]));
            for seq in 0..config.rows_per_key {
                let g = group_id(key * config.rows_per_key + seq, config);
                states[2].insert(OwnedRow::new(vec![
                    Some((key as i64).into()),
                    Some((g as i64).into()),
                    Some(payload(key, seq, config.value_bytes).into()),
                ]));
                if (seq + 1).is_multiple_of(256) {
                    states[2].try_flush().await.unwrap();
                }
            }
            states[0].try_flush().await.unwrap();
            states[2].try_flush().await.unwrap();
        }
    }
    let mut groups_by_epoch = vec![Vec::new(); config.fixture_epochs];
    for g in 0..groups(config) {
        // Write each contiguous vnode range in its own committed epoch. Each flush
        // contains both state tables, so object-level L0 ranges may still overlap.
        let pk = [Some(risingwave_common::types::ScalarImpl::Int64(g as i64))];
        let vnode = states[4].compute_vnode_by_pk(&pk);
        groups_by_epoch[vnode.to_index() * config.fixture_epochs / config.vnode_count].push(g);
    }
    for (index, epoch_groups) in groups_by_epoch.into_iter().enumerate() {
        for (offset, g) in epoch_groups.into_iter().enumerate() {
            if config.hash_agg_min {
                for value in 0..config.group_rows {
                    states[6].insert(min_input(g, value));
                }
            }
            for id in [4, 5] {
                let mut row = vec![
                    Some((g as i64).into()),
                    Some((config.group_rows as i64).into()),
                ];
                if config.hash_agg_min && id == 4 {
                    // Materialized MIN does not store an encoded value state.
                    row.push(None);
                }
                states[id].insert(OwnedRow::new(row));
                if (offset + 1).is_multiple_of(256) {
                    states[id].try_flush().await.unwrap();
                }
            }
        }
        let next_epoch = test_epoch(index as u64 + 2);
        env.storage.start_epoch(next_epoch, table_ids(config));
        for state in &mut states {
            state
                .commit_assert_no_update_vnode_bitmap(EpochPair::new_test_epoch(next_epoch))
                .await
                .unwrap();
        }
        env.commit_epoch(test_epoch(index as u64 + 1)).await;
    }
    (*env.storage.get_pinned_version()).clone()
}

fn min_input(group: usize, value: usize) -> OwnedRow {
    OwnedRow::new(vec![
        Some((group as i64).into()),
        Some((value as i64).into()),
    ])
}

fn shuffle(values: &mut [usize]) {
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    for i in (1..values.len()).rev() {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        values.swap(i, (seed % (i as u64 + 1)) as usize);
    }
}

// Inspect actual SST metadata outside measurement. In particular, reject a singleton fixture.
pub(super) async fn layout(
    url: &str,
    version: &HummockVersion,
    config: &Config,
) -> serde_json::Value {
    let store = store(url, false, storage_options(config)).await;
    let mut table_vnodes: BTreeMap<u32, BTreeSet<usize>> = BTreeMap::new();
    let mut agg_vnodes = BTreeSet::new();
    let mut agg_keys = 0;
    let mut agg_cache_weight = 0;
    let mut table_blocks: BTreeMap<u32, usize> = BTreeMap::new();
    let mut table_bytes: BTreeMap<u32, u64> = BTreeMap::new();
    for info in sstables(version) {
        let mut stats = StoreLocalStatistic::default();
        let table = store.sstable(&info, &mut stats).await.unwrap();
        for (index, meta) in table.meta.block_metas.iter().enumerate() {
            let key = FullKey::decode(&meta.smallest_key);
            let id = key.user_key.table_id.as_raw_id();
            table_vnodes
                .entry(id)
                .or_default()
                .insert(key.user_key.get_vnode_id());
            *table_blocks.entry(id).or_default() += 1;
            *table_bytes.entry(id).or_default() += meta.len as u64;
            if id == 4 {
                // Block boundaries alone cannot establish vnode coverage: a block can
                // contain multiple vnodes. Inspect all aggregation keys outside timing,
                // using a separate store so the measured Foyer cache stays fresh.
                let block = store
                    .get(&table, index, CachePolicy::Disable, &mut stats)
                    .await
                    .unwrap();
                agg_cache_weight +=
                    block.estimated_memory_weight() + std::mem::size_of::<SstableBlockIndex>();
                let mut iter = risingwave_storage::hummock::BlockIterator::new(block);
                iter.seek_to_first();
                while iter.is_valid() {
                    agg_vnodes.insert(iter.key().user_key.get_vnode_id());
                    agg_keys += 1;
                    if !iter.try_next() {
                        break;
                    }
                }
            }
        }
        stats.discard();
    }
    assert_eq!(agg_keys, groups(config));
    assert!(
        agg_vnodes.len() > 1,
        "aggregation fixture is still singleton"
    );
    assert!(agg_vnodes.iter().all(|v| *v < config.vnode_count));
    json!({"block_start_vnodes": table_vnodes, "aggregation_vnodes": agg_vnodes,
        "aggregation_keys": agg_keys, "aggregation_decoded_cache_weight_bytes": agg_cache_weight, "table_blocks": table_blocks, "table_compressed_block_bytes": table_bytes})
}

pub(super) async fn replay(
    url: &str,
    config: &Config,
    version: &HummockVersion,
    scheduler: bool,
    dest: &Path,
    actor: ActorContextRef,
) {
    assert!(
        !config.hash_agg_min || config.replay_stage == ReplayStage::HashAgg,
        "the retractable MIN fixture requires isolated HashAgg replay"
    );
    let shape = layout(url, version, config).await;
    std::fs::write(
        dest.join("layout.json"),
        serde_json::to_vec_pretty(&shape).unwrap(),
    )
    .unwrap();
    let store = store(url, scheduler, storage_options(config)).await;
    let env = environment(store.clone(), config).await;
    env.storage.update_version_and_wait(version.clone()).await;
    assert_eq!(&*env.storage.get_pinned_version(), version);
    let fixture_epoch = test_epoch(config.fixture_epochs as u64);
    let read_epoch = test_epoch(config.fixture_epochs as u64 + 1);
    let next_epoch = test_epoch(config.fixture_epochs as u64 + 2);
    env.storage.start_epoch(read_epoch, table_ids(config));
    let chunk_size = actor.config.developer.chunk_size;
    let (mut tx_l, source_l) = MockSource::channel();
    let (mut tx_r, source_r) = MockSource::channel();
    let (input, group_index, chunks) = if config.replay_stage == ReplayStage::Materialize {
        let types = [DataType::Int64, DataType::Int64];
        let schema = Schema::new(types.iter().cloned().map(Field::unnamed).collect());
        let input = source_l
            .stop_on_finish(false)
            .into_executor(schema, vec![0]);
        let mut order: Vec<_> = (0..groups(config)).collect();
        shuffle(&mut order);
        let chunks = order
            .chunks(config.probe_chunk_size)
            .map(|ids| {
                let rows: Vec<_> = ids
                    .iter()
                    .map(|&id| {
                        (
                            Op::Delete,
                            OwnedRow::new(vec![
                                Some((id as i64).into()),
                                Some((config.group_rows as i64).into()),
                            ]),
                        )
                    })
                    .collect();
                StreamChunk::from_rows(&rows, &types)
            })
            .collect::<Vec<_>>();
        (input, 0, chunks)
    } else if config.hash_agg_min {
        let types = [DataType::Int64, DataType::Int64];
        let schema = Schema::new(types.iter().cloned().map(Field::unnamed).collect());
        let input = source_l
            .stop_on_finish(false)
            .into_executor(schema, vec![0, 1]);
        let mut order: Vec<_> = (0..groups(config)).collect();
        shuffle(&mut order);
        // Retract all rows of a group in one chunk so every output is an exact DELETE.
        let chunks = order
            .chunks(config.probe_chunk_size / config.group_rows)
            .map(|groups| {
                let rows = groups
                    .iter()
                    .flat_map(|&group| {
                        (0..config.group_rows)
                            .map(move |value| (Op::Delete, min_input(group, value)))
                    })
                    .collect::<Vec<_>>();
                StreamChunk::from_rows(&rows, &types)
            })
            .collect();
        drop(source_r);
        (input, 0, chunks)
    } else if config.workload == Workload::HashAgg {
        let schema = Schema::new(vec![Field::unnamed(DataType::Int64)]);
        let input = source_l
            .stop_on_finish(false)
            .into_executor(schema, vec![0]);
        let mut order: Vec<_> = (0..config.keys * config.rows_per_key).collect();
        shuffle(&mut order);
        let chunks = order
            .chunks(config.probe_chunk_size)
            .map(|ids| {
                let rows: Vec<_> = ids
                    .iter()
                    .map(|id| {
                        (
                            Op::Delete,
                            OwnedRow::new(vec![Some((group_id(*id, config) as i64).into())]),
                        )
                    })
                    .collect();
                StreamChunk::from_rows(&rows, &[DataType::Int64])
            })
            .collect::<Vec<_>>();
        drop(source_r);
        (input, 0, chunks)
    } else {
        let mut state = Vec::new();
        for id in 0..4 {
            state.push(
                StateTable::from_table_catalog(
                    &catalog(id, config),
                    env.storage.clone(),
                    vnodes(config),
                )
                .await,
            );
        }
        let [left, degree_left, right, degree_right]: [_; 4] = state.try_into().ok().unwrap();
        let schema = Schema::new(types().into_iter().map(Field::unnamed).collect());
        let left_input = source_l
            .stop_on_finish(false)
            .into_executor(schema.clone(), vec![0, 1]);
        let right_input = source_r
            .stop_on_finish(false)
            .into_executor(schema, vec![0, 1]);
        let info = ExecutorInfo::for_test(
            Schema::new(
                types()
                    .into_iter()
                    .chain(types())
                    .map(Field::unnamed)
                    .collect(),
            ),
            vec![0, 1, 3, 4],
            "DistributedReplayJoin".into(),
            0,
        );
        let join = HashJoinExecutor::<Key128, HummockStorage, {JoinType::Inner}, MemoryEncoding>::new_with_cache_size(
            actor.clone(), info.clone(), left_input, right_input,
            JoinParams::new(vec![0], vec![1]), JoinParams::new(vec![0], vec![1]),
            vec![false], (0..6).collect(), None, vec![], left, degree_left, right, degree_right,
            Arc::new(AtomicU64::new(0)), false, Arc::new(StreamingMetrics::unused()), chunk_size,
            actor.config.developer.high_join_amplification_threshold, None, vec![],
        );
        let mut order: Vec<_> = (0..config.keys).collect();
        shuffle(&mut order);
        let chunks = order
            .chunks(config.probe_chunk_size)
            .map(|ids| {
                let rows: Vec<_> = ids
                    .iter()
                    .map(|key| {
                        (
                            Op::Delete,
                            OwnedRow::new(vec![
                                Some((*key as i64).into()),
                                Some((*key as i64).into()),
                                Some("probe".into()),
                            ]),
                        )
                    })
                    .collect();
                StreamChunk::from_rows(&rows, &types())
            })
            .collect();
        ((info, join).into(), 4, chunks)
    };
    let input_rows = match config.replay_stage {
        ReplayStage::Materialize => groups(config),
        _ => config.keys * config.rows_per_key,
    };
    let input_chunks = chunks.len();
    let timing = (config.replay_stage != ReplayStage::Pipeline)
        .then(|| Arc::new(Mutex::new(StageTiming::default())));
    let input = if let Some(timing) = &timing {
        (
            input.info().clone(),
            TimedInput {
                input,
                timing: timing.clone(),
                final_epoch: next_epoch,
            },
        )
            .into()
    } else {
        input
    };
    let schema = Schema::new(vec![
        Field::unnamed(DataType::Int64);
        if config.hash_agg_min { 3 } else { 2 }
    ]);
    let info = ExecutorInfo::for_test(schema.clone(), vec![0], "DistributedReplayAgg".into(), 1);
    let input = if config.replay_stage == ReplayStage::Materialize {
        input
    } else {
        let mut agg_calls = vec![AggCall::from_pretty("(count:int8)")];
        let mut storages = vec![AggStateStorage::Value];
        if config.hash_agg_min {
            agg_calls.push(AggCall::from_pretty("(min:int8 $1:int8)"));
            storages.push(AggStateStorage::MaterializedInput {
                table: StateTable::from_table_catalog(
                    &catalog(6, config),
                    env.storage.clone(),
                    vnodes(config),
                )
                .await,
                mapping: StateTableColumnMapping::new(vec![0, 1], None),
                order_columns: vec![ColumnOrder::new(1, OrderType::ascending())],
            });
        }
        let aggregate = HashAggExecutor::<Key64, HummockStorage>::new(AggExecutorArgs {
            version: PbAggNodeVersion::LATEST,
            input,
            actor_ctx: actor.clone(),
            info: info.clone(),
            extreme_cache_size: actor.config.developer.unsafe_extreme_cache_size,
            agg_calls,
            row_count_index: 0,
            storages,
            intermediate_state_table: StateTable::from_table_catalog(
                &catalog(4, config),
                env.storage.clone(),
                vnodes(config),
            )
            .await,
            distinct_dedup_tables: Default::default(),
            watermark_epoch: Arc::new(AtomicU64::new(0)),
            extra: HashAggExecutorExtraArgs {
                group_key_indices: vec![group_index],
                chunk_size,
                max_dirty_groups_heap_size: actor
                    .config
                    .developer
                    .hash_agg_max_dirty_groups_heap_size,
                emit_on_window_close: false,
            },
        })
        .unwrap();
        (info.clone(), aggregate).into()
    };
    let output = if config.replay_stage == ReplayStage::HashAgg {
        input
    } else {
        let conflict_behavior = if config.checked_materialize {
            // A dispatcher keeps Overwrite on the checked path.
            assert_ne!(actor.initial_dispatch_num, 0);
            ConflictBehavior::Overwrite
        } else {
            ConflictBehavior::NoCheck
        };
        let materialize =
            MaterializeExecutor::<_, risingwave_common::util::value_encoding::BasicSerde>::new(
                input,
                schema,
                env.storage.clone(),
                vec![ColumnOrder::new(0, OrderType::ascending())],
                actor,
                vnodes(config),
                &catalog(5, config),
                Arc::new(AtomicU64::new(0)),
                conflict_behavior,
                vec![],
                Arc::new(StreamingMetrics::unused()),
                None,
                false,
                LocalBarrierManager::for_test(),
            )
            .await;
        (info, materialize).into()
    };
    let mut output = output.execute();
    tx_l.push_barrier_with_prev_epoch_for_test(read_epoch, fixture_epoch, false);
    if config.workload == Workload::JoinRegroup {
        tx_r.push_barrier_with_prev_epoch_for_test(read_epoch, fixture_epoch, false);
    }
    assert!(matches!(
        output.next().await.unwrap().unwrap(),
        Message::Barrier(_)
    ));
    assert!(output.next().now_or_never().is_none());
    assert!(
        config.warm_every_nth_block.is_none(),
        "distributed cases start with fresh caches"
    );
    StoreLocalStatistic::flush_all();
    let before = metrics();
    std::fs::write(dest.join("before.prom"), &before).unwrap();
    let start = Instant::now();
    for chunk in chunks {
        tx_l.push_chunk(chunk);
    }
    env.storage.start_epoch(next_epoch, table_ids(config));
    tx_l.push_barrier_with_prev_epoch_for_test(next_epoch, read_epoch, false);
    if config.workload == Workload::JoinRegroup {
        tx_r.push_barrier_with_prev_epoch_for_test(next_epoch, read_epoch, false);
    }
    let mut rows = Vec::new();
    loop {
        match output.next().await.unwrap().unwrap() {
            Message::Chunk(chunk) => {
                rows.extend(chunk.rows().map(|(op, row)| (op, row.into_owned_row())));
                assert!(rows.len() <= 3 * config.keys * config.rows_per_key);
            }
            Message::Barrier(barrier) => {
                assert_eq!(barrier.epoch.curr, next_epoch);
                assert!(output.next().now_or_never().is_none());
                break;
            }
            other => panic!("unexpected message {other:?}"),
        }
    }
    let end = Instant::now();
    let elapsed = (end - start).as_secs_f64();
    let final_barrier_ms = timing.map(|timing| {
        let timing = timing.lock().unwrap();
        assert_eq!(timing.chunk_us.len(), input_chunks);
        std::fs::write(
            dest.join("chunk-us.json"),
            serde_json::to_vec(&timing.chunk_us).unwrap(),
        )
        .unwrap();
        assert_eq!(timing.windows.last().unwrap().rows, input_rows);
        std::fs::write(
            dest.join("read-windows.json"),
            serde_json::to_vec(&timing.windows).unwrap(),
        )
        .unwrap();
        (end - timing.final_barrier.unwrap()).as_secs_f64() * 1e3
    });
    StoreLocalStatistic::flush_all();
    let after = metrics();
    std::fs::write(dest.join("after.prom"), &after).unwrap();
    let delta = |name| counter(&after, name) - counter(&before, name);
    let materialize_misses =
        delta("stream_materialize_cache_total_count") - delta("stream_materialize_cache_hit_count");
    let table_block_delta = |table_id, kind| {
        let table_id = format!("table_id=\"{table_id}\"");
        let kind = format!("type=\"{kind}\"");
        let value = |raw: &str| {
            counter(
                &raw.lines()
                    .filter(|line| line.contains(&table_id) && line.contains(&kind))
                    .collect::<Vec<_>>()
                    .join("\n"),
                "state_store_sst_store_block_request_counts",
            )
        };
        value(&after) - value(&before)
    };
    let materialize_meta = table_block_delta(5, "meta_total");
    let materialize_meta_batch_retained = table_block_delta(5, "meta_batch_retained");
    let materialize_meta_batch_hits = table_block_delta(5, "meta_batch_ready");
    let hash_agg_meta_batch_hits = table_block_delta(4, "meta_batch_ready");
    if config.checked_materialize && config.replay_stage != ReplayStage::HashAgg {
        assert!(
            materialize_misses > 0.0 && materialize_meta > 0.0,
            "Materialize did not read old rows from an SST"
        );
    }
    if config.replay_stage == ReplayStage::HashAgg {
        assert!(table_block_delta(4, "meta_total") > 0.0);
    }
    let read_batch = storage_options(config).read_batch;
    if !read_batch.enabled || !read_batch.meta_reuse {
        assert_eq!(materialize_meta_batch_retained, 0.0);
        assert_eq!(materialize_meta_batch_hits, 0.0);
        assert_eq!(hash_agg_meta_batch_hits, 0.0);
    }
    for name in [
        "object_store_write_bytes",
        "object_store_failure_count",
        "object_store_request_retry_count",
    ] {
        assert_eq!(delta(name), 0.0, "{name}");
    }
    assert_eq!(
        delta("object_store_read_requests"),
        delta("object_store_read_request_duration_seconds_count")
    );
    assert_eq!(&*env.storage.get_pinned_version(), version);
    let physical = |raw: &str| {
        counter(
            &raw.lines()
                .filter(|line| line.contains("type=\"read\""))
                .collect::<Vec<_>>()
                .join("\n"),
            "object_store_operation_latency_count",
        )
    };
    let min_blocks = table_block_delta(6, "data_total");
    if config.hash_agg_min {
        assert!(min_blocks > 0.0, "MIN did not read materialized input");
    }
    let mut expected_counts = vec![config.group_rows as i64; groups(config)];
    let mut pending_update = None;
    for (op, row) in &rows {
        let g = row.datum_at(0).unwrap().into_int64() as usize;
        let count = row.datum_at(1).unwrap().into_int64();
        assert!(g < groups(config) && count > 0);
        if config.hash_agg_min {
            assert_eq!(*op, Op::Delete);
            assert_eq!(row.datum_at(2).unwrap().into_int64(), 0);
        }
        match op {
            Op::Delete => {
                assert!(pending_update.is_none());
                assert_eq!(expected_counts[g], count);
                expected_counts[g] = 0;
            }
            Op::UpdateDelete => {
                assert!(pending_update.is_none());
                assert_eq!(expected_counts[g], count);
                pending_update = Some((g, count));
            }
            Op::UpdateInsert => {
                let (old_group, old_count) = pending_update.take().expect("unpaired update");
                assert_eq!(g, old_group);
                assert!(
                    count < old_count,
                    "delete-only input cannot increase a count"
                );
                expected_counts[g] = count;
            }
            Op::Insert => panic!("delete-only input cannot create or resurrect a group"),
        }
    }
    assert!(pending_update.is_none());
    assert!(expected_counts.iter().all(|count| *count == 0));
    let state_tables: &[u32] = match config.replay_stage {
        ReplayStage::HashAgg if config.hash_agg_min => &[4, 6],
        ReplayStage::HashAgg => &[4],
        ReplayStage::Materialize => &[5],
        ReplayStage::Pipeline if config.workload == Workload::JoinRegroup => &[0, 4, 5],
        ReplayStage::Pipeline => &[4, 5],
    };
    for &id in state_tables {
        let reader = env
            .storage
            .new_read_snapshot(
                HummockReadEpoch::NoWait(read_epoch),
                NewReadSnapshotOptions {
                    table_id: TableId::new(id),
                    table_option: Default::default(),
                },
            )
            .await
            .unwrap();
        for vnode in 0..config.vnode_count {
            let mut iter = reader
                .iter(
                    prefixed_range_with_vnode(
                        (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                        VirtualNode::from_index(vnode),
                    ),
                    ReadOptions::default(),
                )
                .await
                .unwrap();
            assert!(
                iter.try_next().await.unwrap().is_none(),
                "remaining rows in table {id}, vnode {vnode}"
            );
        }
    }
    let result = json!({"workload": config.workload, "topology": "one_actor_all_vnodes_no_network_exchange", "vnode_count": config.vnode_count,
        "groups": groups(config), "input_rows": input_rows, "input_chunks": input_chunks,
        "rows": rows.len(), "final_barriers": 1, "final_barrier_ms": final_barrier_ms,
        "replay_stage": config.replay_stage,
        "output_chunk_size": chunk_size, "elapsed_s": elapsed, "scheduler": scheduler,
        "sanity_check_enabled": cfg!(debug_assertions) && !config.disable_sanity_check,
        "physical_reads": physical(&after) - physical(&before), "read_bytes": delta("object_store_read_bytes"),
        "logical_reads": delta("object_store_read_requests"), "layout": shape,
        "checked_materialize": config.checked_materialize, "materialize_misses": materialize_misses,
        "hash_agg_meta": table_block_delta(4, "meta_total"),
        "materialized_input_blocks": min_blocks,
        "hash_agg_meta_batch_hits": hash_agg_meta_batch_hits,
        "materialize_meta": materialize_meta, "materialize_meta_batch_retained": materialize_meta_batch_retained,
        "materialize_meta_batch_hits": materialize_meta_batch_hits,
        "block_cache_capacity_bytes": store.block_cache().memory().capacity()});
    std::fs::write(
        dest.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    println!("{result}");
}
