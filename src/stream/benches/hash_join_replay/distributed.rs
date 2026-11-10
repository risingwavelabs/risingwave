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

use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::Key64;
use risingwave_hummock_sdk::key::FullKey;
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
        vec![DataType::Int64; 2]
    };
    let pk = if id < 4 { vec![0, 1] } else { vec![0] };
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
        table.value_indices = vec![1];
    }
    table
}

pub(super) async fn make_fixture(url: &str, config: &Config) -> HummockVersion {
    let store = store(url, false, config).await;
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
    for id in 0..6 {
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
    for g in 0..groups(config) {
        for id in [4, 5] {
            states[id].insert(OwnedRow::new(vec![
                Some((g as i64).into()),
                Some((config.group_rows as i64).into()),
            ]));
            if (g + 1).is_multiple_of(256) {
                states[id].try_flush().await.unwrap();
            }
        }
    }
    env.storage.start_epoch(test_epoch(2), table_ids(config));
    for state in &mut states {
        state
            .commit_assert_no_update_vnode_bitmap(EpochPair::new_test_epoch(test_epoch(2)))
            .await
            .unwrap();
    }
    env.commit_epoch(test_epoch(1)).await;
    (*env.storage.get_pinned_version()).clone()
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
    let store = store(url, false, config).await;
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
    let shape = layout(url, version, config).await;
    std::fs::write(
        dest.join("layout.json"),
        serde_json::to_vec_pretty(&shape).unwrap(),
    )
    .unwrap();
    let store = store(url, scheduler, config).await;
    let env = environment(store.clone(), config).await;
    env.storage.update_version_and_wait(version.clone()).await;
    assert_eq!(&*env.storage.get_pinned_version(), version);
    env.storage.start_epoch(test_epoch(2), table_ids(config));
    let chunk_size = actor.config.developer.chunk_size;
    let (mut tx_l, source_l) = MockSource::channel();
    let (mut tx_r, source_r) = MockSource::channel();
    let (input, group_index, chunks) = if config.workload == Workload::HashAgg {
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
    let schema = Schema::new(vec![Field::unnamed(DataType::Int64); 2]);
    let info = ExecutorInfo::for_test(schema.clone(), vec![0], "DistributedReplayAgg".into(), 1);
    let aggregate = HashAggExecutor::<Key64, HummockStorage>::new(AggExecutorArgs {
        version: PbAggNodeVersion::LATEST,
        input,
        actor_ctx: actor.clone(),
        info: info.clone(),
        extreme_cache_size: actor.config.developer.unsafe_extreme_cache_size,
        agg_calls: vec![AggCall::from_pretty("(count:int8)")],
        row_count_index: 0,
        storages: vec![AggStateStorage::Value],
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
            max_dirty_groups_heap_size: actor.config.developer.hash_agg_max_dirty_groups_heap_size,
            emit_on_window_close: false,
        },
    })
    .unwrap();
    let aggregate: Executor = (info.clone(), aggregate).into();
    let materialize =
        MaterializeExecutor::<_, risingwave_common::util::value_encoding::BasicSerde>::new(
            aggregate,
            schema,
            env.storage.clone(),
            vec![ColumnOrder::new(0, OrderType::ascending())],
            actor,
            vnodes(config),
            &catalog(5, config),
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::NoCheck,
            vec![],
            Arc::new(StreamingMetrics::unused()),
            None,
            false,
            LocalBarrierManager::for_test(),
        )
        .await;
    let mut output = Executor::from((info, materialize)).execute();
    tx_l.push_barrier_with_prev_epoch_for_test(test_epoch(2), test_epoch(1), false);
    if config.workload == Workload::JoinRegroup {
        tx_r.push_barrier_with_prev_epoch_for_test(test_epoch(2), test_epoch(1), false);
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
    env.storage.start_epoch(test_epoch(3), table_ids(config));
    tx_l.push_barrier_with_prev_epoch_for_test(test_epoch(3), test_epoch(2), false);
    if config.workload == Workload::JoinRegroup {
        tx_r.push_barrier_with_prev_epoch_for_test(test_epoch(3), test_epoch(2), false);
    }
    let mut rows = Vec::new();
    loop {
        match output.next().await.unwrap().unwrap() {
            Message::Chunk(chunk) => {
                rows.extend(chunk.rows().map(|(op, row)| (op, row.into_owned_row())));
                assert!(rows.len() <= 3 * config.keys * config.rows_per_key);
            }
            Message::Barrier(barrier) => {
                assert_eq!(barrier.epoch.curr, test_epoch(3));
                assert!(output.next().now_or_never().is_none());
                break;
            }
            other => panic!("unexpected message {other:?}"),
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    StoreLocalStatistic::flush_all();
    let after = metrics();
    std::fs::write(dest.join("after.prom"), &after).unwrap();
    let delta = |name| counter(&after, name) - counter(&before, name);
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
    let mut expected_counts = vec![config.group_rows as i64; groups(config)];
    let mut pending_update = None;
    for (op, row) in &rows {
        let g = row.datum_at(0).unwrap().into_int64() as usize;
        let count = row.datum_at(1).unwrap().into_int64();
        assert!(g < groups(config) && count > 0);
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
    for id in if config.workload == Workload::JoinRegroup {
        vec![0, 4, 5]
    } else {
        vec![4, 5]
    } {
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
        "groups": groups(config), "input_rows": config.keys * config.rows_per_key, "rows": rows.len(), "final_barriers": 1,
        "output_chunk_size": chunk_size, "elapsed_s": elapsed, "scheduler": scheduler,
        "sanity_check_enabled": cfg!(debug_assertions) && !config.disable_sanity_check,
        "physical_reads": physical(&after) - physical(&before), "read_bytes": delta("object_store_read_bytes"),
        "logical_reads": delta("object_store_read_requests"), "layout": shape,
        "block_cache_capacity_bytes": store.block_cache().memory().capacity()});
    std::fs::write(
        dest.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    println!("{result}");
}
