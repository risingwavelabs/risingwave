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

//! A single committed changelog fixture shared by fresh real-cache readers.
use std::ops::Bound::{Excluded, Included};
use std::time::Duration;

use risingwave_hummock_sdk::key::TableKey;
use risingwave_hummock_test::get_notification_client_for_test;
use risingwave_hummock_test::test_utils::register_tables_with_id_for_test;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_pb::common::worker_node::Property;
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::store::{
    CHECK_BYTES_EQUAL, ChangeLogValue, OpConsistencyLevel, ReadLogOptions, SealCurrentEpochOptions,
    StateStoreReadLog, TryWaitEpochOptions,
};

use super::*;

pub(super) async fn run(url: &str, config: &Config, dir: &Path) {
    assert!(config.keys <= VirtualNode::COUNT_FOR_TEST);
    assert!(config.rows_per_key >= 3);
    let table_id = TableId::new(233);
    let options = storage_options(config);
    let writer_store = store(url, false, options.clone()).await;
    assert!(
        writer_store
            .store()
            .list("", None, Some(1))
            .await
            .unwrap()
            .next()
            .await
            .is_none(),
        "changelog replay requires an empty dedicated bucket"
    );
    let (env, manager, cluster, worker) = setup_compute_env(8080).await;
    let meta = Arc::new(MockHummockMetaClient::new(manager.clone(), worker as _));
    let writer = HummockStorage::for_test(
        Arc::new(options.clone()),
        writer_store.clone(),
        meta.clone(),
        get_notification_client_for_test(env.clone(), manager.clone(), cluster.clone(), worker)
            .await,
    )
    .await
    .unwrap();
    register_tables_with_id_for_test(
        writer.compaction_catalog_manager_ref(),
        &manager,
        &[table_id],
    )
    .await;
    for epoch in 1..=4 {
        writer.start_epoch(test_epoch(epoch), HashSet::from([table_id]));
    }
    let values = (0..config.keys)
        .map(|vnode| {
            (0..config.rows_per_key)
                .map(|row| {
                    (
                        Bytes::from(payload(vnode, row, config.value_bytes)),
                        Bytes::from(payload(vnode + config.keys, row, config.value_bytes)),
                    )
                })
                .collect_vec()
        })
        .collect_vec();
    let mut local_options = NewLocalOptions::for_test(table_id);
    local_options.op_consistency_level = OpConsistencyLevel::ConsistentOldValue {
        check_old_value: CHECK_BYTES_EQUAL.clone(),
        is_log_store: true,
    };
    let mut local = writer.new_local(local_options).await;
    local
        .init(InitOptions::new(EpochPair::new_test_epoch(test_epoch(1))))
        .await
        .unwrap();
    for epoch in 1..=3 {
        for (vnode, rows) in values.iter().enumerate() {
            for (row, (old, new)) in rows.iter().enumerate() {
                let mut key = VirtualNode::from_index(vnode).to_be_bytes().to_vec();
                key.extend_from_slice(&(row as u32).to_be_bytes());
                let key = TableKey(Bytes::from(key));
                match (epoch, row % 3) {
                    (1, 0 | 1) => local.insert(key, old.clone(), None).unwrap(),
                    (2, 0) => local.insert(key, new.clone(), Some(old.clone())).unwrap(),
                    (2, 1) => local.delete(key, old.clone()).unwrap(),
                    (2, 2) => local.insert(key, new.clone(), None).unwrap(),
                    (3, 0) => local
                        .insert(key, Bytes::from_static(b"later epoch"), Some(new.clone()))
                        .unwrap(),
                    _ => {}
                }
                if (row + 1).is_multiple_of(config.probe_chunk_size) {
                    local.try_flush().await.unwrap();
                }
            }
        }
        local.flush().await.unwrap();
        local.seal_current_epoch(test_epoch(epoch + 1), SealCurrentEpochOptions::for_test());
        let result = writer
            .seal_and_sync_epoch(test_epoch(epoch), HashSet::from([table_id]))
            .await
            .unwrap();
        meta.commit_epoch_with_change_log(test_epoch(epoch), result, Some(vec![test_epoch(epoch)]))
            .await
            .unwrap();
    }
    writer
        .try_wait_epoch(
            HummockReadEpoch::Committed(test_epoch(3)),
            TryWaitEpochOptions { table_id },
        )
        .await
        .unwrap();
    let version = writer.get_pinned_version();
    let logs = meta
        .get_table_change_logs(
            false,
            Some(test_epoch(2)),
            Some(test_epoch(2)),
            Some([table_id].into_iter().collect()),
            false,
            None,
        )
        .await
        .unwrap();
    let infos = logs
        .get(&table_id)
        .unwrap()
        .filter_epoch((test_epoch(2), test_epoch(2)))
        .flat_map(|log| log.new_value.iter().chain(log.old_value.iter()))
        .cloned()
        .collect_vec();
    assert!(!infos.is_empty());
    let mut layout = Vec::new();
    let mut block_ids = Vec::new();
    let mut layout_stats = StoreLocalStatistic::default();
    for info in &infos {
        let table = writer_store.sstable(info, &mut layout_stats).await.unwrap();
        block_ids.extend((0..table.block_count()).map(|block| SstableBlockIndex {
            sst_id: info.object_id,
            block_idx: block as _,
        }));
        layout.push(
            json!({"object_id":info.object_id.as_raw_id(), "blocks":table.meta.block_metas.len(),
            "bytes":table.meta.estimated_size}),
        );
    }
    layout_stats.discard();
    assert!(!block_ids.is_empty());
    block_ids.sort_unstable_by_key(|id| (id.sst_id, id.block_idx));
    block_ids.dedup();
    let output = dir.join("changelog-results");
    std::fs::create_dir(&output)
        .expect("preserve previous results; use a fresh directory and bucket");
    std::fs::write(
        output.join("effective-storage-options.txt"),
        format!("{options:#?}"),
    )
    .unwrap();
    std::fs::write(
        output.join("fixture.json"),
        serde_json::to_vec_pretty(&json!({
            "version":version.id.as_raw_id(), "config":config, "ssts":layout,
            "epoch":test_epoch(2), "vnode_count":config.keys,
        }))
        .unwrap(),
    )
    .unwrap();
    if config.disable_sanity_check {
        risingwave_storage::hummock::utils::disable_sanity_check();
    }
    let mut results = Vec::new();
    for repeat in 0..config.repeats {
        for (case, end, take, warm) in [
            ("long_cold", config.rows_per_key, config.rows_per_key, None),
            (
                "cache_holes",
                config.rows_per_key,
                config.rows_per_key,
                Some(3),
            ),
            ("short_range", 2, 2, None),
            ("early_stop", config.rows_per_key, 1, None),
        ] {
            let mut arms = vec![(false, false), (false, true), (true, true)];
            if !repeat.is_multiple_of(2) {
                arms.reverse();
            }
            for (prefetch, scheduler) in arms {
                let mut opts = options.clone();
                opts.blockwise_prefetch.changelog_enabled = prefetch;
                let cache = store(url, scheduler, opts.clone()).await;
                // Notification subscriptions are keyed by host address. Reusing the writer's
                // identity makes still-live observers replace and reconnect one another.
                let reader_worker = cluster
                    .add_worker(
                        WorkerType::ComputeNode,
                        HostAddress {
                            host: "127.0.0.1".to_owned(),
                            port: 8081 + results.len() as i32,
                        },
                        Property {
                            is_serving: true,
                            parallelism: 1,
                            ..Default::default()
                        },
                        Default::default(),
                    )
                    .await
                    .unwrap();
                assert_ne!(reader_worker, worker);
                let reader_meta = Arc::new(MockHummockMetaClient::new(
                    manager.clone(),
                    reader_worker as _,
                ));
                let reader = HummockStorage::for_test(
                    Arc::new(opts),
                    cache.clone(),
                    reader_meta,
                    get_notification_client_for_test(
                        env.clone(),
                        manager.clone(),
                        cluster.clone(),
                        reader_worker,
                    )
                    .await,
                )
                .await
                .unwrap();
                reader
                    .try_wait_epoch(
                        HummockReadEpoch::Committed(test_epoch(3)),
                        TryWaitEpochOptions { table_id },
                    )
                    .await
                    .unwrap();
                assert_eq!(&*reader.get_pinned_version(), &*version);
                if let Some(step) = warm {
                    let mut warm_stats = StoreLocalStatistic::default();
                    for info in &infos {
                        let table = cache.sstable(info, &mut warm_stats).await.unwrap();
                        for block in (0..table.block_count()).step_by(step) {
                            cache
                                .get(
                                    &table,
                                    block,
                                    CachePolicy::Fill(Hint::Normal),
                                    &mut warm_stats,
                                )
                                .await
                                .unwrap();
                        }
                    }
                    warm_stats.discard();
                }
                let before = metrics();
                let prefetch_metrics = global_hummock_state_store_metrics(MetricLevel::Debug);
                let consumed = prefetch_metrics
                    .blockwise_prefetch_events
                    .with_label_values(&["consumed"]);
                let discarded = prefetch_metrics
                    .blockwise_prefetch_events
                    .with_label_values(&["discarded"]);
                let consumed_before = consumed.get();
                let discarded_before = discarded.get();
                let started = Instant::now();
                let rows = futures::future::join_all(values.iter().enumerate().map(
                    |(vnode, expected)| {
                        let reader = &reader;
                        async move {
                            let vnode = VirtualNode::from_index(vnode);
                            let range = prefixed_range_with_vnode(
                                (
                                    Included(Bytes::from(0u32.to_be_bytes().to_vec())),
                                    Excluded(Bytes::from((end as u32).to_be_bytes().to_vec())),
                                ),
                                vnode,
                            );
                            let mut iter = reader
                                .iter_log(
                                    (test_epoch(2), test_epoch(2)),
                                    range,
                                    ReadLogOptions { table_id },
                                )
                                .await
                                .unwrap();
                            let mut count = 0;
                            while let Some((key, value)) = iter.try_next().await.unwrap() {
                                assert_eq!(key.vnode_part(), vnode);
                                assert_eq!(
                                    u32::from_be_bytes(key.key_part().try_into().unwrap()) as usize,
                                    count
                                );
                                let (old, new) = &expected[count];
                                let expected = match count % 3 {
                                    0 => ChangeLogValue::Update {
                                        old_value: old.as_ref(),
                                        new_value: new.as_ref(),
                                    },
                                    1 => ChangeLogValue::Delete(old.as_ref()),
                                    _ => ChangeLogValue::Insert(new.as_ref()),
                                };
                                assert_eq!(value, expected);
                                count += 1;
                                if count == take && take < end {
                                    break;
                                }
                            }
                            assert_eq!(count, take);
                            count
                        }
                    },
                ))
                .await
                .into_iter()
                .sum::<usize>();
                let elapsed_s = started.elapsed().as_secs_f64();
                // Join outstanding producers through cache-only gets. This supplies no outer
                // fetch closure, so it cannot create new remote reads for untouched blocks.
                // Do this after timing and before the physical-count snapshot, including early stop.
                tokio::time::timeout(Duration::from_secs(10), async {
                    for id in &block_ids {
                        cache.block_cache().get(id).await.unwrap();
                    }
                    // A receiver may wake on another thread just before the sender drops its
                    // admission guard. Wait for that final accounting transition as well.
                    while counter(&metrics(), "object_store_read_in_flight") != 0.0
                        || counter(&metrics(), "object_store_read_scheduler_pending") != 0.0
                    {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                StoreLocalStatistic::flush_all();
                let after = metrics();
                let delta = |name| counter(&after, name) - counter(&before, name);
                assert_eq!(delta("object_store_write_bytes"), 0.0);
                assert_eq!(delta("object_store_failure_count"), 0.0);
                assert_eq!(delta("object_store_request_retry_count"), 0.0);
                assert_eq!(counter(&after, "object_store_read_in_flight"), 0.0);
                assert_eq!(counter(&after, "object_store_read_scheduler_pending"), 0.0);
                if prefetch && case == "long_cold" {
                    assert!(
                        consumed.get() > consumed_before,
                        "fixture must exercise multi-block lookahead"
                    );
                }
                assert_eq!(&*reader.get_pinned_version(), &*version);
                let result = json!({"repeat":repeat, "case":case, "prefetch":prefetch, "scheduler":scheduler,
                    "rows":rows, "elapsed_s":elapsed_s,
                    "physical_reads":delta("object_store_operation_bytes_count"),
                    "physical_bytes":delta("object_store_operation_bytes_sum"),
                    "logical_reads":delta("object_store_read_requests"),
                    "prefetch_consumed":consumed.get() - consumed_before,
                    "prefetch_discarded":discarded.get() - discarded_before,
                });
                let stem = format!("{repeat}-{case}-prefetch-{prefetch}-scheduler-{scheduler}");
                std::fs::write(output.join(format!("{stem}-before.prom")), before).unwrap();
                std::fs::write(output.join(format!("{stem}-after.prom")), after).unwrap();
                println!("{result}");
                results.push(result);
                std::fs::write(
                    output.join("results.json"),
                    serde_json::to_vec_pretty(&results).unwrap(),
                )
                .unwrap();
            }
        }
    }
}
