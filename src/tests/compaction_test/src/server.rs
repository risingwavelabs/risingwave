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

use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use clap::Parser;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::config::{load_config, StorageConfig};
use risingwave_common::util::addr::HostAddr;
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch, FIRST_VERSION_ID};
use risingwave_pb::common::WorkerType;
use risingwave_pb::hummock::{
    GroupHummockVersion, HummockVersion, HummockVersionDelta, PinnedSnapshotsSummary,
};
use risingwave_rpc_client::{HummockMetaClient, MetaClient};
use risingwave_storage::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use risingwave_storage::hummock::{
    HummockStateStoreIter, HummockStorage, TieredCacheMetricsBuilder,
};
use risingwave_storage::monitor::{
    HummockMetrics, MonitoredStateStore, MonitoredStateStoreIter, ObjectStoreMetrics,
    StateStoreMetrics,
};
use risingwave_storage::store::ReadOptions;
use risingwave_storage::StateStoreImpl::HummockStateStore;
use risingwave_storage::{StateStore, StateStoreImpl, StateStoreIter};
use tokio::task::JoinHandle;
use tracing::trace;

use crate::{CompactionTestOpts, TestToolConfig};

struct CompactionTestMetrics {
    num_check_total: u64,
    num_uncheck: u64,
}

impl CompactionTestMetrics {
    fn new() -> CompactionTestMetrics {
        Self {
            num_check_total: 0,
            num_uncheck: 0,
        }
    }
}

async fn start_embeded_meta(meta_addr: String) {
    let opts = risingwave_meta::MetaNodeOpts::parse_from([
        "meta-node",
        "--listen-addr",
        &meta_addr,
        // "0.0.0.0:5690",
        "--backend",
        "mem",
    ]);
    risingwave_meta::start(opts).await
}

async fn start_embeded_compactor(meta_addr: String, client_addr: String, state_store: String) {
    // let meta_addr = embeded_meta_addr.clone();
    // let state_store = opts.state_store.clone();
    // let client_addr = client_addr.clone();
    let opts = risingwave_compactor::CompactorOpts::parse_from([
        "compactor-node",
        "--host",
        "127.0.0.1:6660",
        "--client-address",
        &client_addr,
        "--meta-address",
        &meta_addr,
        "--state-store",
        &state_store,
    ]);
    risingwave_compactor::start(opts).await
}

/// Steps to use the compaction test tool
///
/// 1. Start the cluster with full-compaction-test config: `./risedev d full-compaction-test`
/// 2. Ingest enough L0 SSTs, for example we can use the tpch-bench tool
/// 3. Disable hummock manager commit new epochs: `./risedev ctl hummock disable-commit-epoch`, and
/// it will print the current max committed epoch in Meta.
/// 4. Use the test tool to replay hummock version deltas and trigger compactions:
/// `cargo run -r --bin compaction-test -- --state-store hummock+s3://your-bucket -t <table_id>`
pub async fn compaction_test_serve(
    _listen_addr: SocketAddr,
    client_addr: HostAddr,
    opts: CompactionTestOpts,
) -> anyhow::Result<()> {
    // todo: start a new meta service as a tokio task

    let embeded_meta_addr = "http://127.0.0.1:5790".to_string();
    let _meta_handle = tokio::spawn(start_embeded_meta(embeded_meta_addr.clone()));

    // Wait for meta starts
    tokio::time::sleep(Duration::from_secs(1)).await;
    tracing::info!("Started embeded Meta");

    // start a new compactor and register to the new meta
    let _compactor_handle = tokio::spawn(start_embeded_compactor(
        embeded_meta_addr.clone(),
        client_addr.to_string(),
        opts.state_store.clone(),
    ));

    tracing::info!("Started embeded compactor");

    let version_deltas = pull_version_deltas("http://127.0.0.1:5690", &client_addr).await?;

    tracing::info!("Pulled delta logs from Meta");

    // start the replay tool and register to the new meta
    let replay_handle = tokio::spawn(start_replay(opts, version_deltas));

    // Join the replay handle
    replay_handle.await?
}

async fn pull_version_deltas(
    meta_addr: &str,
    client_addr: &HostAddr,
) -> anyhow::Result<Vec<HummockVersionDelta>> {
    // Register to the cluster.
    // We reuse the RiseCtl worker type here
    let meta_client =
        MetaClient::register_new(meta_addr, WorkerType::RiseCtl, client_addr, 0).await?;
    let worker_id = meta_client.worker_id();
    tracing::info!("Assigned worker id {}", worker_id);
    meta_client.activate(&client_addr).await.unwrap();

    let (handle, shutdown_tx) =
        MetaClient::start_heartbeat_loop(meta_client.clone(), Duration::from_millis(1000), vec![]);
    let res = meta_client
        .list_version_deltas(0, u32::MAX, u64::MAX)
        .await
        .unwrap()
        .version_deltas;

    if let Err(err) = shutdown_tx.send(()) {
        tracing::warn!("Failed to send shutdown to heartbeat task: {:?}", err);
    }

    handle.await?;
    Ok(res)
}

async fn start_replay(
    // client_addr: HostAddr,
    opts: CompactionTestOpts,
    version_delta_logs: Vec<HummockVersionDelta>,
) -> anyhow::Result<()> {
    client_addr = HostAddr {
        host: "127.0.0.1".to_string(),
        port: 6670,
    };
    tracing::info!("Client address is {}", client_address);

    let mut metric = CompactionTestMetrics::new();
    let config: TestToolConfig = load_config(&opts.config_path).unwrap();
    tracing::info!(
        "Starting replay with config {:?} and opts {:?}",
        config,
        opts
    );

    // Register to the cluster.
    // We reuse the RiseCtl worker type here
    let meta_client =
        MetaClient::register_new(&opts.meta_address, WorkerType::RiseCtl, &client_addr, 0).await?;
    let worker_id = meta_client.worker_id();
    tracing::info!("Assigned worker id {}", worker_id);
    meta_client.activate(&client_addr).await.unwrap();

    let sub_tasks = vec![MetaClient::start_heartbeat_loop(
        meta_client.clone(),
        Duration::from_millis(1000),
        vec![],
    )];

    // let latest_version = meta_client.get_current_version().await?;
    // // Wait for the unpin of latest snapshot
    // wait_unpin_of_latest_snapshot(&meta_client, latest_version.max_committed_epoch).await;

    // Resets the current hummock version
    let version_before_reset = meta_client.reset_current_version().await?;
    tracing::info!(
        "Reset hummock version id: {}, max_committed_epoch: {}",
        version_before_reset.id,
        version_before_reset.max_committed_epoch
    );

    // Creates a hummock state store *after* we reset the hummock version
    let storage_config = Arc::new(config.storage.clone());
    let hummock =
        create_hummock_store_with_metrics(&meta_client, storage_config.clone(), &opts).await?;

    // Replay version deltas from FIRST_VERSION_ID to the version before reset
    let mut modified_compaction_groups = HashSet::<CompactionGroupId>::new();
    let mut replay_count: u64 = 0;
    // let (start_version, end_version) = (FIRST_VERSION_ID + 1, version_before_reset.id);
    let mut replayed_epochs = vec![];
    let mut check_result_task: Option<JoinHandle<_>> = None;

    for delta in version_delta_logs {
        let (current_version, compaction_groups) = meta_client.replay_version_delta(delta).await?;
        let (version_id, max_committed_epoch) =
            (current_version.id, current_version.max_committed_epoch);
        tracing::info!(
            "Replayed version delta version_id: {}, max_committed_epoch: {}, compaction_groups: {:?}",
            version_id,
            max_committed_epoch,
            compaction_groups
        );

        hummock
            .inner()
            .update_version_and_wait(GroupHummockVersion {
                hummock_version: Some(current_version.clone()),
                ..Default::default()
            })
            .await;

        replay_count += 1;
        replayed_epochs.push(max_committed_epoch);
        compaction_groups
            .into_iter()
            .map(|c| modified_compaction_groups.insert(c))
            .count();

        // We can custom more conditions for compaction triggering
        // For now I just use a static way here
        if replay_count % opts.compaction_trigger_frequency == 0
            && !modified_compaction_groups.is_empty()
        {
            // join previously spawned check result task
            if let Some(handle) = check_result_task {
                handle.await??;
            }

            metric.num_check_total += 1;

            // pop the latest epoch
            replayed_epochs.pop();
            let mut epochs = vec![max_committed_epoch];
            epochs.extend(
                pin_old_snapshots(&meta_client, &mut replayed_epochs, 1)
                    .await
                    .into_iter(),
            );

            let old_version_iters = open_hummock_iters(&hummock, &epochs, opts.table_id).await?;

            tracing::info!(
                "Trigger compaction for version {}, epoch {} compaction_groups: {:?}",
                version_id,
                max_committed_epoch,
                modified_compaction_groups,
            );

            // Try trigger multiple rounds of compactions but doesn't wait for finish
            let is_multi_round = opts.compaction_trigger_rounds > 1;
            for _ in 0..opts.compaction_trigger_rounds {
                meta_client
                    .trigger_compaction_deterministic(
                        version_id,
                        Vec::from_iter(modified_compaction_groups.iter().copied()),
                    )
                    .await?;
                if is_multi_round {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }

            let old_task_num = meta_client.get_assigned_compact_task_num().await?;
            // Poll for compaction task status
            let (schedule_ok, version_diff) =
                poll_compaction_schedule_status(&meta_client, old_task_num).await;

            tracing::info!(
                "Compaction schedule_ok {}, version_diff {}",
                schedule_ok,
                version_diff,
            );
            let (compaction_ok, new_version) = poll_compaction_tasks_status(
                &meta_client,
                schedule_ok,
                version_diff as u32,
                &current_version,
            )
            .await;

            tracing::info!(
                "Compaction schedule_ok {}, version_diff {} compaction_ok {}",
                schedule_ok,
                version_diff,
                compaction_ok,
            );

            let (new_version_id, new_committed_epoch) =
                (new_version.id, new_version.max_committed_epoch);
            assert!(
                new_version_id >= version_id,
                "new_version_id: {}, epoch: {}",
                new_version_id,
                new_committed_epoch
            );
            assert_eq!(max_committed_epoch, new_committed_epoch);

            if new_version_id != version_id {
                hummock
                    .inner()
                    .update_version_and_wait(GroupHummockVersion {
                        hummock_version: Some(new_version),
                        ..Default::default()
                    })
                    .await;

                let new_version_iters =
                    open_hummock_iters(&hummock, &epochs, opts.table_id).await?;

                // spawn a task to check the results
                check_result_task = Some(tokio::spawn(check_compaction_results(
                    new_version_id,
                    old_version_iters,
                    new_version_iters,
                )));
            } else {
                check_result_task = None;
                metric.num_uncheck += 1;
            }
            modified_compaction_groups.clear();
            replayed_epochs.clear();
        }
    }

    // join previously spawned check result task if any
    if let Some(handle) = check_result_task {
        handle.await??;
    }
    tracing::info!(
        "Replay finished. Check times: {}, Uncheck times: {}",
        metric.num_check_total,
        metric.num_uncheck
    );

    for (join_handle, shutdown_sender) in sub_tasks {
        if let Err(err) = shutdown_sender.send(()) {
            tracing::warn!("Failed to send shutdown: {:?}", err);
            continue;
        }
        if let Err(err) = join_handle.await {
            tracing::warn!("Failed to join shutdown: {:?}", err);
        }
    }

    Ok(())
}

#[allow(dead_code)]
async fn wait_unpin_of_latest_snapshot(meta_client: &MetaClient, latest_snapshot: HummockEpoch) {
    // Wait for the unpin of latest snapshot
    loop {
        let resp = meta_client
            .risectl_get_pinned_snapshots_summary()
            .await
            .unwrap();
        if let Some(PinnedSnapshotsSummary {
            pinned_snapshots, ..
        }) = resp.summary
        {
            let item = pinned_snapshots
                .iter()
                .find(|snapshot| snapshot.minimal_pinned_snapshot >= latest_snapshot);
            match item {
                None => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Some(_) => {
                    tracing::info!("latest snapshot {} have unpinned", latest_snapshot);
                    break;
                }
            }
        }
    }
}

async fn pin_old_snapshots(
    meta_client: &MetaClient,
    replayed_epochs: &mut [HummockEpoch],
    num: usize,
) -> Vec<HummockEpoch> {
    let mut old_epochs = vec![];
    for &epoch in replayed_epochs.iter().rev().take(num) {
        old_epochs.push(epoch);
        let _ = meta_client.pin_specific_snapshot(epoch).await;
    }
    old_epochs
}

/// Poll the compaction task assignment to aware whether scheduling is success.
/// Returns (whether scheduling is success, expected number of new versions)
async fn poll_compaction_schedule_status(
    meta_client: &MetaClient,
    old_task_num: usize,
) -> (bool, i32) {
    let poll_timeout = Duration::from_secs(2);
    let poll_interval = Duration::from_millis(20);
    let mut poll_duration_cnt = Duration::from_millis(0);
    let mut new_task_num = meta_client.get_assigned_compact_task_num().await.unwrap();
    let mut schedule_ok = false;
    loop {
        // New task has been scheduled
        if new_task_num > old_task_num {
            schedule_ok = true;
            break;
        }

        if poll_duration_cnt >= poll_timeout {
            break;
        }
        tokio::time::sleep(poll_interval).await;
        poll_duration_cnt += poll_interval;
        new_task_num = meta_client.get_assigned_compact_task_num().await.unwrap();
    }
    (
        schedule_ok,
        (new_task_num as i32 - old_task_num as i32).abs(),
    )
}

async fn poll_compaction_tasks_status(
    meta_client: &MetaClient,
    schedule_ok: bool,
    version_diff: u32,
    base_version: &HummockVersion,
) -> (bool, HummockVersion) {
    // Polls current version to check whether its id become large,
    // which means compaction tasks have finished. If schedule ok,
    // we poll for a little long while.
    let poll_timeout = if schedule_ok {
        Duration::from_secs(120)
    } else {
        Duration::from_secs(5)
    };
    let poll_interval = Duration::from_millis(50);
    let mut duration_cnt = Duration::from_millis(0);
    let mut compaction_ok = false;

    let mut cur_version = meta_client.get_current_version().await.unwrap();
    loop {
        if (cur_version.id > base_version.id)
            && (cur_version.id - base_version.id >= version_diff as u64)
        {
            tracing::info!(
                "Collected all of compact tasks. Actual version diff {}",
                cur_version.id - base_version.id
            );
            compaction_ok = true;
            break;
        }
        if duration_cnt >= poll_timeout {
            break;
        }
        tokio::time::sleep(poll_interval).await;
        duration_cnt += poll_interval;
        cur_version = meta_client.get_current_version().await.unwrap();
    }
    (compaction_ok, cur_version)
}

async fn open_hummock_iters(
    hummock: &MonitoredStateStore<HummockStorage>,
    snapshots: &[HummockEpoch],
    table_id: u32,
) -> anyhow::Result<BTreeMap<HummockEpoch, MonitoredStateStoreIter<HummockStateStoreIter>>> {
    let mut results = BTreeMap::new();
    for &epoch in snapshots.iter() {
        let iter = hummock
            .iter::<_, Vec<u8>>(
                None,
                ..,
                ReadOptions {
                    epoch,
                    table_id: TableId { table_id },
                    retention_seconds: None,
                },
            )
            .await?;
        results.insert(epoch, iter);
    }
    Ok(results)
}

pub async fn check_compaction_results(
    version_id: u64,
    mut expect_results: BTreeMap<HummockEpoch, MonitoredStateStoreIter<HummockStateStoreIter>>,
    mut actual_resutls: BTreeMap<HummockEpoch, MonitoredStateStoreIter<HummockStateStoreIter>>,
) -> anyhow::Result<()> {
    let epochs = expect_results.keys().cloned().collect_vec();
    tracing::info!(
        "Check results for version: id: {}, epochs: {:?}",
        version_id,
        epochs,
    );

    let combined = expect_results.iter_mut().zip_eq(actual_resutls.iter_mut());
    for ((_, expect_iter), (_, actual_iter)) in combined {
        while let Some(kv_expect) = expect_iter.next().await? {
            let kv_actual = actual_iter.next().await?.unwrap();
            assert_eq!(kv_expect.0, kv_actual.0, "Key mismatch");
            assert_eq!(kv_expect.1, kv_actual.1, "Value mismatch");
        }
    }
    Ok(())
}

struct StorageMetrics {
    pub hummock_metrics: Arc<HummockMetrics>,
    pub state_store_metrics: Arc<StateStoreMetrics>,
    pub object_store_metrics: Arc<ObjectStoreMetrics>,
}

pub async fn create_hummock_store_with_metrics(
    meta_client: &MetaClient,
    storage_config: Arc<StorageConfig>,
    opts: &CompactionTestOpts,
) -> anyhow::Result<MonitoredStateStore<HummockStorage>> {
    let metrics = StorageMetrics {
        hummock_metrics: Arc::new(HummockMetrics::unused()),
        state_store_metrics: Arc::new(StateStoreMetrics::unused()),
        object_store_metrics: Arc::new(ObjectStoreMetrics::unused()),
    };

    let state_store_impl = StateStoreImpl::new(
        &opts.state_store,
        "",
        storage_config,
        Arc::new(MonitoredHummockMetaClient::new(
            meta_client.clone(),
            metrics.hummock_metrics.clone(),
        )),
        metrics.state_store_metrics.clone(),
        metrics.object_store_metrics.clone(),
        TieredCacheMetricsBuilder::unused(),
    )
    .await?;

    if let HummockStateStore(hummock_state_store) = state_store_impl {
        Ok(hummock_state_store)
    } else {
        Err(anyhow!("only Hummock state store is supported!"))
    }
}
