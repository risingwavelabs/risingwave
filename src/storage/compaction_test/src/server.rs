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
use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_common::config::{load_config, StorageConfig};
use risingwave_common::util::addr::HostAddr;
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch, FIRST_VERSION_ID};
use risingwave_pb::common::WorkerType;
use risingwave_pb::hummock::{pin_version_response, GroupHummockVersion, HummockVersion};
use risingwave_rpc_client::{HummockMetaClient, MetaClient};
use risingwave_storage::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use risingwave_storage::hummock::{HummockStorage, TieredCacheMetricsBuilder};
use risingwave_storage::monitor::{
    HummockMetrics, MonitoredStateStore, ObjectStoreMetrics, StateStoreMetrics,
};
use risingwave_storage::store::ReadOptions;
use risingwave_storage::StateStoreImpl::HummockStateStore;
use risingwave_storage::{StateStore, StateStoreImpl, StateStoreIter};

use crate::{CompactionTestOpts, TestToolConfig};

struct Metrics {
    pub hummock_metrics: Arc<HummockMetrics>,
    pub state_store_metrics: Arc<StateStoreMetrics>,
    pub object_store_metrics: Arc<ObjectStoreMetrics>,
}

/// This tool will be started after we generate enough L0 SSTs to Hummock.
/// Fetches and runs compaction tasks.
pub async fn compaction_test_serve(
    _listen_addr: SocketAddr,
    client_addr: HostAddr,
    opts: CompactionTestOpts,
) -> anyhow::Result<()> {
    let config: TestToolConfig = load_config(&opts.config_path).unwrap();
    tracing::info!(
        "Starting compaciton test tool with config {:?} and opts {:?}",
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

    let (_shutdown_sender, mut shutdown_recv) = tokio::sync::oneshot::channel::<()>();
    let join_handle = tokio::spawn(async move {
        tokio::select! {
            _ = &mut shutdown_recv => {
                for (join_handle, shutdown_sender) in sub_tasks {
                    if let Err(err) = shutdown_sender.send(()) {
                        tracing::warn!("Failed to send shutdown: {:?}", err);
                        continue;
                    }
                    if let Err(err) = join_handle.await {
                        tracing::warn!("Failed to join shutdown: {:?}", err);
                    }
                }
            },
        }
    });

    // Replay version deltas from FIRST_VERSION_ID to the version before reset
    let local_version_manager = hummock.local_version_manager();
    let mut modified_compaction_groups = HashSet::<CompactionGroupId>::new();
    let mut replay_count: u64 = 0;
    let (start_version, end_version) = (FIRST_VERSION_ID + 1, version_before_reset.id + 1);
    let mut replayed_epochs = vec![];
    for id in start_version..end_version {
        let (version_new, compaction_groups) = meta_client.replay_version_delta(id).await?;
        let (version_id, max_committed_epoch) = (version_new.id, version_new.max_committed_epoch);
        tracing::info!(
            "Replayed version delta version_id: {}, max_committed_epoch: {}, compaction_groups: {:?}",
            version_id,
            max_committed_epoch,
            compaction_groups
        );

        local_version_manager.try_update_pinned_version(
            pin_version_response::Payload::PinnedVersion(GroupHummockVersion {
                hummock_version: Some(version_new),
                ..Default::default()
            }),
        );

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
            // pop the latest epoch
            replayed_epochs.pop();

            let mut epochs = vec![max_committed_epoch];
            epochs.extend(
                pin_old_snapshots(&meta_client, &mut replayed_epochs, 1)
                    .await
                    .into_iter(),
            );

            // Get kv pairs of snapshots
            let expect_results = scan_epochs(&hummock, &epochs, opts.table_id).await?;
            let old_task_num = meta_client.get_assigned_compact_task_num().await?;
            let old_version = meta_client.get_current_version().await?;

            tracing::info!(
                "Trigger compaction for version {}, epoch {} compaction_groups: {:?}",
                version_id,
                max_committed_epoch,
                modified_compaction_groups,
            );

            // Trigger compactions but doesn't wait for finish
            meta_client
                .trigger_compaction_deterministic(
                    version_id,
                    Vec::from_iter(modified_compaction_groups.iter().copied()),
                )
                .await?;

            // Poll for compaction task status
            let (schedule_ok, task_num) =
                poll_compaction_schedule_status(&meta_client, old_task_num).await;
            let (compaction_ok, new_version) =
                poll_compaction_tasks_status(&meta_client, schedule_ok, task_num, &old_version)
                    .await;

            tracing::info!(
                "Compaction schedule_ok {}, task_num {} compaction_ok {}",
                schedule_ok,
                task_num,
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
            tracing::info!(
                "Check results for version: id: {}, epochs: {:?}",
                new_version_id,
                epochs,
            );

            local_version_manager.try_update_pinned_version(
                pin_version_response::Payload::PinnedVersion(GroupHummockVersion {
                    hummock_version: Some(new_version),
                    ..Default::default()
                }),
            );

            check_compaction_results(&expect_results, &hummock, opts.table_id).await?;

            modified_compaction_groups.clear();
            replayed_epochs.clear();
        }
    }
    tracing::info!("Replay finished");
    tokio::try_join!(join_handle)?;
    Ok(())
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
/// Returns (whether scheduling is success, number of tasks)
async fn poll_compaction_schedule_status(
    meta_client: &MetaClient,
    old_task_num: usize,
) -> (bool, usize) {
    let poll_timeout = Duration::from_secs(2);
    let poll_interval = Duration::from_millis(20);
    let mut poll_duration_cnt = Duration::from_millis(0);
    let mut new_size = meta_client.get_assigned_compact_task_num().await.unwrap();
    let mut schedule_ok = false;
    loop {
        if new_size > old_task_num {
            schedule_ok = true;
            break;
        }

        if poll_duration_cnt >= poll_timeout {
            break;
        }
        tokio::time::sleep(poll_interval).await;
        poll_duration_cnt += poll_interval;
        new_size = meta_client.get_assigned_compact_task_num().await.unwrap();
    }
    (schedule_ok, new_size - old_task_num)
}

async fn poll_compaction_tasks_status(
    meta_client: &MetaClient,
    schedule_ok: bool,
    task_num: usize,
    old_version: &HummockVersion,
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
        if (cur_version.id > old_version.id) && (cur_version.id - old_version.id >= task_num as u64)
        {
            tracing::info!("Collected all of compact tasks");
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

async fn scan_epochs(
    hummock: &MonitoredStateStore<HummockStorage>,
    snapshots: &[HummockEpoch],
    table_id: u32,
) -> anyhow::Result<BTreeMap<HummockEpoch, Vec<(Bytes, Bytes)>>> {
    let mut results = BTreeMap::new();
    for &epoch in snapshots.iter() {
        let scan_res = hummock
            .scan::<_, Vec<u8>>(
                None,
                ..,
                None,
                ReadOptions {
                    epoch,
                    table_id: TableId { table_id },
                    retention_seconds: None,
                },
            )
            .await?;
        results.insert(epoch, scan_res);
    }
    Ok(results)
}

pub async fn check_compaction_results(
    expect: &BTreeMap<HummockEpoch, Vec<(Bytes, Bytes)>>,
    hummock: &MonitoredStateStore<HummockStorage>,
    table_id: u32,
) -> anyhow::Result<()> {
    for (&epoch, exp) in expect.iter() {
        let mut iter = hummock
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

        for kv1 in exp.iter() {
            let kv2 = iter.next().await?.unwrap();
            assert_eq!(kv1.0, kv2.0);
            assert_eq!(kv1.1, kv2.1);
        }
    }
    Ok(())
}

pub async fn create_hummock_store_with_metrics(
    meta_client: &MetaClient,
    storage_config: Arc<StorageConfig>,
    opts: &CompactionTestOpts,
) -> anyhow::Result<MonitoredStateStore<HummockStorage>> {
    let metrics = Metrics {
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
