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
use itertools::Itertools;
use risingwave_common::config::{load_config, StorageConfig};
use risingwave_common::util::addr::HostAddr;
use risingwave_common_service::observer_manager::ObserverManager;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManager;
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch, HummockReadEpoch, FIRST_VERSION_ID};
use risingwave_pb::common::WorkerType;
use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use risingwave_storage::hummock::{HummockStorage, TieredCacheMetricsBuilder};
use risingwave_storage::monitor::{
    HummockMetrics, MonitoredStateStore, ObjectStoreMetrics, StateStoreMetrics,
};
use risingwave_storage::store::ReadOptions;
use risingwave_storage::StateStoreImpl::HummockStateStore;
use risingwave_storage::{StateStore, StateStoreImpl};

use crate::observer::SimpleObserverNode;
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
    let mut meta_client = MetaClient::new(&opts.meta_address).await.unwrap();
    let worker_id = meta_client
        .register(WorkerType::RiseCtl, &client_addr, 0)
        .await
        .unwrap();
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

    // Starts an ObserverManager to init the local version
    let compactor_observer_node = SimpleObserverNode::new(hummock.local_version_manager());
    let observer_manager = ObserverManager::new(
        meta_client.clone(),
        client_addr.clone(),
        Box::new(compactor_observer_node),
        WorkerType::RiseCtl,
    )
    .await;
    let observer_join_handle = observer_manager.start().await.unwrap();

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

    let local_version_manager = hummock.local_version_manager();
    // Replay version deltas from FIRST_VERSION_ID to the version before reset
    let mut modified_compaction_groups = HashSet::<CompactionGroupId>::new();
    let mut replay_count: u64 = 0;
    let (start_version, end_version) = (FIRST_VERSION_ID + 1, version_before_reset.id + 1);
    let mut replayed_epochs = vec![];
    for id in start_version..end_version {
        let (version_id, max_committed_epoch, compaction_groups) =
            meta_client.replay_version_delta(id).await?;
        tracing::info!(
            "Replayed version delta version_id: {}, max_committed_epoch: {}, compaction_groups: {:?}",
            version_id,
            max_committed_epoch,
            compaction_groups
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
            replayed_epochs.pop(); // pop the latest epoch
            local_version_manager
                .try_wait_epoch(HummockReadEpoch::Committed(max_committed_epoch))
                .await?;

            let mut epochs = vec![max_committed_epoch];
            epochs.extend(
                pin_old_snapshots(&meta_client, &mut replayed_epochs, 1)
                    .await
                    .into_iter(),
            );

            // Get kv pairs of snapshots
            let expect_results = scan_epochs(&hummock, &epochs).await?;
            tracing::info!(
                "Trigger compaction for version {}, epoch {} compaction_groups: {:?}",
                version_id,
                max_committed_epoch,
                modified_compaction_groups,
            );
            // When this function returns, the compaction is finished.
            // The returned version deltas is order by id in ascending order
            let (new_version_id, new_committed_epoch) = meta_client
                .trigger_compaction_deterministic(
                    version_id,
                    Vec::from_iter(modified_compaction_groups.iter().copied()),
                )
                .await?;

            assert!(
                new_version_id >= version_id,
                "new_version_id: {}, epoch: {}",
                new_version_id,
                new_committed_epoch
            );
            assert_eq!(max_committed_epoch, new_committed_epoch);
            let actual_results = scan_epochs(&hummock, &epochs).await?;
            tracing::info!(
                "Check results for version: id: {}, epochs: {:?}",
                new_version_id,
                epochs,
            );
            check_results(&expect_results, &actual_results);

            modified_compaction_groups.clear();
            replayed_epochs.clear();
        }
    }
    tracing::info!("Replay finished");
    tokio::try_join!(join_handle, observer_join_handle)?;
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

async fn scan_epochs(
    hummock: &MonitoredStateStore<HummockStorage>,
    snapshots: &[HummockEpoch],
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
                    table_id: None,
                    retention_seconds: None,
                },
            )
            .await?;
        results.insert(epoch, scan_res);
    }
    Ok(results)
}

pub fn check_results(
    expect: &BTreeMap<HummockEpoch, Vec<(Bytes, Bytes)>>,
    actual: &BTreeMap<HummockEpoch, Vec<(Bytes, Bytes)>>,
) {
    expect
        .values()
        .zip_eq(actual.values())
        .for_each(|(exp, act)| {
            exp.iter().zip_eq(act.iter()).for_each(|(kv1, kv2)| {
                assert_eq!(kv1.0, kv2.0);
                assert_eq!(kv1.1, kv2.1);
            })
        });
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

    let filter_key_extractor_manager = Arc::new(FilterKeyExtractorManager::default());
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
        filter_key_extractor_manager.clone(),
        TieredCacheMetricsBuilder::unused(),
    )
    .await?;

    if let HummockStateStore(hummock_state_store) = state_store_impl {
        Ok(hummock_state_store)
    } else {
        Err(anyhow!("only Hummock state store is supported!"))
    }
}
