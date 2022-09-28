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

use std::collections::HashSet;
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
use risingwave_hummock_sdk::{CompactionGroupId, HummockReadEpoch, FIRST_VERSION_ID};
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
        compaction_groups
            .into_iter()
            .map(|c| modified_compaction_groups.insert(c))
            .count();

        // We can custom more conditions for compaction triggering
        // For now I just use a static way here
        if replay_count % opts.compaction_trigger_frequency == 0
            && !modified_compaction_groups.is_empty()
        {
            local_version_manager
                .try_wait_epoch(HummockReadEpoch::Committed(max_committed_epoch))
                .await?;
            let expect_result = hummock
                .scan::<_, Vec<u8>>(
                    None,
                    ..,
                    None,
                    ReadOptions {
                        epoch: max_committed_epoch,
                        table_id: None,
                        retention_seconds: None,
                    },
                )
                .await?;
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
                    Vec::from_iter(modified_compaction_groups.into_iter()),
                )
                .await?;

            assert!(
                new_version_id >= version_id,
                "new_version_id: {}, epoch: {}",
                new_version_id,
                new_committed_epoch
            );
            assert_eq!(max_committed_epoch, new_committed_epoch);
            let actual_result = hummock
                .scan::<_, Vec<u8>>(
                    None,
                    ..,
                    None,
                    ReadOptions {
                        epoch: new_committed_epoch,
                        table_id: None,
                        retention_seconds: None,
                    },
                )
                .await?;
            tracing::info!(
                "Check result for version: id: {}, max_committed_epoch: {}",
                new_version_id,
                new_committed_epoch,
            );
            check_result(&expect_result, &actual_result);

            modified_compaction_groups = HashSet::new();
        }
    }
    tracing::info!("Replay finished");
    tokio::try_join!(join_handle, observer_join_handle)?;
    Ok(())
}

pub fn check_result(expect: &[(Bytes, Bytes)], actual: &[(Bytes, Bytes)]) {
    expect.iter().zip_eq(actual.iter()).for_each(|(kv1, kv2)| {
        assert_eq!(kv1.0, kv2.0);
        assert_eq!(kv1.1, kv2.1);
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
