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

use std::sync::Arc;
use std::time::Duration;

use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_meta::hummock::test_utils::{add_ssts, setup_compute_env};
use risingwave_meta::hummock::{start_local_notification_receiver, MockHummockMetaClient};
use risingwave_meta::manager::LocalNotification;
use risingwave_pb::common::WorkerNode;
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::hummock::SstableIdManager;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn test_sstable_id_manager() {
    sync_point::reset();
    let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));
    let sstable_id_manager = Arc::new(SstableIdManager::new(hummock_meta_client.clone(), 5));

    // Block filling cache after fetching ids.
    sync_point::hook("MAP_NEXT_SST_ID.BEFORE_FILL_CACHE", || async {
        sync_point::wait_timeout("MAP_NEXT_SST_ID.SIG_FILL_CACHE", Duration::from_secs(10))
            .await
            .unwrap();
    });

    // Start the task that fetches new ids.
    let sstable_id_manager_clone = sstable_id_manager.clone();
    let leader_task = tokio::spawn(async move {
        sstable_id_manager_clone.get_new_sst_id().await.unwrap();
    });
    sync_point::wait_timeout("MAP_NEXT_SST_ID.AFTER_FETCH", Duration::from_secs(10))
        .await
        .unwrap();

    // Start tasks that waits to be notified.
    let mut follower_tasks = vec![];
    for _ in 0..3 {
        let sstable_id_manager_clone = sstable_id_manager.clone();
        let follower_task = tokio::spawn(async move {
            sstable_id_manager_clone.get_new_sst_id().await.unwrap();
        });
        sync_point::wait_timeout("MAP_NEXT_SST_ID.AS_FOLLOWER", Duration::from_secs(10))
            .await
            .unwrap();
        follower_tasks.push(follower_task);
    }

    // Continue to fill cache.
    sync_point::on("MAP_NEXT_SST_ID.SIG_FILL_CACHE").await;

    leader_task.await.unwrap();
    for follower_task in follower_tasks {
        follower_task.await.unwrap();
    }
}

#[cfg(feature = "failpoints")]
#[tokio::test]
#[serial]
async fn test_failpoints_fetch_ids() {
    sync_point::reset();
    let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));
    let sstable_id_manager = Arc::new(SstableIdManager::new(hummock_meta_client.clone(), 5));

    // Block fetching ids.
    sync_point::hook("MAP_NEXT_SST_ID.BEFORE_FETCH", || async {
        sync_point::wait_timeout("MAP_NEXT_SST_ID.SIG_FETCH", Duration::from_secs(10))
            .await
            .unwrap();
        sync_point::remove_action("MAP_NEXT_SST_ID.BEFORE_FETCH");
    });

    // Start the task that fetches new ids.
    let sstable_id_manager_clone = sstable_id_manager.clone();
    let leader_task = tokio::spawn(async move {
        fail::cfg("get_new_sst_ids_err", "return").unwrap();
        sstable_id_manager_clone.get_new_sst_id().await.unwrap_err();
        fail::remove("get_new_sst_ids_err");
    });
    sync_point::wait_timeout("MAP_NEXT_SST_ID.AS_LEADER", Duration::from_secs(10))
        .await
        .unwrap();

    // Start tasks that waits to be notified.
    let mut follower_tasks = vec![];
    for _ in 0..3 {
        let sstable_id_manager_clone = sstable_id_manager.clone();
        let follower_task = tokio::spawn(async move {
            sstable_id_manager_clone.get_new_sst_id().await.unwrap();
        });
        sync_point::wait_timeout("MAP_NEXT_SST_ID.AS_FOLLOWER", Duration::from_secs(10))
            .await
            .unwrap();
        follower_tasks.push(follower_task);
    }

    // Continue to fetch ids.
    sync_point::on("MAP_NEXT_SST_ID.SIG_FETCH").await;

    leader_task.await.unwrap();
    // Failed leader task doesn't block follower tasks.
    for follower_task in follower_tasks {
        follower_task.await.unwrap();
    }
}

#[tokio::test]
#[serial]
async fn test_local_notification_receiver() {
    sync_point::reset();

    let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let (join_handle, shutdown_sender) = start_local_notification_receiver(
        hummock_manager.clone(),
        hummock_manager.compactor_manager_ref_for_test(),
        env.notification_manager_ref(),
    )
    .await;

    // Test cancel compaction task
    let _sst_infos = add_ssts(1, hummock_manager.as_ref(), context_id).await;
    let task = hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hummock_manager.list_all_tasks_ids().await.len(), 1);
    env.notification_manager()
        .notify_local_subscribers(LocalNotification::CompactionTaskNeedCancel(task))
        .await;
    sync_point::wait_timeout(
        "AFTER_CANCEL_COMPACTION_TASK_ASYNC",
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(hummock_manager.list_all_tasks_ids().await.len(), 0);

    // Test release hummock contexts
    env.notification_manager()
        .notify_local_subscribers(LocalNotification::WorkerNodeIsDeleted(WorkerNode {
            id: context_id,
            ..Default::default()
        }))
        .await;
    sync_point::wait_timeout(
        "AFTER_RELEASE_HUMMOCK_CONTEXTS_ASYNC",
        Duration::from_secs(10),
    )
    .await
    .unwrap();

    shutdown_sender.send(()).unwrap();
    join_handle.await.unwrap();
}
