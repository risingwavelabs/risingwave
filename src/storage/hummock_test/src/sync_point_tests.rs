// Copyright 2023 RisingWave Labs
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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use risingwave_common::catalog::hummock::CompactionFilterFlag;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::key::{next_key, user_key};
use risingwave_hummock_sdk::HummockVersionId;
use risingwave_meta::hummock::compaction::compaction_config::CompactionConfigBuilder;
use risingwave_meta::hummock::compaction::{default_level_selector, ManualCompactionOption};
use risingwave_meta::hummock::test_utils::{
    add_ssts, setup_compute_env, setup_compute_env_with_config,
};
use risingwave_meta::hummock::{HummockManagerRef, MockHummockMetaClient};
use risingwave_meta::manager::LocalNotification;
use risingwave_meta::storage::MemStore;
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::hummock::compactor::{Compactor, CompactorContext};
use risingwave_storage::hummock::SstableIdManager;
use risingwave_storage::store::{LocalStateStore, NewLocalOptions, ReadOptions};
use risingwave_storage::StateStore;
use serial_test::serial;

use super::compactor_tests::tests::{
    flush_and_commit, get_hummock_storage, prepare_compactor_and_filter,
};
use crate::get_notification_client_for_test;

#[tokio::test]
#[cfg(feature = "sync_point")]
#[serial]
async fn test_syncpoints_sstable_id_manager() {
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

#[cfg(all(feature = "sync_point", feature = "failpoints"))]
#[tokio::test]
#[serial]
async fn test_syncpoints_test_failpoints_fetch_ids() {
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
#[cfg(feature = "sync_point")]
#[serial]
async fn test_syncpoints_test_local_notification_receiver() {
    let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    // Test cancel compaction task
    let _sst_infos = add_ssts(1, hummock_manager.as_ref(), context_id).await;
    let mut task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_level_selector(),
        )
        .await
        .unwrap()
        .unwrap();
    task.task_status = TaskStatus::ManualCanceled as i32;
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
        .notify_local_subscribers(LocalNotification::WorkerNodeIsDeleted(worker_node))
        .await;
    sync_point::wait_timeout(
        "AFTER_RELEASE_HUMMOCK_CONTEXTS_ASYNC",
        Duration::from_secs(10),
    )
    .await
    .unwrap();
}

pub async fn compact_once(
    hummock_manager_ref: HummockManagerRef<MemStore>,
    compact_ctx: Arc<CompactorContext>,
) {
    // 2. get compact task
    let manual_compcation_option = ManualCompactionOption {
        level: 0,
        ..Default::default()
    };
    // 2. get compact task
    let mut compact_task = hummock_manager_ref
        .manual_get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            manual_compcation_option,
        )
        .await
        .unwrap()
        .unwrap();
    compact_task.gc_delete_keys = false;
    let compactor = hummock_manager_ref.get_idle_compactor().await.unwrap();
    hummock_manager_ref
        .assign_compaction_task(&compact_task, compactor.context_id())
        .await
        .unwrap();

    let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN;
    compact_task.compaction_filter_mask = compaction_filter_flag.bits();
    // 3. compact
    let (_tx, rx) = tokio::sync::oneshot::channel();
    Compactor::compact(compact_ctx, compact_task.clone(), rx).await;
}

#[tokio::test]
#[cfg(feature = "sync_point")]
#[serial]
async fn test_syncpoints_get_in_delete_range_boundary() {
    let config = CompactionConfigBuilder::new()
        .level0_tier_compact_file_number(1)
        .max_bytes_for_level_base(4096)
        .build();
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env_with_config(8080, config).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));
    let existing_table_id: u32 = 1;

    let storage = get_hummock_storage(
        hummock_meta_client.clone(),
        get_notification_client_for_test(env, hummock_manager_ref.clone(), worker_node.clone()),
        &hummock_manager_ref,
        TableId::from(existing_table_id),
    )
    .await;
    let compact_ctx = Arc::new(prepare_compactor_and_filter(
        &storage,
        &hummock_meta_client,
        existing_table_id,
    ));

    let compactor_manager = hummock_manager_ref.compactor_manager_ref_for_test();
    compactor_manager.add_compactor(worker_node.id, u64::MAX);

    let mut local = storage
        .new_local(NewLocalOptions::for_test(existing_table_id.into()))
        .await;

    // 1. add sstables
    let val0 = Bytes::from(b"0"[..].repeat(1 << 10)); // 1024 Byte value
    let val1 = Bytes::from(b"1"[..].repeat(1 << 10)); // 1024 Byte value

    local.init(100);
    let mut start_key = b"aaa".to_vec();
    for _ in 0..10 {
        local
            .insert(
                Bytes::copy_from_slice(start_key.as_slice()),
                val0.clone(),
                None,
            )
            .unwrap();
        start_key = next_key(&start_key);
    }
    local
        .insert(Bytes::from(b"ggg".as_slice()), val0.clone(), None)
        .unwrap();
    local
        .insert(Bytes::from(b"hhh".as_slice()), val0.clone(), None)
        .unwrap();
    local
        .insert(Bytes::from(b"kkk".as_slice()), val0.clone(), None)
        .unwrap();
    local.flush(Vec::new()).await.unwrap();
    local.seal_current_epoch(101);
    flush_and_commit(&hummock_meta_client, &storage, 100).await;
    compact_once(hummock_manager_ref.clone(), compact_ctx.clone()).await;

    local
        .insert(Bytes::from(b"aaa".as_slice()), val1.clone(), None)
        .unwrap();
    local
        .insert(Bytes::from(b"bbb".as_slice()), val1.clone(), None)
        .unwrap();
    local
        .flush(vec![(
            Bytes::from(b"ggg".as_slice()),
            Bytes::from(b"hhh".as_slice()),
        )])
        .await
        .unwrap();
    local.seal_current_epoch(102);
    flush_and_commit(&hummock_meta_client, &storage, 101).await;
    compact_once(hummock_manager_ref.clone(), compact_ctx.clone()).await;

    local
        .insert(Bytes::from(b"hhh".as_slice()), val1.clone(), None)
        .unwrap();
    local
        .insert(Bytes::from(b"iii".as_slice()), val1.clone(), None)
        .unwrap();
    local
        .flush(vec![(
            Bytes::from(b"jjj".as_slice()),
            Bytes::from(b"kkk".as_slice()),
        )])
        .await
        .unwrap();
    local.seal_current_epoch(103);
    flush_and_commit(&hummock_meta_client, &storage, 102).await;
    // move this two file to the same level.
    compact_once(hummock_manager_ref.clone(), compact_ctx.clone()).await;

    local
        .insert(Bytes::from(b"lll".as_slice()), val1.clone(), None)
        .unwrap();
    local
        .insert(Bytes::from(b"mmm".as_slice()), val1.clone(), None)
        .unwrap();
    local.flush(Vec::new()).await.unwrap();
    local.seal_current_epoch(u64::MAX);
    flush_and_commit(&hummock_meta_client, &storage, 103).await;
    // move this two file to the same level.
    compact_once(hummock_manager_ref.clone(), compact_ctx.clone()).await;

    // 4. get the latest version and check
    let version = hummock_manager_ref.get_current_version().await;
    let base_level = &version
        .get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into())
        .levels[4];
    assert_eq!(base_level.table_infos.len(), 3);
    assert!(
        base_level.table_infos[0]
            .key_range
            .as_ref()
            .unwrap()
            .right_exclusive
    );
    assert_eq!(
        user_key(&base_level.table_infos[0].key_range.as_ref().unwrap().right),
        user_key(&base_level.table_infos[1].key_range.as_ref().unwrap().left),
    );
    storage.wait_version(version).await;
    let read_options = ReadOptions {
        ignore_range_tombstone: false,

        prefix_hint: None,
        table_id: TableId::from(existing_table_id),
        retention_seconds: None,
        read_version_from_backup: false,
    };
    let get_result = storage
        .get(b"hhh", 120, read_options.clone())
        .await
        .unwrap();
    assert_eq!(get_result.unwrap(), val1);
    let get_result = storage
        .get(b"ggg", 120, read_options.clone())
        .await
        .unwrap();
    assert!(get_result.is_none());
    let get_result = storage
        .get(b"aaa", 120, read_options.clone())
        .await
        .unwrap();
    assert_eq!(get_result.unwrap(), val1);
    let get_result = storage
        .get(b"aab", 120, read_options.clone())
        .await
        .unwrap();
    assert_eq!(get_result.unwrap(), val0);
    let skip_flag = Arc::new(AtomicBool::new(false));
    let skip_flag_hook = skip_flag.clone();
    sync_point::hook("HUMMOCK_V2::GET::SKIP_BY_NO_FILE", move || {
        let flag = skip_flag_hook.clone();
        async move {
            flag.store(true, Ordering::Release);
        }
    });
    let get_result = storage
        .get(b"kkk", 120, read_options.clone())
        .await
        .unwrap();
    assert_eq!(get_result.unwrap(), val0);
    assert!(skip_flag.load(Ordering::Acquire));
}

#[tokio::test]
#[cfg(feature = "sync_point")]
#[serial]
async fn test_syncpoints_hummock_version_safe_point() {
    let (_env, hummock_manager, _, _) = setup_compute_env(80).await;
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        HummockVersionId::MAX
    );
    let v = hummock_manager.get_current_version().await;
    let sp = hummock_manager.register_safe_point().await;
    assert_eq!(v.id, sp.id);
    assert_eq!(hummock_manager.get_min_pinned_version_id().await, v.id);
    hummock_manager.unregister_safe_point(sp.id).await;
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        HummockVersionId::MAX
    );

    let sp = hummock_manager.register_safe_point().await;
    assert_eq!(hummock_manager.get_min_pinned_version_id().await, v.id);
    drop(sp);
    sync_point::wait_timeout(
        "UNREGISTER_HUMMOCK_VERSION_SAFE_POINT",
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        HummockVersionId::MAX
    );
}
