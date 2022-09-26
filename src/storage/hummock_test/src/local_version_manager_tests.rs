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

use bytes::Bytes;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::HummockSstableId;
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_pb::hummock::pin_version_response::Payload;
use risingwave_pb::hummock::HummockVersion;
use risingwave_storage::hummock::build_shared_buffer_item_batches;
use risingwave_storage::hummock::conflict_detector::ConflictDetector;
use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
use risingwave_storage::hummock::local_version_manager::LocalVersionManager;
use risingwave_storage::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use risingwave_storage::hummock::shared_buffer::UncommittedData;
use risingwave_storage::hummock::test_utils::{
    default_config_for_test, gen_dummy_batch, gen_dummy_batch_several_keys, gen_dummy_sst_info,
};
use risingwave_storage::storage_value::StorageValue;
use tokio::sync::mpsc;
use tokio::sync::mpsc::unbounded_channel;

#[tokio::test]
async fn test_update_pinned_version() {
    let opt = Arc::new(default_config_for_test());
    let (_, hummock_manager_ref, _, worker_node) = setup_compute_env(8080).await;
    let local_version_manager = LocalVersionManager::for_test(
        opt.clone(),
        mock_sstable_store(),
        Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        )),
        ConflictDetector::new_from_config(opt),
    );

    let pinned_version = local_version_manager.get_pinned_version();
    let initial_version_id = pinned_version.id();
    let initial_max_commit_epoch = pinned_version.max_committed_epoch();

    let epochs: Vec<u64> = vec![
        initial_max_commit_epoch + 1,
        initial_max_commit_epoch + 2,
        initial_max_commit_epoch + 3,
        initial_max_commit_epoch + 4,
    ];
    let batches: Vec<Vec<(Bytes, StorageValue)>> =
        epochs.iter().map(|e| gen_dummy_batch(*e)).collect();

    // Fill shared buffer with a dummy empty batch in epochs[0] and epochs[1]
    for i in 0..2 {
        local_version_manager
            .write_shared_buffer(
                epochs[i],
                StaticCompactionGroupId::StateDefault.into(),
                batches[i].clone(),
                Default::default(),
            )
            .await
            .unwrap();
        let local_version = local_version_manager.get_local_version();
        assert_eq!(
            local_version.get_shared_buffer(epochs[i]).unwrap().size(),
            SharedBufferBatch::measure_batch_size(&build_shared_buffer_item_batches(
                batches[i].clone(),
                epochs[i]
            ))
        );
    }

    local_version_manager
        .write_shared_buffer(
            epochs[2],
            StaticCompactionGroupId::StateDefault.into(),
            batches[2].clone(),
            Default::default(),
        )
        .await
        .unwrap();
    let local_version = local_version_manager.get_local_version();
    assert!(local_version.get_shared_buffer(epochs[2]).is_some(),);

    let build_batch = |pairs, epoch| {
        SharedBufferBatch::new(
            build_shared_buffer_item_batches(pairs, epoch),
            epoch,
            unbounded_channel().0,
            StaticCompactionGroupId::StateDefault.into(),
            0,
        )
    };

    let read_version = local_version_manager.read_filter::<_, &[u8]>(epochs[0], &(..));
    assert_eq!(
        read_version.shared_buffer_data,
        vec![vec![vec![UncommittedData::Batch(build_batch(
            batches[0].clone(),
            epochs[0]
        ))]]]
    );

    let read_version = local_version_manager.read_filter::<_, &[u8]>(epochs[1], &(..));
    assert_eq!(
        read_version.shared_buffer_data,
        vec![
            vec![vec![UncommittedData::Batch(build_batch(
                batches[1].clone(),
                epochs[1]
            ))]],
            vec![vec![UncommittedData::Batch(build_batch(
                batches[0].clone(),
                epochs[0]
            ))]]
        ]
    );

    let read_version = local_version_manager.read_filter::<_, &[u8]>(epochs[2], &(..));
    assert_eq!(
        read_version.shared_buffer_data,
        vec![
            vec![vec![UncommittedData::Batch(build_batch(
                batches[2].clone(),
                epochs[2]
            ))]],
            vec![vec![UncommittedData::Batch(build_batch(
                batches[1].clone(),
                epochs[1]
            ))]],
            vec![vec![UncommittedData::Batch(build_batch(
                batches[0].clone(),
                epochs[0]
            ))]]
        ]
    );

    let _ = local_version_manager
        .sync_shared_buffer(epochs[0])
        .await
        .unwrap();

    // Update version for epochs[0]
    let version = HummockVersion {
        id: initial_version_id + 1,
        max_committed_epoch: epochs[0],
        ..Default::default()
    };
    local_version_manager.try_update_pinned_version(Payload::PinnedVersion(version));
    let local_version = local_version_manager.get_local_version();
    assert!(local_version.get_shared_buffer(epochs[0]).is_none());
    assert_eq!(
        local_version.get_shared_buffer(epochs[1]).unwrap().size(),
        SharedBufferBatch::measure_batch_size(&build_shared_buffer_item_batches(
            batches[1].clone(),
            epochs[1]
        ))
    );

    let _ = local_version_manager
        .sync_shared_buffer(epochs[1])
        .await
        .unwrap();

    // Update version for epochs[1]
    let version = HummockVersion {
        id: initial_version_id + 2,
        max_committed_epoch: epochs[1],
        ..Default::default()
    };
    local_version_manager.try_update_pinned_version(Payload::PinnedVersion(version));
    let local_version = local_version_manager.get_local_version();
    assert!(local_version.get_shared_buffer(epochs[0]).is_none());
    assert!(local_version.get_shared_buffer(epochs[1]).is_none());

    let _ = local_version_manager
        .sync_shared_buffer(epochs[2])
        .await
        .unwrap();
    // Update version for epochs[2]
    let version = HummockVersion {
        id: initial_version_id + 3,
        max_committed_epoch: epochs[2],
        ..Default::default()
    };

    local_version_manager.try_update_pinned_version(Payload::PinnedVersion(version));
    assert!(local_version.get_shared_buffer(epochs[0]).is_none());
    assert!(local_version.get_shared_buffer(epochs[1]).is_none());
}

#[tokio::test]
async fn test_update_uncommitted_ssts() {
    let mut opt = default_config_for_test();
    opt.share_buffers_sync_parallelism = 2;
    opt.sstable_size_mb = 1;
    let opt = Arc::new(opt);
    let (_, hummock_manager_ref, _, worker_node) = setup_compute_env(8080).await;
    let local_version_manager = LocalVersionManager::for_test(
        opt.clone(),
        mock_sstable_store(),
        Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        )),
        ConflictDetector::new_from_config(opt),
    );

    let pinned_version = local_version_manager.get_pinned_version();
    let max_commit_epoch = pinned_version.max_committed_epoch();
    let initial_id = pinned_version.id();
    let version = pinned_version.version();

    let epochs: Vec<u64> = vec![max_commit_epoch + 1, max_commit_epoch + 2];
    let kvs: Vec<Vec<(Bytes, StorageValue)>> = epochs
        .iter()
        .map(|e| gen_dummy_batch_several_keys(*e, 2000))
        .collect();
    let mut batches = Vec::with_capacity(kvs.len());

    // Fill shared buffer with dummy batches
    for i in 0..2 {
        local_version_manager
            .write_shared_buffer(
                epochs[i],
                StaticCompactionGroupId::StateDefault.into(),
                kvs[i].clone(),
                Default::default(),
            )
            .await
            .unwrap();
        let local_version = local_version_manager.get_local_version();
        let batch = SharedBufferBatch::new(
            build_shared_buffer_item_batches(kvs[i].clone(), epochs[i]),
            epochs[i],
            mpsc::unbounded_channel().0,
            StaticCompactionGroupId::StateDefault.into(),
            Default::default(),
        );
        assert_eq!(
            local_version.get_shared_buffer(epochs[i]).unwrap().size(),
            batch.size(),
        );
        batches.push(batch);
    }

    // Update uncommitted sst for epochs[0]
    let sst1 = gen_dummy_sst_info(1, vec![batches[0].clone()]);
    {
        let (payload, task_size) = {
            let mut local_version_guard = local_version_manager.local_version().write();
            local_version_guard.advance_max_sync_epoch(epochs[0]);
            let (payload, task_size) = local_version_guard.start_syncing(epochs[0]);
            {
                assert_eq!(1, payload.len());
                assert_eq!(1, payload[0].len());
                assert_eq!(payload[0][0], UncommittedData::Batch(batches[0].clone()));
                assert_eq!(task_size, batches[0].size());
            }
            (payload, task_size)
        };
        // Check uncommitted ssts
        local_version_manager
            .run_sync_upload_task(payload, task_size, epochs[0])
            .await
            .unwrap();
        let epoch_uncommitted_ssts = local_version_manager
            .get_local_version()
            .get_synced_ssts(epochs[0])
            .clone();
        assert_eq!(epoch_uncommitted_ssts.len(), 2);
        assert_eq!(
            epoch_uncommitted_ssts
                .first()
                .unwrap()
                .1
                .key_range
                .as_ref()
                .unwrap()
                .left,
            sst1.key_range.as_ref().unwrap().left,
        );
        assert_eq!(
            epoch_uncommitted_ssts
                .last()
                .unwrap()
                .1
                .key_range
                .as_ref()
                .unwrap()
                .right,
            sst1.key_range.as_ref().unwrap().right,
        );
    }

    let local_version = local_version_manager.get_local_version();
    // Check shared buffer
    assert!(local_version.get_shared_buffer(epochs[0]).is_none());
    assert_eq!(
        local_version.get_shared_buffer(epochs[1]).unwrap().size(),
        batches[1].size(),
    );

    // Check pinned version
    assert_eq!(local_version.pinned_version().version(), version);
    assert_eq!(local_version.iter_shared_buffer().count(), 1);

    // Update uncommitted sst for epochs[1]
    let sst2 = gen_dummy_sst_info(2, vec![batches[1].clone()]);
    {
        let (payload, task_size) = {
            let mut local_version_guard = local_version_manager.local_version().write();
            local_version_guard.advance_max_sync_epoch(epochs[1]);
            let (payload, task_size) = local_version_guard.start_syncing(epochs[1]);
            {
                assert_eq!(1, payload.len());
                assert_eq!(1, payload[0].len());
                assert_eq!(payload[0][0], UncommittedData::Batch(batches[1].clone()));
                assert_eq!(task_size, batches[1].size());
            }
            (payload, task_size)
        };

        local_version_manager
            .run_sync_upload_task(payload, task_size, epochs[1])
            .await
            .unwrap();
        let epoch_uncommitted_ssts = local_version_manager
            .get_local_version()
            .get_synced_ssts(epochs[1])
            .clone();
        assert_eq!(epoch_uncommitted_ssts.len(), 2);
        assert_eq!(
            epoch_uncommitted_ssts
                .first()
                .unwrap()
                .1
                .key_range
                .as_ref()
                .unwrap()
                .left,
            sst2.key_range.as_ref().unwrap().left,
        );
        assert_eq!(
            epoch_uncommitted_ssts
                .last()
                .unwrap()
                .1
                .key_range
                .as_ref()
                .unwrap()
                .right,
            sst2.key_range.as_ref().unwrap().right,
        );
    }
    let local_version = local_version_manager.get_local_version();
    // Check shared buffer
    for epoch in &epochs {
        assert!(local_version.get_shared_buffer(*epoch).is_none());
    }
    // Check pinned version
    assert_eq!(local_version.pinned_version().version(), version);

    // Update version for epochs[0]
    let version = HummockVersion {
        id: initial_id + 1,
        max_committed_epoch: epochs[0],
        ..Default::default()
    };
    assert!(
        local_version_manager.try_update_pinned_version(Payload::PinnedVersion(version.clone()))
    );
    let local_version = local_version_manager.get_local_version();
    // Check shared buffer
    assert!(local_version.get_shared_buffer(epochs[0]).is_none());
    assert!(local_version.get_shared_buffer(epochs[1]).is_none());
    // Check pinned version
    assert_eq!(local_version.pinned_version().version(), version);
    assert!(local_version.get_shared_buffer(epochs[0]).is_none());

    // Update version for epochs[1]
    let version = HummockVersion {
        id: initial_id + 2,
        max_committed_epoch: epochs[1],
        ..Default::default()
    };
    local_version_manager.try_update_pinned_version(Payload::PinnedVersion(version.clone()));
    let local_version = local_version_manager.get_local_version();
    assert!(local_version.get_shared_buffer(epochs[0]).is_none());
    assert!(local_version.get_shared_buffer(epochs[1]).is_none());
    // Check pinned version
    assert_eq!(local_version.pinned_version().version(), version);
    // Check uncommitted ssts
    assert!(local_version.get_shared_buffer(epochs[0]).is_none());
    assert!(local_version.get_shared_buffer(epochs[1]).is_none());
}

#[tokio::test]
async fn test_clear_shared_buffer() {
    let opt = Arc::new(default_config_for_test());
    let (_, hummock_manager_ref, _, worker_node) = setup_compute_env(8080).await;
    let local_version_manager = LocalVersionManager::for_test(
        opt.clone(),
        mock_sstable_store(),
        Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        )),
        ConflictDetector::new_from_config(opt),
    );

    let pinned_version = local_version_manager.get_pinned_version();
    let initial_max_commit_epoch = pinned_version.max_committed_epoch();

    let epochs: Vec<u64> = vec![initial_max_commit_epoch + 1, initial_max_commit_epoch + 2];
    let batches: Vec<Vec<(Bytes, StorageValue)>> =
        epochs.iter().map(|e| gen_dummy_batch(*e)).collect();

    // Fill shared buffer with a dummy empty batch in epochs[0] and epochs[1]
    for i in 0..2 {
        local_version_manager
            .write_shared_buffer(
                epochs[i],
                StaticCompactionGroupId::StateDefault.into(),
                batches[i].clone(),
                Default::default(),
            )
            .await
            .unwrap();
        let local_version = local_version_manager.get_local_version();
        assert_eq!(
            local_version.get_shared_buffer(epochs[i]).unwrap().size(),
            SharedBufferBatch::measure_batch_size(&build_shared_buffer_item_batches(
                batches[i].clone(),
                epochs[i]
            ))
        );
    }

    // Clear shared buffer and check
    local_version_manager.clear_shared_buffer().await;
    let local_version = local_version_manager.get_local_version();
    assert_eq!(local_version.iter_shared_buffer().count(), 0);

    assert_eq!(
        local_version_manager
            .get_sstable_id_manager()
            .global_watermark_sst_id(),
        HummockSstableId::MAX
    );
}

#[tokio::test]
async fn test_sst_gc_watermark() {
    let opt = Arc::new(default_config_for_test());
    let (_, hummock_manager_ref, _, worker_node) = setup_compute_env(8080).await;
    let local_version_manager = LocalVersionManager::for_test(
        opt.clone(),
        mock_sstable_store(),
        Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_node.id,
        )),
        ConflictDetector::new_from_config(opt),
    );

    let pinned_version = local_version_manager.get_pinned_version();
    let initial_version_id = pinned_version.id();
    let initial_max_commit_epoch = pinned_version.max_committed_epoch();

    let epochs: Vec<u64> = vec![initial_max_commit_epoch + 1, initial_max_commit_epoch + 2];
    let batches: Vec<Vec<(Bytes, StorageValue)>> =
        epochs.iter().map(|e| gen_dummy_batch(*e)).collect();

    assert_eq!(
        local_version_manager
            .get_sstable_id_manager()
            .global_watermark_sst_id(),
        HummockSstableId::MAX
    );

    for i in 0..2 {
        local_version_manager
            .write_shared_buffer(
                epochs[i],
                StaticCompactionGroupId::StateDefault.into(),
                batches[i].clone(),
                Default::default(),
            )
            .await
            .unwrap();
    }

    assert_eq!(
        local_version_manager
            .get_sstable_id_manager()
            .global_watermark_sst_id(),
        HummockSstableId::MAX
    );

    for epoch in &epochs {
        let _ = local_version_manager
            .sync_shared_buffer(*epoch)
            .await
            .unwrap();

        // Global watermark determined by epoch 0.
        assert_eq!(
            local_version_manager
                .get_sstable_id_manager()
                .global_watermark_sst_id(),
            1
        );
    }

    let version = HummockVersion {
        id: initial_version_id + 1,
        max_committed_epoch: epochs[0],
        ..Default::default()
    };
    // Watermark held by epoch 0 is removed.
    local_version_manager.try_update_pinned_version(Payload::PinnedVersion(version));
    // Global watermark determined by epoch 1.
    assert_eq!(
        local_version_manager
            .get_sstable_id_manager()
            .global_watermark_sst_id(),
        2
    );

    let version = HummockVersion {
        id: initial_version_id + 2,
        max_committed_epoch: epochs[1],
        ..Default::default()
    };
    local_version_manager.try_update_pinned_version(Payload::PinnedVersion(version));
    assert_eq!(
        local_version_manager
            .get_sstable_id_manager()
            .global_watermark_sst_id(),
        HummockSstableId::MAX
    );
}
