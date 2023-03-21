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

use std::ops::Bound;
use std::ops::Bound::Unbounded;
use std::sync::Arc;

use bytes::Bytes;
use futures::{pin_mut, TryStreamExt};
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::{
    HummockEpoch, HummockReadEpoch, HummockSstableObjectId, LocalSstableInfo,
};
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
use risingwave_storage::hummock::test_utils::{count_stream, default_opts_for_test};
use risingwave_storage::hummock::{CachePolicy, HummockStorage};
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::*;

use crate::get_notification_client_for_test;
use crate::test_utils::{with_hummock_storage_v2, HummockStateStoreTestTrait, TestIngestBatch};

#[tokio::test]
async fn test_empty_read_v2() {
    let (hummock_storage, _meta_client) = with_hummock_storage_v2(Default::default()).await;
    assert!(hummock_storage
        .get(
            Bytes::from("test_key"),
            u64::MAX,
            ReadOptions {
                prefix_hint: None,
                ignore_range_tombstone: false,
                retention_seconds: None,
                table_id: TableId { table_id: 2333 },
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap()
        .is_none());
    let stream = hummock_storage
        .iter(
            (Unbounded, Unbounded),
            u64::MAX,
            ReadOptions {
                prefix_hint: None,
                ignore_range_tombstone: false,
                retention_seconds: None,
                table_id: TableId { table_id: 2333 },
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap();
    pin_mut!(stream);
    assert!(stream.try_next().await.unwrap().is_none());
}

#[tokio::test]
async fn test_basic_v2() {
    let (hummock_storage, meta_client) = with_hummock_storage_v2(Default::default()).await;
    test_basic_inner(hummock_storage, meta_client).await;
}

async fn test_basic_inner(
    hummock_storage: impl HummockStateStoreTestTrait,
    meta_client: Arc<MockHummockMetaClient>,
) {
    let anchor = Bytes::from("aa");

    // First batch inserts the anchor and others.
    let mut batch1 = vec![
        (anchor.clone(), StorageValue::new_put("111")),
        (Bytes::from("bb"), StorageValue::new_put("222")),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
    let mut batch2 = vec![
        (Bytes::from("cc"), StorageValue::new_put("333")),
        (anchor.clone(), StorageValue::new_put("111111")),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Third batch deletes the anchor
    let mut batch3 = vec![
        (Bytes::from("dd"), StorageValue::new_put("444")),
        (Bytes::from("ee"), StorageValue::new_put("555")),
        (anchor.clone(), StorageValue::new_delete()),
    ];

    // Make sure the batch is sorted.
    batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    let mut local = hummock_storage.new_local(Default::default()).await;

    // epoch 0 is reserved by storage service
    let epoch1: u64 = 1;
    local.init(epoch1);

    // try to write an empty batch, and hummock should write nothing
    let size = local
        .ingest_batch(
            vec![],
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    assert_eq!(size, 0);

    // Write the first batch.
    local
        .ingest_batch(
            batch1,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    let epoch2 = epoch1 + 1;
    local.seal_current_epoch(epoch2);

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch1,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));
    let value = hummock_storage
        .get(
            Bytes::from("bb"),
            epoch1,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("222"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = hummock_storage
        .get(
            Bytes::from("ab"),
            epoch1,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write the second batch.
    local
        .ingest_batch(
            batch2,
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    let epoch3 = epoch2 + 1;
    local.seal_current_epoch(epoch3);

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch2,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));

    // Write the third batch.

    local
        .ingest_batch(
            batch3,
            vec![],
            WriteOptions {
                epoch: epoch3,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    local.seal_current_epoch(u64::MAX);

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch3,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Get non-existent maximum key.
    let value = hummock_storage
        .get(
            Bytes::from("ff"),
            epoch3,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write aa bb
    let iter = hummock_storage
        .iter(
            (Bound::Unbounded, Bound::Included(Bytes::from("ee"))),
            epoch1,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap();
    let len = count_stream(iter).await;
    assert_eq!(len, 2);

    // Get the anchor value at the first snapshot
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch1,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch2,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));
    // Update aa, write cc
    let iter = hummock_storage
        .iter(
            (Bound::Unbounded, Bound::Included(Bytes::from("ee"))),
            epoch2,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap();
    let len = count_stream(iter).await;
    assert_eq!(len, 3);

    // Delete aa, write dd,ee
    let iter = hummock_storage
        .iter(
            (Bound::Unbounded, Bound::Included(Bytes::from("ee"))),
            epoch3,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap();
    let len = count_stream(iter).await;
    assert_eq!(len, 4);
    let ssts = hummock_storage
        .seal_and_sync_epoch(epoch1)
        .await
        .unwrap()
        .uncommitted_ssts;
    meta_client.commit_epoch(epoch1, ssts).await.unwrap();
    hummock_storage
        .try_wait_epoch(HummockReadEpoch::Committed(epoch1))
        .await
        .unwrap();
    let value = hummock_storage
        .get(
            Bytes::from("bb"),
            epoch2,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("222"));
    let value = hummock_storage
        .get(
            Bytes::from("dd"),
            epoch2,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap();
    assert!(value.is_none());
}

#[tokio::test]
async fn test_state_store_sync_v2() {
    let (hummock_storage, meta_client) = with_hummock_storage_v2(Default::default()).await;
    test_state_store_sync_inner(hummock_storage, meta_client).await;
}

async fn test_state_store_sync_inner(
    hummock_storage: impl HummockStateStoreTestTrait,
    _meta_client: Arc<MockHummockMetaClient>,
) {
    let mut epoch: HummockEpoch = hummock_storage.get_pinned_version().max_committed_epoch() + 1;

    // ingest 16B batch
    let mut batch1 = vec![
        (Bytes::from("aaaa"), StorageValue::new_put("1111")),
        (Bytes::from("bbbb"), StorageValue::new_put("2222")),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    let mut local = hummock_storage
        .new_local(NewLocalOptions::for_test(Default::default()))
        .await;
    local.init(epoch);
    local
        .ingest_batch(
            batch1,
            vec![],
            WriteOptions {
                epoch,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // ingest 24B batch
    let mut batch2 = vec![
        (Bytes::from("cccc"), StorageValue::new_put("3333")),
        (Bytes::from("dddd"), StorageValue::new_put("4444")),
        (Bytes::from("eeee"), StorageValue::new_put("5555")),
    ];
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    local
        .ingest_batch(
            batch2,
            vec![],
            WriteOptions {
                epoch,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // TODO: Uncomment the following lines after flushed sstable can be accessed.
    // FYI: https://github.com/risingwavelabs/risingwave/pull/1928#discussion_r852698719
    // shared buffer threshold size should have been reached and will trigger a flush
    // then ingest the batch
    // assert_eq!(
    //     (24 + 8 * 3) as u64,
    //     hummock_storage.shared_buffer_manager().size() as u64
    // );

    epoch += 1;
    local.seal_current_epoch(epoch);

    // ingest more 8B then will trigger a sync behind the scene
    let mut batch3 = vec![(Bytes::from("eeee"), StorageValue::new_put("5555"))];
    batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    local
        .ingest_batch(
            batch3,
            vec![],
            WriteOptions {
                epoch,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // TODO: Uncomment the following lines after flushed sstable can be accessed.
    // FYI: https://github.com/risingwavelabs/risingwave/pull/1928#discussion_r852698719
    // 16B in total with 8B epoch appended to the key
    // assert_eq!(
    //     16 as u64,
    //     hummock_storage.shared_buffer_manager().size() as u64
    // );

    local.seal_current_epoch(u64::MAX);

    // trigger a sync
    hummock_storage
        .seal_and_sync_epoch(epoch - 1)
        .await
        .unwrap();
    hummock_storage.seal_and_sync_epoch(epoch).await.unwrap();

    // TODO: Uncomment the following lines after flushed sstable can be accessed.
    // FYI: https://github.com/risingwavelabs/risingwave/pull/1928#discussion_r852698719
    // assert_eq!(0, hummock_storage.shared_buffer_manager().size());
}

#[tokio::test]
/// Fix this when we finished epoch management.
#[ignore]
async fn test_reload_storage() {
    let sstable_store = mock_sstable_store();
    let hummock_options = Arc::new(default_opts_for_test());
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let (hummock_storage, meta_client) = with_hummock_storage_v2(Default::default()).await;
    let anchor = Bytes::from("aa");

    // First batch inserts the anchor and others.
    let mut batch1 = vec![
        (anchor.clone(), StorageValue::new_put("111")),
        (Bytes::from("bb"), StorageValue::new_put("222")),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
    let mut batch2 = vec![
        (Bytes::from("cc"), StorageValue::new_put("333")),
        (anchor.clone(), StorageValue::new_put("111111")),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // epoch 0 is reserved by storage service
    let epoch1: u64 = 1;

    // Un-comment it when the unit test is re-enabled.
    // // Write the first batch.
    // hummock_storage
    //     .ingest_batch(
    //         batch1,
    //         vec![],
    //         WriteOptions {
    //             epoch: epoch1,
    //             table_id: Default::default(),
    //         },
    //     )
    //     .await
    //     .unwrap();

    // Mock something happened to storage internal, and storage is reloaded.
    drop(hummock_storage);
    let hummock_storage = HummockStorage::for_test(
        hummock_options,
        sstable_store.clone(),
        meta_client.clone(),
        get_notification_client_for_test(env, hummock_manager_ref, worker_node),
    )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch1,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = hummock_storage
        .get(
            Bytes::from("ab"),
            epoch1,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write the second batch.
    let epoch2 = epoch1 + 1;
    // TODO: recover the comment if the test is needed
    // hummock_storage
    //     .ingest_batch(
    //         batch2,
    //         vec![],
    //         WriteOptions {
    //             epoch: epoch2,
    //             table_id: Default::default(),
    //         },
    //     )
    //     .await
    //     .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch2,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));

    // Write aa bb
    let iter = hummock_storage
        .iter(
            (Bound::Unbounded, Bound::Included(Bytes::from("ee"))),
            epoch1,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap();
    let len = count_stream(iter).await;
    assert_eq!(len, 2);

    // Get the anchor value at the first snapshot
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch1,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch2,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));
    // Update aa, write cc
    let iter = hummock_storage
        .iter(
            (Bound::Unbounded, Bound::Included(Bytes::from("ee"))),
            epoch2,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                cache_policy: CachePolicy::Fill,
            },
        )
        .await
        .unwrap();
    let len = count_stream(iter).await;
    assert_eq!(len, 3);
}

#[tokio::test]
async fn test_write_anytime_v2() {
    let (hummock_storage, meta_client) = with_hummock_storage_v2(Default::default()).await;
    test_write_anytime_inner(hummock_storage, meta_client).await;
}

async fn test_write_anytime_inner(
    hummock_storage: impl HummockStateStoreTestTrait,
    _meta_client: Arc<MockHummockMetaClient>,
) {
    let initial_epoch = hummock_storage.get_pinned_version().max_committed_epoch();

    let epoch1 = initial_epoch + 1;

    let assert_old_value = |epoch| {
        let hummock_storage = &hummock_storage;
        async move {
            // check point get
            assert_eq!(
                "111".as_bytes(),
                hummock_storage
                    .get(
                        Bytes::from("aa"),
                        epoch,
                        ReadOptions {
                            ignore_range_tombstone: false,

                            prefix_hint: None,
                            table_id: Default::default(),
                            retention_seconds: None,
                            read_version_from_backup: false,
                            prefetch_options: Default::default(),
                            cache_policy: CachePolicy::Fill,
                        }
                    )
                    .await
                    .unwrap()
                    .unwrap()
            );
            assert_eq!(
                "222".as_bytes(),
                hummock_storage
                    .get(
                        Bytes::from("bb"),
                        epoch,
                        ReadOptions {
                            ignore_range_tombstone: false,

                            prefix_hint: None,
                            table_id: Default::default(),
                            retention_seconds: None,
                            read_version_from_backup: false,
                            prefetch_options: Default::default(),
                            cache_policy: CachePolicy::Fill,
                        }
                    )
                    .await
                    .unwrap()
                    .unwrap()
            );
            assert_eq!(
                "333".as_bytes(),
                hummock_storage
                    .get(
                        Bytes::from("cc"),
                        epoch,
                        ReadOptions {
                            ignore_range_tombstone: false,

                            prefix_hint: None,
                            table_id: Default::default(),
                            retention_seconds: None,
                            read_version_from_backup: false,
                            prefetch_options: Default::default(),
                            cache_policy: CachePolicy::Fill,
                        }
                    )
                    .await
                    .unwrap()
                    .unwrap()
            );
            // check iter
            let iter = hummock_storage
                .iter(
                    (
                        Bound::Included(Bytes::from("aa")),
                        Bound::Included(Bytes::from("cc")),
                    ),
                    epoch,
                    ReadOptions {
                        ignore_range_tombstone: false,

                        prefix_hint: None,
                        table_id: Default::default(),
                        retention_seconds: None,
                        read_version_from_backup: false,
                        prefetch_options: Default::default(),
                        cache_policy: CachePolicy::Fill,
                    },
                )
                .await
                .unwrap();
            futures::pin_mut!(iter);
            assert_eq!(
                (
                    FullKey::for_test(TableId::default(), b"aa".to_vec().into(), epoch),
                    Bytes::from("111")
                ),
                iter.try_next().await.unwrap().unwrap()
            );
            assert_eq!(
                (
                    FullKey::for_test(TableId::default(), b"bb".to_vec().into(), epoch),
                    Bytes::from("222")
                ),
                iter.try_next().await.unwrap().unwrap()
            );
            assert_eq!(
                (
                    FullKey::for_test(TableId::default(), b"cc".to_vec().into(), epoch),
                    Bytes::from("333")
                ),
                iter.try_next().await.unwrap().unwrap()
            );
            assert!(iter.try_next().await.unwrap().is_none());
        }
    };

    let batch1 = vec![
        (Bytes::from("aa"), StorageValue::new_put("111")),
        (Bytes::from("bb"), StorageValue::new_put("222")),
        (Bytes::from("cc"), StorageValue::new_put("333")),
    ];

    let mut local = hummock_storage.new_local(NewLocalOptions::default()).await;
    local.init(epoch1);

    local
        .ingest_batch(
            batch1.clone(),
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    assert_old_value(epoch1).await;

    let assert_new_value = |epoch| {
        let hummock_storage = &hummock_storage;
        async move {
            // check point get
            assert_eq!(
                "111_new".as_bytes(),
                hummock_storage
                    .get(
                        Bytes::from("aa"),
                        epoch,
                        ReadOptions {
                            ignore_range_tombstone: false,

                            prefix_hint: None,
                            table_id: Default::default(),
                            retention_seconds: None,
                            read_version_from_backup: false,
                            prefetch_options: Default::default(),
                            cache_policy: CachePolicy::Fill,
                        }
                    )
                    .await
                    .unwrap()
                    .unwrap()
            );

            assert!(hummock_storage
                .get(
                    Bytes::from("bb"),
                    epoch,
                    ReadOptions {
                        ignore_range_tombstone: false,

                        prefix_hint: None,
                        table_id: Default::default(),
                        retention_seconds: None,
                        read_version_from_backup: false,
                        prefetch_options: Default::default(),
                        cache_policy: CachePolicy::Fill,
                    }
                )
                .await
                .unwrap()
                .is_none());
            assert_eq!(
                "333".as_bytes(),
                hummock_storage
                    .get(
                        Bytes::from("cc"),
                        epoch,
                        ReadOptions {
                            ignore_range_tombstone: false,

                            prefix_hint: None,
                            table_id: Default::default(),
                            retention_seconds: None,
                            read_version_from_backup: false,
                            prefetch_options: Default::default(),
                            cache_policy: CachePolicy::Fill,
                        }
                    )
                    .await
                    .unwrap()
                    .unwrap()
            );
            let iter = hummock_storage
                .iter(
                    (
                        Bound::Included(Bytes::from("aa")),
                        Bound::Included(Bytes::from("cc")),
                    ),
                    epoch,
                    ReadOptions {
                        ignore_range_tombstone: false,

                        prefix_hint: None,
                        table_id: Default::default(),
                        retention_seconds: None,
                        read_version_from_backup: false,
                        prefetch_options: Default::default(),
                        cache_policy: CachePolicy::Fill,
                    },
                )
                .await
                .unwrap();
            futures::pin_mut!(iter);
            assert_eq!(
                (
                    FullKey::for_test(TableId::default(), b"aa".to_vec().into(), epoch),
                    Bytes::from("111_new")
                ),
                iter.try_next().await.unwrap().unwrap()
            );
            assert_eq!(
                (
                    FullKey::for_test(TableId::default(), b"cc".to_vec().into(), epoch),
                    Bytes::from("333")
                ),
                iter.try_next().await.unwrap().unwrap()
            );
            assert!(iter.try_next().await.unwrap().is_none());
        }
    };

    // Update aa, delete bb, cc unchanged
    let batch2 = vec![
        (Bytes::from("aa"), StorageValue::new_put("111_new")),
        (Bytes::from("bb"), StorageValue::new_delete()),
    ];

    local
        .ingest_batch(
            batch2,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    assert_new_value(epoch1).await;

    let epoch2 = epoch1 + 1;
    local.seal_current_epoch(epoch2);

    // Write to epoch2
    local
        .ingest_batch(
            batch1,
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    local.seal_current_epoch(u64::MAX);
    // Assert epoch 1 unchanged
    assert_new_value(epoch1).await;
    // Assert epoch 2 correctness
    assert_old_value(epoch2).await;

    let ssts1 = hummock_storage
        .seal_and_sync_epoch(epoch1)
        .await
        .unwrap()
        .uncommitted_ssts;
    assert_new_value(epoch1).await;
    assert_old_value(epoch2).await;

    let ssts2 = hummock_storage
        .seal_and_sync_epoch(epoch2)
        .await
        .unwrap()
        .uncommitted_ssts;
    assert_new_value(epoch1).await;
    assert_old_value(epoch2).await;

    assert!(!ssts1.is_empty());
    assert!(!ssts2.is_empty());
}

#[tokio::test]
async fn test_delete_get_v2() {
    let (hummock_storage, meta_client) = with_hummock_storage_v2(Default::default()).await;
    test_delete_get_inner(hummock_storage, meta_client).await;
}

async fn test_delete_get_inner(
    hummock_storage: impl HummockStateStoreTestTrait,
    meta_client: Arc<MockHummockMetaClient>,
) {
    let initial_epoch = hummock_storage.get_pinned_version().max_committed_epoch();
    let epoch1 = initial_epoch + 1;
    let batch1 = vec![
        (Bytes::from("aa"), StorageValue::new_put("111")),
        (Bytes::from("bb"), StorageValue::new_put("222")),
    ];
    let mut local = hummock_storage.new_local(NewLocalOptions::default()).await;
    local.init(epoch1);
    local
        .ingest_batch(
            batch1,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    let ssts = hummock_storage
        .seal_and_sync_epoch(epoch1)
        .await
        .unwrap()
        .uncommitted_ssts;
    meta_client.commit_epoch(epoch1, ssts).await.unwrap();
    let epoch2 = initial_epoch + 2;

    local.seal_current_epoch(epoch2);
    let batch2 = vec![(Bytes::from("bb"), StorageValue::new_delete())];
    local
        .ingest_batch(
            batch2,
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    local.seal_current_epoch(u64::MAX);
    let ssts = hummock_storage
        .seal_and_sync_epoch(epoch2)
        .await
        .unwrap()
        .uncommitted_ssts;
    meta_client.commit_epoch(epoch2, ssts).await.unwrap();
    hummock_storage
        .try_wait_epoch(HummockReadEpoch::Committed(epoch2))
        .await
        .unwrap();
    assert!(hummock_storage
        .get(
            Bytes::from("bb"),
            epoch2,
            ReadOptions {
                ignore_range_tombstone: false,

                prefix_hint: None,
                table_id: Default::default(),
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill,
            }
        )
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn test_multiple_epoch_sync_v2() {
    let (hummock_storage, meta_client) = with_hummock_storage_v2(Default::default()).await;
    test_multiple_epoch_sync_inner(hummock_storage, meta_client).await;
}

async fn test_multiple_epoch_sync_inner(
    hummock_storage: impl HummockStateStoreTestTrait,
    meta_client: Arc<MockHummockMetaClient>,
) {
    let initial_epoch = hummock_storage.get_pinned_version().max_committed_epoch();
    let epoch1 = initial_epoch + 1;
    let batch1 = vec![
        (Bytes::from("aa"), StorageValue::new_put("111")),
        (Bytes::from("bb"), StorageValue::new_put("222")),
    ];

    let mut local = hummock_storage.new_local(NewLocalOptions::default()).await;
    local.init(epoch1);
    local
        .ingest_batch(
            batch1,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    let epoch2 = initial_epoch + 2;
    local.seal_current_epoch(epoch2);
    let batch2 = vec![(Bytes::from("bb"), StorageValue::new_delete())];
    local
        .ingest_batch(
            batch2,
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    let epoch3 = initial_epoch + 3;
    let batch3 = vec![
        (Bytes::from("aa"), StorageValue::new_put("444")),
        (Bytes::from("bb"), StorageValue::new_put("555")),
    ];
    local.seal_current_epoch(epoch3);
    local
        .ingest_batch(
            batch3,
            vec![],
            WriteOptions {
                epoch: epoch3,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    local.seal_current_epoch(u64::MAX);
    let test_get = || {
        let hummock_storage_clone = &hummock_storage;
        async move {
            assert_eq!(
                hummock_storage_clone
                    .get(
                        Bytes::from("bb"),
                        epoch1,
                        ReadOptions {
                            ignore_range_tombstone: false,

                            prefix_hint: None,
                            table_id: Default::default(),
                            retention_seconds: None,
                            read_version_from_backup: false,
                            prefetch_options: Default::default(),
                            cache_policy: CachePolicy::Fill,
                        }
                    )
                    .await
                    .unwrap()
                    .unwrap(),
                "222".as_bytes()
            );
            assert!(hummock_storage_clone
                .get(
                    Bytes::from("bb"),
                    epoch2,
                    ReadOptions {
                        ignore_range_tombstone: false,

                        prefix_hint: None,
                        table_id: Default::default(),
                        retention_seconds: None,
                        read_version_from_backup: false,
                        prefetch_options: Default::default(),
                        cache_policy: CachePolicy::Fill,
                    }
                )
                .await
                .unwrap()
                .is_none());
            assert_eq!(
                hummock_storage_clone
                    .get(
                        Bytes::from("bb"),
                        epoch3,
                        ReadOptions {
                            ignore_range_tombstone: false,

                            prefix_hint: None,
                            table_id: Default::default(),
                            retention_seconds: None,
                            read_version_from_backup: false,
                            prefetch_options: Default::default(),
                            cache_policy: CachePolicy::Fill,
                        }
                    )
                    .await
                    .unwrap()
                    .unwrap(),
                "555".as_bytes()
            );
        }
    };
    test_get().await;
    hummock_storage.seal_epoch(epoch1, false);
    let sync_result2 = hummock_storage.seal_and_sync_epoch(epoch2).await.unwrap();
    let sync_result3 = hummock_storage.seal_and_sync_epoch(epoch3).await.unwrap();
    test_get().await;
    meta_client
        .commit_epoch(epoch2, sync_result2.uncommitted_ssts)
        .await
        .unwrap();
    meta_client
        .commit_epoch(epoch3, sync_result3.uncommitted_ssts)
        .await
        .unwrap();
    hummock_storage
        .try_wait_epoch(HummockReadEpoch::Committed(epoch3))
        .await
        .unwrap();
    test_get().await;
}

#[tokio::test]
async fn test_gc_watermark_and_clear_shared_buffer() {
    let (hummock_storage, meta_client) = with_hummock_storage_v2(Default::default()).await;

    assert_eq!(
        hummock_storage
            .sstable_object_id_manager()
            .global_watermark_object_id(),
        HummockSstableObjectId::MAX
    );

    let mut local_hummock_storage = hummock_storage
        .new_local(NewLocalOptions::for_test(Default::default()))
        .await;

    let initial_epoch = hummock_storage.get_pinned_version().max_committed_epoch();
    let epoch1 = initial_epoch + 1;
    local_hummock_storage.init(epoch1);
    local_hummock_storage
        .insert(Bytes::from("aa"), Bytes::from("111"), None)
        .unwrap();
    local_hummock_storage
        .insert(Bytes::from("bb"), Bytes::from("222"), None)
        .unwrap();
    local_hummock_storage.flush(Vec::new()).await.unwrap();

    assert_eq!(
        hummock_storage
            .sstable_object_id_manager()
            .global_watermark_object_id(),
        HummockSstableObjectId::MAX
    );

    let epoch2 = initial_epoch + 2;
    local_hummock_storage.seal_current_epoch(epoch2);
    local_hummock_storage
        .delete(Bytes::from("bb"), Bytes::from("222"))
        .unwrap();
    local_hummock_storage.flush(Vec::new()).await.unwrap();

    assert_eq!(
        hummock_storage
            .sstable_object_id_manager()
            .global_watermark_object_id(),
        HummockSstableObjectId::MAX
    );
    let min_object_id = |sync_result: &SyncResult| {
        sync_result
            .uncommitted_ssts
            .iter()
            .map(|LocalSstableInfo { sst_info, .. }| sst_info.get_object_id())
            .min()
            .unwrap()
    };
    local_hummock_storage.seal_current_epoch(u64::MAX);
    let sync_result1 = hummock_storage.seal_and_sync_epoch(epoch1).await.unwrap();
    let min_object_id_epoch1 = min_object_id(&sync_result1);
    assert_eq!(
        hummock_storage
            .sstable_object_id_manager()
            .global_watermark_object_id(),
        min_object_id_epoch1,
    );
    let sync_result2 = hummock_storage.seal_and_sync_epoch(epoch2).await.unwrap();
    let min_object_id_epoch2 = min_object_id(&sync_result2);
    assert_eq!(
        hummock_storage
            .sstable_object_id_manager()
            .global_watermark_object_id(),
        min_object_id_epoch1,
    );
    meta_client
        .commit_epoch(epoch1, sync_result1.uncommitted_ssts)
        .await
        .unwrap();
    hummock_storage
        .try_wait_epoch(HummockReadEpoch::Committed(epoch1))
        .await
        .unwrap();

    assert_eq!(
        hummock_storage
            .sstable_object_id_manager()
            .global_watermark_object_id(),
        min_object_id_epoch2,
    );

    hummock_storage.clear_shared_buffer().await.unwrap();

    let read_version = local_hummock_storage.read_version();

    let read_version = read_version.read();
    assert!(read_version.staging().imm.is_empty());
    assert!(read_version.staging().sst.is_empty());
    assert_eq!(read_version.committed().max_committed_epoch(), epoch1);
    assert_eq!(
        hummock_storage
            .sstable_object_id_manager()
            .global_watermark_object_id(),
        HummockSstableObjectId::MAX
    );
}
