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

use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

use bytes::{BufMut, Bytes};
use futures::TryStreamExt;
use parking_lot::RwLock;
use risingwave_common::cache::CachePriority;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{map_table_key_range, FullKey, UserKey, TABLE_PREFIX_LEN};
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::hummock::store::version::{read_filter_for_batch, read_filter_for_local};
use risingwave_storage::hummock::CachePolicy;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::*;
use risingwave_storage::StateStore;

use crate::test_utils::{prepare_hummock_test_env, TestIngestBatch};

#[tokio::test]
async fn test_storage_basic() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;
    test_env.register_table_id(TEST_TABLE_ID).await;
    let mut hummock_storage = test_env
        .storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;

    // First batch inserts the anchor and others.
    let mut batch1 = vec![
        (Bytes::from("aa"), StorageValue::new_put("111")),
        (Bytes::from("bb"), StorageValue::new_put("222")),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
    let mut batch2 = vec![
        (Bytes::from("cc"), StorageValue::new_put("333")),
        (Bytes::from("aa"), StorageValue::new_put("111111")),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Third batch deletes the anchor
    let mut batch3 = vec![
        (Bytes::from("dd"), StorageValue::new_put("444")),
        (Bytes::from("ee"), StorageValue::new_put("555")),
        (Bytes::from("aa"), StorageValue::new_delete()),
    ];

    // Make sure the batch is sorted.
    batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // epoch 0 is reserved by storage service
    let epoch1: u64 = 1;
    hummock_storage.init(epoch1);

    // Write the first batch.
    hummock_storage
        .ingest_batch(
            batch1,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = test_env
        .storage
        .get(
            Bytes::from("aa"),
            epoch1,
            ReadOptions {
                ignore_range_tombstone: false,
                table_id: TEST_TABLE_ID,
                retention_seconds: None,

                prefix_hint: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill(CachePriority::High),
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));
    let value = test_env
        .storage
        .get(
            Bytes::from("bb"),
            epoch1,
            ReadOptions {
                ignore_range_tombstone: false,
                table_id: TEST_TABLE_ID,
                retention_seconds: None,

                prefix_hint: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill(CachePriority::High),
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("222"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = test_env
        .storage
        .get(
            Bytes::from("ab"),
            epoch1,
            ReadOptions {
                ignore_range_tombstone: false,
                table_id: TEST_TABLE_ID,
                retention_seconds: None,

                prefix_hint: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill(CachePriority::High),
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    let epoch2 = epoch1 + 1;
    hummock_storage.seal_current_epoch(epoch2);
    hummock_storage
        .ingest_batch(
            batch2,
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = test_env
        .storage
        .get(
            Bytes::from("aa"),
            epoch2,
            ReadOptions {
                ignore_range_tombstone: false,
                table_id: TEST_TABLE_ID,
                retention_seconds: None,

                prefix_hint: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill(CachePriority::High),
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));

    // Write the third batch.
    let epoch3 = epoch2 + 1;
    hummock_storage.seal_current_epoch(epoch3);
    hummock_storage
        .ingest_batch(
            batch3,
            vec![],
            WriteOptions {
                epoch: epoch3,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = test_env
        .storage
        .get(
            Bytes::from("aa"),
            epoch3,
            ReadOptions {
                ignore_range_tombstone: false,
                table_id: TEST_TABLE_ID,
                retention_seconds: None,

                prefix_hint: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill(CachePriority::High),
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Get non-existent maximum key.
    let value = test_env
        .storage
        .get(
            Bytes::from("ff"),
            epoch3,
            ReadOptions {
                ignore_range_tombstone: false,
                table_id: TEST_TABLE_ID,
                retention_seconds: None,

                prefix_hint: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill(CachePriority::High),
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write aa bb
    let iter = test_env
        .storage
        .iter(
            (Unbounded, Included(Bytes::from("ee"))),
            epoch1,
            ReadOptions {
                ignore_range_tombstone: false,
                table_id: TEST_TABLE_ID,
                retention_seconds: None,

                prefix_hint: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill(CachePriority::High),
            },
        )
        .await
        .unwrap();
    futures::pin_mut!(iter);
    assert_eq!(
        Some((
            FullKey::for_test(TEST_TABLE_ID, b"aa".to_vec().into(), epoch1),
            Bytes::copy_from_slice(&b"111"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(
        Some((
            FullKey::for_test(TEST_TABLE_ID, b"bb".to_vec().into(), epoch1),
            Bytes::copy_from_slice(&b"222"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(None, iter.try_next().await.unwrap());

    // Get the anchor value at the first snapshot
    let value = test_env
        .storage
        .get(
            Bytes::from("aa"),
            epoch1,
            ReadOptions {
                ignore_range_tombstone: false,
                table_id: TEST_TABLE_ID,
                retention_seconds: None,

                prefix_hint: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill(CachePriority::High),
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = test_env
        .storage
        .get(
            Bytes::from("aa"),
            epoch2,
            ReadOptions {
                ignore_range_tombstone: false,
                table_id: TEST_TABLE_ID,
                retention_seconds: None,

                prefix_hint: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill(CachePriority::High),
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));
    // Update aa, write cc
    let iter = test_env
        .storage
        .iter(
            (Unbounded, Included(Bytes::from("ee"))),
            epoch2,
            ReadOptions {
                ignore_range_tombstone: false,
                table_id: TEST_TABLE_ID,
                retention_seconds: None,

                prefix_hint: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill(CachePriority::High),
            },
        )
        .await
        .unwrap();
    futures::pin_mut!(iter);
    assert_eq!(
        Some((
            FullKey::for_test(TEST_TABLE_ID, b"aa".to_vec().into(), epoch2),
            Bytes::copy_from_slice(&b"111111"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(
        Some((
            FullKey::for_test(TEST_TABLE_ID, b"bb".to_vec().into(), epoch1),
            Bytes::copy_from_slice(&b"222"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(
        Some((
            FullKey::for_test(TEST_TABLE_ID, b"cc".to_vec().into(), epoch2),
            Bytes::copy_from_slice(&b"333"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(None, iter.try_next().await.unwrap());

    // Delete aa, write dd,ee
    let iter = test_env
        .storage
        .iter(
            (Unbounded, Included(Bytes::from("ee"))),
            epoch3,
            ReadOptions {
                ignore_range_tombstone: false,
                table_id: TEST_TABLE_ID,
                retention_seconds: None,

                prefix_hint: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill(CachePriority::High),
            },
        )
        .await
        .unwrap();
    futures::pin_mut!(iter);
    assert_eq!(
        Some((
            FullKey::for_test(TEST_TABLE_ID, b"bb".to_vec().into(), epoch1),
            Bytes::copy_from_slice(&b"222"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(
        Some((
            FullKey::for_test(TEST_TABLE_ID, b"cc".to_vec().into(), epoch2),
            Bytes::copy_from_slice(&b"333"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(
        Some((
            FullKey::for_test(TEST_TABLE_ID, b"dd".to_vec().into(), epoch3),
            Bytes::copy_from_slice(&b"444"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(
        Some((
            FullKey::for_test(TEST_TABLE_ID, b"ee".to_vec().into(), epoch3),
            Bytes::copy_from_slice(&b"555"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(None, iter.try_next().await.unwrap());

    // TODO: add more test cases after sync is supported
}

#[tokio::test]
async fn test_state_store_sync() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;
    test_env.register_table_id(TEST_TABLE_ID).await;
    let mut hummock_storage = test_env
        .storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;

    let read_version = hummock_storage.read_version();

    let epoch1: _ = read_version.read().committed().max_committed_epoch() + 1;
    hummock_storage.init(epoch1);

    // ingest 16B batch
    let mut batch1 = vec![
        (Bytes::from("aaaa"), StorageValue::new_put("1111")),
        (Bytes::from("bbbb"), StorageValue::new_put("2222")),
    ];

    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    hummock_storage
        .ingest_batch(
            batch1,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: TEST_TABLE_ID,
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
    hummock_storage
        .ingest_batch(
            batch2,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    let epoch2 = epoch1 + 1;
    hummock_storage.seal_current_epoch(epoch2);

    // ingest more 8B then will trigger a sync behind the scene
    let mut batch3 = vec![(Bytes::from("eeee"), StorageValue::new_put("6666"))];
    batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    hummock_storage
        .ingest_batch(
            batch3,
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    let res = test_env.storage.seal_and_sync_epoch(epoch1).await.unwrap();
    test_env
        .meta_client
        .commit_epoch(epoch1, res.uncommitted_ssts)
        .await
        .unwrap();
    test_env.storage.try_wait_epoch_for_test(epoch1).await;
    {
        // after sync 1 epoch
        let read_version = hummock_storage.read_version();
        assert_eq!(1, read_version.read().staging().imm.len());
        assert!(read_version.read().staging().sst.is_empty());
    }

    {
        let kv_map = [
            ("aaaa", "1111"),
            ("bbbb", "2222"),
            ("cccc", "3333"),
            ("dddd", "4444"),
            ("eeee", "5555"),
        ];

        for (k, v) in kv_map {
            let value = test_env
                .storage
                .get(
                    Bytes::from(k.to_owned()),
                    epoch1,
                    ReadOptions {
                        ignore_range_tombstone: false,
                        table_id: TEST_TABLE_ID,
                        retention_seconds: None,

                        prefix_hint: None,
                        read_version_from_backup: false,
                        prefetch_options: Default::default(),
                        cache_policy: CachePolicy::Fill(CachePriority::High),
                    },
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(value, Bytes::from(v));
        }
    }

    let res = test_env.storage.seal_and_sync_epoch(epoch2).await.unwrap();
    test_env
        .meta_client
        .commit_epoch(epoch2, res.uncommitted_ssts)
        .await
        .unwrap();
    test_env.storage.try_wait_epoch_for_test(epoch2).await;
    {
        // after sync all epoch
        let read_version = hummock_storage.read_version();
        assert!(read_version.read().staging().imm.is_empty());
        assert!(read_version.read().staging().sst.is_empty());
    }

    {
        let kv_map = [
            ("aaaa", "1111"),
            ("bbbb", "2222"),
            ("cccc", "3333"),
            ("dddd", "4444"),
            ("eeee", "6666"),
        ];

        for (k, v) in kv_map {
            let value = test_env
                .storage
                .get(
                    Bytes::from(k.to_owned()),
                    epoch2,
                    ReadOptions {
                        ignore_range_tombstone: false,
                        table_id: TEST_TABLE_ID,
                        retention_seconds: None,

                        prefix_hint: None,
                        read_version_from_backup: false,
                        prefetch_options: Default::default(),
                        cache_policy: CachePolicy::Fill(CachePriority::High),
                    },
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(value, Bytes::from(v));
        }
    }

    // test iter
    {
        let iter = test_env
            .storage
            .iter(
                (Unbounded, Included(Bytes::from("eeee"))),
                epoch1,
                ReadOptions {
                    ignore_range_tombstone: false,
                    table_id: TEST_TABLE_ID,
                    retention_seconds: None,

                    prefix_hint: None,
                    read_version_from_backup: false,
                    prefetch_options: Default::default(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                },
            )
            .await
            .unwrap();
        futures::pin_mut!(iter);

        let kv_map = [
            (b"aaaa", "1111", epoch1),
            (b"bbbb", "2222", epoch1),
            (b"cccc", "3333", epoch1),
            (b"dddd", "4444", epoch1),
            (b"eeee", "5555", epoch1),
        ];

        for (k, v, e) in kv_map {
            let result = iter.try_next().await.unwrap();
            assert_eq!(
                result,
                Some((
                    FullKey::for_test(TEST_TABLE_ID, k.to_vec().into(), e),
                    Bytes::from(v)
                ))
            );
        }

        assert!(iter.try_next().await.unwrap().is_none());
    }

    {
        let iter = test_env
            .storage
            .iter(
                (Unbounded, Included(Bytes::from("eeee"))),
                epoch2,
                ReadOptions {
                    ignore_range_tombstone: false,
                    table_id: TEST_TABLE_ID,
                    retention_seconds: None,

                    prefix_hint: None,
                    read_version_from_backup: false,
                    prefetch_options: Default::default(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                },
            )
            .await
            .unwrap();

        futures::pin_mut!(iter);

        let kv_map = [
            (b"aaaa", "1111", epoch1),
            (b"bbbb", "2222", epoch1),
            (b"cccc", "3333", epoch1),
            (b"dddd", "4444", epoch1),
            (b"eeee", "6666", epoch2),
        ];

        for (k, v, e) in kv_map {
            let result = iter.try_next().await.unwrap();
            assert_eq!(
                result,
                Some((
                    FullKey::for_test(TEST_TABLE_ID, k.to_vec().into(), e),
                    Bytes::from(v)
                ))
            );
        }
    }
}

#[tokio::test]
async fn test_delete_get() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;
    test_env.register_table_id(TEST_TABLE_ID).await;
    let mut hummock_storage = test_env
        .storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;

    let initial_epoch = hummock_storage
        .read_version()
        .read()
        .committed()
        .max_committed_epoch();

    let epoch1 = initial_epoch + 1;

    hummock_storage.init(epoch1);
    let batch1 = vec![
        (Bytes::from("aa"), StorageValue::new_put("111")),
        (Bytes::from("bb"), StorageValue::new_put("222")),
    ];
    hummock_storage
        .ingest_batch(
            batch1,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    let res = test_env.storage.seal_and_sync_epoch(epoch1).await.unwrap();
    test_env
        .meta_client
        .commit_epoch(epoch1, res.uncommitted_ssts)
        .await
        .unwrap();
    let epoch2 = initial_epoch + 2;
    hummock_storage.seal_current_epoch(epoch2);
    let batch2 = vec![(Bytes::from("bb"), StorageValue::new_delete())];
    hummock_storage
        .ingest_batch(
            batch2,
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();
    let res = test_env.storage.seal_and_sync_epoch(epoch2).await.unwrap();
    test_env
        .meta_client
        .commit_epoch(epoch2, res.uncommitted_ssts)
        .await
        .unwrap();
    test_env.storage.try_wait_epoch_for_test(epoch2).await;
    assert!(test_env
        .storage
        .get(
            Bytes::from("bb"),
            epoch2,
            ReadOptions {
                ignore_range_tombstone: false,
                prefix_hint: None,

                table_id: TEST_TABLE_ID,
                retention_seconds: None,
                read_version_from_backup: false,
                prefetch_options: Default::default(),
                cache_policy: CachePolicy::Fill(CachePriority::High),
            }
        )
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn test_multiple_epoch_sync() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;
    test_env.register_table_id(TEST_TABLE_ID).await;
    let mut hummock_storage = test_env
        .storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;

    let initial_epoch = hummock_storage
        .read_version()
        .read()
        .committed()
        .max_committed_epoch();

    let epoch1 = initial_epoch + 1;
    hummock_storage.init(epoch1);
    let batch1 = vec![
        (Bytes::from("aa"), StorageValue::new_put("111")),
        (Bytes::from("bb"), StorageValue::new_put("222")),
    ];
    hummock_storage
        .ingest_batch(
            batch1,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    let epoch2 = initial_epoch + 2;
    hummock_storage.seal_current_epoch(epoch2);
    let batch2 = vec![(Bytes::from("bb"), StorageValue::new_delete())];
    hummock_storage
        .ingest_batch(
            batch2,
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    let epoch3 = initial_epoch + 3;
    hummock_storage.seal_current_epoch(epoch3);
    let batch3 = vec![
        (Bytes::from("aa"), StorageValue::new_put("444")),
        (Bytes::from("bb"), StorageValue::new_put("555")),
    ];
    hummock_storage
        .ingest_batch(
            batch3,
            vec![],
            WriteOptions {
                epoch: epoch3,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();
    let test_get = || {
        let hummock_storage_clone = &test_env.storage;
        async move {
            assert_eq!(
                hummock_storage_clone
                    .get(
                        Bytes::from("bb"),
                        epoch1,
                        ReadOptions {
                            ignore_range_tombstone: false,
                            table_id: TEST_TABLE_ID,
                            retention_seconds: None,

                            prefix_hint: None,
                            read_version_from_backup: false,
                            prefetch_options: Default::default(),
                            cache_policy: CachePolicy::Fill(CachePriority::High),
                        },
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
                        table_id: TEST_TABLE_ID,
                        retention_seconds: None,

                        prefix_hint: None,
                        read_version_from_backup: false,
                        prefetch_options: Default::default(),
                        cache_policy: CachePolicy::Fill(CachePriority::High),
                    },
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
                            table_id: TEST_TABLE_ID,
                            retention_seconds: None,

                            prefix_hint: None,
                            read_version_from_backup: false,
                            prefetch_options: Default::default(),
                            cache_policy: CachePolicy::Fill(CachePriority::High),
                        },
                    )
                    .await
                    .unwrap()
                    .unwrap(),
                "555".as_bytes()
            );
        }
    };
    test_get().await;
    test_env.storage.seal_epoch(epoch1, false);
    let sync_result2 = test_env.storage.seal_and_sync_epoch(epoch2).await.unwrap();
    let sync_result3 = test_env.storage.seal_and_sync_epoch(epoch3).await.unwrap();

    test_get().await;
    test_env
        .meta_client
        .commit_epoch(epoch2, sync_result2.uncommitted_ssts)
        .await
        .unwrap();
    test_env
        .meta_client
        .commit_epoch(epoch3, sync_result3.uncommitted_ssts)
        .await
        .unwrap();
    test_env.storage.try_wait_epoch_for_test(epoch3).await;
    test_get().await;
}

#[tokio::test]
async fn test_iter_with_min_epoch() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;
    test_env.register_table_id(TEST_TABLE_ID).await;
    let mut hummock_storage = test_env
        .storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;

    let epoch1 = (31 * 1000) << 16;

    let gen_key = |index: usize| -> String { format!("key_{}", index) };

    let gen_val = |index: usize| -> String { format!("val_{}", index) };

    // epoch 1 write
    let batch_epoch1: Vec<(Bytes, StorageValue)> = (0..10)
        .map(|index| {
            (
                Bytes::from(gen_key(index)),
                StorageValue::new_put(gen_val(index)),
            )
        })
        .collect();

    hummock_storage.init(epoch1);

    hummock_storage
        .ingest_batch(
            batch_epoch1,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    let epoch2 = (32 * 1000) << 16;
    hummock_storage.seal_current_epoch(epoch2);
    // epoch 2 write
    let batch_epoch2: Vec<(Bytes, StorageValue)> = (20..30)
        .map(|index| {
            (
                Bytes::from(gen_key(index)),
                StorageValue::new_put(gen_val(index)),
            )
        })
        .collect();

    hummock_storage
        .ingest_batch(
            batch_epoch2,
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    {
        // test before sync
        {
            let iter = test_env
                .storage
                .iter(
                    (Unbounded, Unbounded),
                    epoch1,
                    ReadOptions {
                        ignore_range_tombstone: false,
                        table_id: TEST_TABLE_ID,
                        retention_seconds: None,
                        prefix_hint: None,
                        read_version_from_backup: false,
                        prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                        cache_policy: CachePolicy::Fill(CachePriority::High),
                    },
                )
                .await
                .unwrap();

            futures::pin_mut!(iter);

            let result: Vec<_> = iter.try_collect().await.unwrap();
            assert_eq!(10, result.len());
        }

        {
            let iter = test_env
                .storage
                .iter(
                    (Unbounded, Unbounded),
                    epoch2,
                    ReadOptions {
                        ignore_range_tombstone: false,
                        table_id: TEST_TABLE_ID,
                        retention_seconds: None,
                        prefix_hint: None,
                        read_version_from_backup: false,
                        prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                        cache_policy: CachePolicy::Fill(CachePriority::High),
                    },
                )
                .await
                .unwrap();

            let result: Vec<_> = iter.try_collect().await.unwrap();
            assert_eq!(20, result.len());
        }

        {
            let iter = test_env
                .storage
                .iter(
                    (Unbounded, Unbounded),
                    epoch2,
                    ReadOptions {
                        ignore_range_tombstone: false,
                        table_id: TEST_TABLE_ID,
                        retention_seconds: Some(1),
                        prefix_hint: None,
                        read_version_from_backup: false,
                        prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                        cache_policy: CachePolicy::Fill(CachePriority::High),
                    },
                )
                .await
                .unwrap();

            futures::pin_mut!(iter);

            let result: Vec<_> = iter.try_collect().await.unwrap();
            assert_eq!(10, result.len());
        }
    }

    {
        // test after sync

        let sync_result1 = test_env.storage.seal_and_sync_epoch(epoch1).await.unwrap();
        let sync_result2 = test_env.storage.seal_and_sync_epoch(epoch2).await.unwrap();
        test_env
            .meta_client
            .commit_epoch(epoch1, sync_result1.uncommitted_ssts)
            .await
            .unwrap();
        test_env
            .meta_client
            .commit_epoch(epoch2, sync_result2.uncommitted_ssts)
            .await
            .unwrap();
        test_env.storage.try_wait_epoch_for_test(epoch2).await;

        {
            let iter = test_env
                .storage
                .iter(
                    (Unbounded, Unbounded),
                    epoch1,
                    ReadOptions {
                        ignore_range_tombstone: false,
                        table_id: TEST_TABLE_ID,
                        retention_seconds: None,
                        prefix_hint: None,
                        read_version_from_backup: false,
                        prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                        cache_policy: CachePolicy::Fill(CachePriority::High),
                    },
                )
                .await
                .unwrap();

            futures::pin_mut!(iter);

            let result: Vec<_> = iter.try_collect().await.unwrap();
            assert_eq!(10, result.len());
        }

        {
            let iter = test_env
                .storage
                .iter(
                    (Unbounded, Unbounded),
                    epoch2,
                    ReadOptions {
                        ignore_range_tombstone: false,
                        table_id: TEST_TABLE_ID,
                        retention_seconds: None,
                        prefix_hint: None,
                        read_version_from_backup: false,
                        prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                        cache_policy: CachePolicy::Fill(CachePriority::High),
                    },
                )
                .await
                .unwrap();

            futures::pin_mut!(iter);

            let result: Vec<_> = iter.try_collect().await.unwrap();
            assert_eq!(20, result.len());
        }

        {
            let iter = test_env
                .storage
                .iter(
                    (Unbounded, Unbounded),
                    epoch2,
                    ReadOptions {
                        ignore_range_tombstone: false,
                        table_id: TEST_TABLE_ID,
                        retention_seconds: Some(1),
                        prefix_hint: None,
                        read_version_from_backup: false,
                        prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                        cache_policy: CachePolicy::Fill(CachePriority::High),
                    },
                )
                .await
                .unwrap();

            futures::pin_mut!(iter);

            let result: Vec<_> = iter.try_collect().await.unwrap();
            assert_eq!(10, result.len());
        }
    }
}

#[tokio::test]
async fn test_hummock_version_reader() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;
    test_env.register_table_id(TEST_TABLE_ID).await;
    let mut hummock_storage = test_env
        .storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;
    let hummock_version_reader = test_env.storage.version_reader();

    let epoch1 = (31 * 1000) << 16;

    let gen_key = |index: usize| -> String { format!("key_{}", index) };

    let gen_val = |index: usize| -> String { format!("val_{}", index) };

    // epoch 1 write
    let batch_epoch1: Vec<(Bytes, StorageValue)> = (0..10)
        .map(|index| {
            (
                Bytes::from(gen_key(index)),
                StorageValue::new_put(gen_val(index)),
            )
        })
        .collect();

    let epoch2 = (32 * 1000) << 16;
    // epoch 2 write
    let batch_epoch2: Vec<(Bytes, StorageValue)> = (20..30)
        .map(|index| {
            (
                Bytes::from(gen_key(index)),
                StorageValue::new_put(gen_val(index)),
            )
        })
        .collect();

    let epoch3 = (33 * 1000) << 16;
    // epoch 3 write
    let batch_epoch3: Vec<(Bytes, StorageValue)> = (40..50)
        .map(|index| {
            (
                Bytes::from(gen_key(index)),
                StorageValue::new_put(gen_val(index)),
            )
        })
        .collect();
    {
        hummock_storage.init(epoch1);
        hummock_storage
            .ingest_batch(
                batch_epoch1,
                vec![],
                WriteOptions {
                    epoch: epoch1,
                    table_id: TEST_TABLE_ID,
                },
            )
            .await
            .unwrap();

        hummock_storage.seal_current_epoch(epoch2);
        hummock_storage
            .ingest_batch(
                batch_epoch2,
                vec![],
                WriteOptions {
                    epoch: epoch2,
                    table_id: TEST_TABLE_ID,
                },
            )
            .await
            .unwrap();

        hummock_storage.seal_current_epoch(epoch3);
        hummock_storage
            .ingest_batch(
                batch_epoch3,
                vec![],
                WriteOptions {
                    epoch: epoch3,
                    table_id: TEST_TABLE_ID,
                },
            )
            .await
            .unwrap();

        {
            // test before sync
            {
                let read_snapshot = read_filter_for_local(
                    epoch1,
                    TEST_TABLE_ID,
                    &(Unbounded, Unbounded),
                    hummock_storage.read_version(),
                )
                .unwrap();

                let iter = hummock_version_reader
                    .iter(
                        (Unbounded, Unbounded),
                        epoch1,
                        ReadOptions {
                            ignore_range_tombstone: false,
                            table_id: TEST_TABLE_ID,
                            retention_seconds: None,
                            prefix_hint: None,
                            read_version_from_backup: false,
                            prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                            cache_policy: CachePolicy::Fill(CachePriority::High),
                        },
                        read_snapshot,
                    )
                    .await
                    .unwrap();

                let result: Vec<_> = iter.try_collect().await.unwrap();
                assert_eq!(10, result.len());
            }

            {
                let read_snapshot = read_filter_for_local(
                    epoch2,
                    TEST_TABLE_ID,
                    &(Unbounded, Unbounded),
                    hummock_storage.read_version(),
                )
                .unwrap();

                let iter = hummock_version_reader
                    .iter(
                        (Unbounded, Unbounded),
                        epoch2,
                        ReadOptions {
                            ignore_range_tombstone: false,
                            table_id: TEST_TABLE_ID,
                            retention_seconds: None,
                            prefix_hint: None,
                            read_version_from_backup: false,
                            prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                            cache_policy: CachePolicy::Fill(CachePriority::High),
                        },
                        read_snapshot,
                    )
                    .await
                    .unwrap();

                let result: Vec<_> = iter.try_collect().await.unwrap();
                assert_eq!(20, result.len());
            }

            {
                let read_snapshot = read_filter_for_local(
                    epoch2,
                    TEST_TABLE_ID,
                    &(Unbounded, Unbounded),
                    hummock_storage.read_version(),
                )
                .unwrap();

                let iter = hummock_version_reader
                    .iter(
                        (Unbounded, Unbounded),
                        epoch2,
                        ReadOptions {
                            ignore_range_tombstone: false,
                            table_id: TEST_TABLE_ID,
                            retention_seconds: Some(1),
                            prefix_hint: None,
                            read_version_from_backup: false,
                            prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                            cache_policy: CachePolicy::Fill(CachePriority::High),
                        },
                        read_snapshot,
                    )
                    .await
                    .unwrap();

                let result: Vec<_> = iter.try_collect().await.unwrap();
                assert_eq!(10, result.len());
            }
        }

        {
            let basic_read_version =
                Arc::new(RwLock::new(hummock_storage.read_version().read().clone()));

            let sync_result1 = test_env.storage.seal_and_sync_epoch(epoch1).await.unwrap();
            test_env
                .meta_client
                .commit_epoch(epoch1, sync_result1.uncommitted_ssts)
                .await
                .unwrap();
            test_env.storage.try_wait_epoch_for_test(epoch1).await;

            let sync_result2 = test_env.storage.seal_and_sync_epoch(epoch2).await.unwrap();
            test_env
                .meta_client
                .commit_epoch(epoch2, sync_result2.uncommitted_ssts)
                .await
                .unwrap();
            test_env.storage.try_wait_epoch_for_test(epoch2).await;
            let read_version_2 =
                Arc::new(RwLock::new(hummock_storage.read_version().read().clone()));

            let sync_result3 = test_env.storage.seal_and_sync_epoch(epoch3).await.unwrap();
            test_env
                .meta_client
                .commit_epoch(epoch3, sync_result3.uncommitted_ssts)
                .await
                .unwrap();
            test_env.storage.try_wait_epoch_for_test(epoch3).await;
            let read_version_3 =
                Arc::new(RwLock::new(hummock_storage.read_version().read().clone()));

            {
                let read_snapshot = read_filter_for_batch(
                    epoch1,
                    TEST_TABLE_ID,
                    &(Unbounded, Unbounded),
                    vec![
                        basic_read_version.clone(),
                        read_version_2.clone(),
                        read_version_3.clone(),
                    ],
                )
                .unwrap();

                assert_eq!(
                    read_version_3.read().committed().max_committed_epoch(),
                    read_snapshot.2.max_committed_epoch()
                );

                let iter = hummock_version_reader
                    .iter(
                        (Unbounded, Unbounded),
                        epoch1,
                        ReadOptions {
                            ignore_range_tombstone: false,
                            table_id: TEST_TABLE_ID,
                            retention_seconds: None,
                            prefix_hint: None,
                            read_version_from_backup: false,
                            prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                            cache_policy: CachePolicy::Fill(CachePriority::High),
                        },
                        read_snapshot,
                    )
                    .await
                    .unwrap();

                let result: Vec<_> = iter.try_collect().await.unwrap();
                assert_eq!(10, result.len());
            }

            {
                let read_snapshot = read_filter_for_batch(
                    epoch2,
                    TEST_TABLE_ID,
                    &(Unbounded, Unbounded),
                    vec![
                        basic_read_version.clone(),
                        read_version_2.clone(),
                        read_version_3.clone(),
                    ],
                )
                .unwrap();

                assert_eq!(
                    read_version_3.read().committed().max_committed_epoch(),
                    read_snapshot.2.max_committed_epoch()
                );

                let iter = hummock_version_reader
                    .iter(
                        (Unbounded, Unbounded),
                        epoch2,
                        ReadOptions {
                            ignore_range_tombstone: false,
                            table_id: TEST_TABLE_ID,
                            retention_seconds: None,
                            prefix_hint: None,
                            read_version_from_backup: false,
                            prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                            cache_policy: CachePolicy::Fill(CachePriority::High),
                        },
                        read_snapshot,
                    )
                    .await
                    .unwrap();

                let result: Vec<_> = iter.try_collect().await.unwrap();
                assert_eq!(20, result.len());
            }

            {
                let read_snapshot = read_filter_for_batch(
                    epoch2,
                    TEST_TABLE_ID,
                    &(Unbounded, Unbounded),
                    vec![
                        basic_read_version.clone(),
                        read_version_2.clone(),
                        read_version_3.clone(),
                    ],
                )
                .unwrap();

                assert_eq!(
                    read_version_3.read().committed().max_committed_epoch(),
                    read_snapshot.2.max_committed_epoch()
                );

                let iter = hummock_version_reader
                    .iter(
                        (Unbounded, Unbounded),
                        epoch2,
                        ReadOptions {
                            ignore_range_tombstone: false,
                            table_id: TEST_TABLE_ID,
                            retention_seconds: Some(1),
                            prefix_hint: None,
                            read_version_from_backup: false,
                            prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                            cache_policy: CachePolicy::Fill(CachePriority::High),
                        },
                        read_snapshot,
                    )
                    .await
                    .unwrap();

                let result: Vec<_> = iter.try_collect().await.unwrap();
                assert_eq!(10, result.len());
            }

            {
                let read_snapshot = read_filter_for_batch(
                    epoch3,
                    TEST_TABLE_ID,
                    &(Unbounded, Unbounded),
                    vec![
                        basic_read_version.clone(),
                        read_version_2.clone(),
                        read_version_3.clone(),
                    ],
                )
                .unwrap();

                assert_eq!(
                    read_version_3.read().committed().max_committed_epoch(),
                    read_snapshot.2.max_committed_epoch()
                );

                let iter = hummock_version_reader
                    .iter(
                        (Unbounded, Unbounded),
                        epoch3,
                        ReadOptions {
                            ignore_range_tombstone: false,
                            table_id: TEST_TABLE_ID,
                            retention_seconds: None,
                            prefix_hint: None,
                            read_version_from_backup: false,
                            prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                            cache_policy: CachePolicy::Fill(CachePriority::High),
                        },
                        read_snapshot,
                    )
                    .await
                    .unwrap();

                let result: Vec<_> = iter.try_collect().await.unwrap();
                assert_eq!(30, result.len());
            }

            {
                let start_key = Bytes::from(gen_key(25));
                let end_key = Bytes::from(gen_key(50));

                let key_range = map_table_key_range((Included(start_key), Excluded(end_key)));

                {
                    let read_snapshot = read_filter_for_batch(
                        epoch2,
                        TEST_TABLE_ID,
                        &key_range,
                        vec![
                            basic_read_version.clone(),
                            read_version_2.clone(),
                            read_version_3.clone(),
                        ],
                    )
                    .unwrap();

                    assert_eq!(
                        read_version_3.read().committed().max_committed_epoch(),
                        read_snapshot.2.max_committed_epoch()
                    );

                    let iter = hummock_version_reader
                        .iter(
                            key_range.clone(),
                            epoch2,
                            ReadOptions {
                                ignore_range_tombstone: false,
                                table_id: TEST_TABLE_ID,
                                retention_seconds: None,
                                prefix_hint: None,
                                read_version_from_backup: false,
                                prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                                cache_policy: CachePolicy::Fill(CachePriority::High),
                            },
                            read_snapshot,
                        )
                        .await
                        .unwrap();

                    let result: Vec<_> = iter.try_collect().await.unwrap();
                    assert_eq!(8, result.len());
                }

                {
                    let read_snapshot = read_filter_for_batch(
                        epoch3,
                        TEST_TABLE_ID,
                        &key_range,
                        vec![
                            basic_read_version.clone(),
                            read_version_2.clone(),
                            read_version_3.clone(),
                        ],
                    )
                    .unwrap();

                    assert_eq!(
                        read_version_3.read().committed().max_committed_epoch(),
                        read_snapshot.2.max_committed_epoch()
                    );

                    let iter = hummock_version_reader
                        .iter(
                            key_range.clone(),
                            epoch3,
                            ReadOptions {
                                ignore_range_tombstone: false,
                                table_id: TEST_TABLE_ID,
                                retention_seconds: None,
                                prefix_hint: None,
                                read_version_from_backup: false,
                                prefetch_options: PrefetchOptions::new_for_exhaust_iter(),
                                cache_policy: CachePolicy::Fill(CachePriority::High),
                            },
                            read_snapshot,
                        )
                        .await
                        .unwrap();

                    let result: Vec<_> = iter.try_collect().await.unwrap();
                    assert_eq!(18, result.len());
                }
            }
        }
    }
}

#[tokio::test]
async fn test_get_with_min_epoch() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;
    test_env.register_table_id(TEST_TABLE_ID).await;
    let mut hummock_storage = test_env
        .storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;

    let epoch1 = (31 * 1000) << 16;
    hummock_storage.init(epoch1);

    let gen_key = |index: usize| -> Vec<u8> {
        UserKey::for_test(TEST_TABLE_ID, format!("key_{}", index)).encode()
    };

    let gen_val = |index: usize| -> String { format!("val_{}", index) };

    // epoch 1 write
    let batch_epoch1: Vec<(Bytes, StorageValue)> = (0..10)
        .map(|index| {
            (
                Bytes::from(gen_key(index)),
                StorageValue::new_put(gen_val(index)),
            )
        })
        .collect();

    hummock_storage
        .ingest_batch(
            batch_epoch1,
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    let epoch2 = (32 * 1000) << 16;
    hummock_storage.seal_current_epoch(epoch2);
    // epoch 2 write
    let batch_epoch2: Vec<(Bytes, StorageValue)> = (20..30)
        .map(|index| {
            (
                Bytes::from(gen_key(index)),
                StorageValue::new_put(gen_val(index)),
            )
        })
        .collect();

    hummock_storage
        .ingest_batch(
            batch_epoch2,
            vec![],
            WriteOptions {
                epoch: epoch2,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    {
        // test before sync
        let k = Bytes::from(gen_key(0));
        let prefix_hint = {
            let mut ret = Vec::with_capacity(TABLE_PREFIX_LEN + k.len());
            ret.put_u32(TEST_TABLE_ID.table_id());
            ret.put_slice(k.as_ref());
            ret
        };
        {
            let v = test_env
                .storage
                .get(
                    k.clone(),
                    epoch1,
                    ReadOptions {
                        ignore_range_tombstone: false,
                        table_id: TEST_TABLE_ID,
                        retention_seconds: None,
                        prefix_hint: None,
                        read_version_from_backup: false,
                        prefetch_options: Default::default(),
                        cache_policy: CachePolicy::Fill(CachePriority::High),
                    },
                )
                .await
                .unwrap();
            assert!(v.is_some());
        }

        {
            let v = test_env
                .storage
                .get(
                    k.clone(),
                    epoch1,
                    ReadOptions {
                        ignore_range_tombstone: false,
                        table_id: TEST_TABLE_ID,
                        retention_seconds: None,
                        prefix_hint: Some(Bytes::from(prefix_hint.clone())),
                        read_version_from_backup: false,
                        prefetch_options: Default::default(),
                        cache_policy: CachePolicy::Fill(CachePriority::High),
                    },
                )
                .await
                .unwrap();
            assert!(v.is_some());
        }

        {
            let v = test_env
                .storage
                .get(
                    k.clone(),
                    epoch2,
                    ReadOptions {
                        ignore_range_tombstone: false,
                        table_id: TEST_TABLE_ID,
                        retention_seconds: None,
                        prefix_hint: Some(Bytes::from(prefix_hint.clone())),
                        read_version_from_backup: false,
                        prefetch_options: Default::default(),
                        cache_policy: CachePolicy::Fill(CachePriority::High),
                    },
                )
                .await
                .unwrap();
            assert!(v.is_some());
        }

        {
            let v = test_env
                .storage
                .get(
                    k.clone(),
                    epoch2,
                    ReadOptions {
                        ignore_range_tombstone: false,
                        table_id: TEST_TABLE_ID,
                        retention_seconds: Some(1),
                        prefix_hint: Some(Bytes::from(prefix_hint.clone())),
                        read_version_from_backup: false,
                        prefetch_options: Default::default(),
                        cache_policy: CachePolicy::Fill(CachePriority::High),
                    },
                )
                .await
                .unwrap();
            assert!(v.is_none());
        }
    }

    // test after sync

    let sync_result1 = test_env.storage.seal_and_sync_epoch(epoch1).await.unwrap();
    let sync_result2 = test_env.storage.seal_and_sync_epoch(epoch2).await.unwrap();
    test_env
        .meta_client
        .commit_epoch(epoch1, sync_result1.uncommitted_ssts)
        .await
        .unwrap();
    test_env
        .meta_client
        .commit_epoch(epoch2, sync_result2.uncommitted_ssts)
        .await
        .unwrap();

    test_env.storage.try_wait_epoch_for_test(epoch2).await;
    let k = Bytes::from(gen_key(0));
    let prefix_hint = {
        let mut ret = Vec::with_capacity(TABLE_PREFIX_LEN + k.len());
        ret.put_u32(TEST_TABLE_ID.table_id());
        ret.put_slice(k.as_ref());
        ret
    };

    {
        let v = test_env
            .storage
            .get(
                k.clone(),
                epoch1,
                ReadOptions {
                    ignore_range_tombstone: false,
                    table_id: TEST_TABLE_ID,
                    retention_seconds: None,

                    prefix_hint: None,
                    read_version_from_backup: false,
                    prefetch_options: Default::default(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                },
            )
            .await
            .unwrap();
        assert!(v.is_some());
    }

    {
        let v = test_env
            .storage
            .get(
                k.clone(),
                epoch1,
                ReadOptions {
                    ignore_range_tombstone: false,
                    table_id: TEST_TABLE_ID,
                    retention_seconds: None,

                    prefix_hint: Some(Bytes::from(prefix_hint.clone())),
                    read_version_from_backup: false,
                    prefetch_options: Default::default(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                },
            )
            .await
            .unwrap();

        assert!(v.is_some());
    }

    {
        let k = Bytes::from(gen_key(0));
        let v = test_env
            .storage
            .get(
                k.clone(),
                epoch2,
                ReadOptions {
                    ignore_range_tombstone: false,
                    table_id: TEST_TABLE_ID,
                    retention_seconds: None,

                    prefix_hint: Some(Bytes::from(prefix_hint.clone())),
                    read_version_from_backup: false,
                    prefetch_options: Default::default(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                },
            )
            .await
            .unwrap();
        assert!(v.is_some());
    }

    {
        let k = Bytes::from(gen_key(0));
        let v = test_env
            .storage
            .get(
                k.clone(),
                epoch2,
                ReadOptions {
                    ignore_range_tombstone: false,
                    table_id: TEST_TABLE_ID,
                    retention_seconds: Some(1),

                    prefix_hint: Some(Bytes::from(prefix_hint.clone())),
                    read_version_from_backup: false,
                    prefetch_options: Default::default(),
                    cache_policy: CachePolicy::Fill(CachePriority::High),
                },
            )
            .await
            .unwrap();
        assert!(v.is_none());
    }
}
