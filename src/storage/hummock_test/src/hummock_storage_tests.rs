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

use std::ops::Bound::{Included, Unbounded};
use std::sync::Arc;

use bytes::Bytes;
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
use risingwave_storage::hummock::store::state_store::HummockStorage;
use risingwave_storage::hummock::store::{ReadOptions, StateStore};
use risingwave_storage::hummock::test_utils::default_config_for_test;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::WriteOptions;
use risingwave_storage::StateStoreIter;

use crate::test_utils::prepare_local_version_manager;

#[tokio::test]
async fn test_storage_basic() {
    let sstable_store = mock_sstable_store();
    let hummock_options = Arc::new(default_config_for_test());
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));

    let uploader = prepare_local_version_manager(
        hummock_options.clone(),
        env,
        hummock_manager_ref,
        worker_node,
    )
    .await;

    let hummock_storage = HummockStorage::for_test(
        hummock_options,
        sstable_store,
        hummock_meta_client.clone(),
        uploader.clone(),
    )
    .unwrap();

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

    // Write the first batch.
    hummock_storage
        .ingest_batch(
            batch1,
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            &Bytes::from("aa"),
            epoch1,
            ReadOptions {
                table_id: Default::default(),
                retention_seconds: None,
                check_bloom_filter: true,
                prefix_hint: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));
    let value = hummock_storage
        .get(
            &Bytes::from("bb"),
            epoch1,
            ReadOptions {
                table_id: Default::default(),
                retention_seconds: None,
                check_bloom_filter: true,
                prefix_hint: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("222"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = hummock_storage
        .get(
            &Bytes::from("ab"),
            epoch1,
            ReadOptions {
                table_id: Default::default(),
                retention_seconds: None,
                check_bloom_filter: true,
                prefix_hint: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    let epoch2 = epoch1 + 1;
    hummock_storage
        .ingest_batch(
            batch2,
            WriteOptions {
                epoch: epoch2,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            &Bytes::from("aa"),
            epoch2,
            ReadOptions {
                table_id: Default::default(),
                retention_seconds: None,
                check_bloom_filter: true,
                prefix_hint: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));

    // Write the third batch.
    let epoch3 = epoch2 + 1;
    hummock_storage
        .ingest_batch(
            batch3,
            WriteOptions {
                epoch: epoch3,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            &Bytes::from("aa"),
            epoch3,
            ReadOptions {
                table_id: Default::default(),
                retention_seconds: None,
                check_bloom_filter: true,
                prefix_hint: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Get non-existent maximum key.
    let value = hummock_storage
        .get(
            &Bytes::from("ff"),
            epoch3,
            ReadOptions {
                table_id: Default::default(),
                retention_seconds: None,
                check_bloom_filter: true,
                prefix_hint: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write aa bb
    let mut iter = hummock_storage
        .iter(
            (Unbounded, Included(b"ee".to_vec())),
            epoch1,
            ReadOptions {
                table_id: Default::default(),
                retention_seconds: None,
                check_bloom_filter: true,
                prefix_hint: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        Some((
            Bytes::copy_from_slice(&b"aa"[..]),
            Bytes::copy_from_slice(&b"111"[..])
        )),
        iter.next().await.unwrap()
    );
    assert_eq!(
        Some((
            Bytes::copy_from_slice(&b"bb"[..]),
            Bytes::copy_from_slice(&b"222"[..])
        )),
        iter.next().await.unwrap()
    );
    assert_eq!(None, iter.next().await.unwrap());

    // Get the anchor value at the first snapshot
    let value = hummock_storage
        .get(
            &Bytes::from("aa"),
            epoch1,
            ReadOptions {
                table_id: Default::default(),
                retention_seconds: None,
                check_bloom_filter: true,
                prefix_hint: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = hummock_storage
        .get(
            &Bytes::from("aa"),
            epoch2,
            ReadOptions {
                table_id: Default::default(),
                retention_seconds: None,
                check_bloom_filter: true,
                prefix_hint: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));
    // Update aa, write cc
    let mut iter = hummock_storage
        .iter(
            (Unbounded, Included(b"ee".to_vec())),
            epoch2,
            ReadOptions {
                table_id: Default::default(),
                retention_seconds: None,
                check_bloom_filter: true,
                prefix_hint: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        Some((
            Bytes::copy_from_slice(&b"aa"[..]),
            Bytes::copy_from_slice(&b"111111"[..])
        )),
        iter.next().await.unwrap()
    );
    assert_eq!(
        Some((
            Bytes::copy_from_slice(&b"bb"[..]),
            Bytes::copy_from_slice(&b"222"[..])
        )),
        iter.next().await.unwrap()
    );
    assert_eq!(
        Some((
            Bytes::copy_from_slice(&b"cc"[..]),
            Bytes::copy_from_slice(&b"333"[..])
        )),
        iter.next().await.unwrap()
    );
    assert_eq!(None, iter.next().await.unwrap());

    // Delete aa, write dd,ee
    let mut iter = hummock_storage
        .iter(
            (Unbounded, Included(b"ee".to_vec())),
            epoch3,
            ReadOptions {
                table_id: Default::default(),
                retention_seconds: None,
                check_bloom_filter: true,
                prefix_hint: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        Some((
            Bytes::copy_from_slice(&b"bb"[..]),
            Bytes::copy_from_slice(&b"222"[..])
        )),
        iter.next().await.unwrap()
    );
    assert_eq!(
        Some((
            Bytes::copy_from_slice(&b"cc"[..]),
            Bytes::copy_from_slice(&b"333"[..])
        )),
        iter.next().await.unwrap()
    );
    assert_eq!(
        Some((
            Bytes::copy_from_slice(&b"dd"[..]),
            Bytes::copy_from_slice(&b"444"[..])
        )),
        iter.next().await.unwrap()
    );
    assert_eq!(
        Some((
            Bytes::copy_from_slice(&b"ee"[..]),
            Bytes::copy_from_slice(&b"555"[..])
        )),
        iter.next().await.unwrap()
    );
    assert_eq!(None, iter.next().await.unwrap());

    // TODO: add more test cases after sync is supported
}
