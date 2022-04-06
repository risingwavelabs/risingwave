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
use risingwave_meta::hummock::test_utils::setup_compute_env;
use super::iterator::UserIterator;
use super::HummockStorage;
use crate::hummock::iterator::test_utils::{
    mock_sstable_store, mock_sstable_store_with_object_store,
};
use crate::hummock::key::key_with_epoch;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::mock::{MockHummockMetaClient, MockHummockMetaService};
use crate::hummock::test_utils::{default_config_for_test, test_value_of, TEST_KEYS_COUNT};
use crate::hummock::value::HummockValue;
use crate::monitor::StateStoreMetrics;
use crate::object::InMemObjectStore;
use crate::storage_value::StorageValue;

#[tokio::test]
async fn test_basic() {
    let object_client = Arc::new(ObjectStoreImpl::Mem(InMemObjectStore::new()));
    let sstable_store = mock_sstable_store_with_object_store(object_client.clone());
    let hummock_options = Arc::new(default_config_for_test());
    let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));
    let local_version_manager = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let hummock_storage = HummockStorage::with_default_stats(
        hummock_options,
        sstable_store,
        local_version_manager,
        meta_client.clone(),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();
    let anchor = Bytes::from("aa");

    // First batch inserts the anchor and others.
    let mut batch1 = vec![
        (anchor.clone(), StorageValue::new_default_put("111")),
        (Bytes::from("bb"), StorageValue::new_default_put("222")),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
    let mut batch2 = vec![
        (Bytes::from("cc"), StorageValue::new_default_put("333")),
        (anchor.clone(), StorageValue::new_default_put("111111")),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Third batch deletes the anchor
    let mut batch3 = vec![
        (Bytes::from("dd"), StorageValue::new_default_put("444")),
        (Bytes::from("ee"), StorageValue::new_default_put("555")),
        (anchor.clone(), StorageValue::new_default_delete()),
    ];

    // Make sure the batch is sorted.
    batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // epoch 0 is reserved by storage service
    let epoch1: u64 = 1;

    // Write the first batch.
    hummock_storage.ingest_batch(batch1, epoch1).await.unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111"));
    let value = hummock_storage
        .get(&Bytes::from("bb"), epoch1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("222"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = hummock_storage
        .get(&Bytes::from("ab"), epoch1)
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write the second batch.
    let epoch2 = epoch1 + 1;
    hummock_storage.ingest_batch(batch2, epoch2).await.unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch2).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111111"));

    // Write the third batch.
    let epoch3 = epoch2 + 1;
    hummock_storage.ingest_batch(batch3, epoch3).await.unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch3).await.unwrap();
    assert_eq!(value, None);

    // Get non-existent maximum key.
    let value = hummock_storage
        .get(&Bytes::from("ff"), epoch3)
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write aa bb
    let mut iter = hummock_storage
        .iter(..=b"ee".to_vec(), epoch1)
        .await
        .unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 2);

    // Get the anchor value at the first snapshot
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = hummock_storage.get(&anchor, epoch2).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111111"));
    // Update aa, write cc
    let mut iter = hummock_storage
        .iter(..=b"ee".to_vec(), epoch2)
        .await
        .unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 3);

    // Delete aa, write dd,ee
    let mut iter = hummock_storage
        .iter(..=b"ee".to_vec(), epoch3)
        .await
        .unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 4);
    hummock_storage.sync(Some(epoch1)).await.unwrap();
    meta_client.commit_epoch(epoch1).await.unwrap();
    hummock_storage.wait_epoch(epoch1).await.unwrap();
    let value = hummock_storage
        .get(&Bytes::from("bb"), epoch2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("222"));
    let value = hummock_storage
        .get(&Bytes::from("dd"), epoch2)
        .await
        .unwrap();
    assert!(value.is_none());
}

async fn count_iter(iter: &mut HummockStateStoreIter<'_>) -> usize {
    let mut c: usize = 0;
    while iter.next().await.unwrap().is_some() {
        c += 1
    }
    c
}

#[tokio::test]
async fn test_failpoint_read_upload() {
    let mem_upload_err = "mem_upload_err";
    let mem_read_err = "mem_read_err";
    let sstable_store = mock_sstable_store();
    let hummock_options = Arc::new(default_config_for_test());

    let local_version_manager = Arc::new(LocalVersionManager::new(sstable_store.clone()));

    let hummock_storage = HummockStorage::with_default_stats(
        hummock_options,
        sstable_store,
        local_version_manager,
        Arc::new(MockHummockMetaClient::new(Arc::new(
            MockHummockMetaService::new(),
        ))),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();

    let anchor = Bytes::from("aa");
    let mut batch1 = vec![
        (anchor.clone(), Some(StorageValue::from(Bytes::from("111")))),
        (
            Bytes::from("bb"),
            Some(StorageValue::from(Bytes::from("111"))),
        ),
    ];
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    let mut batch2 = vec![
        (Bytes::from("cc"), Some(Bytes::from("333"))),
        (anchor.clone(), Some(Bytes::from("111111"))),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Third batch deletes the anchor
    let mut batch3 = vec![
        (Bytes::from("dd"), Some(Bytes::from("444"))),
        (Bytes::from("ee"), Some(Bytes::from("555"))),
        (anchor.clone(), None),
    ];

    // Make sure the batch is sorted.
    batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    let epoch1: u64 = 1;

    hummock_storage
        .write_batch(batch1.into_iter().map(|(k, v)| (k, v.into())), epoch1)
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111"));
    // // Write second batch.
    let epoch2 = epoch1 + 1;
    hummock_storage
        .write_batch(batch2.into_iter().map(|(k, v)| (k, v.into())), epoch2)
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch2).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111111"));

    // Write third batch.
    let epoch3 = epoch2 + 1;
    hummock_storage
        .write_batch(batch3.into_iter().map(|(k, v)| (k, v.into())), epoch3)
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch3).await.unwrap();
    assert_eq!(value, None);

    // Injection failure to upload object_store
    fail::cfg(mem_upload_err, "return").unwrap();
    // compact the sst(epoch1) from shared_buffer to object_store, return err because injection
    // failure delete sst(epoch1) from buffer_manager
    let result = hummock_storage.sync(Some(epoch1)).await;
    assert!(result.is_err());
    hummock_storage.shared_buffer_manager.delete_before(epoch1);

    // get anchor(epoch1) from object_store, return Ok(None)
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap();
    assert_eq!(value, None);
    // close injection failure(upload object_store)
    fail::remove(mem_upload_err);

    // aa cc (object_store no bb)
    let mut iter = hummock_storage
        .range_scan(..=b"ee".to_vec(), epoch2)
        .await
        .unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 2);

    // cc dd ee(object_store no bb , aa=>none)
    let mut iter = hummock_storage
        .range_scan(..=b"ee".to_vec(), epoch3)
        .await
        .unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 3);

    // sync success and delete shared_buffer
    hummock_storage.sync(Some(epoch2)).await.unwrap();
    hummock_storage.sync(Some(epoch3)).await.unwrap();
    hummock_storage.shared_buffer_manager.delete_before(epoch3);

    // Injection failure to read object_store
    fail::cfg(mem_read_err, "return").unwrap();

    // return Err(read_err)
    let result = hummock_storage.get(&anchor, epoch2).await;
    assert!(result.is_err());
    let result = hummock_storage.range_scan(..=b"ee".to_vec(), epoch3).await;
    assert!(result.is_err());
    fail::remove(mem_read_err);

    // close injection failure(read) , back to normal
    let value = hummock_storage.get(&anchor, epoch4).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111111"));
    let value = hummock_storage.get(&anchor, epoch5).await.unwrap();
    assert_eq!(value, None);

    let mut iter = hummock_storage
        .range_scan(..=b"ee".to_vec(), epoch3)
        .await
        .unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 3);
}
#[tokio::test]
async fn test_failpoint_buffer_drop() {
    let mem_read_err = "mem_read_err";
    let object_client = Arc::new(InMemObjectStore::new());
    let sstable_store = mock_sstable_store_with_object_store(object_client.clone());
    let hummock_options = Arc::new(default_config_for_test());

    let local_version_manager = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let hummock_storage = HummockStorage::with_default_stats(
        hummock_options,
        sstable_store,
        local_version_manager,
        Arc::new(MockHummockMetaClient::new(Arc::new(
            MockHummockMetaService::new(),
        ))),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();
    let anchor = Bytes::from("aa");
    // Write first batch.
    let batch1 = (0..TEST_KEYS_COUNT).map(|i| {
        (
            Bytes::from(key_with_epoch(
                format!("key_test_{:05}", i).as_bytes().to_vec(),
                0,
            )),
            HummockValue::Put(Bytes::from(test_value_of(i))),
        )
    });
    // Write second batch.
    let mut batch2 = vec![
        (Bytes::from("cc"), Some(Bytes::from("333"))),
        (anchor.clone(), Some(Bytes::from("111111"))),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // epoch 1 is reserved by storage service
    let epoch1: u64 = 1;

    // Write first batch.
    hummock_storage.write_batch(batch1, epoch1).await.unwrap();
    let epoch2 = epoch1 + 1;
    hummock_storage
        .write_batch(batch2.into_iter().map(|(k, v)| (k, v.into())), epoch2)
        .await
        .unwrap();
    fail::cfg(mem_read_err, "return").unwrap();
    // get an iter before commit
    let mut iter = hummock_storage
        .range_scan(..=b"key_test_10001".to_vec(), epoch1)
        .await
        .unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, TEST_KEYS_COUNT);
    let test1 = key_with_epoch(format!("key_test_{:05}", 100).as_bytes().to_vec(), 0);
    hummock_storage
        .shared_buffer_manager()
        .get(&test1, 0..2)
        .unwrap();
    // commit epoch1
    hummock_storage
        .hummock_meta_client()
        .commit_epoch(epoch1)
        .await
        .unwrap();
    hummock_storage.sync(Some(epoch1)).await.unwrap();

    assert_eq!(
        hummock_storage
            .local_version_manager()
            .get_version()
            .unwrap()
            .max_committed_epoch(),
        epoch1
    );
    // commit epoch2
    hummock_storage
        .hummock_meta_client()
        .commit_epoch(epoch2)
        .await
        .unwrap();
    hummock_storage.sync(Some(epoch2)).await.unwrap();

    assert_eq!(
        hummock_storage
            .local_version_manager()
            .get_version()
            .unwrap()
            .max_committed_epoch(),
        epoch2
    );
    // epoch1 in buffer?
    hummock_storage
        .shared_buffer_manager()
        .get(&test1, 0..2)
        .unwrap();
    // iter is_valid?
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, TEST_KEYS_COUNT);
    fail::remove(mem_read_err);
}
#[tokio::test]
/// Fix this when we finished epoch management.
#[ignore]
async fn test_reload_storage() {
    let object_store = Arc::new(ObjectStoreImpl::Mem(InMemObjectStore::new()));
    let sstable_store = mock_sstable_store_with_object_store(object_store.clone());
    let hummock_options = Arc::new(default_config_for_test());
    let local_version_manager = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));

    let hummock_storage = HummockStorage::with_default_stats(
        hummock_options,
        sstable_store.clone(),
        local_version_manager.clone(),
        hummock_meta_client.clone(),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();
    let anchor = Bytes::from("aa");

    // First batch inserts the anchor and others.
    let mut batch1 = vec![
        (anchor.clone(), StorageValue::new_default_put("111")),
        (Bytes::from("bb"), StorageValue::new_default_put("222")),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
    let mut batch2 = vec![
        (Bytes::from("cc"), StorageValue::new_default_put("333")),
        (anchor.clone(), StorageValue::new_default_put("111111")),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // epoch 0 is reserved by storage service
    let epoch1: u64 = 1;

    // Write the first batch.
    hummock_storage.ingest_batch(batch1, epoch1).await.unwrap();

    // Mock something happened to storage internal, and storage is reloaded.
    drop(hummock_storage);
    let hummock_storage = HummockStorage::with_default_stats(
        Arc::new(default_config_for_test()),
        sstable_store,
        local_version_manager,
        hummock_meta_client,
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = hummock_storage
        .get(&Bytes::from("ab"), epoch1)
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write the second batch.
    let epoch2 = epoch1 + 1;
    hummock_storage.ingest_batch(batch2, epoch2).await.unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch2).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111111"));

    // Write aa bb
    let mut iter = hummock_storage
        .iter(..=b"ee".to_vec(), epoch1)
        .await
        .unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 2);

    // Get the anchor value at the first snapshot
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = hummock_storage.get(&anchor, epoch2).await.unwrap().unwrap();
    assert_eq!(value, Bytes::from("111111"));
    // Update aa, write cc
    let mut iter = hummock_storage
        .iter(..=b"ee".to_vec(), epoch2)
        .await
        .unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 3);
}
