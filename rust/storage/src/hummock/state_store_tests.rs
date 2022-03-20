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
//
use std::sync::Arc;

use bytes::Bytes;

use super::iterator::UserIterator;
use super::HummockStorage;
use crate::hummock::iterator::test_utils::mock_sstable_store_with_object_store;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::mock::{MockHummockMetaClient, MockHummockMetaService};
use crate::hummock::test_utils::default_config_for_test;
use crate::monitor::StateStoreMetrics;
use crate::object::InMemObjectStore;

#[tokio::test]
async fn test_basic() {
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

    // First batch inserts the anchor and others.
    let mut batch1 = vec![
        (anchor.clone(), Some(Bytes::from("111"))),
        (Bytes::from("bb"), Some(Bytes::from("222"))),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
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

    // epoch 0 is reserved by storage service
    let epoch1: u64 = 1;

    // Write first batch.
    hummock_storage
        .write_batch(batch1.into_iter().map(|(k, v)| (k, v.into())), epoch1)
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = hummock_storage
        .get(&Bytes::from("ab"), epoch1)
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write second batch.
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

    // Get non-existent maximum key.
    let value = hummock_storage
        .get(&Bytes::from("ff"), epoch3)
        .await
        .unwrap();
    assert_eq!(value, None);

    // write aa bb
    let mut iter = hummock_storage
        .range_scan(..=b"ee".to_vec(), epoch1)
        .await
        .unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 2);

    // Get the anchor value at the first snapshot
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = hummock_storage.get(&anchor, epoch2).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111111"));
    // update aa, write cc
    let mut iter = hummock_storage
        .range_scan(..=b"ee".to_vec(), epoch2)
        .await
        .unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 3);

    // delete aa, write dd,ee
    let mut iter = hummock_storage
        .range_scan(..=b"ee".to_vec(), epoch3)
        .await
        .unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 4);
}

async fn count_iter(iter: &mut UserIterator<'_>) -> usize {
    let mut c: usize = 0;
    while iter.is_valid() {
        c += 1;
        iter.next().await.unwrap();
    }
    c
}

#[tokio::test]
/// Fix this when we finished epoch management.
#[ignore]
async fn test_reload_storage() {
    let object_store = Arc::new(InMemObjectStore::new());
    let sstable_store = mock_sstable_store_with_object_store(object_store.clone());
    let hummock_options = Arc::new(default_config_for_test());
    let local_version_manager = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let hummock_meta_client = Arc::new(MockHummockMetaClient::new(Arc::new(
        MockHummockMetaService::new(),
    )));

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
        (anchor.clone(), Some(Bytes::from("111"))),
        (Bytes::from("bb"), Some(Bytes::from("222"))),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
    let mut batch2 = vec![
        (Bytes::from("cc"), Some(Bytes::from("333"))),
        (anchor.clone(), Some(Bytes::from("111111"))),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // epoch 0 is reserved by storage service
    let epoch1: u64 = 1;

    // Write first batch.
    hummock_storage
        .write_batch(batch1.into_iter().map(|(k, v)| (k, v.into())), epoch1)
        .await
        .unwrap();

    // Mock somthing happened to storage internal, and storage is reloaded.
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
    assert_eq!(Bytes::from(value), Bytes::from("111"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = hummock_storage
        .get(&Bytes::from("ab"), epoch1)
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write second batch.
    let epoch2 = epoch1 + 1;
    hummock_storage
        .write_batch(batch2.into_iter().map(|(k, v)| (k, v.into())), epoch2)
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage.get(&anchor, epoch2).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111111"));

    // write aa bb
    let mut iter = hummock_storage
        .range_scan(..=b"ee".to_vec(), epoch1)
        .await
        .unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 2);

    // Get the anchor value at the first snapshot
    let value = hummock_storage.get(&anchor, epoch1).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = hummock_storage.get(&anchor, epoch2).await.unwrap().unwrap();
    assert_eq!(Bytes::from(value), Bytes::from("111111"));
    // update aa, write cc
    let mut iter = hummock_storage
        .range_scan(..=b"ee".to_vec(), epoch2)
        .await
        .unwrap();
    iter.rewind().await.unwrap();
    let len = count_iter(&mut iter).await;
    assert_eq!(len, 3);
}
