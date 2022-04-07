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

#[cfg(test)]
use std::sync::Arc;

use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::MockHummockMetaClient;

use super::*;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::test_utils::default_config_for_test;
use crate::object::{InMemObjectStore, ObjectStoreImpl};

macro_rules! assert_count_range_scan {
    ($storage:expr, $range:expr, $expect_count:expr, $epoch:expr) => {{
        let mut it = $storage.iter::<_, Vec<u8>>($range, $epoch).await.unwrap();
        let mut count = 0;
        loop {
            match it.next().await.unwrap() {
                Some(_) => count += 1,
                None => break,
            }
        }
        assert_eq!(count, $expect_count);
    }};
}

macro_rules! assert_count_reverse_range_scan {
    ($storage:expr, $range:expr, $expect_count:expr, $epoch:expr) => {{
        let mut it = $storage
            .reverse_iter::<_, Vec<u8>>($range, $epoch)
            .await
            .unwrap();
        let mut count = 0;
        loop {
            match it.next().await.unwrap() {
                Some(_) => count += 1,
                None => break,
            }
        }
        assert_eq!(count, $expect_count);
    }};
}

#[tokio::test]
async fn test_snapshot() {
    let remote_dir = "hummock_001";
    let object_store = Arc::new(ObjectStoreImpl::Mem(InMemObjectStore::new()));
    let sstable_store = Arc::new(SstableStore::new(
        object_store.clone(),
        remote_dir.to_string(),
        Arc::new(StateStoreMetrics::unused()),
        64 << 20,
        64 << 20,
    ));
    let vm = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));

    let hummock_options = Arc::new(default_config_for_test());
    let hummock_storage = HummockStorage::with_default_stats(
        hummock_options,
        sstable_store,
        vm.clone(),
        mock_hummock_meta_client.clone(),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();

    let epoch1: u64 = 1;
    hummock_storage
        .ingest_batch(
            vec![
                (Bytes::from("1"), StorageValue::new_default_put("test")),
                (Bytes::from("2"), StorageValue::new_default_put("test")),
            ],
            epoch1,
        )
        .await
        .unwrap();
    hummock_storage.sync(Some(epoch1)).await.unwrap();
    assert_count_range_scan!(hummock_storage, .., 2, epoch1);

    let epoch2 = epoch1 + 1;
    hummock_storage
        .ingest_batch(
            vec![
                (Bytes::from("1"), StorageValue::new_default_delete()),
                (Bytes::from("3"), StorageValue::new_default_put("test")),
                (Bytes::from("4"), StorageValue::new_default_put("test")),
            ],
            epoch2,
        )
        .await
        .unwrap();
    hummock_storage.sync(Some(epoch2)).await.unwrap();
    assert_count_range_scan!(hummock_storage, .., 3, epoch2);
    assert_count_range_scan!(hummock_storage, .., 2, epoch1);

    let epoch3 = epoch2 + 1;
    hummock_storage
        .ingest_batch(
            vec![
                (Bytes::from("2"), StorageValue::new_default_delete()),
                (Bytes::from("3"), StorageValue::new_default_delete()),
                (Bytes::from("4"), StorageValue::new_default_delete()),
            ],
            epoch3,
        )
        .await
        .unwrap();
    hummock_storage.sync(Some(epoch3)).await.unwrap();
    assert_count_range_scan!(hummock_storage, .., 0, epoch3);
    assert_count_range_scan!(hummock_storage, .., 3, epoch2);
    assert_count_range_scan!(hummock_storage, .., 2, epoch1);
}

#[tokio::test]
async fn test_snapshot_range_scan() {
    let object_store = Arc::new(ObjectStoreImpl::Mem(InMemObjectStore::new()));
    let remote_dir = "hummock_001";
    let sstable_store = Arc::new(SstableStore::new(
        object_store.clone(),
        remote_dir.to_string(),
        Arc::new(StateStoreMetrics::unused()),
        64 << 20,
        64 << 20,
    ));
    let vm = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));
    let hummock_options = Arc::new(default_config_for_test());
    let hummock_storage = HummockStorage::with_default_stats(
        hummock_options,
        sstable_store,
        vm.clone(),
        mock_hummock_meta_client.clone(),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();

    let epoch: u64 = 1;

    hummock_storage
        .ingest_batch(
            vec![
                (Bytes::from("1"), StorageValue::new_default_put("test")),
                (Bytes::from("2"), StorageValue::new_default_put("test")),
                (Bytes::from("3"), StorageValue::new_default_put("test")),
                (Bytes::from("4"), StorageValue::new_default_put("test")),
            ],
            epoch,
        )
        .await
        .unwrap();
    hummock_storage.sync(Some(epoch)).await.unwrap();

    macro_rules! key {
        ($idx:expr) => {
            Bytes::from(stringify!($idx)).to_vec()
        };
    }

    assert_count_range_scan!(hummock_storage, key!(2)..=key!(3), 2, epoch);
    assert_count_range_scan!(hummock_storage, key!(2)..key!(3), 1, epoch);
    assert_count_range_scan!(hummock_storage, key!(2).., 3, epoch);
    assert_count_range_scan!(hummock_storage, ..=key!(3), 3, epoch);
    assert_count_range_scan!(hummock_storage, ..key!(3), 2, epoch);
    assert_count_range_scan!(hummock_storage, .., 4, epoch);
}

#[tokio::test]
async fn test_snapshot_reverse_range_scan() {
    let object_store = Arc::new(ObjectStoreImpl::Mem(InMemObjectStore::new()));
    let remote_dir = "/test";
    let sstable_store = Arc::new(SstableStore::new(
        object_store.clone(),
        remote_dir.to_string(),
        Arc::new(StateStoreMetrics::unused()),
        64 << 20,
        64 << 20,
    ));
    let vm = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;
    let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_node.id,
    ));
    let hummock_options = Arc::new(default_config_for_test());
    let hummock_storage = HummockStorage::with_default_stats(
        hummock_options,
        sstable_store.clone(),
        vm.clone(),
        mock_hummock_meta_client.clone(),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();

    let epoch = 1;
    hummock_storage
        .ingest_batch(
            vec![
                (Bytes::from("1"), StorageValue::new_default_put("test")),
                (Bytes::from("2"), StorageValue::new_default_put("test")),
                (Bytes::from("3"), StorageValue::new_default_put("test")),
                (Bytes::from("4"), StorageValue::new_default_put("test")),
            ],
            epoch,
        )
        .await
        .unwrap();
    hummock_storage.sync(Some(epoch)).await.unwrap();

    macro_rules! key {
        ($idx:expr) => {
            Bytes::from(stringify!($idx)).to_vec()
        };
    }

    assert_count_reverse_range_scan!(hummock_storage, key!(3)..=key!(2), 2, epoch);
    assert_count_reverse_range_scan!(hummock_storage, key!(3)..key!(2), 1, epoch);
    assert_count_reverse_range_scan!(hummock_storage, key!(3)..key!(1), 2, epoch);
    assert_count_reverse_range_scan!(hummock_storage, key!(3)..=key!(1), 3, epoch);
    assert_count_reverse_range_scan!(hummock_storage, key!(3)..key!(0), 3, epoch);
    assert_count_reverse_range_scan!(hummock_storage, .., 4, epoch);
}
