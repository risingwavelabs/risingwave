#[cfg(test)]
use std::sync::Arc;

use risingwave_pb::hummock::SstableInfo;

use super::*;
use crate::hummock::iterator::test_utils::{
    default_builder_opt_for_test, iterator_test_key_of, iterator_test_key_of_epoch,
};
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::mock::{MockHummockMetaClient, MockHummockMetaService};
use crate::hummock::value::HummockValue;
use crate::hummock::SSTableBuilder;
use crate::object::{InMemObjectStore, ObjectStore};

const TEST_KEY_TABLE_ID: u64 = 233;

async fn gen_and_upload_table(
    object_store: Arc<dyn ObjectStore>,
    remote_dir: &str,
    vm: Arc<LocalVersionManager>,
    hummock_meta_client: &dyn HummockMetaClient,
    kv_pairs: Vec<(usize, HummockValue<Vec<u8>>)>,
    epoch: u64,
) {
    if kv_pairs.is_empty() {
        return;
    }
    let table_id = hummock_meta_client.get_new_table_id().await.unwrap();

    let mut b = SSTableBuilder::new(default_builder_opt_for_test());
    for kv in kv_pairs {
        b.add(
            &iterator_test_key_of_epoch(TEST_KEY_TABLE_ID, kv.0, epoch),
            kv.1.as_slice(),
        );
    }
    let (data, meta) = b.finish();
    // get remote table
    let sstable_store = Arc::new(SstableStore::new(object_store, remote_dir.to_string()));
    let sst = Sstable { id: table_id, meta };
    sstable_store
        .put(&sst, data, CachePolicy::Fill)
        .await
        .unwrap();

    let version = hummock_meta_client
        .add_tables(
            epoch,
            vec![SstableInfo {
                id: table_id,
                key_range: Some(risingwave_pb::hummock::KeyRange {
                    left: sst.meta.smallest_key,
                    right: sst.meta.largest_key,
                    inf: false,
                }),
            }],
        )
        .await
        .unwrap();
    vm.try_set_version(version);
    hummock_meta_client.commit_epoch(epoch).await.ok();
}

async fn gen_and_upload_table_with_sstable_store(
    sstable_store: SstableStoreRef,
    vm: Arc<LocalVersionManager>,
    hummock_meta_client: &dyn HummockMetaClient,
    kv_pairs: Vec<(usize, HummockValue<Vec<u8>>)>,
    epoch: u64,
) {
    if kv_pairs.is_empty() {
        return;
    }
    let table_id = hummock_meta_client.get_new_table_id().await.unwrap();

    let mut b = SSTableBuilder::new(default_builder_opt_for_test());
    for kv in kv_pairs {
        b.add(
            &iterator_test_key_of_epoch(TEST_KEY_TABLE_ID, kv.0, epoch),
            kv.1.as_slice(),
        );
    }
    let (data, meta) = b.finish();
    let sst = Sstable { id: table_id, meta };
    sstable_store
        .put(&sst, data, CachePolicy::Fill)
        .await
        .unwrap();

    let version = hummock_meta_client
        .add_tables(
            epoch,
            vec![SstableInfo {
                id: table_id,
                key_range: Some(risingwave_pb::hummock::KeyRange {
                    left: sst.meta.smallest_key,
                    right: sst.meta.largest_key,
                    inf: false,
                }),
            }],
        )
        .await
        .unwrap();
    vm.try_set_version(version);
    hummock_meta_client.commit_epoch(epoch).await.ok();
}

macro_rules! assert_count_range_scan {
    ($storage:expr, $range:expr, $expect_count:expr, $epoch:expr) => {{
        let mut it = $storage
            .range_scan::<_, Vec<u8>>($range, $epoch)
            .await
            .unwrap();
        it.rewind().await.unwrap();
        let mut count = 0;
        while it.is_valid() {
            count += 1;
            it.next().await.unwrap();
        }
        assert_eq!(count, $expect_count);
    }};
}

macro_rules! assert_count_reverse_range_scan {
    ($storage:expr, $range:expr, $expect_count:expr, $epoch:expr) => {{
        let mut it = $storage
            .reverse_range_scan::<_, Vec<u8>>($range, $epoch)
            .await
            .unwrap();
        it.rewind().await.unwrap();
        let mut count = 0;
        while it.is_valid() {
            count += 1;
            it.next().await.unwrap();
        }
        assert_eq!(count, $expect_count);
    }};
}

#[tokio::test]
async fn test_snapshot() {
    let remote_dir = "hummock_001";
    let object_store = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
    let sstable_store = Arc::new(SstableStore::new(
        object_store.clone(),
        remote_dir.to_string(),
    ));
    let vm = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let mock_hummock_meta_service = Arc::new(MockHummockMetaService::new());
    let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        mock_hummock_meta_service.clone(),
    ));

    let hummock_options = HummockOptions::default_for_test();
    let hummock_storage = HummockStorage::with_default_stats(
        hummock_options,
        sstable_store,
        vm.clone(),
        mock_hummock_meta_client.clone(),
    )
    .await
    .unwrap();

    let epoch1: u64 = 1;
    gen_and_upload_table(
        object_store.clone(),
        remote_dir,
        vm.clone(),
        mock_hummock_meta_client.as_ref(),
        vec![
            (1, HummockValue::Put(b"test".to_vec())),
            (2, HummockValue::Put(b"test".to_vec())),
        ],
        epoch1,
    )
    .await;
    assert_count_range_scan!(hummock_storage, .., 2, epoch1);

    let epoch2 = epoch1 + 1;
    gen_and_upload_table(
        object_store.clone(),
        remote_dir,
        vm.clone(),
        mock_hummock_meta_client.as_ref(),
        vec![
            (1, HummockValue::Delete),
            (3, HummockValue::Put(b"test".to_vec())),
            (4, HummockValue::Put(b"test".to_vec())),
        ],
        epoch2,
    )
    .await;
    assert_count_range_scan!(hummock_storage, .., 3, epoch2);
    assert_count_range_scan!(hummock_storage, .., 2, epoch1);

    let epoch3 = epoch2 + 1;
    gen_and_upload_table(
        object_store.clone(),
        remote_dir,
        vm.clone(),
        mock_hummock_meta_client.as_ref(),
        vec![
            (2, HummockValue::Delete),
            (3, HummockValue::Delete),
            (4, HummockValue::Delete),
        ],
        epoch3,
    )
    .await;
    assert_count_range_scan!(hummock_storage, .., 0, epoch3);
    assert_count_range_scan!(hummock_storage, .., 3, epoch2);
    assert_count_range_scan!(hummock_storage, .., 2, epoch1);
}

#[tokio::test]
async fn test_snapshot_range_scan() {
    let object_store = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
    let remote_dir = "hummock_001";
    let sstable_store = Arc::new(SstableStore::new(
        object_store.clone(),
        remote_dir.to_string(),
    ));
    let vm = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let mock_hummock_meta_service = Arc::new(MockHummockMetaService::new());
    let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        mock_hummock_meta_service.clone(),
    ));
    let hummock_options = HummockOptions::default_for_test();
    let hummock_storage = HummockStorage::with_default_stats(
        hummock_options,
        sstable_store,
        vm.clone(),
        mock_hummock_meta_client.clone(),
    )
    .await
    .unwrap();

    let epoch: u64 = 1;

    gen_and_upload_table(
        object_store.clone(),
        remote_dir,
        vm.clone(),
        mock_hummock_meta_client.as_ref(),
        vec![
            (1, HummockValue::Put(b"test".to_vec())),
            (2, HummockValue::Put(b"test".to_vec())),
            (3, HummockValue::Put(b"test".to_vec())),
            (4, HummockValue::Put(b"test".to_vec())),
        ],
        epoch,
    )
    .await;

    macro_rules! key {
        ($idx:expr) => {
            user_key(&iterator_test_key_of(TEST_KEY_TABLE_ID, $idx)).to_vec()
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
    let object_store = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
    let remote_dir = "/test";
    let sstable_store = Arc::new(SstableStore::new(
        object_store.clone(),
        remote_dir.to_string(),
    ));
    let vm = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let mock_hummock_meta_service = Arc::new(MockHummockMetaService::new());
    let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        mock_hummock_meta_service.clone(),
    ));
    let hummock_options = HummockOptions::default_for_test();
    let hummock_storage = HummockStorage::with_default_stats(
        hummock_options,
        sstable_store.clone(),
        vm.clone(),
        mock_hummock_meta_client.clone(),
    )
    .await
    .unwrap();

    let epoch = 1;

    gen_and_upload_table_with_sstable_store(
        sstable_store,
        vm.clone(),
        mock_hummock_meta_client.as_ref(),
        vec![
            (1, HummockValue::Put(b"test".to_vec())),
            (2, HummockValue::Put(b"test".to_vec())),
            (3, HummockValue::Put(b"test".to_vec())),
            (4, HummockValue::Put(b"test".to_vec())),
        ],
        epoch,
    )
    .await;

    macro_rules! key {
        ($idx:expr) => {
            user_key(&iterator_test_key_of(TEST_KEY_TABLE_ID, $idx)).to_vec()
        };
    }

    assert_count_reverse_range_scan!(hummock_storage, key!(3)..=key!(2), 2, epoch);
    assert_count_reverse_range_scan!(hummock_storage, key!(3)..key!(2), 1, epoch);
    assert_count_reverse_range_scan!(hummock_storage, key!(3)..key!(1), 2, epoch);
    assert_count_reverse_range_scan!(hummock_storage, key!(3)..=key!(1), 3, epoch);
    assert_count_reverse_range_scan!(hummock_storage, key!(3)..key!(0), 3, epoch);
    assert_count_reverse_range_scan!(hummock_storage, .., 4, epoch);
}
