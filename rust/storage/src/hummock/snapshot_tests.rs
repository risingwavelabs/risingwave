#[cfg(test)]
use std::sync::Arc;

use super::*;
use crate::hummock::cloud::gen_remote_sstable;
use crate::hummock::iterator::test_utils::{
    default_builder_opt_for_test, iterator_test_key_of, iterator_test_key_of_epoch,
};
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::mock::{MockHummockMetaClient, MockHummockMetaService};
use crate::hummock::snapshot::HummockSnapshot;
use crate::hummock::value::HummockValue;
use crate::hummock::SSTableBuilder;
use crate::object::{InMemObjectStore, ObjectStore};

const TEST_KEY_TABLE_ID: u64 = 233;

async fn gen_and_upload_table(
    obj_client: Arc<dyn ObjectStore>,
    remote_dir: &str,
    vm: Arc<LocalVersionManager>,
    hummock_meta_client: &dyn HummockMetaClient,
    kv_pairs: Vec<(usize, HummockValue<Vec<u8>>)>,
    epoch: u64,
) {
    if kv_pairs.is_empty() {
        return;
    }
    let table_id = hummock_meta_client
        .get_new_table_id(GetNewTableIdRequest {})
        .await
        .table_id;

    let mut b = SSTableBuilder::new(default_builder_opt_for_test());
    for kv in kv_pairs {
        b.add(
            &iterator_test_key_of_epoch(TEST_KEY_TABLE_ID, kv.0, epoch),
            kv.1,
        );
    }
    let (data, meta) = b.finish();
    // get remote table
    let table = gen_remote_sstable(obj_client, table_id, data, meta, remote_dir)
        .await
        .unwrap();
    hummock_meta_client
        .add_tables(AddTablesRequest {
            context_identifier: 0,
            tables: vec![SstableInfo {
                id: table.id,
                key_range: Some(risingwave_pb::hummock::KeyRange {
                    left: table.meta.smallest_key,
                    right: table.meta.largest_key,
                    inf: false,
                }),
            }],
            epoch,
        })
        .await;
    // TODO #2336 we need to maintain local version.
    vm.update_local_version(hummock_meta_client).await;
}

macro_rules! assert_count_range_scan {
    ($snapshot:expr, $range:expr, $expect_count:expr) => {{
        let mut it = $snapshot.range_scan::<_, Vec<u8>>($range).await.unwrap();
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
    ($snapshot:expr, $range:expr, $expect_count:expr) => {{
        let mut it = $snapshot
            .reverse_range_scan::<_, Vec<u8>>($range)
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
    let remote_dir = "/test";
    let obj_client = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
    let vm = Arc::new(LocalVersionManager::new(obj_client.clone(), remote_dir));
    let mock_hummock_meta_service = Arc::new(MockHummockMetaService::new());
    let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        mock_hummock_meta_service.clone(),
    ));
    let mut epoch: u64 = 1;
    gen_and_upload_table(
        obj_client.clone(),
        remote_dir,
        vm.clone(),
        mock_hummock_meta_client.as_ref(),
        vec![
            (1, HummockValue::Put(b"test".to_vec())),
            (2, HummockValue::Put(b"test".to_vec())),
        ],
        epoch,
    )
    .await;
    let snapshot_1 = HummockSnapshot::new(epoch, vm.clone());
    assert_count_range_scan!(snapshot_1, .., 2);

    epoch += 1;
    gen_and_upload_table(
        obj_client.clone(),
        remote_dir,
        vm.clone(),
        mock_hummock_meta_client.as_ref(),
        vec![
            (1, HummockValue::Delete),
            (3, HummockValue::Put(b"test".to_vec())),
            (4, HummockValue::Put(b"test".to_vec())),
        ],
        epoch,
    )
    .await;
    let snapshot_2 = HummockSnapshot::new(epoch, vm.clone());
    assert_count_range_scan!(snapshot_2, .., 3);
    assert_count_range_scan!(snapshot_1, .., 2);

    epoch += 1;
    gen_and_upload_table(
        obj_client.clone(),
        remote_dir,
        vm.clone(),
        mock_hummock_meta_client.as_ref(),
        vec![
            (2, HummockValue::Delete),
            (3, HummockValue::Delete),
            (4, HummockValue::Delete),
        ],
        epoch,
    )
    .await;
    let snapshot_3 = HummockSnapshot::new(epoch, vm.clone());
    assert_count_range_scan!(snapshot_3, .., 0);
    assert_count_range_scan!(snapshot_2, .., 3);
    assert_count_range_scan!(snapshot_1, .., 2);
}

#[tokio::test]
async fn test_snapshot_range_scan() {
    let obj_client = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
    let remote_dir = "/test";
    let vm = Arc::new(LocalVersionManager::new(obj_client.clone(), remote_dir));
    let mock_hummock_meta_service = Arc::new(MockHummockMetaService::new());
    let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        mock_hummock_meta_service.clone(),
    ));
    let epoch = 1;

    gen_and_upload_table(
        obj_client.clone(),
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

    let snapshot = HummockSnapshot::new(epoch, vm.clone());
    assert_count_range_scan!(snapshot, key!(2)..=key!(3), 2);
    assert_count_range_scan!(snapshot, key!(2)..key!(3), 1);
    assert_count_range_scan!(snapshot, key!(2).., 3);
    assert_count_range_scan!(snapshot, ..=key!(3), 3);
    assert_count_range_scan!(snapshot, ..key!(3), 2);
    assert_count_range_scan!(snapshot, .., 4);
}

#[tokio::test]
async fn test_snapshot_reverse_range_scan() {
    let obj_client = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
    let remote_dir = "/test";
    let vm = Arc::new(LocalVersionManager::new(obj_client.clone(), remote_dir));
    let mock_hummock_meta_service = Arc::new(MockHummockMetaService::new());
    let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        mock_hummock_meta_service.clone(),
    ));
    let epoch = 1;

    gen_and_upload_table(
        obj_client.clone(),
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

    let snapshot = HummockSnapshot::new(epoch, vm.clone());
    assert_count_reverse_range_scan!(snapshot, key!(3)..=key!(2), 2);
    assert_count_reverse_range_scan!(snapshot, key!(3)..key!(2), 1);
    assert_count_reverse_range_scan!(snapshot, key!(3)..key!(1), 2);
    assert_count_reverse_range_scan!(snapshot, key!(3)..=key!(1), 3);
    assert_count_reverse_range_scan!(snapshot, key!(3)..key!(0), 3);
    assert_count_reverse_range_scan!(snapshot, .., 4);
}
