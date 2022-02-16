use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result};

use crate::manager::{Epoch, SINGLE_VERSION_EPOCH};
use crate::storage::{
    MemStore, MetaStore, Operation, Precondition, SledMetaStore, DEFAULT_COLUMN_FAMILY,
};

async fn test_meta_store_basic(store: &dyn MetaStore) -> Result<()> {
    let (k, v, version) = (b"key_1", b"value_1", Epoch::from(1));
    assert!(store.put(k, v, version).await.is_ok());
    let val = store.get(k, version).await.unwrap();
    assert_eq!(val.as_slice(), v);
    let val = store
        .get_cf(DEFAULT_COLUMN_FAMILY, k, version)
        .await
        .unwrap();
    assert_eq!(val.as_slice(), v);
    assert!(store.get_cf("test_cf", k, version).await.is_err());

    assert!(store.put_cf("test_cf", k, v, version).await.is_ok());
    let val = store.get_cf("test_cf", k, version).await.unwrap();
    assert_eq!(val.as_slice(), v);

    assert!(store.delete(k, version).await.is_ok());
    assert_eq!(store.list().await.unwrap().len(), 0);

    assert!(store.delete_cf("test_cf", k, version).await.is_ok());
    assert_eq!(store.list_cf("test_cf").await.unwrap().len(), 0);

    let batch_values: Vec<(Vec<u8>, Vec<u8>, Epoch)> = vec![
        (b"key_1".to_vec(), b"value_1".to_vec(), Epoch::from(2)),
        (b"key_2".to_vec(), b"value_2".to_vec(), Epoch::from(2)),
        (b"key_3".to_vec(), b"value_3".to_vec(), Epoch::from(2)),
    ];
    assert!(store.put_batch(batch_values).await.is_ok());

    let batch_values: Vec<(&str, Vec<u8>, Vec<u8>, Epoch)> = vec![
        (
            "test_cf",
            b"key_1".to_vec(),
            b"value_1".to_vec(),
            Epoch::from(2),
        ),
        (
            "test_cf",
            b"key_2".to_vec(),
            b"value_2".to_vec(),
            Epoch::from(2),
        ),
    ];
    assert!(store.put_batch_cf(batch_values).await.is_ok());

    assert_eq!(store.list().await.unwrap().len(), 3);
    assert_eq!(store.list_cf("test_cf").await.unwrap().len(), 2);
    assert_eq!(
        store
            .list_batch_cf(vec!["test_cf", DEFAULT_COLUMN_FAMILY])
            .await
            .unwrap()
            .len(),
        2
    );

    assert!(store
        .put(b"key_3", b"value_3_new", Epoch::from(3))
        .await
        .is_ok());
    let mut values = store.list().await.unwrap();
    values.sort();
    let expected: Vec<Vec<u8>> = vec![
        b"value_1".to_vec(),
        b"value_2".to_vec(),
        b"value_3_new".to_vec(),
    ];
    assert_eq!(values, expected);

    assert!(store.delete_all(b"key_1").await.is_ok());
    assert!(store.delete_all(b"key_2").await.is_ok());
    assert!(store.delete_all(b"key_3").await.is_ok());
    assert_eq!(store.list().await.unwrap().len(), 0);

    assert!(store.delete_all_cf("test_cf", b"key_1").await.is_ok());
    assert!(store.delete_all_cf("test_cf", b"key_2").await.is_ok());
    assert_eq!(store.list_cf("test_cf").await.unwrap().len(), 0);

    Ok(())
}

async fn test_meta_store_transaction(meta_store: &dyn MetaStore) -> Result<()> {
    let cf = "test_trx_cf";
    let mut kvs = vec![];
    for i in 1..=5 {
        kvs.push((
            format!("key{}", i).as_bytes().to_vec(),
            "value1".as_bytes().to_vec(),
            Some(10),
        ));
    }

    // empty preconditions
    let ops = kvs
        .iter()
        .take(2)
        .map(|(key, value, version)| {
            Operation::Put(
                cf.to_owned(),
                key.to_owned(),
                value.to_owned(),
                version.to_owned(),
            )
        })
        .collect_vec();
    let mut trx = meta_store.get_transaction();
    trx.add_operations(ops);
    meta_store.commit_transaction(&mut trx).await.unwrap();
    let result = meta_store.list_cf(cf).await.unwrap();
    let expected = kvs
        .iter()
        .take(2)
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .map(|(_, value, _)| value)
        .cloned()
        .collect_vec();
    assert_eq!(result, expected);

    // preconditions evaluated to true
    let mut trx = meta_store.get_transaction();
    trx.add_preconditions(vec![Precondition::KeyExists {
        cf: cf.to_owned(),
        key: kvs[0].0.to_owned(),
        version: kvs[0].2,
    }]);
    trx.add_operations(vec![Operation::Put(
        cf.to_owned(),
        kvs[2].0.to_owned(),
        kvs[2].1.to_owned(),
        kvs[2].2,
    )]);
    meta_store.commit_transaction(&mut trx).await.unwrap();
    assert_eq!(3, meta_store.list_cf(cf).await.unwrap().len());

    let mut trx = meta_store.get_transaction();
    let ops = kvs
        .iter()
        .map(|(key, _, _)| Operation::Delete(cf.to_owned(), key.to_owned(), None))
        .collect_vec();
    trx.add_operations(ops);
    meta_store.commit_transaction(&mut trx).await.unwrap();
    assert_eq!(0, meta_store.list_cf(cf).await.unwrap().len());

    // preconditions evaluated to false
    let mut trx = meta_store.get_transaction();
    trx.add_preconditions(vec![Precondition::KeyExists {
        cf: cf.to_owned(),
        key: kvs[4].0.to_owned(),
        version: kvs[4].2,
    }]);
    trx.add_operations(vec![Operation::Put(
        cf.to_owned(),
        kvs[3].0.to_owned(),
        kvs[3].1.to_owned(),
        kvs[3].2,
    )]);
    let expected = ErrorCode::InternalError(String::from("transaction aborted"));
    assert_eq!(
        meta_store
            .commit_transaction(&mut trx)
            .await
            .unwrap_err()
            .inner(),
        &expected
    );
    assert_eq!(0, meta_store.list_cf(cf).await.unwrap().len());

    Ok(())
}

async fn test_meta_store_keys_share_prefix(meta_store: &dyn MetaStore) -> Result<()> {
    let cf = "test_overlapped_key_cf";
    let batch = vec![
        (
            cf,
            "key1".as_bytes().to_owned(),
            "value1".as_bytes().to_owned(),
            Epoch::from(1),
        ),
        (
            cf,
            "key11".as_bytes().to_owned(),
            "value11".as_bytes().to_owned(),
            Epoch::from(1),
        ),
        (
            cf,
            "key11".as_bytes().to_owned(),
            "value11".as_bytes().to_owned(),
            Epoch::from(2),
        ),
        (
            cf,
            "key111".as_bytes().to_owned(),
            "value111".as_bytes().to_owned(),
            Epoch::from(1),
        ),
    ];
    meta_store.put_batch_cf(batch.clone()).await.unwrap();
    assert_eq!(3, meta_store.list_cf(cf).await.unwrap().len());
    // keys share the same prefix are not affected
    meta_store.delete_all_cf(cf, &batch[1].1).await.unwrap();
    assert_eq!(2, meta_store.list_cf(cf).await.unwrap().len());
    for (cf, key, _, _) in &batch {
        meta_store.delete_all_cf(cf, key).await.unwrap();
    }

    Ok(())
}

async fn test_meta_store_overlapped_cf(meta_store: &dyn MetaStore) -> Result<()> {
    let cf1 = "test_overlapped_cf1";
    let cf2 = "test_overlapped_cf11";
    let cf3 = "test_overlapped_cf111";
    let batch = vec![
        (
            cf1,
            "key1".as_bytes().to_owned(),
            "value1".as_bytes().to_owned(),
            SINGLE_VERSION_EPOCH,
        ),
        (
            cf2,
            "key1".as_bytes().to_owned(),
            "value1".as_bytes().to_owned(),
            SINGLE_VERSION_EPOCH,
        ),
        (
            cf3,
            "key1".as_bytes().to_owned(),
            "value1".as_bytes().to_owned(),
            SINGLE_VERSION_EPOCH,
        ),
    ];
    meta_store.put_batch_cf(batch.clone()).await.unwrap();
    assert_eq!(1, meta_store.list_cf(cf1).await.unwrap().len());
    assert_eq!(1, meta_store.list_cf(cf2).await.unwrap().len());
    assert_eq!(1, meta_store.list_cf(cf3).await.unwrap().len());
    for (cf, key, _, _) in &batch {
        meta_store.delete_all_cf(cf, key).await.unwrap();
        assert_eq!(0, meta_store.list_cf(cf).await.unwrap().len());
    }

    Ok(())
}

#[tokio::test]
async fn test_mem_store() -> Result<()> {
    let store = MemStore::new();
    test_meta_store_basic(&store).await.unwrap();
    test_meta_store_keys_share_prefix(&store).await.unwrap();
    test_meta_store_overlapped_cf(&store).await.unwrap();
    Ok(())
}

#[tokio::test]
async fn test_sled_store() -> Result<()> {
    let store = SledMetaStore::new(None).unwrap();
    test_meta_store_basic(&store).await.unwrap();
    test_meta_store_transaction(&store).await.unwrap();
    test_meta_store_keys_share_prefix(&store).await.unwrap();
    test_meta_store_overlapped_cf(&store).await.unwrap();
    Ok(())
}
