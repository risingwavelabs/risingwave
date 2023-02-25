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

use assert_matches::assert_matches;
use async_trait::async_trait;
use itertools::Itertools;

use crate::storage::{
    Key, MemStore, MetaStore, MetaStoreError, MetaStoreResult, Operation, Snapshot, Transaction,
    Value,
};

const TEST_DEFAULT_CF: &str = "TEST_DEFAULT";

#[async_trait]
trait MetaStoreTestExt: MetaStore {
    async fn put(&self, key: Key, value: Value) -> MetaStoreResult<()>;
    async fn get(&self, key: &[u8]) -> MetaStoreResult<Value>;
    async fn delete(&self, key: &[u8]) -> MetaStoreResult<()>;
    async fn list(&self) -> MetaStoreResult<Vec<(Vec<u8>, Vec<u8>)>>;
}

#[async_trait]
impl<S: MetaStore> MetaStoreTestExt for S {
    async fn put(&self, key: Key, value: Value) -> MetaStoreResult<()> {
        self.put_cf(TEST_DEFAULT_CF, key, value).await
    }

    async fn get(&self, key: &[u8]) -> MetaStoreResult<Value> {
        self.get_cf(TEST_DEFAULT_CF, key).await
    }

    async fn delete(&self, key: &[u8]) -> MetaStoreResult<()> {
        self.delete_cf(TEST_DEFAULT_CF, key).await
    }

    async fn list(&self) -> MetaStoreResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.list_cf(TEST_DEFAULT_CF).await
    }
}

async fn test_meta_store_basic<S: MetaStore>(store: &S) -> MetaStoreResult<()> {
    let (k, v) = (b"key_1", b"value_1");
    assert!(store.put(k.to_vec(), v.to_vec()).await.is_ok());
    let val = store.get(k).await.unwrap();
    assert_eq!(val.as_slice(), v);
    let val = store.get_cf(TEST_DEFAULT_CF, k).await.unwrap();
    assert_eq!(val.as_slice(), v);
    assert!(store.get_cf("test_cf", k).await.is_err());

    assert!(store
        .put_cf("test_cf", k.to_vec(), v.to_vec())
        .await
        .is_ok());
    let val = store.get_cf("test_cf", k).await.unwrap();
    assert_eq!(val.as_slice(), v);

    assert!(store.delete(k).await.is_ok());
    assert_eq!(store.list().await.unwrap().len(), 0);

    assert!(store.delete_cf("test_cf", k).await.is_ok());
    assert_eq!(store.list_cf("test_cf").await.unwrap().len(), 0);

    let batch: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (b"key_1".to_vec(), b"value_1".to_vec()),
        (b"key_2".to_vec(), b"value_2".to_vec()),
        (b"key_3".to_vec(), b"value_3".to_vec()),
    ];
    for (key, value) in batch {
        store.put(key, value).await?;
    }

    let batch: Vec<(&str, Vec<u8>, Vec<u8>)> = vec![
        ("test_cf", b"key_1".to_vec(), b"value_1".to_vec()),
        ("test_cf", b"key_2".to_vec(), b"value_2".to_vec()),
    ];
    for (cf, key, value) in batch {
        store.put_cf(cf, key, value).await?;
    }

    assert_eq!(store.list().await.unwrap().len(), 3);
    assert_eq!(store.list_cf("test_cf").await.unwrap().len(), 2);
    {
        let snapshot = store.snapshot().await;
        let vals = snapshot.list_cf("test_cf").await?;
        assert_eq!(vals.len(), 2);
        let vals = snapshot.list_cf(TEST_DEFAULT_CF).await?;
        assert_eq!(vals.len(), 3);
    }

    assert!(store
        .put(b"key_3".to_vec(), b"value_3_new".to_vec())
        .await
        .is_ok());
    let mut values = store
        .list()
        .await
        .unwrap()
        .into_iter()
        .map(|(_, v)| v)
        .collect_vec();
    values.sort();
    let expected: Vec<Vec<u8>> = vec![
        b"value_1".to_vec(),
        b"value_2".to_vec(),
        b"value_3_new".to_vec(),
    ];
    assert_eq!(values, expected);

    Ok(())
}

async fn test_meta_store_transaction<S: MetaStore>(meta_store: &S) -> MetaStoreResult<()> {
    let cf = "test_trx_cf";
    let mut kvs = vec![];
    for i in 1..=5 {
        kvs.push((
            format!("key{}", i).as_bytes().to_vec(),
            "value1".as_bytes().to_vec(),
        ));
    }

    // empty preconditions
    let ops = kvs
        .iter()
        .take(2)
        .map(|(key, value)| Operation::Put {
            cf: cf.to_owned(),
            key: key.to_owned(),
            value: value.to_owned(),
        })
        .collect_vec();
    let mut trx = Transaction::default();
    trx.add_operations(ops);
    meta_store.txn(trx).await.unwrap();
    let result = meta_store
        .list_cf(cf)
        .await
        .unwrap()
        .into_iter()
        .map(|(_, v)| v)
        .collect_vec();
    let expected = kvs
        .iter()
        .take(2)
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .map(|(_, value)| value)
        .cloned()
        .collect_vec();
    assert_eq!(result, expected);

    // preconditions evaluated to true
    let mut trx = Transaction::default();
    trx.check_exists(cf.to_owned(), kvs[0].0.to_owned());
    trx.put(cf.to_owned(), kvs[2].0.to_owned(), kvs[2].1.to_owned());
    meta_store.txn(trx).await.unwrap();
    assert_eq!(3, meta_store.list_cf(cf).await.unwrap().len());

    let mut trx = Transaction::default();
    let ops = kvs
        .iter()
        .map(|(key, _)| Operation::Delete {
            cf: cf.to_owned(),
            key: key.to_owned(),
        })
        .collect_vec();
    trx.add_operations(ops);
    meta_store.txn(trx).await.unwrap();
    assert_eq!(0, meta_store.list_cf(cf).await.unwrap().len());

    // preconditions evaluated to false
    let mut trx = Transaction::default();
    trx.check_exists(cf.to_owned(), kvs[4].0.to_owned());
    trx.put(cf.to_owned(), kvs[3].0.to_owned(), kvs[3].1.to_owned());
    assert_matches!(
        meta_store.txn(trx).await.unwrap_err(),
        MetaStoreError::TransactionAbort()
    );
    assert_eq!(0, meta_store.list_cf(cf).await.unwrap().len());

    Ok(())
}

async fn test_meta_store_keys_share_prefix<S: MetaStore>(meta_store: &S) -> MetaStoreResult<()> {
    let cf = "test_overlapped_key_cf";
    let batch = vec![
        (
            cf,
            "key1".as_bytes().to_owned(),
            "value1".as_bytes().to_owned(),
        ),
        (
            cf,
            "key11".as_bytes().to_owned(),
            "value11".as_bytes().to_owned(),
        ),
        (
            cf,
            "key11".as_bytes().to_owned(),
            "value11".as_bytes().to_owned(),
        ),
        (
            cf,
            "key111".as_bytes().to_owned(),
            "value111".as_bytes().to_owned(),
        ),
    ];
    for (cf, key, value) in batch.clone() {
        meta_store.put_cf(cf, key, value).await?;
    }
    assert_eq!(3, meta_store.list_cf(cf).await.unwrap().len());

    Ok(())
}

async fn test_meta_store_overlapped_cf<S: MetaStore>(meta_store: &S) -> MetaStoreResult<()> {
    let cf1 = "test_overlapped_cf1";
    let cf2 = "test_overlapped_cf11";
    let cf3 = "test_overlapped_cf111";
    let batch = vec![
        (
            cf1,
            "key1".as_bytes().to_owned(),
            "value1".as_bytes().to_owned(),
        ),
        (
            cf2,
            "key1".as_bytes().to_owned(),
            "value1".as_bytes().to_owned(),
        ),
        (
            cf3,
            "key1".as_bytes().to_owned(),
            "value1".as_bytes().to_owned(),
        ),
    ];
    for (cf, key, value) in batch.clone() {
        meta_store.put_cf(cf, key, value).await?;
    }
    assert_eq!(1, meta_store.list_cf(cf1).await.unwrap().len());
    assert_eq!(1, meta_store.list_cf(cf2).await.unwrap().len());
    assert_eq!(1, meta_store.list_cf(cf3).await.unwrap().len());

    Ok(())
}

#[tokio::test]
async fn test_mem_store() -> MetaStoreResult<()> {
    let store = MemStore::default();
    test_meta_store_basic(&store).await.unwrap();
    test_meta_store_keys_share_prefix(&store).await.unwrap();
    test_meta_store_overlapped_cf(&store).await.unwrap();
    test_meta_store_transaction(&store).await.unwrap();
    Ok(())
}
