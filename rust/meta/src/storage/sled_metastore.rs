use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::RwError;
use risingwave_common::error::{ErrorCode, Result};
use sled::transaction::TransactionError;
use sled::{Batch, IVec};

use crate::manager::{Epoch, SINGLE_VERSION_EPOCH};
use crate::storage::transaction::{Operation, Precondition, Transaction};
use crate::storage::Operation::Delete;
use crate::storage::OperationOption::{WithPrefix, WithVersion};
use crate::storage::{
    ColumnFamilyUtils, Key, KeyValue, KeyValueVersion, KeyWithVersion, MetaStore, OperationOption,
    Value, DEFAULT_COLUMN_FAMILY,
};

impl From<sled::Error> for crate::storage::Error {
    fn from(e: sled::Error) -> Self {
        crate::storage::Error::StorageError(e.to_string())
    }
}

pub struct SledMetaStore {
    /// The rwlock is to ensure serializable isolation.
    db: Arc<RwLock<sled::Db>>,
}

/// `SledMetaStore` stores a key composed of `KeyValue.key` and `KeyValue.version`.
/// Currently the implementation involves some additional Vec<u8> copy or clone, but it should be OK
/// for now because `SledMetaStore` is for testing purpose only and performance is not the major
/// concern.
impl SledMetaStore {
    pub fn new(db_path: &std::path::Path) -> Result<SledMetaStore> {
        let db =
            sled::open(db_path).map_err(|e| crate::storage::Error::StorageError(e.to_string()))?;
        Ok(SledMetaStore {
            db: Arc::new(RwLock::new(db)),
        })
    }

    /// `get` key value pairs matched by `OperationOption`s.
    /// For each (Key, Vec<OperationOption>) in `keys_with_opts`,
    /// If `WithPrefix` is specified, key value pair is matched by prefix `Key`.
    /// If `WithVersion` is not specified, the greatest version of this key value pair is matched.
    /// Otherwise, the specific version is matched if any.
    /// `WithPrefix` and `WithVersion` are compatible.
    fn get_impl(&self, keys_with_opts: Vec<(Key, Vec<OperationOption>)>) -> Result<Vec<KeyValue>> {
        let db_guard = self.db.read().unwrap();
        let mut batch_result = vec![];
        for (key, opts) in keys_with_opts {
            let mut target_key_version = None;
            let mut key_prefix: Option<&[u8]> = None;
            for opt in &opts {
                match opt {
                    OperationOption::WithPrefix() => {
                        key_prefix = Some(&key);
                    }
                    OperationOption::WithVersion(version) => {
                        target_key_version = Some(version);
                    }
                }
            }
            let mut is_point_read_greatest_version = false;
            if target_key_version.is_none() && key_prefix.is_none() {
                // get the greatest version for the key
                key_prefix = Some(&key);
                is_point_read_greatest_version = true;
            }
            match key_prefix {
                Some(prefix) => {
                    // range read, or point read the greatest version
                    let prefix_matched_kvs = db_guard
                        .scan_prefix(prefix)
                        .map(|r| {
                            r.map_err(|e| {
                                RwError::from(crate::storage::Error::StorageError(e.to_string()))
                            })
                        })
                        .collect::<Result<Vec<(IVec, IVec)>>>()?
                        .into_iter()
                        .map(|(k, v)| {
                            KeyValue::new(
                                KeyWithVersion::get_key(&k),
                                v.to_vec(),
                                KeyWithVersion::get_version(&k).into_inner(),
                            )
                        })
                        .collect_vec();
                    let kvs: Vec<KeyValue> = match target_key_version {
                        Some(key_version) => {
                            // get all kvs with the specified key_version
                            prefix_matched_kvs
                                .into_iter()
                                .filter(|kv| {
                                    prefix.len() <= kv.key().len() && kv.version() == *key_version
                                })
                                .collect()
                        }
                        None => {
                            // get all kvs with their greatest versions
                            let mut greatest_version: HashMap<Key, KeyValueVersion> =
                                HashMap::new();
                            prefix_matched_kvs.iter().for_each(|kv| {
                                if (is_point_read_greatest_version && *kv.key() != key)
                                    || prefix.len() > kv.key().len()
                                {
                                    return;
                                }
                                match greatest_version.get(kv.key()) {
                                    Some(cur_version) => {
                                        if *cur_version < kv.version() {
                                            greatest_version.insert(kv.key().clone(), kv.version());
                                        }
                                    }
                                    None => {
                                        greatest_version.insert(kv.key().clone(), kv.version());
                                    }
                                }
                            });
                            prefix_matched_kvs
                                .into_iter()
                                .filter(|kv| match greatest_version.get(kv.key()) {
                                    Some(version) => *version == kv.version(),
                                    None => false,
                                })
                                .collect()
                        }
                    };
                    batch_result.extend(kvs);
                }
                None => {
                    // point read
                    let kvs = match target_key_version {
                        Some(key_version) => {
                            // get the kv with the specified key_version
                            let result = db_guard
                                .get(KeyWithVersion::compose_key_version(&key, *key_version))
                                .map_err(|e| crate::storage::Error::StorageError(e.to_string()))?
                                .map(|iv| iv.to_vec());
                            match result {
                                Some(item) => vec![KeyValue::new(key, item, *key_version)],
                                None => vec![],
                            }
                        }
                        None => {
                            panic!()
                        }
                    };
                    batch_result.extend(kvs);
                }
            }
        }
        Ok(batch_result)
    }

    /// refer to `Operation::Put`
    fn put_impl(&self, kvs: &[(Key, KeyValueVersion, Value)]) -> Result<()> {
        let mut trx = self.get_transaction();
        for (key, version, value) in kvs.iter() {
            trx.add_operations(vec![Operation::Put(
                key.to_vec(),
                value.to_vec(),
                vec![WithVersion(*version)],
            )]);
        }
        trx.commit().map_err(|e| e.into())
    }

    /// refer to `Operation::Delete`
    fn delete_impl(&self, keys_with_opts: Vec<(Key, Vec<OperationOption>)>) -> Result<()> {
        let mut trx = self.get_transaction();
        for (key, opts) in keys_with_opts {
            trx.add_operations(vec![Operation::Delete(key, opts)]);
        }
        trx.commit().map_err(|e| e.into())
    }
}

#[async_trait]
impl MetaStore for SledMetaStore {
    async fn list(&self) -> Result<Vec<Vec<u8>>> {
        self.list_cf(DEFAULT_COLUMN_FAMILY).await
    }

    async fn put(&self, key: &[u8], value: &[u8], version: Epoch) -> Result<()> {
        self.put_cf(DEFAULT_COLUMN_FAMILY, key, value, version)
            .await
    }

    async fn put_batch(&self, tuples: Vec<(Vec<u8>, Vec<u8>, Epoch)>) -> Result<()> {
        self.put_batch_cf(
            tuples
                .into_iter()
                .map(|(k, v, e)| (DEFAULT_COLUMN_FAMILY, k, v, e))
                .collect_vec(),
        )
        .await
    }

    async fn get(&self, key: &[u8], version: Epoch) -> Result<Vec<u8>> {
        self.get_cf(DEFAULT_COLUMN_FAMILY, key, version).await
    }

    async fn delete(&self, key: &[u8], version: Epoch) -> Result<()> {
        self.delete_cf(DEFAULT_COLUMN_FAMILY, key, version).await
    }

    async fn delete_all(&self, key: &[u8]) -> Result<()> {
        self.delete_all_cf(DEFAULT_COLUMN_FAMILY, key).await
    }

    async fn list_cf(&self, cf: &str) -> Result<Vec<Vec<u8>>> {
        Ok(self.list_batch_cf(vec![cf]).await?.pop().unwrap())
    }

    async fn list_batch_cf(&self, cfs: Vec<&str>) -> Result<Vec<Vec<Vec<u8>>>> {
        let mut batch_result = vec![vec![]; cfs.len()];
        let keys_with_opts = cfs
            .iter()
            .map(|cf| {
                (
                    ColumnFamilyUtils::prefix_key_with_cf(&[], cf.as_bytes()),
                    vec![WithPrefix()],
                )
            })
            .collect_vec();
        for kv in self.get_impl(keys_with_opts)? {
            for (index, cf) in cfs.iter().enumerate() {
                if ColumnFamilyUtils::get_cf_from_prefixed_key(kv.key()).as_slice() == cf.as_bytes()
                {
                    batch_result[index].push(kv.value().clone());
                    continue;
                }
            }
        }
        Ok(batch_result)
    }

    async fn put_cf(&self, cf: &str, key: &[u8], value: &[u8], version: Epoch) -> Result<()> {
        self.put_impl(&[(
            ColumnFamilyUtils::prefix_key_with_cf(key, cf.as_bytes()),
            version.into_inner(),
            value.to_vec(),
        )])
    }

    async fn put_batch_cf(&self, tuples: Vec<(&str, Vec<u8>, Vec<u8>, Epoch)>) -> Result<()> {
        let ops = tuples
            .into_iter()
            .map(|(cf, k, v, e)| {
                (
                    ColumnFamilyUtils::prefix_key_with_cf(k.as_slice(), cf.as_bytes()),
                    e.into_inner(),
                    v,
                )
            })
            .collect_vec();
        self.put_impl(&ops)
    }

    async fn get_cf(&self, cf: &str, key: &[u8], version: Epoch) -> Result<Vec<u8>> {
        let result = self
            .get_impl(vec![(
                ColumnFamilyUtils::prefix_key_with_cf(key, cf.as_bytes()),
                vec![WithVersion(version.into_inner())],
            )])
            .map(|kvs| kvs.into_iter().map(|kv| kv.into_value()).last())?;
        result.ok_or_else(|| ErrorCode::ItemNotFound("entry not found".to_string()).into())
    }

    async fn delete_cf(&self, cf: &str, key: &[u8], version: Epoch) -> Result<()> {
        self.delete_impl(vec![(
            ColumnFamilyUtils::prefix_key_with_cf(key, cf.as_bytes()),
            vec![WithVersion(version.into_inner())],
        )])
    }

    async fn delete_all_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        self.delete_impl(vec![(
            ColumnFamilyUtils::prefix_key_with_cf(key, cf.as_bytes()),
            vec![],
        )])
    }

    fn get_transaction(&self) -> Box<dyn Transaction> {
        Box::new(SledTransaction::new(self.db.clone()))
    }
}

pub struct SledTransaction {
    db: Arc<RwLock<sled::Db>>,
    preconditions: Vec<Precondition>,
    operations: Vec<Operation>,
}

impl SledTransaction {
    fn new(db: Arc<RwLock<sled::Db>>) -> SledTransaction {
        SledTransaction {
            db,
            preconditions: vec![],
            operations: vec![],
        }
    }

    fn apply_operations(
        tx_db: &sled::transaction::TransactionalTree,
        operations: &[Operation],
        operations_meta: &[Option<Batch>],
    ) -> std::result::Result<(), sled::transaction::UnabortableTransactionError> {
        for (index, operation) in operations.iter().enumerate() {
            match operation {
                Operation::Put(k, v, opts) => {
                    SledTransaction::apply_put(tx_db, k, v, opts)?;
                }
                Operation::Delete(k, opts) => {
                    SledTransaction::apply_delete(
                        tx_db,
                        k,
                        opts,
                        operations_meta.get(index).unwrap(),
                    )?;
                }
            }
        }
        Ok(())
    }

    fn apply_put(
        tx_db: &sled::transaction::TransactionalTree,
        key: &[u8],
        value: &[u8],
        opts: &[OperationOption],
    ) -> std::result::Result<(), sled::transaction::UnabortableTransactionError> {
        let mut key_version = SINGLE_VERSION_EPOCH.into_inner();
        for opt in opts.iter() {
            if let OperationOption::WithVersion(version) = opt {
                key_version = *version;
            }
        }
        let composed_key = KeyWithVersion::compose_key_version(key, key_version);
        tx_db.insert(composed_key.as_slice(), value)?;
        tx_db.flush();
        Ok(())
    }

    /// sled doesn't have a atomic range delete, so we combine a range get and batch delete here.
    /// The atomicity is guaranteed by `db_guard`.
    fn apply_delete(
        tx_db: &sled::transaction::TransactionalTree,
        key: &Key,
        opts: &[OperationOption],
        operations_meta: &Option<Batch>,
    ) -> std::result::Result<(), sled::transaction::UnabortableTransactionError> {
        let mut key_version = None;
        for opt in opts.iter() {
            if let OperationOption::WithVersion(version) = opt {
                key_version = Some(*version);
            }
        }
        match key_version {
            Some(version) => {
                // delete specified version
                let composed_key = KeyWithVersion::compose_key_version(key, version);
                tx_db.remove(composed_key.as_slice())?;
            }
            None => {
                // delete all versions
                tx_db.apply_batch(operations_meta.as_ref().unwrap())?;
            }
        }

        tx_db.flush();
        Ok(())
    }

    fn check(
        &self,
        precondition: &Precondition,
        tx_db: &sled::transaction::TransactionalTree,
    ) -> std::result::Result<bool, sled::transaction::UnabortableTransactionError> {
        match precondition {
            Precondition::KeyExists { key, version } => {
                let composed_key = KeyWithVersion::compose_key_version(
                    key,
                    version.unwrap_or(SINGLE_VERSION_EPOCH.into_inner()),
                );
                match tx_db.get(composed_key)? {
                    None => Ok(false),
                    Some(_) => Ok(true),
                }
            }
        }
    }
}

/// sled support serializable transactions
impl Transaction for SledTransaction {
    fn add_preconditions(&mut self, preconditions: Vec<Precondition>) {
        self.preconditions.extend(preconditions);
    }

    fn add_operations(&mut self, operations: Vec<Operation>) {
        self.operations.extend(operations);
    }

    fn commit(&self) -> std::result::Result<(), crate::storage::Error> {
        let db_guard = self.db.write().unwrap();
        // workaround for sled's lack of range delete
        let mut operations_meta = vec![None; self.operations.len()];
        for (index, operation) in self.operations.iter().enumerate() {
            if let Delete(key, opts) = operation {
                if opts.iter().any(|opt| matches!(opt, WithVersion(_))) {
                    continue;
                }
                // Version is not specified, so perform a range delete.
                let mut batch = Batch::default();
                db_guard
                    .scan_prefix(key)
                    .collect::<std::result::Result<Vec<(IVec, IVec)>, sled::Error>>()?
                    .into_iter()
                    .map(|(k, v)| {
                        KeyValue::new(
                            KeyWithVersion::get_key(&k),
                            v.to_vec(),
                            KeyWithVersion::get_version(&k).into_inner(),
                        )
                    })
                    .filter(|kv| kv.key() == key)
                    .for_each(|kv| {
                        batch.remove(KeyWithVersion::compose_key_version(kv.key(), kv.version()))
                    });
                operations_meta[index] = Some(batch);
            }
        }

        db_guard
            .transaction(|tx_db| {
                let check_result: std::result::Result<
                    Vec<bool>,
                    sled::transaction::UnabortableTransactionError,
                > = self
                    .preconditions
                    .iter()
                    .map(|precondition| self.check(precondition, tx_db))
                    .collect();
                if check_result?.into_iter().all(|b| b) {
                    // all preconditions are met
                    SledTransaction::apply_operations(tx_db, &self.operations, &operations_meta)?;
                } else {
                    // some preconditions are not met
                    return sled::transaction::abort(());
                }
                Ok(())
            })
            .map_err(|e: TransactionError<()>| match e {
                TransactionError::Abort(_) => crate::storage::Error::TransactionAbort(),
                TransactionError::Storage(err) => {
                    crate::storage::Error::StorageError(err.to_string())
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use super::*;

    #[tokio::test]
    async fn test_sled_metastore_basic() -> Result<()> {
        let tempdir = tempfile::tempdir().unwrap();
        let meta_store = SledMetaStore::new(tempdir.path())?;
        let result = meta_store.get_impl(vec![("key1".as_bytes().to_vec(), vec![])])?;
        assert!(result.is_empty());
        let result = meta_store.put_impl(&[(
            "key1".as_bytes().to_vec(),
            SINGLE_VERSION_EPOCH.into_inner(),
            "value1".as_bytes().to_vec(),
        )]);
        assert!(result.is_ok());
        let result = meta_store.get_impl(vec![("key1".as_bytes().to_vec(), vec![])])?;
        assert_eq!(
            result,
            vec![KeyValue::new(
                "key1".as_bytes().to_vec(),
                "value1".as_bytes().to_vec(),
                SINGLE_VERSION_EPOCH.into_inner()
            )]
        );
        meta_store.delete_impl(vec![("key1".as_bytes().to_vec(), vec![])])?;
        let result = meta_store.get_impl(vec![("key1".as_bytes().to_vec(), vec![])])?;
        assert!(result.is_empty());

        meta_store.put_impl(&[(
            "key2".as_bytes().to_vec(),
            SINGLE_VERSION_EPOCH.into_inner(),
            "value2".as_bytes().to_vec(),
        )])?;
        drop(meta_store);
        let meta_store = SledMetaStore::new(tempdir.path())?;
        let result = meta_store.get_impl(vec![("key2".as_bytes().to_vec(), vec![])]);
        assert_eq!(
            result.unwrap(),
            vec![KeyValue::new(
                "key2".as_bytes().to_vec(),
                "value2".as_bytes().to_vec(),
                SINGLE_VERSION_EPOCH.into_inner()
            )]
        );

        meta_store
            .put_batch_cf(vec![
                (
                    "cf/cf1",
                    "k22".as_bytes().to_vec(),
                    "v22".as_bytes().to_vec(),
                    Epoch::from(15),
                ),
                (
                    "cf/cf1",
                    "k1".as_bytes().to_vec(),
                    "v2".as_bytes().to_vec(),
                    Epoch::from(20),
                ),
                (
                    "cf/cf1",
                    "k1".as_bytes().to_vec(),
                    "v3".as_bytes().to_vec(),
                    Epoch::from(30),
                ),
                (
                    "cf/cf1",
                    "k1".as_bytes().to_vec(),
                    "v1".as_bytes().to_vec(),
                    Epoch::from(10),
                ),
                (
                    "cf/another_cf",
                    "another_k1".as_bytes().to_vec(),
                    "another_v1".as_bytes().to_vec(),
                    Epoch::from(2),
                ),
            ])
            .await?;
        let result = meta_store
            .list_batch_cf(vec!["cf/cf1", "cf/another_cf"])
            .await?;
        assert_eq!(2, result.len());
        assert!(result[0]
            .iter()
            .map(|v| std::str::from_utf8(v).unwrap())
            .sorted()
            .eq(vec!["v22", "v3"]));
        assert!(result[1]
            .iter()
            .map(|v| std::str::from_utf8(v).unwrap())
            .sorted()
            .eq(vec!["another_v1"]));

        Ok(())
    }

    #[tokio::test]
    async fn test_sled_metastore_with_op_options() -> Result<()> {
        let tempdir = tempfile::tempdir().unwrap();
        let meta_store = SledMetaStore::new(tempdir.path())?;
        // put with one kv
        meta_store.put_impl(&[(
            "key1".as_bytes().to_vec(),
            118,
            "value1_greatest_version".as_bytes().to_vec(),
        )])?;

        // put several kvs with different versions
        meta_store.put_impl(&[
            (
                "key2".as_bytes().to_vec(),
                115,
                "value2_smallest_version".as_bytes().to_vec(),
            ),
            (
                "key2".as_bytes().to_vec(),
                121,
                "value2_greatest_version".as_bytes().to_vec(),
            ),
            (
                "key2".as_bytes().to_vec(),
                118,
                "value2_medium_version".as_bytes().to_vec(),
            ),
        ])?;

        // get the kv with greatest version
        let result = meta_store.get_impl(vec![("key2".as_bytes().to_vec(), vec![])])?;
        assert_eq!(
            vec![KeyValue::new(
                "key2".as_bytes().to_vec(),
                "value2_greatest_version".as_bytes().to_vec(),
                121
            )],
            result
        );

        // get the kv with specified version
        let result = meta_store.get_impl(vec![(
            "key2".as_bytes().to_vec(),
            vec![OperationOption::WithVersion(118)],
        )])?;
        assert_eq!(
            vec![KeyValue::new(
                "key2".as_bytes().to_vec(),
                "value2_medium_version".as_bytes().to_vec(),
                118
            )],
            result
        );

        // Key with specified version doesn't exist. There is kv with the same key but different
        // version.
        let result = meta_store.get_impl(vec![(
            "key2".as_bytes().to_vec(),
            vec![OperationOption::WithVersion(123)],
        )])?;
        assert!(result.is_empty());

        // Key with specified version doesn't exist. There is no kvs with the same key.
        let result = meta_store.get_impl(vec![(
            "key0".as_bytes().to_vec(),
            vec![OperationOption::WithVersion(118)],
        )])?;
        assert!(result.is_empty());

        // Key with greatest version doesn't exist. There is no kvs with the same key.
        let result = meta_store.get_impl(vec![("key0".as_bytes().to_vec(), vec![])])?;
        assert!(result.is_empty());

        // get kvs prefixed by specified string with greatest version
        let mut result = meta_store.get_impl(vec![(
            "key".as_bytes().to_vec(),
            vec![OperationOption::WithPrefix()],
        )])?;
        result.sort_by_key(|kv| kv.key().clone());
        assert_eq!(
            vec![
                KeyValue::new(
                    "key1".as_bytes().to_vec(),
                    "value1_greatest_version".as_bytes().to_vec(),
                    118
                ),
                KeyValue::new(
                    "key2".as_bytes().to_vec(),
                    "value2_greatest_version".as_bytes().to_vec(),
                    121
                )
            ],
            result
        );

        // kvs prefixed by specified prefix with default version doesn't exist. The prefix string
        // matches no kvs.
        let result = meta_store.get_impl(vec![(
            "key2-".as_bytes().to_vec(),
            vec![OperationOption::WithPrefix()],
        )])?;
        assert!(result.is_empty());

        // kvs prefixed by specified string with specified version doesn't exist. The prefix string
        // matches no kvs.
        let result = meta_store.get_impl(vec![(
            "key1-".as_bytes().to_vec(),
            vec![
                OperationOption::WithVersion(118),
                OperationOption::WithPrefix(),
            ],
        )])?;
        assert!(result.is_empty());

        // get kvs prefixed by specified string with specified version
        let mut result = meta_store.get_impl(vec![(
            "key".as_bytes().to_vec(),
            vec![
                OperationOption::WithVersion(118),
                OperationOption::WithPrefix(),
            ],
        )])?;
        result.sort_by_key(|kv| kv.key().clone());
        assert_eq!(
            vec![
                KeyValue::new(
                    "key1".as_bytes().to_vec(),
                    "value1_greatest_version".as_bytes().to_vec(),
                    118
                ),
                KeyValue::new(
                    "key2".as_bytes().to_vec(),
                    "value2_medium_version".as_bytes().to_vec(),
                    118
                )
            ],
            result
        );

        // delete specific version
        meta_store.delete_impl(vec![
            ("key1".as_bytes().to_vec(), vec![WithVersion(118)]),
            ("key2".as_bytes().to_vec(), vec![WithVersion(118)]),
        ])?;
        let result = meta_store.get_impl(vec![(
            "key".as_bytes().to_vec(),
            vec![
                OperationOption::WithVersion(118),
                OperationOption::WithPrefix(),
            ],
        )])?;
        assert!(result.is_empty());

        // delete all versions
        meta_store.delete_impl(vec![("key2".as_bytes().to_vec(), vec![])])?;
        let result = meta_store.get_impl(vec![("key2".as_bytes().to_vec(), vec![])])?;
        assert!(result.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_sled_transaction() -> Result<()> {
        let tempdir = tempfile::tempdir().unwrap();
        let meta_store = SledMetaStore::new(tempdir.path())?;

        let mut trx = meta_store.get_transaction();
        trx.add_preconditions(vec![]);
        trx.add_operations(vec![
            Operation::Put(
                "key1".as_bytes().to_vec(),
                "value1".as_bytes().to_vec(),
                vec![WithVersion(10)],
            ),
            Operation::Put(
                "key11".as_bytes().to_vec(),
                "value11".as_bytes().to_vec(),
                vec![WithVersion(10)],
            ),
        ]);
        trx.commit()?;
        let mut result = meta_store.get_impl(vec![(
            "key1".as_bytes().to_vec(),
            vec![OperationOption::WithPrefix()],
        )])?;
        result.sort_by_key(|kv| kv.key().clone());
        // the operations are executed
        assert_eq!(
            vec![
                KeyValue::new("key1".as_bytes().to_vec(), "value1".as_bytes().to_vec(), 10),
                KeyValue::new(
                    "key11".as_bytes().to_vec(),
                    "value11".as_bytes().to_vec(),
                    10
                )
            ],
            result
        );

        let mut trx = meta_store.get_transaction();
        trx.add_preconditions(vec![Precondition::KeyExists {
            key: "key111".as_bytes().to_vec(),
            version: None,
        }]);
        trx.add_operations(vec![Operation::Put(
            "key1111".as_bytes().to_vec(),
            "value1111".as_bytes().to_vec(),
            vec![WithVersion(15)],
        )]);
        let result = trx.commit();
        assert!(result.is_err());
        assert_matches!(
            result.unwrap_err(),
            crate::storage::Error::TransactionAbort()
        );
        Ok(())
    }
}
