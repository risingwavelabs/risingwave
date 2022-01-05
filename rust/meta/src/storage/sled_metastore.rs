use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::RwError;
use risingwave_common::error::{ErrorCode, Result};
use sled::transaction::TransactionError;
use sled::IVec;

use crate::manager::{Epoch, SINGLE_VERSION_EPOCH};
use crate::storage::transaction::{Operation, Precondition, Transaction};
use crate::storage::OperationOption::{WithPrefix, WithVersion};
use crate::storage::{
    ColumnFamilyUtils, KeyWithVersion, MetaStore, OperationOption, DEFAULT_COLUMN_FAMILY,
};

impl From<sled::Error> for crate::storage::Error {
    fn from(e: sled::Error) -> Self {
        crate::storage::Error::StorageError(e.to_string())
    }
}

pub struct SledMetaStore {
    db: Arc<sled::Db>,
}

impl SledMetaStore {
    pub fn new(db_path: &std::path::Path) -> Result<SledMetaStore> {
        let db =
            sled::open(db_path).map_err(|e| crate::storage::Error::StorageError(e.to_string()))?;
        Ok(SledMetaStore { db: Arc::new(db) })
    }

    fn get_key_with_version(key: &[u8], epoch: Epoch) -> KeyWithVersion {
        KeyWithVersion::compose(key, epoch)
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

    async fn delete_all(&self, _key: &[u8]) -> Result<()> {
        unimplemented!()
    }

    async fn list_cf(&self, cf: &str) -> Result<Vec<Vec<u8>>> {
        self.get_v2(
            ColumnFamilyUtils::prefix_key_with_cf("".as_bytes(), cf.as_bytes()),
            vec![WithPrefix()],
        )
        .await
        .map(|kvs| kvs.into_iter().map(|(_k, v)| v).collect_vec())
    }

    async fn list_batch_cf(&self, _cfs: Vec<&str>) -> Result<Vec<Vec<Vec<u8>>>> {
        unimplemented!()
    }

    async fn put_cf(&self, cf: &str, key: &[u8], value: &[u8], version: Epoch) -> Result<()> {
        self.put_v2(
            ColumnFamilyUtils::prefix_key_with_cf(key, cf.as_bytes()),
            // TODO fix unnecessary copy
            value.to_vec(),
            vec![WithVersion(version)],
        )
        .await
    }

    async fn put_batch_cf(&self, tuples: Vec<(&str, Vec<u8>, Vec<u8>, Epoch)>) -> Result<()> {
        let mut trx = self.get_transaction();
        let ops = tuples
            .into_iter()
            .map(|(cf, k, v, e)| {
                Operation::Put(
                    ColumnFamilyUtils::prefix_key_with_cf(k.as_slice(), cf.as_bytes()),
                    v,
                    vec![WithVersion(e)],
                )
            })
            .collect_vec();
        trx.add_operations(ops);
        trx.commit().map_err(|e| e.into())
    }

    async fn get_cf(&self, cf: &str, key: &[u8], version: Epoch) -> Result<Vec<u8>> {
        let result = self
            .get_v2(
                ColumnFamilyUtils::prefix_key_with_cf(key, cf.as_bytes()),
                vec![WithVersion(version)],
            )
            .await
            .map(|kvs| kvs.into_iter().map(|(_k, v)| v).reduce(|_pre, cur| cur))?;
        result.ok_or_else(|| ErrorCode::ItemNotFound("entry not found".to_string()).into())
    }

    async fn delete_cf(&self, cf: &str, key: &[u8], version: Epoch) -> Result<()> {
        self.delete_v2(
            ColumnFamilyUtils::prefix_key_with_cf(key, cf.as_bytes()),
            vec![WithVersion(version)],
        )
        .await
    }

    async fn delete_all_cf(&self, _cf: &str, _key: &[u8]) -> Result<()> {
        unimplemented!()
    }

    async fn get_v2(
        &self,
        key: Vec<u8>,
        opts: Vec<OperationOption>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut key_version = &SINGLE_VERSION_EPOCH;
        let mut key_prefix: Option<&Vec<u8>> = None;
        for opt in &opts {
            match opt {
                OperationOption::WithPrefix() => {
                    key_prefix = Some(&key);
                }
                OperationOption::WithVersion(epoch) => {
                    key_version = epoch;
                }
            }
        }
        match key_prefix {
            Some(prefix) => {
                let result: Result<Vec<(IVec, IVec)>> = self
                    .db
                    .scan_prefix(prefix)
                    .map(|r| {
                        r.map_err(|e| {
                            RwError::from(crate::storage::Error::StorageError(e.to_string()))
                        })
                    })
                    .collect();
                let kvs = result?
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .filter(|(k, _v)| KeyWithVersion::get_version(k) == *key_version)
                    .map(|(k, v)| (KeyWithVersion::get_key(&k), v))
                    .collect();
                Ok(kvs)
            }
            None => {
                let composed_key = SledMetaStore::get_key_with_version(&key, *key_version);
                let result = self
                    .db
                    .get(composed_key.into_inner())
                    .map_err(|e| crate::storage::Error::StorageError(e.to_string()))?
                    .map(|iv| iv.to_vec());
                match result {
                    Some(item) => Ok(vec![(key, item)]),
                    None => Ok(vec![]),
                }
            }
        }
    }

    async fn put_v2(&self, key: Vec<u8>, value: Vec<u8>, opts: Vec<OperationOption>) -> Result<()> {
        let mut trx = self.get_transaction();
        trx.add_operations(vec![Operation::Put(key, value, opts)]);
        trx.commit().map_err(RwError::from)
    }

    async fn delete_v2(&self, key: Vec<u8>, opts: Vec<OperationOption>) -> Result<()> {
        let mut trx = self.get_transaction();
        trx.add_operations(vec![Operation::Delete(key, opts)]);
        trx.commit().map_err(RwError::from)
    }

    fn get_transaction(&self) -> Box<dyn Transaction> {
        Box::new(SledTransaction::new(self.db.clone()))
    }
}

pub struct SledTransaction {
    db: Arc<sled::Db>,
    preconditions: Vec<Precondition>,
    operations: Vec<Operation>,
}

impl SledTransaction {
    fn new(db: Arc<sled::Db>) -> SledTransaction {
        SledTransaction {
            db,
            preconditions: vec![],
            operations: vec![],
        }
    }

    fn apply_operations(
        tx_db: &sled::transaction::TransactionalTree,
        operations: &[Operation],
    ) -> std::result::Result<(), sled::transaction::UnabortableTransactionError> {
        for operation in operations.iter() {
            match operation {
                Operation::Put(k, v, opts) => {
                    SledTransaction::apply_put(tx_db, k, v, opts)?;
                }
                Operation::Delete(k, opts) => {
                    SledTransaction::apply_delete(tx_db, k, opts)?;
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
        let mut key_version = &SINGLE_VERSION_EPOCH;
        for opt in opts.iter() {
            if let OperationOption::WithVersion(epoch) = opt {
                key_version = epoch;
            }
        }
        let composed_key = SledMetaStore::get_key_with_version(key, *key_version);
        tx_db.insert(composed_key.into_inner().as_slice(), value)?;
        tx_db.flush();
        Ok(())
    }

    fn apply_delete(
        tx_db: &sled::transaction::TransactionalTree,
        key: &[u8],
        opts: &[OperationOption],
    ) -> std::result::Result<(), sled::transaction::UnabortableTransactionError> {
        let mut key_version = &SINGLE_VERSION_EPOCH;
        for opt in opts.iter() {
            if let OperationOption::WithVersion(epoch) = opt {
                key_version = epoch;
            }
        }
        let composed_key = SledMetaStore::get_key_with_version(key, *key_version);
        tx_db.remove(composed_key.into_inner().as_slice())?;
        tx_db.flush();
        Ok(())
    }

    fn check(
        &self,
        precondition: &Precondition,
        tx_db: &sled::transaction::TransactionalTree,
    ) -> std::result::Result<bool, sled::transaction::UnabortableTransactionError> {
        match precondition {
            Precondition::KeyExists(key_exists) => {
                let composed_key = SledMetaStore::get_key_with_version(
                    key_exists.key(),
                    key_exists.version().unwrap_or(SINGLE_VERSION_EPOCH),
                );
                match tx_db.get(composed_key.into_inner())? {
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
        self.db
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
                    SledTransaction::apply_operations(tx_db, &self.operations)?;
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
    use crate::storage::transaction::KeyExists;

    #[tokio::test]
    async fn test_sled_metastore_basic() -> Result<()> {
        let tempdir = tempfile::tempdir().unwrap();
        let meta_store = SledMetaStore::new(tempdir.path())?;
        let result = meta_store
            .get_v2("key1".as_bytes().to_vec(), vec![])
            .await?;
        assert!(result.is_empty());
        let result = meta_store
            .put_v2(
                "key1".as_bytes().to_vec(),
                "value1".as_bytes().to_vec(),
                vec![],
            )
            .await;
        assert!(result.is_ok());
        let result = meta_store
            .get_v2("key1".as_bytes().to_vec(), vec![])
            .await?;
        assert_eq!(
            result,
            vec![("key1".as_bytes().to_vec(), "value1".as_bytes().to_vec())]
        );
        let result = meta_store
            .delete_v2("key1".as_bytes().to_vec(), vec![])
            .await;
        assert!(result.is_ok());
        let result = meta_store
            .get_v2("key1".as_bytes().to_vec(), vec![])
            .await?;
        assert!(result.is_empty());

        meta_store
            .put_v2(
                "key2".as_bytes().to_vec(),
                "value2".as_bytes().to_vec(),
                vec![],
            )
            .await?;
        drop(meta_store);
        let meta_store = SledMetaStore::new(tempdir.path())?;
        let result = meta_store.get_v2("key2".as_bytes().to_vec(), vec![]).await;
        assert_eq!(
            result.unwrap(),
            vec![("key2".as_bytes().to_vec(), "value2".as_bytes().to_vec())]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sled_metastore_with_op_options() -> Result<()> {
        let tempdir = tempfile::tempdir().unwrap();
        let meta_store = SledMetaStore::new(tempdir.path())?;
        meta_store
            .put_v2(
                "key1".as_bytes().to_vec(),
                "value1".as_bytes().to_vec(),
                vec![OperationOption::WithVersion(Epoch::from(123))],
            )
            .await?;
        meta_store
            .put_v2(
                "key2".as_bytes().to_vec(),
                "value2".as_bytes().to_vec(),
                vec![OperationOption::WithVersion(Epoch::from(123))],
            )
            .await?;
        let result = meta_store
            .get_v2("key1".as_bytes().to_vec(), vec![])
            .await?;
        assert!(result.is_empty());
        let result = meta_store
            .get_v2(
                "key0".as_bytes().to_vec(),
                vec![OperationOption::WithVersion(Epoch::from(123))],
            )
            .await?;
        assert!(result.is_empty());
        let result = meta_store
            .get_v2(
                "key1".as_bytes().to_vec(),
                vec![OperationOption::WithVersion(Epoch::from(123))],
            )
            .await?;
        assert_eq!(
            vec![("key1".as_bytes().to_vec(), "value1".as_bytes().to_vec())],
            result
        );
        let result = meta_store
            .get_v2(
                "key2".as_bytes().to_vec(),
                vec![OperationOption::WithVersion(Epoch::from(123))],
            )
            .await?;
        assert_eq!(
            vec![("key2".as_bytes().to_vec(), "value2".as_bytes().to_vec())],
            result
        );
        let result = meta_store
            .get_v2(
                "key".as_bytes().to_vec(),
                vec![OperationOption::WithPrefix()],
            )
            .await?;
        assert!(result.is_empty());
        let result = meta_store
            .get_v2(
                "key".as_bytes().to_vec(),
                vec![
                    OperationOption::WithVersion(Epoch::from(123)),
                    OperationOption::WithPrefix(),
                ],
            )
            .await?;
        assert_eq!(
            vec![
                ("key1".as_bytes().to_vec(), "value1".as_bytes().to_vec()),
                ("key2".as_bytes().to_vec(), "value2".as_bytes().to_vec())
            ],
            result
        );
        let result = meta_store
            .get_v2(
                "key".as_bytes().to_vec(),
                vec![
                    OperationOption::WithPrefix(),
                    OperationOption::WithVersion(Epoch::from(123)),
                ],
            )
            .await?;
        assert_eq!(
            vec![
                ("key1".as_bytes().to_vec(), "value1".as_bytes().to_vec()),
                ("key2".as_bytes().to_vec(), "value2".as_bytes().to_vec())
            ],
            result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_sled_transaction() -> Result<()> {
        let tempdir = tempfile::tempdir().unwrap();
        let meta_store = SledMetaStore::new(tempdir.path())?;
        let mut trx = meta_store.get_transaction();
        trx.add_preconditions(vec![]);
        trx.add_operations(vec![]);
        trx.commit()?;

        let mut trx = meta_store.get_transaction();
        trx.add_preconditions(vec![]);
        trx.add_operations(vec![
            Operation::Put(
                "key1".as_bytes().to_vec(),
                "value1".as_bytes().to_vec(),
                vec![],
            ),
            Operation::Put(
                "key11".as_bytes().to_vec(),
                "value11".as_bytes().to_vec(),
                vec![],
            ),
        ]);
        trx.commit()?;
        let result = meta_store
            .get_v2(
                "key1".as_bytes().to_vec(),
                vec![OperationOption::WithPrefix()],
            )
            .await?;
        // the operations are executed
        assert_eq!(
            vec![
                ("key1".as_bytes().to_vec(), "value1".as_bytes().to_vec()),
                ("key11".as_bytes().to_vec(), "value11".as_bytes().to_vec())
            ],
            result
        );

        let mut trx = meta_store.get_transaction();
        trx.add_preconditions(vec![Precondition::KeyExists(KeyExists::new(
            "key111".as_bytes().to_vec(),
            None,
        ))]);
        trx.add_operations(vec![Operation::Put(
            "key1111".as_bytes().to_vec(),
            "value1111".as_bytes().to_vec(),
            vec![],
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
