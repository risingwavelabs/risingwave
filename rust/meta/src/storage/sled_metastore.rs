use std::collections::HashSet;
use std::sync::RwLock;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::RwError;
use risingwave_common::error::{ErrorCode, Result};
use sled::transaction::TransactionError;
use sled::{Batch, IVec};

use crate::manager::{Epoch, SINGLE_VERSION_EPOCH};
use crate::storage::transaction::{Operation, Precondition, Transaction};
use crate::storage::Operation::Delete;
use crate::storage::{
    ColumnFamily, Key, KeyValue, KeyValueVersion, KeyWithVersion, MetaStore, Value,
};

impl From<sled::Error> for crate::storage::Error {
    fn from(e: sled::Error) -> Self {
        crate::storage::Error::StorageError(e.to_string())
    }
}

/// Mimic the cf interface for sled
struct ColumnFamilyUtils {}
impl ColumnFamilyUtils {
    /// Return a composed key which is cf + raw key
    pub fn get_composed_key(key: impl AsRef<[u8]>, cf: impl AsRef<[u8]>) -> Vec<u8> {
        let cf_len: u8 = cf
            .as_ref()
            .len()
            .try_into()
            .expect("cf length out of u8 range");
        [&[cf_len], cf.as_ref(), key.as_ref()].concat()
    }

    /// Extract the raw key from a composed key (cf + raw key)
    pub fn get_raw_key(composed_key: impl AsRef<[u8]>) -> Vec<u8> {
        let cf_len = composed_key.as_ref().get(0).expect("invalid composed_key");
        composed_key.as_ref()[*cf_len as usize + 1..].to_vec()
    }
}

pub struct SledMetaStore {
    /// The rwlock is to ensure serializable isolation.
    db: RwLock<sled::Db>,
}

/// `SledMetaStore` stores a key composed of `KeyValue.key` and `KeyValue.version`.
/// Currently the implementation involves some additional Vec<u8> copy or clone, but it should be OK
/// for now because `SledMetaStore` is for testing purpose only and performance is not the major
/// concern.
impl SledMetaStore {
    pub fn new(db_path: Option<&std::path::Path>) -> Result<SledMetaStore> {
        let db = match db_path {
            None => sled::Config::default()
                .mode(sled::Mode::HighThroughput)
                .temporary(true)
                .flush_every_ms(None)
                .open()
                .unwrap(),
            Some(db_path) => sled::Config::default()
                .mode(sled::Mode::HighThroughput)
                .path(db_path)
                .flush_every_ms(None)
                .open()
                .unwrap(),
        };
        Ok(SledMetaStore {
            db: RwLock::new(db),
        })
    }

    /// `get` key value pairs matched by `OperationOption`s.
    /// For each (Key, Vec<OperationOption>) in `keys_with_opts`,
    /// If `WithPrefix` is specified, key value pair is matched by prefix `Key`.
    /// If `WithVersion` is not specified, the greatest version of this key value pair is matched.
    /// Otherwise, the specific version is matched if any.
    /// `WithPrefix` and `WithVersion` are compatible.
    fn get_impl(
        &self,
        keys_with_opts: &[(ColumnFamily, Key, Option<KeyValueVersion>, bool)],
    ) -> Result<Vec<Vec<KeyValue>>> {
        let db_guard = self.db.read().unwrap();
        let mut batch_result = vec![];
        for (cf, key, version, by_prefix) in keys_with_opts {
            let (range_start, range_end) = {
                if *by_prefix {
                    KeyWithVersion::range_lookup_key_range(ColumnFamilyUtils::get_composed_key(
                        key, cf,
                    ))
                } else {
                    KeyWithVersion::point_lookup_key_range(ColumnFamilyUtils::get_composed_key(
                        key, cf,
                    ))
                }
            };

            // 1. Get all matched kvs with any versions
            let matched_keys = db_guard
                .range(range_start.as_slice()..range_end.as_slice())
                .map(|r| {
                    r.map_err(|e| RwError::from(crate::storage::Error::StorageError(e.to_string())))
                })
                .collect::<Result<Vec<(IVec, IVec)>>>()?
                .into_iter()
                .map(|(k, v)| {
                    let (composed_key, version) = KeyWithVersion::deserialize(k.as_ref()).unwrap();
                    KeyValue::new(
                        cf.to_string(),
                        ColumnFamilyUtils::get_raw_key(composed_key),
                        v.to_vec(),
                        version,
                    )
                });
            // 2. Select kvs with target version
            let matched_versions = match version {
                Some(key_version) => {
                    // Get all kvs with the specified key_version
                    matched_keys
                        .filter(|kv| kv.version() == *key_version)
                        .collect_vec()
                }
                None => {
                    // Get all kvs with their greatest versions. Entries with the same key are
                    // already ordered by version desc.
                    let mut seen: HashSet<Key> = HashSet::new();
                    matched_keys
                        .filter(|kv| {
                            if seen.contains(kv.key()) {
                                return false;
                            }
                            // TODO: avoid clone
                            seen.insert(kv.key().to_owned());
                            true
                        })
                        .collect_vec()
                }
            };
            batch_result.push(matched_versions);
        }
        Ok(batch_result)
    }

    fn get_impl_flatten(
        &self,
        keys_with_opts: &[(ColumnFamily, Key, Option<KeyValueVersion>, bool)],
    ) -> Result<Vec<KeyValue>> {
        self.get_impl(keys_with_opts)
            .map(|result| result.into_iter().flatten().collect_vec())
    }

    /// refer to `Operation::Put`
    async fn put_impl(&self, kvs: Vec<(ColumnFamily, Key, KeyValueVersion, Value)>) -> Result<()> {
        let mut trx = self.get_transaction();
        trx.add_operations(
            kvs.into_iter()
                .map(|(cf, key, version, value)| Operation::Put(cf, key, value, Some(version)))
                .collect_vec(),
        );
        self.commit_transaction(&mut trx).await
    }

    /// refer to `Operation::Delete`
    async fn delete_impl(
        &self,
        keys_with_opts: Vec<(ColumnFamily, Key, Option<KeyValueVersion>)>,
    ) -> Result<()> {
        let mut trx = self.get_transaction();
        trx.add_operations(
            keys_with_opts
                .into_iter()
                .map(|(cf, key, version)| Operation::Delete(cf, key, version))
                .collect_vec(),
        );
        self.commit_transaction(&mut trx).await
    }

    fn apply_operations(
        tx_db: &sled::transaction::TransactionalTree,
        operations: &[Operation],
        operations_meta: &[Option<Batch>],
    ) -> std::result::Result<(), sled::transaction::UnabortableTransactionError> {
        for (index, operation) in operations.iter().enumerate() {
            match operation {
                Operation::Put(cf, k, v, version) => {
                    SledMetaStore::apply_put(tx_db, cf, k, v, version)?;
                }
                Operation::Delete(cf, k, version) => {
                    SledMetaStore::apply_delete(
                        tx_db,
                        cf,
                        k,
                        version,
                        operations_meta.get(index).unwrap(),
                    )?;
                }
            }
        }
        Ok(())
    }

    fn apply_put(
        tx_db: &sled::transaction::TransactionalTree,
        cf: &str,
        key: &[u8],
        value: &[u8],
        version: &Option<KeyValueVersion>,
    ) -> std::result::Result<(), sled::transaction::UnabortableTransactionError> {
        let version = version.unwrap_or(SINGLE_VERSION_EPOCH.into_inner());
        let composed_key =
            KeyWithVersion::serialize(ColumnFamilyUtils::get_composed_key(key, cf), version);
        tx_db.insert(composed_key.as_slice(), value)?;
        Ok(())
    }

    /// sled doesn't have a atomic range delete, so we combine a range get and batch delete here.
    /// The atomicity is guaranteed by `db_guard`.
    fn apply_delete(
        tx_db: &sled::transaction::TransactionalTree,
        cf: &str,
        key: &Key,
        version: &Option<KeyValueVersion>,
        operations_meta: &Option<Batch>,
    ) -> std::result::Result<(), sled::transaction::UnabortableTransactionError> {
        match version {
            Some(version) => {
                // delete specified version
                let composed_key = KeyWithVersion::serialize(
                    ColumnFamilyUtils::get_composed_key(key, cf),
                    *version,
                );
                tx_db.remove(composed_key.as_slice())?;
            }
            None => {
                // delete all versions
                tx_db.apply_batch(operations_meta.as_ref().unwrap())?;
            }
        }

        Ok(())
    }

    fn check(
        &self,
        precondition: &Precondition,
        tx_db: &sled::transaction::TransactionalTree,
    ) -> std::result::Result<bool, sled::transaction::UnabortableTransactionError> {
        match precondition {
            Precondition::KeyExists { cf, key, version } => {
                let composed_key = KeyWithVersion::serialize(
                    ColumnFamilyUtils::get_composed_key(key, cf),
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

#[async_trait]
impl MetaStore for SledMetaStore {
    async fn list_cf(&self, cf: &str) -> Result<Vec<Vec<u8>>> {
        Ok(self.list_batch_cf(vec![cf]).await?.pop().unwrap())
    }

    async fn list_batch_cf(&self, cfs: Vec<&str>) -> Result<Vec<Vec<Vec<u8>>>> {
        let keys_with_opts = cfs
            .iter()
            .map(|cf| (cf.to_string(), vec![], None, true))
            .collect_vec();
        self.get_impl(&keys_with_opts).map(|batch| {
            batch
                .into_iter()
                .map(|result| result.into_iter().map(|kv| kv.into_value()).collect_vec())
                .collect_vec()
        })
    }

    async fn put_cf(&self, cf: &str, key: &[u8], value: &[u8], version: Epoch) -> Result<()> {
        self.put_impl(vec![(
            cf.to_owned(),
            key.to_owned(),
            version.into_inner(),
            value.to_vec(),
        )])
        .await
    }

    async fn get_cf(&self, cf: &str, key: &[u8], version: Epoch) -> Result<Vec<u8>> {
        let result = self
            .get_impl_flatten(&[(
                cf.to_string(),
                key.to_owned(),
                Some(version.into_inner()),
                false,
            )])
            .map(|kvs| kvs.into_iter().map(|kv| kv.into_value()).last())?;
        result.ok_or_else(|| ErrorCode::ItemNotFound("entry not found".to_string()).into())
    }

    async fn delete_cf(&self, cf: &str, key: &[u8], version: Epoch) -> Result<()> {
        self.delete_impl(vec![(
            cf.to_owned(),
            key.to_owned(),
            Some(version.into_inner()),
        )])
        .await
    }

    async fn delete_all_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        self.delete_impl(vec![(cf.to_owned(), key.to_owned(), None)])
            .await
    }

    async fn commit_transaction(&self, trx: &mut Transaction) -> Result<()> {
        let db_guard = self.db.write().unwrap();
        // workaround for sled's lack of range delete
        let mut operations_meta = vec![None; trx.operations().len()];
        for (index, operation) in trx.operations().iter().enumerate() {
            if let Delete(cf, key, version) = operation {
                if version.is_some() {
                    continue;
                }
                // Version is not specified, so perform a range delete.
                let mut batch = Batch::default();
                let (start_range, end_range) = KeyWithVersion::point_lookup_key_range(
                    ColumnFamilyUtils::get_composed_key(key, cf),
                );
                db_guard
                    .range(start_range.as_slice()..end_range.as_slice())
                    .collect::<std::result::Result<Vec<(IVec, IVec)>, sled::Error>>()
                    .map_err(crate::storage::Error::from)?
                    .into_iter()
                    .for_each(|(k, _)| batch.remove(k));
                operations_meta[index] = Some(batch);
            }
        }

        db_guard
            .transaction(|tx_db| {
                let check_result: std::result::Result<
                    Vec<bool>,
                    sled::transaction::UnabortableTransactionError,
                > = trx
                    .preconditions()
                    .iter()
                    .map(|precondition| self.check(precondition, tx_db))
                    .collect();
                if check_result?.into_iter().all(|b| b) {
                    // all preconditions are met
                    SledMetaStore::apply_operations(tx_db, trx.operations(), &operations_meta)?;
                } else {
                    // some preconditions are not met
                    return sled::transaction::abort(());
                }
                tx_db.flush();
                Ok(())
            })
            .map_err(|e: TransactionError<()>| match e {
                TransactionError::Abort(_) => {
                    RwError::from(crate::storage::Error::TransactionAbort())
                }
                TransactionError::Storage(err) => {
                    RwError::from(crate::storage::Error::StorageError(err.to_string()))
                }
            })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_sled_metastore_basic() -> Result<()> {
        let cf = "cf_test_sled_metastore_basic";
        let sled_root = tempfile::tempdir().unwrap();
        let meta_store = SledMetaStore::new(Some(sled_root.path()))?;
        let result = meta_store.get_impl_flatten(&[(
            cf.to_string(),
            "key1".as_bytes().to_vec(),
            None,
            false,
        )])?;
        assert!(result.is_empty());
        let result = meta_store
            .put_impl(vec![(
                cf.to_string(),
                "key1".as_bytes().to_vec(),
                SINGLE_VERSION_EPOCH.into_inner(),
                "value1".as_bytes().to_vec(),
            )])
            .await;
        assert!(result.is_ok());
        let result = meta_store.get_impl_flatten(&[(
            cf.to_string(),
            "key1".as_bytes().to_vec(),
            None,
            false,
        )])?;
        assert_eq!(
            result,
            vec![KeyValue::new(
                cf.to_string(),
                "key1".as_bytes().to_vec(),
                "value1".as_bytes().to_vec(),
                SINGLE_VERSION_EPOCH.into_inner()
            )]
        );
        meta_store
            .delete_impl(vec![(cf.to_string(), "key1".as_bytes().to_vec(), None)])
            .await?;
        let result = meta_store.get_impl_flatten(&[(
            cf.to_string(),
            "key1".as_bytes().to_vec(),
            None,
            false,
        )])?;
        assert!(result.is_empty());

        meta_store
            .put_impl(vec![(
                cf.to_string(),
                "key2".as_bytes().to_vec(),
                SINGLE_VERSION_EPOCH.into_inner(),
                "value2".as_bytes().to_vec(),
            )])
            .await?;
        drop(meta_store);
        let meta_store = SledMetaStore::new(Some(sled_root.path()))?;
        let result = meta_store.get_impl_flatten(&[(
            cf.to_string(),
            "key2".as_bytes().to_vec(),
            None,
            false,
        )]);
        assert_eq!(
            result.unwrap(),
            vec![KeyValue::new(
                cf.to_string(),
                "key2".as_bytes().to_vec(),
                "value2".as_bytes().to_vec(),
                SINGLE_VERSION_EPOCH.into_inner()
            )]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sled_metastore_prefix_key() -> Result<()> {
        let cf = "cf_test_sled_metastore_prefix_key";
        let sled_root = tempfile::tempdir().unwrap();
        let meta_store = SledMetaStore::new(Some(sled_root.path()))?;
        meta_store
            .put_impl(vec![
                (cf.to_string(), vec![0xfe], 118, "value".as_bytes().to_vec()),
                (cf.to_string(), vec![0xff], 118, "value".as_bytes().to_vec()),
                (
                    cf.to_string(),
                    vec![0xff, 0x01],
                    118,
                    "value".as_bytes().to_vec(),
                ),
            ])
            .await?;
        assert_eq!(
            1,
            meta_store
                .get_impl_flatten(&[(cf.to_string(), vec![0xff], None, false)])?
                .len()
        );
        assert_eq!(
            2,
            meta_store
                .get_impl_flatten(&[(cf.to_string(), vec![0xff], None, true)])?
                .len()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sled_metastore_with_op_options() -> Result<()> {
        let cf = "cf_test_sled_metastore_with_op_options";
        let sled_root = tempfile::tempdir().unwrap();
        let meta_store = SledMetaStore::new(Some(sled_root.path()))?;
        // put with one kv
        meta_store
            .put_impl(vec![(
                cf.to_string(),
                "key1".as_bytes().to_vec(),
                118,
                "value1_greatest_version".as_bytes().to_vec(),
            )])
            .await?;

        // put several kvs with different versions
        meta_store
            .put_impl(vec![
                (
                    cf.to_string(),
                    "key2".as_bytes().to_vec(),
                    115,
                    "value2_smallest_version".as_bytes().to_vec(),
                ),
                (
                    cf.to_string(),
                    "key2".as_bytes().to_vec(),
                    121,
                    "value2_greatest_version".as_bytes().to_vec(),
                ),
                (
                    cf.to_string(),
                    "key2".as_bytes().to_vec(),
                    118,
                    "value2_medium_version".as_bytes().to_vec(),
                ),
                (
                    cf.to_string(),
                    "key222".as_bytes().to_vec(),
                    150,
                    "value222".as_bytes().to_vec(),
                ),
            ])
            .await?;

        // get the kv with greatest version
        let result = meta_store.get_impl_flatten(&[(
            cf.to_string(),
            "key2".as_bytes().to_vec(),
            None,
            false,
        )])?;
        assert_eq!(
            vec![KeyValue::new(
                cf.to_string(),
                "key2".as_bytes().to_vec(),
                "value2_greatest_version".as_bytes().to_vec(),
                121
            )],
            result
        );

        // get the kv with specified version
        let result = meta_store.get_impl_flatten(&[(
            cf.to_string(),
            "key2".as_bytes().to_vec(),
            Some(118),
            false,
        )])?;
        assert_eq!(
            vec![KeyValue::new(
                cf.to_string(),
                "key2".as_bytes().to_vec(),
                "value2_medium_version".as_bytes().to_vec(),
                118
            )],
            result
        );

        // Key with specified version doesn't exist. There is kv with the same key but different
        // version.
        let result = meta_store.get_impl_flatten(&[(
            cf.to_string(),
            "key2".as_bytes().to_vec(),
            Some(123),
            false,
        )])?;
        assert!(result.is_empty());

        // Key with specified version doesn't exist. There is no kvs with the same key.
        let result = meta_store.get_impl_flatten(&[(
            cf.to_string(),
            "key".as_bytes().to_vec(),
            Some(118),
            false,
        )])?;
        assert!(result.is_empty());

        // Key with greatest version doesn't exist. There is no kvs with the same key.
        let result = meta_store.get_impl_flatten(&[(
            cf.to_string(),
            "key".as_bytes().to_vec(),
            None,
            false,
        )])?;
        assert!(result.is_empty());

        // get kvs prefixed by specified string with greatest version
        let mut result = meta_store.get_impl_flatten(&[(
            cf.to_string(),
            "key".as_bytes().to_vec(),
            None,
            true,
        )])?;
        result.sort_by_key(|kv| kv.key().clone());
        assert_eq!(
            vec![
                KeyValue::new(
                    cf.to_string(),
                    "key1".as_bytes().to_vec(),
                    "value1_greatest_version".as_bytes().to_vec(),
                    118
                ),
                KeyValue::new(
                    cf.to_string(),
                    "key2".as_bytes().to_vec(),
                    "value2_greatest_version".as_bytes().to_vec(),
                    121
                ),
                KeyValue::new(
                    cf.to_string(),
                    "key222".as_bytes().to_vec(),
                    "value222".as_bytes().to_vec(),
                    150,
                )
            ],
            result
        );

        // kvs prefixed by specified prefix with default version doesn't exist. The prefix string
        // matches no kvs.
        let result = meta_store.get_impl_flatten(&[(
            cf.to_string(),
            "key2-".as_bytes().to_vec(),
            None,
            true,
        )])?;
        assert!(result.is_empty());

        // kvs prefixed by specified string with specified version doesn't exist. The prefix string
        // matches no kvs.
        let result = meta_store.get_impl_flatten(&[(
            cf.to_string(),
            "key1-".as_bytes().to_vec(),
            Some(118),
            true,
        )])?;
        assert!(result.is_empty());

        // get kvs prefixed by specified string with specified version
        let mut result = meta_store.get_impl_flatten(&[(
            cf.to_string(),
            "key".as_bytes().to_vec(),
            Some(118),
            true,
        )])?;
        result.sort_by_key(|kv| kv.key().clone());
        assert_eq!(
            vec![
                KeyValue::new(
                    cf.to_string(),
                    "key1".as_bytes().to_vec(),
                    "value1_greatest_version".as_bytes().to_vec(),
                    118
                ),
                KeyValue::new(
                    cf.to_string(),
                    "key2".as_bytes().to_vec(),
                    "value2_medium_version".as_bytes().to_vec(),
                    118
                )
            ],
            result
        );

        // delete specific version
        meta_store
            .delete_impl(vec![
                (cf.to_string(), "key1".as_bytes().to_vec(), Some(118)),
                (cf.to_string(), "key2".as_bytes().to_vec(), Some(118)),
            ])
            .await?;
        let result = meta_store.get_impl_flatten(&[(
            cf.to_string(),
            "key".as_bytes().to_vec(),
            Some(118),
            true,
        )])?;
        assert!(result.is_empty());

        // delete all versions
        meta_store
            .delete_impl(vec![(cf.to_string(), "key2".as_bytes().to_vec(), None)])
            .await?;
        let result = meta_store.get_impl_flatten(&[(
            cf.to_string(),
            "key2".as_bytes().to_vec(),
            None,
            false,
        )])?;
        assert!(result.is_empty());

        Ok(())
    }
}
