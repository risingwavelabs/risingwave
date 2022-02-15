use std::collections::{BTreeMap, HashMap};
use std::str;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use risingwave_common::array::RwError;
use risingwave_common::error::ErrorCode::ItemNotFound;
use risingwave_common::error::{ErrorCode, Result};

use crate::manager::Epoch;
use crate::storage::transaction::Transaction;
use crate::storage::{Key, KeyValueVersion, Value};

pub const DEFAULT_COLUMN_FAMILY: &str = "default";

#[derive(Debug, PartialEq, Eq)]
pub struct KeyValue {
    key: Key,
    value: Value,
    version: KeyValueVersion,
}

impl KeyValue {
    pub fn new(key: Key, value: Value, version: KeyValueVersion) -> KeyValue {
        KeyValue {
            key,
            value,
            version,
        }
    }

    pub fn into_value(self) -> Value {
        self.value
    }

    pub fn key(&self) -> &Key {
        &self.key
    }
    pub fn version(&self) -> KeyValueVersion {
        self.version
    }
}

/// `MetaStore` defines the functions used to operate metadata.
#[async_trait]
pub trait MetaStore: Sync + Send + 'static {
    async fn list(&self) -> Result<Vec<Vec<u8>>>;
    async fn put(&self, key: &[u8], value: &[u8], version: Epoch) -> Result<()>;
    async fn put_batch(&self, tuples: Vec<(Vec<u8>, Vec<u8>, Epoch)>) -> Result<()>;
    async fn get(&self, key: &[u8], version: Epoch) -> Result<Vec<u8>>;
    async fn delete(&self, key: &[u8], version: Epoch) -> Result<()>;
    async fn delete_all(&self, key: &[u8]) -> Result<()>;

    async fn list_cf(&self, cf: &str) -> Result<Vec<Vec<u8>>>;
    /// We will need a proper implementation for `list`, `list_cf` and `list_batch_cf` in etcd
    /// MetaStore. In a naive implementation, we need to select the latest version for each key
    /// locally after fetching all versions of given cfs from etcd, which may not meet our
    /// performance expectation.
    async fn list_batch_cf(&self, cfs: Vec<&str>) -> Result<Vec<Vec<Vec<u8>>>>;
    async fn put_cf(&self, cf: &str, key: &[u8], value: &[u8], version: Epoch) -> Result<()>;
    async fn put_batch_cf(&self, tuples: Vec<(&str, Vec<u8>, Vec<u8>, Epoch)>) -> Result<()>;
    async fn get_cf(&self, cf: &str, key: &[u8], version: Epoch) -> Result<Vec<u8>>;
    async fn delete_cf(&self, cf: &str, key: &[u8], version: Epoch) -> Result<()>;
    async fn delete_all_cf(&self, cf: &str, key: &[u8]) -> Result<()>;

    fn get_transaction(&self) -> Transaction {
        Transaction::new()
    }
    async fn commit_transaction(&self, trx: &mut Transaction) -> Result<()>;
}

// Error of metastore
#[derive(Debug)]
pub enum Error {
    StorageError(String),
    TransactionAbort(),
}

impl From<Error> for RwError {
    fn from(e: Error) -> Self {
        match e {
            Error::StorageError(e) => RwError::from(ErrorCode::InternalError(e)),
            Error::TransactionAbort() => {
                RwError::from(ErrorCode::InternalError("transaction aborted".to_owned()))
            }
        }
    }
}

pub type MetaStoreRef = Arc<dyn MetaStore>;

// TODO: introduce etcd as storage engine here.

pub(crate) struct KeyWithVersion {}

impl KeyWithVersion {
    /// The serialized result contains two parts
    /// 1. The key part, which is in memcomparable format.
    /// 2. The version part, which is the bitwise inversion of the given version.
    pub fn serialize(key: impl AsRef<[u8]>, version: KeyValueVersion) -> Vec<u8> {
        KeyWithVersion::serialize_key(key.as_ref())
            .into_iter()
            .chain((KeyValueVersion::MAX - version).to_be_bytes().into_iter())
            .collect()
    }

    /// This range will match entries whose key is the given `key`.
    /// Versions won't be filtered out.
    pub fn point_lookup_key_range(key: impl AsRef<[u8]>) -> (Vec<u8>, Vec<u8>) {
        assert!(!key.as_ref().is_empty());
        let start = KeyWithVersion::serialize_key(key.as_ref());
        let mut end = start.to_owned();
        *end.last_mut().unwrap() = 1;
        (start, end)
    }

    /// This range will match entries whose key is prefixed by the given `key`.
    /// Versions won't be filtered out.
    pub fn range_lookup_key_range(key: impl AsRef<[u8]>) -> (Vec<u8>, Vec<u8>) {
        assert!(!key.as_ref().is_empty());
        let start = KeyWithVersion::serialize_key(key.as_ref());
        let mut end = start.to_owned();
        *end.last_mut().unwrap() = 2;
        (start, end)
    }

    /// Get the key part in memcomparable format.
    fn serialize_key(key: impl AsRef<[u8]>) -> Vec<u8> {
        memcomparable::to_vec(&key.as_ref()).unwrap()
    }

    pub fn deserialize(key_with_version: impl AsRef<[u8]>) -> Option<(Key, KeyValueVersion)> {
        let version_field_size = std::mem::size_of::<KeyValueVersion>();
        if key_with_version.as_ref().len() <= version_field_size {
            return None;
        }
        let key_slice =
            &key_with_version.as_ref()[..key_with_version.as_ref().len() - version_field_size];
        let key = match memcomparable::from_slice::<Vec<u8>>(key_slice) {
            Ok(key) => key,
            Err(_) => return None,
        };
        let version = match key_with_version.as_ref()
            [key_with_version.as_ref().len() - version_field_size..]
            .try_into()
        {
            Ok(version) => KeyValueVersion::MAX - u64::from_be_bytes(version),
            Err(_) => return None,
        };
        Some((key, version))
    }
}

type KeyForMem = (Vec<u8>, String);

pub struct MemStore {
    entities: Mutex<HashMap<KeyForMem, BTreeMap<Epoch, Vec<u8>>>>,
}

impl MemStore {
    #[cfg(test)]
    pub fn new() -> Self {
        MemStore {
            entities: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl MetaStore for MemStore {
    async fn list(&self) -> Result<Vec<Vec<u8>>> {
        self.list_cf(DEFAULT_COLUMN_FAMILY).await
    }

    async fn put(&self, key: &[u8], value: &[u8], version: Epoch) -> Result<()> {
        self.put_cf(DEFAULT_COLUMN_FAMILY, key, value, version)
            .await
    }

    async fn put_batch(&self, tuples: Vec<(Vec<u8>, Vec<u8>, Epoch)>) -> Result<()> {
        let mut entities = self.entities.lock().unwrap();
        for (key, value, version) in tuples {
            match entities.get_mut(&(key.clone(), String::from(DEFAULT_COLUMN_FAMILY))) {
                Some(entry) => {
                    entry.insert(version, value);
                }
                None => {
                    let mut entry = BTreeMap::new();
                    entry.insert(version, value);
                    entities.insert((key, String::from(DEFAULT_COLUMN_FAMILY)), entry);
                }
            }
        }
        Ok(())
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
        let entities = self.entities.lock().unwrap();
        Ok(entities
            .iter()
            .filter(|(k, v)| k.1 == cf && !v.is_empty())
            .map(|(_, v)| v.iter().last().unwrap().1.clone())
            .collect::<Vec<_>>())
    }

    async fn list_batch_cf(&self, cfs: Vec<&str>) -> Result<Vec<Vec<Vec<u8>>>> {
        let entities = self.entities.lock().unwrap();

        Ok(cfs
            .iter()
            .map(|&cf| {
                entities
                    .iter()
                    .filter(|(k, v)| k.1 == cf && !v.is_empty())
                    .map(|(_, v)| v.iter().last().unwrap().1.clone())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>())
    }

    async fn put_cf(&self, cf: &str, key: &[u8], value: &[u8], version: Epoch) -> Result<()> {
        let mut entities = self.entities.lock().unwrap();
        match entities.get_mut(&(key.to_vec(), String::from(cf))) {
            Some(entry) => {
                entry.insert(version, value.to_vec());
            }
            None => {
                let mut entry = BTreeMap::new();
                entry.insert(version, value.to_vec());
                entities.insert((key.to_vec(), String::from(cf)), entry);
            }
        }

        Ok(())
    }

    async fn put_batch_cf(&self, tuples: Vec<(&str, Vec<u8>, Vec<u8>, Epoch)>) -> Result<()> {
        let mut entities = self.entities.lock().unwrap();
        for (cf, key, value, version) in tuples {
            match entities.get_mut(&(key.clone(), String::from(cf))) {
                Some(entry) => {
                    entry.insert(version, value);
                }
                None => {
                    let mut entry = BTreeMap::new();
                    entry.insert(version, value);
                    entities.insert((key, String::from(cf)), entry);
                }
            }
        }
        Ok(())
    }

    async fn get_cf(&self, cf: &str, key: &[u8], version: Epoch) -> Result<Vec<u8>> {
        let entities = self.entities.lock().unwrap();
        entities
            .get(&(key.to_vec(), String::from(cf)))
            .ok_or_else(|| RwError::from(ItemNotFound("entry not found".to_string())))?
            .get(&version)
            .cloned()
            .ok_or_else(|| ItemNotFound("entry not found".to_string()).into())
    }

    async fn delete_cf(&self, cf: &str, key: &[u8], version: Epoch) -> Result<()> {
        let mut entities = self.entities.lock().unwrap();
        entities
            .get_mut(&(key.to_vec(), String::from(cf)))
            .and_then(|entry| entry.remove(&version));

        Ok(())
    }

    async fn delete_all_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        let mut entities = self.entities.lock().unwrap();
        entities.remove(&(key.to_vec(), String::from(cf)));

        Ok(())
    }

    async fn commit_transaction(&self, _trx: &mut Transaction) -> Result<()> {
        unimplemented!()
    }
}

pub struct ColumnFamilyUtils {}
impl ColumnFamilyUtils {
    pub fn prefix_key_with_cf(key: impl AsRef<[u8]>, prefix: impl AsRef<[u8]>) -> Vec<u8> {
        let prefix_len: u8 = prefix
            .as_ref()
            .len()
            .try_into()
            .expect("prefix length out of u8 range");
        [&[prefix_len], prefix.as_ref(), key.as_ref()].concat()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::error::Result;

    use crate::storage::KeyWithVersion;

    #[test]
    fn test_key_with_version() -> Result<()> {
        let composed_key1 = KeyWithVersion::serialize(b"key-1", 1);
        let composed_key2 = KeyWithVersion::serialize(b"key-2", 2);
        let (key1, version1) = KeyWithVersion::deserialize(&composed_key1).unwrap();
        let (key2, version2) = KeyWithVersion::deserialize(&composed_key2).unwrap();
        assert_eq!(key1, b"key-1".as_slice().to_vec());
        assert_eq!(key2, b"key-2".as_slice().to_vec());
        assert_eq!(version1, 1);
        assert_eq!(version2, 2);

        let key = vec![0x1, 0x2, 0xfe];
        assert_eq!(
            KeyWithVersion::serialize_key(&key),
            vec![1, 0x1, 1, 0x2, 1, 0xfe, 0]
        );
        let (start, end) = KeyWithVersion::point_lookup_key_range(&key);
        assert_eq!(
            (start, end),
            (
                vec![1, 0x1, 1, 0x2, 1, 0xfe, 0],
                vec![1, 0x1, 1, 0x2, 1, 0xfe, 1]
            )
        );
        let (start, end) = KeyWithVersion::range_lookup_key_range(&key);
        assert_eq!(
            (start, end),
            (
                vec![1, 0x1, 1, 0x2, 1, 0xfe, 0],
                vec![1, 0x1, 1, 0x2, 1, 0xfe, 2]
            )
        );

        let key = vec![0x01, 0xff];
        let (start, end) = KeyWithVersion::range_lookup_key_range(&key);
        assert_eq!(
            (start, end),
            (vec![1, 0x01, 1, 0xff, 0], vec![1, 0x01, 1, 0xff, 2])
        );

        Ok(())
    }
}
