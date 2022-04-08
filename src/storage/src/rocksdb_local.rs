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

use std::future::Future;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use rocksdb::{DBIterator, ReadOptions, SeekKey, Writable, WriteBatch, WriteOptions, DB};
use tokio::sync::OnceCell;
use tokio::task;

use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{define_state_store_associated_type, StateStore, StateStoreIter};

#[derive(Clone)]
pub struct RocksDBStateStore {
    storage: Arc<OnceCell<RocksDBStorage>>,
    db_path: String,
}

impl RocksDBStateStore {
    pub fn new(db_path: &str) -> Self {
        Self {
            storage: Arc::new(OnceCell::new()),
            db_path: db_path.to_string(),
        }
    }

    pub async fn storage(&self) -> &RocksDBStorage {
        self.storage
            .get_or_init(|| async { RocksDBStorage::new(&self.db_path).await })
            .await
    }
}

impl StateStore for RocksDBStateStore {
    type Iter<'a> = RocksDBStateStoreIter;
    define_state_store_associated_type!();

    fn get<'a>(&'a self, key: &'a [u8], _epoch: u64) -> Self::GetFuture<'_> {
        async move { self.storage().await.get(key).await }
    }

    fn scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Self::ScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            let mut iter = self.iter(key_range, epoch).await?;
            let mut kvs = Vec::with_capacity(limit.unwrap_or_default());

            for _ in 0..limit.unwrap_or(usize::MAX) {
                match iter.next().await? {
                    Some(kv) => kvs.push(kv),
                    None => break,
                }
            }

            Ok(kvs)
        }
    }

    fn reverse_scan<R, B>(
        &self,
        _key_range: R,
        _limit: Option<usize>,
        _epoch: u64,
    ) -> Self::ReverseScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }

    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        _epoch: u64,
    ) -> Self::IngestBatchFuture<'_> {
        async move { self.storage().await.write_batch(kv_pairs).await }
    }

    fn replicate_batch(
        &self,
        _kv_pairs: Vec<(Bytes, StorageValue)>,
        _epoch: u64,
    ) -> Self::ReplicateBatchFuture<'_> {
        async move { unimplemented!() }
    }

    fn iter<R, B>(&self, key_range: R, _epoch: u64) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            let range = (
                key_range.start_bound().map(|b| b.as_ref().to_owned()),
                key_range.end_bound().map(|b| b.as_ref().to_owned()),
            );
            RocksDBStateStoreIter::new(self.clone(), range).await
        }
    }

    fn reverse_iter<R, B>(&self, _key_range: R, _epoch: u64) -> Self::ReverseIterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }

    fn wait_epoch(&self, _epoch: u64) -> Self::WaitEpochFuture<'_> {
        async move { unimplemented!() }
    }

    fn sync(&self, _epoch: Option<u64>) -> Self::SyncFuture<'_> {
        async move { unimplemented!() }
    }
}

pub fn next_prefix(prefix: &[u8]) -> Vec<u8> {
    let pos = prefix.iter().rposition(|b| *b != 0xff).unwrap();
    let (s, e) = (&prefix[..pos], prefix[pos] + 1);
    let mut res = Vec::with_capacity(s.len() + 1);
    res.extend_from_slice(s);
    res.push(e);
    res
}

pub struct RocksDBStateStoreIter {
    iter: Option<Box<DBIterator<Arc<DB>>>>,
    key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
}

impl RocksDBStateStoreIter {
    async fn new(
        store: RocksDBStateStore,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> Result<Self> {
        let mut start_key = vec![];
        let mut is_start_unbounded = false;
        match range.start_bound() {
            Bound::Included(s_key) => {
                start_key = s_key.clone();
            }
            Bound::Unbounded => {
                is_start_unbounded = true;
            }
            _ => {
                return Err(InternalError("invalid range start".to_string()).into());
            }
        };

        let mut iter = store.storage().await.iter().await;
        task::spawn_blocking(move || {
            let seek_key = if is_start_unbounded {
                SeekKey::Start
            } else {
                SeekKey::from(start_key.as_slice())
            };
            iter.seek(seek_key)
                .map_err(|e| RwError::from(InternalError(e)))?;
            Ok(Self {
                iter: Some(Box::new(iter)),
                key_range: range,
            })
        })
        .await?
    }
}

impl StateStoreIter for RocksDBStateStoreIter {
    type Item = (Bytes, Bytes);
    type NextFuture<'a> = impl Future<Output = crate::error::StorageResult<Option<Self::Item>>>;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            let mut end_key = Bytes::new();
            let mut is_end_exclude = false;
            let mut is_end_unbounded = false;
            match self.key_range.end_bound() {
                Bound::Included(e_key) => {
                    end_key = Bytes::from(e_key.clone());
                }
                Bound::Excluded(e_key) => {
                    end_key = Bytes::from(e_key.clone());
                    is_end_exclude = true;
                }
                Bound::Unbounded => {
                    is_end_unbounded = true;
                }
            }

            let mut iter = self.iter.take().unwrap();
            let (kv, iter) = tokio::task::spawn_blocking(move || {
                let result = iter.valid().map_err(|e| RwError::from(InternalError(e)));
                if let Err(e) = result {
                    return (Err(e), iter);
                }
                if !result.unwrap() {
                    return (Ok(None), iter);
                }
                let k = Bytes::from(iter.key().to_vec());
                let v = Bytes::from(iter.value().to_vec());

                if is_end_unbounded {
                    return (Ok(Some((k, v))), iter);
                }
                if k > end_key || (k == end_key && is_end_exclude) {
                    return (Ok(None), iter);
                }
                if let Err(e) = iter.next().map_err(|e| RwError::from(InternalError(e))) {
                    return (Err(e), iter);
                }
                (Ok(Some((k, v))), iter)
            })
            .await
            .unwrap();

            self.iter = Some(iter);
            kv
        }
    }
}

#[derive(Clone)]
pub struct RocksDBStorage {
    db: Arc<DB>,
}

impl RocksDBStorage {
    pub async fn new(path: &str) -> Self {
        let path = path.to_string();
        let db = task::spawn_blocking(move || DB::open_default(path.as_str()).unwrap())
            .await
            .unwrap();
        let storage = RocksDBStorage { db: Arc::new(db) };
        storage.clear_all().await.unwrap();
        storage
    }

    async fn clear_all(&self) -> Result<()> {
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let mut it = db.iter();
            if !it.seek(SeekKey::Start).unwrap() {
                return Ok(());
            }
            let start_key = it.key().to_vec();
            it.seek(SeekKey::End).unwrap();
            let end_key = next_prefix(it.key());
            db.delete_range(start_key.as_slice(), end_key.as_slice())
                .map_err(|e| RwError::from(InternalError(e)))
        })
        .await?
    }

    async fn write_batch(&self, kv_pairs: Vec<(Bytes, StorageValue)>) -> Result<()> {
        let wb = WriteBatch::new();
        for (key, value) in kv_pairs {
            let value = value.user_value();
            if let Some(value) = value {
                if let Err(e) = wb.put(key.as_ref(), value.as_ref()) {
                    return Err(InternalError(e).into());
                }
            } else if let Err(e) = wb.delete(key.as_ref()) {
                return Err(InternalError(e).into());
            }
        }

        let db = self.db.clone();
        task::spawn_blocking(move || {
            let mut opts = WriteOptions::default();
            opts.set_sync(true);
            db.write_opt(&wb, &opts)
                .map_or_else(|e| Err(InternalError(e).into()), |_| Ok(()))
        })
        .await?
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let db = self.db.clone();
        let seek_key = key.to_vec();
        task::spawn_blocking(move || {
            db.get(&seek_key).map_or_else(
                |e| Err(InternalError(e).into()),
                |option_v| Ok(option_v.map(|v| Bytes::from(v.to_vec()))),
            )
        })
        .await?
    }

    async fn iter(&self) -> DBIterator<Arc<DB>> {
        let db = self.db.clone();
        task::spawn_blocking(move || DBIterator::new(db, ReadOptions::default()))
            .await
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[tokio::test]
    async fn test_rocksdb() {
        let rocksdb_state_store = RocksDBStateStore::new("/tmp/default");
        let result = rocksdb_state_store.get("key1".as_bytes(), 0).await;
        assert_eq!(result.unwrap(), None);
        let result = rocksdb_state_store.get("key2".as_bytes(), 0).await;
        assert_eq!(result.unwrap(), None);
        let result = rocksdb_state_store.get("key3".as_bytes(), 0).await;
        assert_eq!(result.unwrap(), None);

        let kv_pairs: Vec<(Bytes, StorageValue)> = vec![
            ("key1".into(), StorageValue::new_default_put("val1")),
            ("key2".into(), StorageValue::new_default_put("val2")),
            ("key3".into(), StorageValue::new_default_put("val3")),
        ];
        rocksdb_state_store.ingest_batch(kv_pairs, 0).await.unwrap();
        let result = rocksdb_state_store
            .get("key1".as_bytes(), 0)
            .await
            .unwrap()
            .unwrap();
        assert!(result.eq(&Bytes::from("val1")));
        let result = rocksdb_state_store
            .get("key2".as_bytes(), 0)
            .await
            .unwrap()
            .unwrap();
        assert!(result.eq(&Bytes::from("val2")));
        let result = rocksdb_state_store
            .get("key3".as_bytes(), 0)
            .await
            .unwrap()
            .unwrap();
        assert!(result.eq(&Bytes::from("val3")));
        let result = rocksdb_state_store.get("key4".as_bytes(), 0).await;
        assert_eq!(result.unwrap(), None);

        let range = "key1".as_bytes().."key3".as_bytes();
        let result = rocksdb_state_store.scan(range, Some(2), 0).await.unwrap();
        assert!(result.get(0).unwrap().0.eq(&Bytes::from("key1")));
        assert!(result.get(1).unwrap().0.eq(&Bytes::from("key2")));
    }
}
