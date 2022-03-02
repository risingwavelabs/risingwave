use std::collections::{btree_map, BTreeMap};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use lazy_static::lazy_static;
use risingwave_common::error::Result;
use tokio::sync::Mutex;

use crate::{StateStore, StateStoreIter};

/// An in-memory state store
#[derive(Clone)]
pub struct MemoryStateStore {
    inner: Arc<Mutex<BTreeMap<Bytes, Bytes>>>,
}

impl Default for MemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

fn to_bytes_range<R, B>(range: R) -> (Bound<Bytes>, Bound<Bytes>)
where
    R: RangeBounds<B> + Send,
    B: AsRef<[u8]>,
{
    let start = match range.start_bound() {
        Included(k) => Included(Bytes::copy_from_slice(k.as_ref())),
        Excluded(k) => Excluded(Bytes::copy_from_slice(k.as_ref())),
        Unbounded => Unbounded,
    };
    let end = match range.end_bound() {
        Included(k) => Included(Bytes::copy_from_slice(k.as_ref())),
        Excluded(k) => Excluded(Bytes::copy_from_slice(k.as_ref())),
        Unbounded => Unbounded,
    };
    (start, end)
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn shared() -> Self {
        lazy_static! {
            static ref STORE: MemoryStateStore = MemoryStateStore::new();
        }
        STORE.clone()
    }

    async fn ingest_batch_inner(&self, kv_pairs: Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        let mut inner = self.inner.lock().await;
        for (key, value) in kv_pairs {
            if let Some(value) = value {
                inner.insert(key, value);
            } else {
                inner.remove(&key);
            }
        }
        Ok(())
    }

    async fn scan_inner<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
    ) -> Result<Vec<(Bytes, Bytes)>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        let mut data = vec![];
        if limit == Some(0) {
            return Ok(vec![]);
        }
        let inner = self.inner.lock().await;
        for (key, value) in inner.range(to_bytes_range(key_range)) {
            data.push((key.clone(), value.clone()));
            if let Some(limit) = limit && data.len() >= limit {
                break;
            }
        }
        Ok(data)
    }

    async fn reverse_scan_inner<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
    ) -> Result<Vec<(Bytes, Bytes)>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        let mut data = vec![];
        if limit == Some(0) {
            return Ok(vec![]);
        }
        let inner = self.inner.lock().await;
        for (key, value) in inner.range(to_bytes_range(key_range)).rev() {
            data.push((key.clone(), value.clone()));
            if let Some(limit) = limit && data.len() >= limit {
                break;
            }
        }
        Ok(data)
    }
}

#[async_trait]
impl StateStore for MemoryStateStore {
    type Iter<'a> = MemoryStateStoreIter;

    async fn get(&self, key: &[u8], _epoch: u64) -> Result<Option<Bytes>> {
        let inner = self.inner.lock().await;
        let res = inner.get(key).cloned();
        Ok(res)
    }

    async fn scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        _epoch: u64,
    ) -> Result<Vec<(Bytes, Bytes)>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        self.scan_inner(key_range, limit).await
    }

    async fn reverse_scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        _epoch: u64,
    ) -> Result<Vec<(Bytes, Bytes)>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        self.reverse_scan_inner(key_range, limit).await
    }

    async fn ingest_batch(&self, kv_pairs: Vec<(Bytes, Option<Bytes>)>, _epoch: u64) -> Result<()> {
        // TODO: actually use epoch and support rollback
        self.ingest_batch_inner(kv_pairs).await
    }

    async fn iter<R, B>(&self, key_range: R, _epoch: u64) -> Result<Self::Iter<'_>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        #[allow(clippy::mutable_key_type)]
        let snapshot: BTreeMap<_, _> = self
            .inner
            .lock()
            .await
            .range(to_bytes_range(key_range))
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();

        Ok(MemoryStateStoreIter::new(snapshot.into_iter()))
    }
}

pub struct MemoryStateStoreIter {
    inner: btree_map::IntoIter<Bytes, Bytes>,
}

impl MemoryStateStoreIter {
    fn new(iter: btree_map::IntoIter<Bytes, Bytes>) -> Self {
        Self { inner: iter }
    }
}

#[async_trait]
impl StateStoreIter for MemoryStateStoreIter {
    type Item = (Bytes, Bytes);

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        Ok(self.inner.next())
    }
}
