use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use lazy_static::lazy_static;
use risingwave_common::error::Result;
use tokio::sync::Mutex;

use crate::{StateStore, StateStoreIter};

type KeyWithEpoch = (Bytes, Reverse<u64>);

/// An in-memory state store
///
/// The in-memory state store is a [`BTreeMap`], which maps (key, epoch) to value. It never does GC,
/// so the memory usage will be high. At the same time, everytime we create a new iterator on
/// `BTreeMap`, it will fully clone the map, so as to act as a snapshot. Therefore, in-memory state
/// store should never be used in production.
#[derive(Clone)]
pub struct MemoryStateStore {
    /// store (key, epoch) -> value
    inner: Arc<Mutex<BTreeMap<KeyWithEpoch, Option<Bytes>>>>,
}

impl Default for MemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

fn to_bytes_range<R, B>(range: R) -> (Bound<KeyWithEpoch>, Bound<KeyWithEpoch>)
where
    R: RangeBounds<B> + Send,
    B: AsRef<[u8]>,
{
    let start = match range.start_bound() {
        Included(k) => Included((Bytes::copy_from_slice(k.as_ref()), Reverse(u64::MAX))),
        Excluded(k) => Excluded((Bytes::copy_from_slice(k.as_ref()), Reverse(0))),
        Unbounded => Unbounded,
    };
    let end = match range.end_bound() {
        Included(k) => Included((Bytes::copy_from_slice(k.as_ref()), Reverse(0))),
        Excluded(k) => Excluded((Bytes::copy_from_slice(k.as_ref()), Reverse(u64::MAX))),
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

    async fn ingest_batch_inner(
        &self,
        kv_pairs: Vec<(Bytes, Option<Bytes>)>,
        epoch: u64,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;
        for (key, value) in kv_pairs {
            inner.insert((key, Reverse(epoch)), value);
        }
        Ok(())
    }

    async fn scan_inner<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
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

        let mut last_key = None;
        for ((key, Reverse(key_epoch)), value) in inner.range(to_bytes_range(key_range)) {
            if *key_epoch > epoch {
                continue;
            }
            if Some(key) != last_key.as_ref() {
                if let Some(value) = value {
                    data.push((key.clone(), value.clone()));
                }
                last_key = Some(key.clone());
            }
            if let Some(limit) = limit && data.len() >= limit {
                break;
            }
        }
        Ok(data)
    }

    async fn reverse_scan_inner<R, B>(
        &self,
        _key_range: R,
        _limit: Option<usize>,
    ) -> Result<Vec<(Bytes, Bytes)>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        todo!()
    }
}

#[async_trait]
impl StateStore for MemoryStateStore {
    type Iter<'a> = MemoryStateStoreIter;

    async fn get(&self, key: &[u8], epoch: u64) -> Result<Option<Bytes>> {
        let res = self.scan_inner(key..=key, Some(1), epoch).await?;
        Ok(match res.as_slice() {
            [] => None,
            [(_, value)] => Some(value.clone()),
            _ => unreachable!(),
        })
    }

    async fn scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Result<Vec<(Bytes, Bytes)>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        self.scan_inner(key_range, limit, epoch).await
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

    async fn ingest_batch(&self, kv_pairs: Vec<(Bytes, Option<Bytes>)>, epoch: u64) -> Result<()> {
        self.ingest_batch_inner(kv_pairs, epoch).await
    }

    async fn iter<R, B>(&self, key_range: R, epoch: u64) -> Result<Self::Iter<'_>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        Ok(MemoryStateStoreIter::new(
            self.scan_inner(key_range, None, epoch)
                .await
                .unwrap()
                .into_iter(),
        ))
    }

    async fn replicate_batch(
        &self,
        _kv_pairs: Vec<(Bytes, Option<Bytes>)>,
        _epoch: u64,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn reverse_iter<R, B>(&self, _key_range: R, _epoch: u64) -> Result<Self::Iter<'_>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        unimplemented!()
    }

    async fn wait_epoch(&self, _epoch: u64) -> Result<()> {
        // memory backend doesn't support wait for epoch, so this is a no-op.
        Ok(())
    }

    async fn sync(&self, _epoch: Option<u64>) -> Result<()> {
        // memory backend doesn't support push to S3, so this is a no-op
        Ok(())
    }
}

pub struct MemoryStateStoreIter {
    inner: std::vec::IntoIter<(Bytes, Bytes)>,
}

impl MemoryStateStoreIter {
    fn new(iter: std::vec::IntoIter<(Bytes, Bytes)>) -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_snapshot_isolation() {
        let state_store = MemoryStateStore::new();
        state_store
            .ingest_batch(
                vec![
                    (b"a".to_vec().into(), Some(b"v1".to_vec().into())),
                    (b"b".to_vec().into(), Some(b"v1".to_vec().into())),
                ],
                0,
            )
            .await
            .unwrap();
        state_store
            .ingest_batch(
                vec![
                    (b"a".to_vec().into(), Some(b"v2".to_vec().into())),
                    (b"b".to_vec().into(), None),
                ],
                1,
            )
            .await
            .unwrap();
        assert_eq!(
            state_store.scan("a"..="b", None, 0).await.unwrap(),
            vec![
                (b"a".to_vec().into(), b"v1".to_vec().into()),
                (b"b".to_vec().into(), b"v1".to_vec().into())
            ]
        );
        assert_eq!(
            state_store.scan("a"..="b", Some(1), 0).await.unwrap(),
            vec![(b"a".to_vec().into(), b"v1".to_vec().into())]
        );
        assert_eq!(
            state_store.scan("a"..="b", None, 1).await.unwrap(),
            vec![(b"a".to_vec().into(), b"v2".to_vec().into())]
        );
        assert_eq!(
            state_store.get(b"a", 0).await.unwrap(),
            Some(b"v1".to_vec().into())
        );
        assert_eq!(
            state_store.get(b"b", 0).await.unwrap(),
            Some(b"v1".to_vec().into())
        );
        assert_eq!(state_store.get(b"c", 0).await.unwrap(), None);
        assert_eq!(
            state_store.get(b"a", 1).await.unwrap(),
            Some(b"v2".to_vec().into())
        );
        assert_eq!(state_store.get(b"b", 1).await.unwrap(), None);
        assert_eq!(state_store.get(b"c", 1).await.unwrap(), None);
    }
}
