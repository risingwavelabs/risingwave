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

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use bytes::Bytes;
use lazy_static::lazy_static;
use tokio::sync::Mutex;

use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{define_state_store_associated_type, StateStore, StateStoreIter};

type KeyWithEpoch = (Bytes, Reverse<u64>);

/// An in-memory state store
///
/// The in-memory state store is a [`BTreeMap`], which maps (key, epoch) to value. It never does GC,
/// so the memory usage will be high. At the same time, every time we create a new iterator on
/// `BTreeMap`, it will fully clone the map, so as to act as a snapshot. Therefore, in-memory state
/// store should never be used in production.
#[derive(Clone)]
pub struct MemoryStateStore {
    /// Stores (key, epoch) -> user value. We currently don't consider value meta here.
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
}

impl StateStore for MemoryStateStore {
    type Iter<'a> = MemoryStateStoreIter;
    define_state_store_associated_type!();

    fn get<'a>(&'a self, key: &'a [u8], epoch: u64) -> Self::GetFuture<'_> {
        async move {
            let range_bounds = key.to_vec()..=key.to_vec();
            let res = self.scan(range_bounds, Some(1), epoch).await?;

            Ok(match res.as_slice() {
                [] => None,
                [(_, value)] => Some(value.clone()),
                _ => unreachable!(),
            })
        }
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
        epoch: u64,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            let mut inner = self.inner.lock().await;
            for (key, value) in kv_pairs {
                inner.insert((key, Reverse(epoch)), value.user_value);
            }
            Ok(())
        }
    }

    fn replicate_batch(
        &self,
        _kv_pairs: Vec<(Bytes, StorageValue)>,
        _epoch: u64,
    ) -> Self::ReplicateBatchFuture<'_> {
        async move { unimplemented!() }
    }

    fn iter<R, B>(&self, key_range: R, epoch: u64) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            Ok(MemoryStateStoreIter::new(
                self.scan(key_range, None, epoch).await.unwrap().into_iter(),
            ))
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
        async move {
            // memory backend doesn't support wait for epoch, so this is a no-op.
            Ok(())
        }
    }

    fn sync(&self, _epoch: Option<u64>) -> Self::SyncFuture<'_> {
        async move {
            // memory backend doesn't support push to S3, so this is a no-op
            Ok(())
        }
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

impl StateStoreIter for MemoryStateStoreIter {
    type Item = (Bytes, Bytes);
    type NextFuture<'a> = impl Future<Output = crate::error::StorageResult<Option<Self::Item>>>;
    fn next(&mut self) -> Self::NextFuture<'_> {
        async move { Ok(self.inner.next()) }
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
                    (
                        b"a".to_vec().into(),
                        StorageValue::new_default_put(b"v1".to_vec()),
                    ),
                    (
                        b"b".to_vec().into(),
                        StorageValue::new_default_put(b"v1".to_vec()),
                    ),
                ],
                0,
            )
            .await
            .unwrap();
        state_store
            .ingest_batch(
                vec![
                    (
                        b"a".to_vec().into(),
                        StorageValue::new_default_put(b"v2".to_vec()),
                    ),
                    (b"b".to_vec().into(), StorageValue::new_default_delete()),
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
