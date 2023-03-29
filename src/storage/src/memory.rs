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

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, DerefMut, RangeBounds};
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use parking_lot::RwLock;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKey};
use risingwave_hummock_sdk::{HummockEpoch, HummockReadEpoch};

use crate::error::StorageResult;
use crate::mem_table::MemtableLocalStateStore;
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{
    define_state_store_associated_type, define_state_store_read_associated_type,
    define_state_store_write_associated_type,
};

pub type BytesUserKey = UserKey<Bytes>;
pub type BytesFullKey = FullKey<Bytes>;
pub type BytesFullKeyRange = (Bound<BytesFullKey>, Bound<BytesFullKey>);

#[allow(clippy::type_complexity)]
pub trait RangeKv: Clone + Send + Sync + 'static {
    fn range(
        &self,
        range: BytesFullKeyRange,
        limit: Option<usize>,
    ) -> StorageResult<Vec<(BytesFullKey, Option<Bytes>)>>;

    fn ingest_batch(
        &self,
        kv_pairs: impl Iterator<Item = (BytesFullKey, Option<Bytes>)>,
    ) -> StorageResult<()>;

    fn flush(&self) -> StorageResult<()>;
}

pub type BTreeMapRangeKv = Arc<
    RwLock<(
        BTreeMap<BytesFullKey, Option<Bytes>>,
        BTreeMap<BytesUserKey, Option<Bytes>>,
    )>,
>;

impl RangeKv for BTreeMapRangeKv {
    fn range(
        &self,
        range: BytesFullKeyRange,
        limit: Option<usize>,
    ) -> StorageResult<Vec<(BytesFullKey, Option<Bytes>)>> {
        let limit = limit.unwrap_or(usize::MAX);
        Ok(self
            .read()
            .0
            .range(range)
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn ingest_batch(
        &self,
        kv_pairs: impl Iterator<Item = (BytesFullKey, Option<Bytes>)>,
    ) -> StorageResult<()> {
        let mut inner = self.write();
        let (full_key_map, user_key_map) = inner.deref_mut();
        println!("OLD SIZE {:?}", full_key_map.len());
        for (key, value) in kv_pairs {
            let is_delete_operation = value.is_none();
            println!("{key:?}, {value:?}");
            let old_value = user_key_map.insert(key.user_key.clone(), value.clone());
            assert_eq!(is_delete_operation, matches!(old_value, Some(Some(_))));
            full_key_map.insert(key, value);
        }
        println!("NEW SIZE {:?}", full_key_map.len());
        Ok(())
    }

    fn flush(&self) -> StorageResult<()> {
        Ok(())
    }
}

pub mod sled {
    use std::fs::create_dir_all;
    use std::ops::RangeBounds;

    use bytes::Bytes;
    use risingwave_hummock_sdk::key::FullKey;

    use crate::error::StorageResult;
    use crate::memory::{BytesFullKey, BytesFullKeyRange, RangeKv, RangeKvStateStore};

    #[derive(Clone)]
    pub struct SledRangeKv {
        inner: sled::Db,
    }

    impl SledRangeKv {
        pub fn new(path: impl AsRef<std::path::Path>) -> Self {
            SledRangeKv {
                inner: sled::open(path).expect("open"),
            }
        }

        pub fn new_temp() -> Self {
            create_dir_all("./.risingwave/sled").expect("should create");
            let path = tempfile::TempDir::new_in("./.risingwave/sled")
                .expect("find temp dir")
                .into_path();
            Self::new(path)
        }
    }

    const EMPTY: u8 = 1;
    const NON_EMPTY: u8 = 0;

    impl RangeKv for SledRangeKv {
        fn range(
            &self,
            range: BytesFullKeyRange,
            limit: Option<usize>,
        ) -> StorageResult<Vec<(BytesFullKey, Option<Bytes>)>> {
            let (left, right) = range;
            let full_key_ref_bound = (
                left.as_ref().map(FullKey::to_ref),
                right.as_ref().map(FullKey::to_ref),
            );
            let left_encoded = left.as_ref().map(|key| key.to_ref().encode_reverse_epoch());
            let right_encoded = right
                .as_ref()
                .map(|key| key.to_ref().encode_reverse_epoch());
            let limit = limit.unwrap_or(usize::MAX);
            let mut ret = vec![];
            for result in self.inner.range((left_encoded, right_encoded)).take(limit) {
                let (key, value) = result?;
                let full_key = FullKey::decode_reverse_epoch(key.as_ref()).copy_into();
                if !full_key_ref_bound.contains(&full_key.to_ref()) {
                    continue;
                }
                let value = match value.as_ref() {
                    [EMPTY] => None,
                    [NON_EMPTY, rest @ ..] => Some(Bytes::from(Vec::from(rest))),
                    _ => unreachable!("malformed value: {:?}", value),
                };
                ret.push((full_key, value))
            }
            Ok(ret)
        }

        fn ingest_batch(
            &self,
            kv_pairs: impl Iterator<Item = (BytesFullKey, Option<Bytes>)>,
        ) -> StorageResult<()> {
            let mut batch = sled::Batch::default();
            for (key, value) in kv_pairs {
                let encoded_key = key.encode_reverse_epoch();
                let key = sled::IVec::from(encoded_key);
                let mut buffer =
                    Vec::with_capacity(value.as_ref().map(|v| v.len()).unwrap_or_default() + 1);
                if let Some(value) = value {
                    buffer.push(NON_EMPTY);
                    buffer.extend_from_slice(value.as_ref());
                } else {
                    buffer.push(EMPTY);
                }
                let value = sled::IVec::from(buffer);
                batch.insert(key, value);
            }
            self.inner.apply_batch(batch)?;
            Ok(())
        }

        fn flush(&self) -> StorageResult<()> {
            Ok(self.inner.flush().map(|_| {})?)
        }
    }

    pub type SledStateStore = RangeKvStateStore<SledRangeKv>;

    impl SledStateStore {
        pub fn new(path: impl AsRef<std::path::Path>) -> Self {
            RangeKvStateStore {
                inner: SledRangeKv::new(path),
            }
        }

        pub fn new_temp() -> Self {
            RangeKvStateStore {
                inner: SledRangeKv::new_temp(),
            }
        }
    }

    #[cfg(test)]
    mod test {
        use std::ops::{Bound, RangeBounds};

        use bytes::Bytes;
        use risingwave_common::catalog::TableId;
        use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKey};

        use crate::memory::sled::SledRangeKv;
        use crate::memory::RangeKv;

        #[test]
        fn test_filter_variable_key_length_false_positive() {
            let table_id = TableId { table_id: 233 };
            let epoch = u64::MAX - u64::from_be_bytes([1, 2, 3, 4, 5, 6, 7, 8]);
            let excluded_short_table_key = [0, 1, 0, 0];
            let included_long_table_key = [0, 1, 0, 0, 1, 2];
            let left_table_key = [0, 1, 0, 0, 1];
            let right_table_key = [0, 1, 1, 1];

            let to_full_key = |table_key: &[u8]| FullKey {
                user_key: UserKey {
                    table_id,
                    table_key: TableKey(Bytes::from(table_key.to_vec())),
                },
                epoch,
            };

            let left_full_key = to_full_key(&left_table_key[..]);
            let right_full_key = to_full_key(&right_table_key[..]);
            let included_long_full_key = to_full_key(&included_long_table_key[..]);
            let excluded_short_full_key = to_full_key(&excluded_short_table_key[..]);

            assert!((
                Bound::Included(left_full_key.to_ref()),
                Bound::Included(right_full_key.to_ref())
            )
                .contains(&included_long_full_key.to_ref()));
            assert!(!(
                Bound::Included(left_full_key.to_ref()),
                Bound::Included(right_full_key.to_ref())
            )
                .contains(&excluded_short_full_key.to_ref()));

            let left_encoded = left_full_key.encode_reverse_epoch();
            let right_encoded = right_full_key.encode_reverse_epoch();

            assert!((
                Bound::Included(left_encoded.clone()),
                Bound::Included(right_encoded.clone())
            )
                .contains(&included_long_full_key.encode_reverse_epoch()));
            assert!((
                Bound::Included(left_encoded),
                Bound::Included(right_encoded)
            )
                .contains(&excluded_short_full_key.encode_reverse_epoch()));

            let sled_range_kv = SledRangeKv::new_temp();
            sled_range_kv
                .ingest_batch(
                    vec![
                        (included_long_full_key.clone(), None),
                        (excluded_short_full_key, None),
                    ]
                    .into_iter(),
                )
                .unwrap();
            let kvs = sled_range_kv
                .range(
                    (
                        Bound::Included(left_full_key),
                        Bound::Included(right_full_key),
                    ),
                    None,
                )
                .unwrap();
            assert_eq!(1, kvs.len());
            assert_eq!(included_long_full_key.to_ref(), kvs[0].0.to_ref());
            assert!(kvs[0].1.is_none());
        }
    }
}

mod batched_iter {
    use itertools::Itertools;

    use super::*;

    /// A utility struct for iterating over a range of keys in a locked `BTreeMap`, which will batch
    /// some records to make a trade-off between the copying overhead and the times of acquiring
    /// the lock.
    ///
    /// Therefore, it's not guaranteed that we're iterating over a consistent snapshot of the map.
    /// Users should handle MVCC by themselves.
    pub struct Iter<R: RangeKv> {
        inner: R,
        range: BytesFullKeyRange,
        current: std::vec::IntoIter<(FullKey<Bytes>, Option<Bytes>)>,
    }

    impl<R: RangeKv> Iter<R> {
        pub fn new(inner: R, range: BytesFullKeyRange) -> Self {
            Self {
                inner,
                range,
                current: Vec::new().into_iter(),
            }
        }
    }

    impl<R: RangeKv> Iter<R> {
        const BATCH_SIZE: usize = 256;

        /// Get the next batch of records and fill the `current` buffer.
        fn refill(&mut self) -> StorageResult<()> {
            assert!(self.current.is_empty());

            let batch = self
                .inner
                .range(
                    (self.range.0.clone(), self.range.1.clone()),
                    Some(Self::BATCH_SIZE),
                )?
                .into_iter()
                .collect_vec();

            if let Some((last_key, _)) = batch.last() {
                let full_key = FullKey::new(
                    last_key.user_key.table_id,
                    TableKey(last_key.user_key.table_key.0.clone()),
                    last_key.epoch,
                );
                self.range.0 = Bound::Excluded(full_key);
            }
            self.current = batch.into_iter();
            Ok(())
        }
    }

    impl<R: RangeKv> Iter<R> {
        #[allow(clippy::type_complexity)]
        pub fn next(&mut self) -> StorageResult<Option<(BytesFullKey, Option<Bytes>)>> {
            match self.current.next() {
                Some((key, value)) => Ok(Some((key, value))),
                None => {
                    self.refill()?;
                    Ok(self.current.next())
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use rand::Rng;
        use risingwave_hummock_sdk::key::FullKey;

        use super::*;
        use crate::memory::sled::SledRangeKv;

        #[test]
        fn test_btreemap_iter_chaos() {
            let map = Arc::new(RwLock::new((BTreeMap::new(), BTreeMap::new())));
            test_iter_chaos_inner(map, 1000);
        }

        #[cfg(not(madsim))]
        #[test]
        fn test_sled_iter_chaos() {
            let map = SledRangeKv::new_temp();
            test_iter_chaos_inner(map, 100);
        }

        fn test_iter_chaos_inner(map: impl RangeKv, count: usize) {
            let key_range = 1..=10000;
            let num_to_bytes = |k: i32| Bytes::from(format!("{:06}", k).as_bytes().to_vec());
            let num_to_full_key =
                |k: i32| FullKey::new(TableId::default(), TableKey(num_to_bytes(k)), 0);
            #[allow(clippy::mutable_key_type)]
            map.ingest_batch(key_range.clone().map(|k| {
                let key = num_to_full_key(k);
                let b = key.user_key.table_key.0.clone();

                (key, Some(b))
            }))
            .unwrap();

            let rand_bound = || {
                let key = rand::thread_rng().gen_range(key_range.clone());
                let key = num_to_full_key(key);
                match rand::thread_rng().gen_range(1..=5) {
                    1 | 2 => Bound::Included(key),
                    3 | 4 => Bound::Excluded(key),
                    _ => Bound::Unbounded,
                }
            };

            for _ in 0..count {
                let range = loop {
                    let range = (rand_bound(), rand_bound());
                    let (start, end) = (range.start_bound(), range.end_bound());

                    // Filter out invalid ranges. Code migrated from `BTreeMap::range`.
                    match (start, end) {
                        (Bound::Excluded(s), Bound::Excluded(e)) if s == e => {
                            continue;
                        }
                        (
                            Bound::Included(s) | Bound::Excluded(s),
                            Bound::Included(e) | Bound::Excluded(e),
                        ) if s > e => {
                            continue;
                        }
                        _ => break range,
                    }
                };

                let v1 = {
                    let mut v = vec![];
                    let mut iter = Iter::new(map.clone(), range.clone());
                    while let Some((key, value)) = iter.next().unwrap() {
                        v.push((key, value));
                    }
                    v
                };
                let v2 = map.range(range, None).unwrap();

                // Items iterated from the batched iterator should be the same as normaliterator.
                assert_eq!(v1, v2);
            }
        }
    }
}

pub type MemoryStateStore = RangeKvStateStore<BTreeMapRangeKv>;

/// An in-memory state store
///
/// The in-memory state store is a [`BTreeMap`], which maps [`FullKey`] to value. It
/// never does GC, so the memory usage will be high. Therefore, in-memory state store should never
/// be used in production.
#[derive(Clone, Default)]
pub struct RangeKvStateStore<R: RangeKv> {
    /// Stores (key, epoch) -> user value.
    inner: R,
}

fn to_full_key_range<R, B>(table_id: TableId, table_key_range: R) -> BytesFullKeyRange
where
    R: RangeBounds<B> + Send,
    B: AsRef<[u8]>,
{
    let start = match table_key_range.start_bound() {
        Included(k) => Included(FullKey::new(
            table_id,
            TableKey(Bytes::from(k.as_ref().to_vec())),
            HummockEpoch::MAX,
        )),
        Excluded(k) => Excluded(FullKey::new(
            table_id,
            TableKey(Bytes::from(k.as_ref().to_vec())),
            0,
        )),
        Unbounded => Included(FullKey::new(
            table_id,
            TableKey(Bytes::from(b"".to_vec())),
            HummockEpoch::MAX,
        )),
    };
    let end = match table_key_range.end_bound() {
        Included(k) => Included(FullKey::new(
            table_id,
            TableKey(Bytes::from(k.as_ref().to_vec())),
            0,
        )),
        Excluded(k) => Excluded(FullKey::new(
            table_id,
            TableKey(Bytes::from(k.as_ref().to_vec())),
            HummockEpoch::MAX,
        )),
        Unbounded => {
            if let Some(next_table_id) = table_id.table_id().checked_add(1) {
                Excluded(FullKey::new(
                    next_table_id.into(),
                    TableKey(Bytes::from(b"".to_vec())),
                    HummockEpoch::MAX,
                ))
            } else {
                Unbounded
            }
        }
    };
    (start, end)
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn shared() -> Self {
        static STORE: LazyLock<MemoryStateStore> = LazyLock::new(MemoryStateStore::new);
        STORE.clone()
    }
}

impl<R: RangeKv> RangeKvStateStore<R> {
    fn scan(
        &self,
        key_range: IterKeyRange,
        epoch: u64,
        table_id: TableId,
        limit: Option<usize>,
    ) -> StorageResult<Vec<(Bytes, Bytes)>> {
        let mut data = vec![];
        if limit == Some(0) {
            return Ok(vec![]);
        }
        let mut last_user_key = None;
        for (key, value) in self
            .inner
            .range(to_full_key_range(table_id, key_range), None)?
        {
            if key.epoch > epoch {
                continue;
            }
            if Some(&key.user_key) != last_user_key.as_ref() {
                if let Some(value) = value {
                    data.push((Bytes::from(key.encode()), value.clone()));
                }
                last_user_key = Some(key.user_key.clone());
            }
            if let Some(limit) = limit && data.len() >= limit {
                break;
            }
        }
        Ok(data)
    }
}

impl<R: RangeKv> StateStoreRead for RangeKvStateStore<R> {
    type IterStream = StreamTypeOfIter<RangeKvStateStoreIter<R>>;

    define_state_store_read_associated_type!();

    fn get(&self, key: Bytes, epoch: u64, read_options: ReadOptions) -> Self::GetFuture<'_> {
        async move {
            let range_bounds = (Bound::Included(key.clone()), Bound::Included(key));
            // We do not really care about vnodes here, so we just use the default value.
            let res = self.scan(range_bounds, epoch, read_options.table_id, Some(1))?;

            Ok(match res.as_slice() {
                [] => None,
                [(_, value)] => Some(value.clone()),
                _ => unreachable!(),
            })
        }
    }

    fn iter(
        &self,
        key_range: IterKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        async move {
            Ok(RangeKvStateStoreIter::new(
                batched_iter::Iter::new(
                    self.inner.clone(),
                    to_full_key_range(read_options.table_id, key_range),
                ),
                epoch,
            )
            .into_stream())
        }
    }
}

impl<R: RangeKv> StateStoreWrite for RangeKvStateStore<R> {
    define_state_store_write_associated_type!();

    fn ingest_batch(
        &self,
        mut kv_pairs: Vec<(Bytes, StorageValue)>,
        delete_ranges: Vec<(Bytes, Bytes)>,
        write_options: WriteOptions,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            let epoch = write_options.epoch;

            let mut delete_keys = BTreeSet::new();
            for del_range in delete_ranges {
                let fullkey_start =
                    FullKey::new(write_options.table_id, TableKey(del_range.0), epoch);
                let fullkey_end =
                    FullKey::new(write_options.table_id, TableKey(del_range.1), epoch);
                for (key, _) in self.inner.range(
                    (Bound::Included(fullkey_start), Bound::Excluded(fullkey_end)),
                    None,
                )? {
                    delete_keys.insert(key.user_key.table_key.0);
                }
            }
            for key in delete_keys {
                kv_pairs.push((key, StorageValue::new_delete()));
            }

            let mut size = 0;
            self.inner
                .ingest_batch(kv_pairs.into_iter().map(|(key, value)| {
                    size += key.len() + value.size();
                    (
                        FullKey::new(write_options.table_id, TableKey(key), epoch),
                        value.user_value,
                    )
                }))?;
            Ok(size)
        }
    }
}

impl<R: RangeKv> StateStore for RangeKvStateStore<R> {
    type Local = MemtableLocalStateStore<Self>;

    type NewLocalFuture<'a> = impl Future<Output = Self::Local> + Send + 'a;

    define_state_store_associated_type!();

    fn try_wait_epoch(&self, _epoch: HummockReadEpoch) -> Self::WaitEpochFuture<'_> {
        async move {
            // memory backend doesn't need to wait for epoch, so this is a no-op.
            Ok(())
        }
    }

    fn sync(&self, _epoch: u64) -> Self::SyncFuture<'_> {
        async move {
            self.inner.flush()?;
            // memory backend doesn't need to push to S3, so this is a no-op
            Ok(SyncResult {
                ..Default::default()
            })
        }
    }

    fn seal_epoch(&self, _epoch: u64, _is_checkpoint: bool) {}

    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        async move { Ok(()) }
    }

    fn new_local(&self, option: NewLocalOptions) -> Self::NewLocalFuture<'_> {
        async move { MemtableLocalStateStore::new(self.clone(), option) }
    }

    fn validate_read_epoch(&self, _epoch: HummockReadEpoch) -> StorageResult<()> {
        Ok(())
    }
}

pub struct RangeKvStateStoreIter<R: RangeKv> {
    inner: batched_iter::Iter<R>,

    epoch: HummockEpoch,

    last_key: Option<UserKey<Bytes>>,

    /// For supporting semantic of `Fuse`
    stopped: bool,
}

impl<R: RangeKv> RangeKvStateStoreIter<R> {
    pub fn new(inner: batched_iter::Iter<R>, epoch: HummockEpoch) -> Self {
        Self {
            inner,
            epoch,
            last_key: None,
            stopped: false,
        }
    }
}

impl<R: RangeKv> StateStoreIter for RangeKvStateStoreIter<R> {
    type Item = StateStoreIterItem;

    type NextFuture<'a> = impl StateStoreIterNextFutureTrait<'a>;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            if self.stopped {
                Ok(None)
            } else {
                let ret = self.next_inner();
                match &ret {
                    Err(_) | Ok(None) => {
                        self.stopped = true;
                    }
                    _ => {}
                }

                ret
            }
        }
    }
}

impl<R: RangeKv> RangeKvStateStoreIter<R> {
    fn next_inner(&mut self) -> StorageResult<Option<StateStoreIterItem>> {
        while let Some((key, value)) = self.inner.next()? {
            if key.epoch > self.epoch {
                continue;
            }
            if Some(key.user_key.as_ref()) != self.last_key.as_ref().map(|key| key.as_ref()) {
                self.last_key = Some(key.user_key.clone());
                if let Some(value) = value {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::sled::SledStateStore;

    #[tokio::test]
    async fn test_snapshot_isolation_memory() {
        let state_store = MemoryStateStore::new();
        test_snapshot_isolation_inner(state_store).await;
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_snapshot_isolation_sled() {
        let state_store = SledStateStore::new_temp();
        test_snapshot_isolation_inner(state_store).await;
    }

    async fn test_snapshot_isolation_inner(state_store: RangeKvStateStore<impl RangeKv>) {
        state_store
            .ingest_batch(
                vec![
                    (b"a".to_vec().into(), StorageValue::new_put(b"v1".to_vec())),
                    (b"b".to_vec().into(), StorageValue::new_put(b"v1".to_vec())),
                ],
                vec![],
                WriteOptions {
                    epoch: 0,
                    table_id: Default::default(),
                },
            )
            .await
            .unwrap();
        state_store
            .ingest_batch(
                vec![
                    (b"a".to_vec().into(), StorageValue::new_put(b"v2".to_vec())),
                    (b"b".to_vec().into(), StorageValue::new_delete()),
                ],
                vec![],
                WriteOptions {
                    epoch: 1,
                    table_id: Default::default(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            state_store
                .scan(
                    (
                        Bound::Included(Bytes::from("a")),
                        Bound::Included(Bytes::from("b")),
                    ),
                    0,
                    TableId::default(),
                    None,
                )
                .unwrap(),
            vec![
                (
                    FullKey::for_test(Default::default(), Bytes::from("a"), 0)
                        .encode()
                        .into(),
                    b"v1".to_vec().into()
                ),
                (
                    FullKey::for_test(Default::default(), Bytes::from("b"), 0)
                        .encode()
                        .into(),
                    b"v1".to_vec().into()
                )
            ]
        );
        assert_eq!(
            state_store
                .scan(
                    (
                        Bound::Included(Bytes::from("a")),
                        Bound::Included(Bytes::from("b")),
                    ),
                    0,
                    TableId::default(),
                    Some(1),
                )
                .unwrap(),
            vec![(
                FullKey::for_test(Default::default(), b"a".to_vec(), 0)
                    .encode()
                    .into(),
                b"v1".to_vec().into()
            )]
        );
        assert_eq!(
            state_store
                .scan(
                    (
                        Bound::Included(Bytes::from("a")),
                        Bound::Included(Bytes::from("b")),
                    ),
                    1,
                    TableId::default(),
                    None,
                )
                .unwrap(),
            vec![(
                FullKey::for_test(Default::default(), b"a".to_vec(), 1)
                    .encode()
                    .into(),
                b"v2".to_vec().into()
            )]
        );
        assert_eq!(
            state_store
                .get(Bytes::from("a"), 0, ReadOptions::default(),)
                .await
                .unwrap(),
            Some(Bytes::from("v1"))
        );
        assert_eq!(
            state_store
                .get(Bytes::copy_from_slice(b"b"), 0, ReadOptions::default(),)
                .await
                .unwrap(),
            Some(b"v1".to_vec().into())
        );
        assert_eq!(
            state_store
                .get(Bytes::copy_from_slice(b"c"), 0, ReadOptions::default(),)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            state_store
                .get(Bytes::copy_from_slice(b"a"), 1, ReadOptions::default(),)
                .await
                .unwrap(),
            Some(b"v2".to_vec().into())
        );
        assert_eq!(
            state_store
                .get(Bytes::from("b"), 1, ReadOptions::default(),)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            state_store
                .get(Bytes::from("c"), 1, ReadOptions::default())
                .await
                .unwrap(),
            None
        );
    }
}
