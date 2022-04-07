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

use std::ops::Bound::{self, *};
use std::sync::Arc;

use risingwave_hummock_sdk::key::{get_epoch, key_with_epoch, user_key as to_user_key, Epoch};

use super::{HummockIterator, MergeIterator};
use crate::hummock::iterator::ReverseUserIterator;
use crate::hummock::local_version_manager::ScopedLocalVersion;
use crate::hummock::value::HummockValue;
use crate::hummock::HummockResult;

pub enum DirectedUserIterator<'a> {
    Forward(UserIterator<'a>),
    Backward(ReverseUserIterator<'a>),
}

impl DirectedUserIterator<'_> {
    pub async fn next(&mut self) -> HummockResult<()> {
        match self {
            Self::Forward(ref mut iter) => iter.next().await,
            Self::Backward(ref mut iter) => iter.next().await,
        }
    }

    #[inline(always)]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Forward(iter) => iter.key(),
            Self::Backward(iter) => iter.key(),
        }
    }

    #[inline(always)]
    pub fn value(&self) -> &[u8] {
        match self {
            Self::Forward(iter) => iter.value(),
            Self::Backward(iter) => iter.value(),
        }
    }

    pub async fn rewind(&mut self) -> HummockResult<()> {
        match self {
            Self::Forward(ref mut iter) => iter.rewind().await,
            Self::Backward(ref mut iter) => iter.rewind().await,
        }
    }

    #[allow(dead_code)]
    pub async fn seek(&mut self, user_key: &[u8]) -> HummockResult<()> {
        match self {
            Self::Forward(ref mut iter) => iter.seek(user_key).await,
            Self::Backward(ref mut iter) => iter.seek(user_key).await,
        }
    }

    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        match self {
            Self::Forward(iter) => iter.is_valid(),
            Self::Backward(iter) => iter.is_valid(),
        }
    }
}

/// [`UserIterator`] can be used by user directly.
pub struct UserIterator<'a> {
    /// Inner table iterator.
    iterator: MergeIterator<'a>,

    /// Last user key
    last_key: Vec<u8>,

    /// Last user value
    last_val: Vec<u8>,

    /// Flag for whether the iterator reach over the right end of the range.
    out_of_range: bool,

    /// Start and end bounds of user key.
    key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),

    /// Only reads values if `ts <= self.read_epoch`.
    read_epoch: Epoch,

    /// Ensures the SSTs needed by `iterator` won't be vacuumed.
    _version: Option<Arc<ScopedLocalVersion>>,
}

// TODO: decide whether this should also impl `HummockIterator`
impl<'a> UserIterator<'a> {
    /// Create [`UserIterator`] with maximum epoch.
    #[cfg(test)]
    pub(crate) fn for_test(
        iterator: MergeIterator<'a>,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> Self {
        Self::new(iterator, key_range, Epoch::MAX, None)
    }

    /// Create [`UserIterator`] with given `read_epoch`.
    pub(crate) fn new(
        iterator: MergeIterator<'a>,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        read_epoch: u64,
        version: Option<Arc<ScopedLocalVersion>>,
    ) -> Self {
        Self {
            iterator,
            out_of_range: false,
            key_range,
            last_key: Vec::new(),
            last_val: Vec::new(),
            read_epoch,
            _version: version,
        }
    }

    /// Gets the iterator move to the next step.
    ///
    /// Returned result:
    /// - if `Ok(())` is returned, it means that the iterator successfully move to the next position
    ///   (may reach to the end and thus not valid)
    /// - if `Err(_) ` is returned, it means that some error happened.
    pub async fn next(&mut self) -> HummockResult<()> {
        while self.iterator.is_valid() {
            let full_key = self.iterator.key();
            let epoch = get_epoch(full_key);
            let key = to_user_key(full_key);

            // handle multi-version
            if self.last_key.as_slice() != key && epoch <= self.read_epoch {
                self.last_key.clear();
                self.last_key.extend_from_slice(key);

                // handle delete operation
                match self.iterator.value() {
                    HummockValue::Put(_, val) => {
                        self.last_val.clear();
                        self.last_val.extend_from_slice(val);

                        // handle range scan
                        match &self.key_range.1 {
                            Included(end_key) => self.out_of_range = key > end_key.as_slice(),
                            Excluded(end_key) => self.out_of_range = key >= end_key.as_slice(),
                            Unbounded => {}
                        };
                        return Ok(());
                    }
                    // It means that the key is deleted from the storage.
                    // Deleted kv and the previous versions (if any) of the key should not be
                    // returned to user.
                    HummockValue::Delete(_) => {}
                }
            }

            self.iterator.next().await?;
        }

        Ok(()) // not valid, EOF
    }

    /// Returns the key with the newest version. Thus no version in it, and only the `user_key` will
    /// be returned.
    ///
    /// The returned key is de-duplicated and thus it will not output the same key, unless the
    /// `rewind` or `seek` methods are called.
    ///
    /// Note: before call the function you need to ensure that the iterator is valid.
    pub fn key(&self) -> &[u8] {
        assert!(self.is_valid());
        self.last_key.as_slice()
    }

    /// The returned value is in the form of user value.
    ///
    /// Note: before call the function you need to ensure that the iterator is valid.
    pub fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        self.last_val.as_slice()
    }

    /// Resets the iterating position to the beginning.
    pub async fn rewind(&mut self) -> HummockResult<()> {
        // Handle range scan
        match &self.key_range.0 {
            Included(begin_key) => {
                let full_key = &key_with_epoch(begin_key.clone(), self.read_epoch);
                self.iterator.seek(full_key).await?;
            }
            Excluded(_) => unimplemented!("excluded begin key is not supported"),
            Unbounded => self.iterator.rewind().await?,
        };

        // Handle multi-version
        self.last_key.clear();
        // Handles range scan when key > end_key
        self.next().await
    }

    /// Resets the iterating position to the first position where the key >= provided key.
    pub async fn seek(&mut self, user_key: &[u8]) -> HummockResult<()> {
        // Handle range scan when key < begin_key
        let user_key = match &self.key_range.0 {
            Included(begin_key) => {
                if begin_key.as_slice() > user_key {
                    begin_key.clone()
                } else {
                    Vec::from(user_key)
                }
            }
            Excluded(_) => unimplemented!("excluded begin key is not supported"),
            Unbounded => Vec::from(user_key),
        };

        let full_key = &key_with_epoch(user_key, self.read_epoch);
        self.iterator.seek(full_key).await?;

        // Handle multi-version
        self.last_key.clear();
        // Handle range scan when key > end_key
        let res = self.next().await;
        res
    }

    /// Indicates whether the iterator can be used.
    pub fn is_valid(&self) -> bool {
        // Handle range scan
        // key >= begin_key is guaranteed by seek/rewind function
        (!self.out_of_range) && self.iterator.is_valid()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound::*;
    use std::sync::Arc;

    use risingwave_hummock_sdk::key::user_key;

    use super::*;
    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, gen_iterator_test_sstable_base,
        gen_iterator_test_sstable_from_kv_pair, iterator_test_key_of, iterator_test_key_of_epoch,
        iterator_test_value_of, mock_sstable_store, TEST_KEYS_COUNT,
    };
    use crate::hummock::iterator::BoxedHummockIterator;
    use crate::hummock::sstable::SSTableIterator;
    use crate::hummock::value::HummockValue;
    use crate::monitor::StateStoreMetrics;

    #[tokio::test]
    async fn test_basic() {
        let sstable_store = mock_sstable_store();
        let table0 = gen_iterator_test_sstable_base(
            0,
            default_builder_opt_for_test(),
            |x| x * 3,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table1 = gen_iterator_test_sstable_base(
            1,
            default_builder_opt_for_test(),
            |x| x * 3 + 1,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table2 = gen_iterator_test_sstable_base(
            2,
            default_builder_opt_for_test(),
            |x| x * 3 + 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let iters: Vec<BoxedHummockIterator> = vec![
            Box::new(SSTableIterator::new(
                Arc::new(table0),
                sstable_store.clone(),
            )),
            Box::new(SSTableIterator::new(
                Arc::new(table1),
                sstable_store.clone(),
            )),
            Box::new(SSTableIterator::new(Arc::new(table2), sstable_store)),
        ];

        let mi = MergeIterator::new(iters, Arc::new(StateStoreMetrics::unused()));
        let mut ui = UserIterator::for_test(mi, (Unbounded, Unbounded));
        ui.rewind().await.unwrap();

        let mut i = 0;
        while ui.is_valid() {
            let key = ui.key();
            let val = ui.value();
            assert_eq!(key, user_key(iterator_test_key_of(i).as_slice()));
            assert_eq!(val, iterator_test_value_of(i).as_slice());
            i += 1;
            ui.next().await.unwrap();
            if i == 0 {
                assert!(!ui.is_valid());
                break;
            }
        }
        assert!(i >= TEST_KEYS_COUNT * 3);
    }

    #[tokio::test]
    async fn test_seek() {
        let sstable_store = mock_sstable_store();
        let table0 = gen_iterator_test_sstable_base(
            0,
            default_builder_opt_for_test(),
            |x| x * 3,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table1 = gen_iterator_test_sstable_base(
            1,
            default_builder_opt_for_test(),
            |x| x * 3 + 1,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table2 = gen_iterator_test_sstable_base(
            2,
            default_builder_opt_for_test(),
            |x| x * 3 + 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let iters: Vec<BoxedHummockIterator> = vec![
            Box::new(SSTableIterator::new(
                Arc::new(table0),
                sstable_store.clone(),
            )),
            Box::new(SSTableIterator::new(
                Arc::new(table1),
                sstable_store.clone(),
            )),
            Box::new(SSTableIterator::new(Arc::new(table2), sstable_store)),
        ];

        let mi = MergeIterator::new(iters, Arc::new(StateStoreMetrics::unused()));
        let mut ui = UserIterator::for_test(mi, (Unbounded, Unbounded));

        // right edge case
        ui.seek(user_key(
            iterator_test_key_of(3 * TEST_KEYS_COUNT).as_slice(),
        ))
        .await
        .unwrap();
        assert!(!ui.is_valid());

        // normal case
        ui.seek(user_key(
            iterator_test_key_of(TEST_KEYS_COUNT + 5).as_slice(),
        ))
        .await
        .unwrap();
        let k = ui.key();
        let v = ui.value();
        assert_eq!(v, iterator_test_value_of(TEST_KEYS_COUNT + 5).as_slice());
        assert_eq!(
            k,
            user_key(iterator_test_key_of(TEST_KEYS_COUNT + 5).as_slice())
        );
        ui.seek(user_key(
            iterator_test_key_of(2 * TEST_KEYS_COUNT + 5).as_slice(),
        ))
        .await
        .unwrap();
        let k = ui.key();
        let v = ui.value();
        assert_eq!(
            v,
            iterator_test_value_of(2 * TEST_KEYS_COUNT + 5).as_slice()
        );
        assert_eq!(
            k,
            user_key(iterator_test_key_of(2 * TEST_KEYS_COUNT + 5).as_slice())
        );

        // left edge case
        ui.seek(user_key(iterator_test_key_of(0).as_slice()))
            .await
            .unwrap();
        let k = ui.key();
        let v = ui.value();
        assert_eq!(v, iterator_test_value_of(0).as_slice());
        assert_eq!(k, user_key(iterator_test_key_of(0).as_slice()));
    }

    #[tokio::test]
    async fn test_delete() {
        let sstable_store = mock_sstable_store();

        // key=[idx, epoch], value
        let kv_pairs = vec![
            (1, 100, HummockValue::put(iterator_test_value_of(1))),
            (2, 300, HummockValue::delete()),
        ];
        let table0 =
            gen_iterator_test_sstable_from_kv_pair(0, kv_pairs, sstable_store.clone()).await;

        let kv_pairs = vec![
            (1, 200, HummockValue::delete()),
            (2, 400, HummockValue::put(iterator_test_value_of(2))),
        ];
        let table1 =
            gen_iterator_test_sstable_from_kv_pair(1, kv_pairs, sstable_store.clone()).await;

        let iters: Vec<BoxedHummockIterator> = vec![
            Box::new(SSTableIterator::new(
                Arc::new(table0),
                sstable_store.clone(),
            )),
            Box::new(SSTableIterator::new(
                Arc::new(table1),
                sstable_store.clone(),
            )),
        ];
        let mi = MergeIterator::new(iters, Arc::new(StateStoreMetrics::unused()));
        let mut ui = UserIterator::for_test(mi, (Unbounded, Unbounded));
        ui.rewind().await.unwrap();

        // verify
        let k = ui.key();
        let v = ui.value();
        assert_eq!(k, user_key(iterator_test_key_of(2).as_slice()));
        assert_eq!(v, iterator_test_value_of(2));

        // only one valid kv pair
        ui.next().await.unwrap();
        assert!(!ui.is_valid());
    }

    // left..=end
    #[tokio::test]
    async fn test_range_inclusive() {
        let sstable_store = mock_sstable_store();
        // key=[idx, epoch], value
        let kv_pairs = vec![
            (0, 200, HummockValue::delete()),
            (0, 100, HummockValue::put(iterator_test_value_of(0))),
            (1, 200, HummockValue::put(iterator_test_value_of(1))),
            (1, 100, HummockValue::delete()),
            (2, 300, HummockValue::put(iterator_test_value_of(2))),
            (2, 200, HummockValue::delete()),
            (2, 100, HummockValue::delete()),
            (3, 100, HummockValue::put(iterator_test_value_of(3))),
            (5, 200, HummockValue::delete()),
            (5, 100, HummockValue::put(iterator_test_value_of(5))),
            (6, 100, HummockValue::put(iterator_test_value_of(6))),
            (7, 200, HummockValue::delete()),
            (7, 100, HummockValue::put(iterator_test_value_of(7))),
            (8, 100, HummockValue::put(iterator_test_value_of(8))),
        ];
        let table =
            gen_iterator_test_sstable_from_kv_pair(0, kv_pairs, sstable_store.clone()).await;
        let iters: Vec<BoxedHummockIterator> = vec![Box::new(SSTableIterator::new(
            Arc::new(table),
            sstable_store,
        ))];
        let mi = MergeIterator::new(iters, Arc::new(StateStoreMetrics::unused()));

        let begin_key = Included(user_key(iterator_test_key_of_epoch(2, 0).as_slice()).to_vec());
        let end_key = Included(user_key(iterator_test_key_of_epoch(7, 0).as_slice()).to_vec());

        let mut ui = UserIterator::for_test(mi, (begin_key, end_key));

        // ----- basic iterate -----
        ui.rewind().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- before-begin-range iterate -----
        ui.seek(user_key(iterator_test_key_of(1).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- begin-range iterate -----
        ui.seek(user_key(iterator_test_key_of(2).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- end-range iterate -----
        ui.seek(user_key(iterator_test_key_of(7).as_slice()))
            .await
            .unwrap();
        assert!(!ui.is_valid());

        // ----- after-end-range iterate -----
        ui.seek(user_key(iterator_test_key_of(8).as_slice()))
            .await
            .unwrap();
        assert!(!ui.is_valid());
    }

    // left..end
    #[tokio::test]
    async fn test_range() {
        let sstable_store = mock_sstable_store();
        // key=[idx, epoch], value
        let kv_pairs = vec![
            (0, 200, HummockValue::delete()),
            (0, 100, HummockValue::put(iterator_test_value_of(0))),
            (1, 200, HummockValue::put(iterator_test_value_of(1))),
            (1, 100, HummockValue::delete()),
            (2, 300, HummockValue::put(iterator_test_value_of(2))),
            (2, 200, HummockValue::delete()),
            (2, 100, HummockValue::delete()),
            (3, 100, HummockValue::put(iterator_test_value_of(3))),
            (5, 200, HummockValue::delete()),
            (5, 100, HummockValue::put(iterator_test_value_of(5))),
            (6, 100, HummockValue::put(iterator_test_value_of(6))),
            (7, 100, HummockValue::put(iterator_test_value_of(7))),
            (8, 100, HummockValue::put(iterator_test_value_of(8))),
        ];
        let table =
            gen_iterator_test_sstable_from_kv_pair(0, kv_pairs, sstable_store.clone()).await;
        let iters: Vec<BoxedHummockIterator> = vec![Box::new(SSTableIterator::new(
            Arc::new(table),
            sstable_store,
        ))];
        let mi = MergeIterator::new(iters, Arc::new(StateStoreMetrics::unused()));

        let begin_key = Included(user_key(iterator_test_key_of_epoch(2, 0).as_slice()).to_vec());
        let end_key = Excluded(user_key(iterator_test_key_of_epoch(7, 0).as_slice()).to_vec());

        let mut ui = UserIterator::for_test(mi, (begin_key, end_key));

        // ----- basic iterate -----
        ui.rewind().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- before-begin-range iterate -----
        ui.seek(user_key(iterator_test_key_of(1).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- begin-range iterate -----
        ui.seek(user_key(iterator_test_key_of(2).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- end-range iterate -----
        ui.seek(user_key(iterator_test_key_of(7).as_slice()))
            .await
            .unwrap();
        assert!(!ui.is_valid());

        // ----- after-end-range iterate -----
        ui.seek(user_key(iterator_test_key_of(8).as_slice()))
            .await
            .unwrap();
        assert!(!ui.is_valid());
    }

    // ..=right
    #[tokio::test]
    async fn test_range_to_inclusive() {
        let sstable_store = mock_sstable_store();
        // key=[idx, epoch], value
        let kv_pairs = vec![
            (0, 200, HummockValue::delete()),
            (0, 100, HummockValue::put(iterator_test_value_of(0))),
            (1, 200, HummockValue::put(iterator_test_value_of(1))),
            (1, 100, HummockValue::delete()),
            (2, 300, HummockValue::put(iterator_test_value_of(2))),
            (2, 200, HummockValue::delete()),
            (2, 100, HummockValue::delete()),
            (3, 100, HummockValue::put(iterator_test_value_of(3))),
            (5, 200, HummockValue::delete()),
            (5, 100, HummockValue::put(iterator_test_value_of(5))),
            (6, 100, HummockValue::put(iterator_test_value_of(6))),
            (7, 200, HummockValue::delete()),
            (7, 100, HummockValue::put(iterator_test_value_of(7))),
            (8, 100, HummockValue::put(iterator_test_value_of(8))),
        ];
        let table =
            gen_iterator_test_sstable_from_kv_pair(0, kv_pairs, sstable_store.clone()).await;
        let iters: Vec<BoxedHummockIterator> = vec![Box::new(SSTableIterator::new(
            Arc::new(table),
            sstable_store,
        ))];
        let mi = MergeIterator::new(iters, Arc::new(StateStoreMetrics::unused()));
        let end_key = Included(user_key(iterator_test_key_of_epoch(7, 0).as_slice()).to_vec());

        let mut ui = UserIterator::for_test(mi, (Unbounded, end_key));

        // ----- basic iterate -----
        ui.rewind().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(1).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- begin-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(1).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- in-range iterate -----
        ui.seek(user_key(iterator_test_key_of(2).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- end-range iterate -----
        ui.seek(user_key(iterator_test_key_of(7).as_slice()))
            .await
            .unwrap();
        assert!(!ui.is_valid());

        // ----- after-end-range iterate -----
        ui.seek(user_key(iterator_test_key_of(8).as_slice()))
            .await
            .unwrap();
        assert!(!ui.is_valid());
    }

    // left..
    #[tokio::test]
    async fn test_range_from() {
        let sstable_store = mock_sstable_store();
        // key=[idx, epoch], value
        let kv_pairs = vec![
            (0, 200, HummockValue::delete()),
            (0, 100, HummockValue::put(iterator_test_value_of(0))),
            (1, 200, HummockValue::put(iterator_test_value_of(1))),
            (1, 100, HummockValue::delete()),
            (2, 300, HummockValue::put(iterator_test_value_of(2))),
            (2, 200, HummockValue::delete()),
            (2, 100, HummockValue::delete()),
            (3, 100, HummockValue::put(iterator_test_value_of(3))),
            (5, 200, HummockValue::delete()),
            (5, 100, HummockValue::put(iterator_test_value_of(5))),
            (6, 100, HummockValue::put(iterator_test_value_of(6))),
            (7, 200, HummockValue::delete()),
            (7, 100, HummockValue::put(iterator_test_value_of(7))),
            (8, 100, HummockValue::put(iterator_test_value_of(8))),
        ];
        let table =
            gen_iterator_test_sstable_from_kv_pair(0, kv_pairs, sstable_store.clone()).await;
        let iters: Vec<BoxedHummockIterator> = vec![Box::new(SSTableIterator::new(
            Arc::new(table),
            sstable_store,
        ))];
        let mi = MergeIterator::new(iters, Arc::new(StateStoreMetrics::unused()));
        let begin_key = Included(user_key(iterator_test_key_of_epoch(2, 0).as_slice()).to_vec());

        let mut ui = UserIterator::for_test(mi, (begin_key, Unbounded));

        // ----- basic iterate -----
        ui.rewind().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(6).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(8).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- begin-range iterate -----
        ui.seek(user_key(iterator_test_key_of(1).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(6).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(8).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- in-range iterate -----
        ui.seek(user_key(iterator_test_key_of(2).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(6).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(8).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- end-range iterate -----
        ui.seek(user_key(iterator_test_key_of(8).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(8).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- after-end-range iterate -----
        ui.seek(user_key(iterator_test_key_of(9).as_slice()))
            .await
            .unwrap();
        assert!(!ui.is_valid());
    }
}
