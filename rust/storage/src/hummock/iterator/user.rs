use std::ops::Bound::{self, *};
use std::sync::Arc;

use super::{HummockIterator, MergeIterator};
use crate::hummock::iterator::ReverseUserIterator;
use crate::hummock::key::{get_epoch, key_with_epoch, user_key as to_user_key, Epoch};
use crate::hummock::value::HummockValue;
use crate::hummock::HummockResult;
use crate::monitor::{StateStoreStats, DEFAULT_STATE_STORE_STATS};

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

    pub fn key(&self) -> &[u8] {
        match self {
            Self::Forward(iter) => iter.key(),
            Self::Backward(iter) => iter.key(),
        }
    }

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

    /// Only read values if `ts <= self.read_epoch`.
    read_epoch: Epoch,

    stats: Arc<StateStoreStats>,
}

// TODO: decide wheher this should also impl `HummockIterator`
impl<'a> UserIterator<'a> {
    /// Create [`UserIterator`] with maximum epoch.
    #[cfg(test)]
    pub(crate) fn new(
        iterator: MergeIterator<'a>,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> Self {
        Self::new_with_epoch(iterator, key_range, Epoch::MAX)
    }

    /// Create [`UserIterator`] with given `read_epoch`.
    pub(crate) fn new_with_epoch(
        iterator: MergeIterator<'a>,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        read_epoch: u64,
    ) -> Self {
        Self {
            iterator,
            out_of_range: false,
            key_range,
            last_key: Vec::new(),
            last_val: Vec::new(),
            read_epoch,
            stats: DEFAULT_STATE_STORE_STATS.clone(),
        }
    }

    /// Get the iterator move to the next step.
    ///
    /// Returned result:
    /// - if `Ok(())` is returned, it means that the iterator successfully move to the next position
    ///   (may reach to the end and thus not valid)
    /// - if `Err(_) ` is returned, it means that some error happended.
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
                    HummockValue::Put(val) => {
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
                    // Deleted kv and the previous verisons (if any) of the key should not be
                    // returned to user.
                    HummockValue::Delete => {}
                }
            }

            self.iterator.next().await?;
        }

        Ok(()) // not valid, EOF
    }

    /// Return the key with the newest version. Thus no version in it, and only the `user_key` will
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

    /// Reset the iterating position to the beginning.
    pub async fn rewind(&mut self) -> HummockResult<()> {
        // handle range scan
        match &self.key_range.0 {
            Included(begin_key) => {
                let full_key = &key_with_epoch(begin_key.clone(), self.read_epoch);
                self.iterator.seek(full_key).await?;
            }
            Excluded(_) => unimplemented!("excluded begin key is not supported"),
            Unbounded => self.iterator.rewind().await?,
        };

        // handle multi-version
        self.last_key.clear();
        // handle range scan when key > end_key
        self.next().await
    }

    /// Reset the iterating position to the first position where the key >= provided key.
    pub async fn seek(&mut self, user_key: &[u8]) -> HummockResult<()> {
        let timer = self.stats.iter_seek_latency.start_timer();
        // handle range scan when key < begin_key
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

        // handle multi-version
        self.last_key.clear();
        // handle range scan when key > end_key
        let res = self.next().await;
        timer.observe_duration();
        res
    }

    /// Indicate whether the iterator can be used.
    pub fn is_valid(&self) -> bool {
        // handle range scan
        // key >= begin_key is guaranteed by seek/rewind function
        (!self.out_of_range) && self.iterator.is_valid()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound::*;
    use std::sync::Arc;

    use itertools::Itertools;

    use super::*;
    use crate::hummock::cloud::gen_remote_sstable;
    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, iterator_test_key_of, iterator_test_key_of_epoch, test_key,
        test_value_of, TestIteratorBuilder, TEST_KEYS_COUNT,
    };
    use crate::hummock::iterator::variants::FORWARD;
    use crate::hummock::iterator::BoxedHummockIterator;
    use crate::hummock::key::user_key;
    use crate::hummock::sstable::{SSTable, SSTableIterator};
    use crate::hummock::value::HummockValue;
    use crate::hummock::SSTableBuilder;
    use crate::object::{InMemObjectStore, ObjectStore};

    #[tokio::test]
    async fn test_basic() {
        let (iters, validators): (Vec<_>, Vec<_>) = (0..3)
            .map(|iter_id| {
                TestIteratorBuilder::<FORWARD>::default()
                    .id(0)
                    .map_key(move |id, x| iterator_test_key_of(id, x * 3 + (iter_id as usize)))
                    .map_value(move |id, x| test_value_of(id, x * 3 + (iter_id as usize) + 1))
                    .finish()
            })
            .unzip();

        let iters: Vec<BoxedHummockIterator> = iters
            .into_iter()
            .map(|x| Box::new(x) as BoxedHummockIterator)
            .collect_vec();

        let mi = MergeIterator::new(iters);
        let mut ui = UserIterator::new(mi, (Unbounded, Unbounded));
        ui.rewind().await.unwrap();

        let mut i = 0;
        while ui.is_valid() {
            let k = ui.key();
            let v = ui.value();
            validators[i % 3].assert_value(i / 3, v);
            validators[i % 3].assert_user_key(i / 3, k);
            i += 1;

            ui.next().await.unwrap();
            if i == TEST_KEYS_COUNT * 3 {
                assert!(!ui.is_valid());
                break;
            }
        }
        assert!(i >= TEST_KEYS_COUNT * 3)
    }

    #[tokio::test]
    async fn test_seek() {
        let (iters, validators): (Vec<_>, Vec<_>) = (0..3)
            .map(|iter_id| {
                TestIteratorBuilder::<FORWARD>::default()
                    .id(0)
                    .total(20)
                    .map_key(move |id, x| iterator_test_key_of(id, x * 3 + (iter_id as usize)))
                    .map_value(move |id, x| test_value_of(id, x * 3 + (iter_id as usize) + 1))
                    .finish()
            })
            .unzip();

        let iters: Vec<BoxedHummockIterator> = iters
            .into_iter()
            .map(|x| Box::new(x) as BoxedHummockIterator)
            .collect_vec();

        let mi = MergeIterator::new(iters);
        let mut ui = UserIterator::new(mi, (Unbounded, Unbounded));
        let test_validator = &validators[2];

        // right edge case
        ui.seek(user_key(test_key!(test_validator, 3 * TEST_KEYS_COUNT)))
            .await
            .unwrap();
        assert!(!ui.is_valid());

        // normal case
        ui.seek(user_key(test_key!(test_validator, 4)))
            .await
            .unwrap();
        let k = ui.key();
        let v = ui.value();
        test_validator.assert_value(4, v);
        test_validator.assert_user_key(4, k);

        ui.seek(user_key(test_key!(test_validator, 17)))
            .await
            .unwrap();
        let k = ui.key();
        let v = ui.value();
        test_validator.assert_value(17, v);
        test_validator.assert_user_key(17, k);

        // left edge case
        ui.seek(user_key(test_key!(test_validator, 0)))
            .await
            .unwrap();
        let k = ui.key();
        let v = ui.value();

        test_validator.assert_user_key(0, k);
        test_validator.assert_value(0, v);
    }

    #[tokio::test]
    async fn test_delete() {
        // key=[table, idx, epoch], value
        let kv_pairs = vec![
            (0, 1, 100, HummockValue::Put(test_value_of(0, 1))),
            (0, 2, 300, HummockValue::Delete),
        ];
        let table0 = add_kv_pair(kv_pairs).await;

        let kv_pairs = vec![
            (0, 1, 200, HummockValue::Delete),
            (0, 2, 400, HummockValue::Put(test_value_of(0, 2))),
        ];
        let table1 = add_kv_pair(kv_pairs).await;

        let iters: Vec<BoxedHummockIterator> = vec![
            Box::new(SSTableIterator::new(Arc::new(table0))),
            Box::new(SSTableIterator::new(Arc::new(table1))),
        ];
        let mi = MergeIterator::new(iters);
        let mut ui = UserIterator::new(mi, (Unbounded, Unbounded));
        ui.rewind().await.unwrap();

        // verify
        let k = ui.key();
        let v = ui.value();
        assert_eq!(k, user_key(iterator_test_key_of(0, 2).as_slice()));
        assert_eq!(v, test_value_of(0, 2));

        // only one valid kv pair
        ui.next().await.unwrap();
        assert!(!ui.is_valid());
    }

    // left..=end
    #[tokio::test]
    async fn test_range_inclusive() {
        // key=[table, idx, epoch], value
        let kv_pairs = vec![
            (0, 0, 200, HummockValue::Delete),
            (0, 0, 100, HummockValue::Put(test_value_of(0, 0))),
            (0, 1, 200, HummockValue::Put(test_value_of(0, 1))),
            (0, 1, 100, HummockValue::Delete),
            (0, 2, 300, HummockValue::Put(test_value_of(0, 2))),
            (0, 2, 200, HummockValue::Delete),
            (0, 2, 100, HummockValue::Delete),
            (0, 3, 100, HummockValue::Put(test_value_of(0, 3))),
            (0, 5, 200, HummockValue::Delete),
            (0, 5, 100, HummockValue::Put(test_value_of(0, 5))),
            (0, 6, 100, HummockValue::Put(test_value_of(0, 6))),
            (0, 7, 200, HummockValue::Delete),
            (0, 7, 100, HummockValue::Put(test_value_of(0, 7))),
            (0, 8, 100, HummockValue::Put(test_value_of(0, 8))),
        ];
        let table = add_kv_pair(kv_pairs).await;
        let iters: Vec<BoxedHummockIterator> =
            vec![Box::new(SSTableIterator::new(Arc::new(table)))];
        let mi = MergeIterator::new(iters);

        let begin_key = Included(user_key(key_range_test_key(0, 2, 0).as_slice()).to_vec());
        let end_key = Included(user_key(key_range_test_key(0, 7, 0).as_slice()).to_vec());

        let mut ui = UserIterator::new(mi, (begin_key, end_key));

        // ----- basic iterate -----
        ui.rewind().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- before-begin-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0, 1).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- begin-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0, 2).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- end-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0, 7).as_slice()))
            .await
            .unwrap();
        assert!(!ui.is_valid());

        // ----- after-end-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0, 8).as_slice()))
            .await
            .unwrap();
        assert!(!ui.is_valid());
    }

    // left..end
    #[tokio::test]
    async fn test_range() {
        // key=[table, idx, epoch], value
        let kv_pairs = vec![
            (0, 0, 200, HummockValue::Delete),
            (0, 0, 100, HummockValue::Put(test_value_of(0, 0))),
            (0, 1, 200, HummockValue::Put(test_value_of(0, 1))),
            (0, 1, 100, HummockValue::Delete),
            (0, 2, 300, HummockValue::Put(test_value_of(0, 2))),
            (0, 2, 200, HummockValue::Delete),
            (0, 2, 100, HummockValue::Delete),
            (0, 3, 100, HummockValue::Put(test_value_of(0, 3))),
            (0, 5, 200, HummockValue::Delete),
            (0, 5, 100, HummockValue::Put(test_value_of(0, 5))),
            (0, 6, 100, HummockValue::Put(test_value_of(0, 6))),
            (0, 7, 100, HummockValue::Put(test_value_of(0, 7))),
            (0, 8, 100, HummockValue::Put(test_value_of(0, 8))),
        ];
        let table = add_kv_pair(kv_pairs).await;
        let iters: Vec<BoxedHummockIterator> =
            vec![Box::new(SSTableIterator::new(Arc::new(table)))];
        let mi = MergeIterator::new(iters);

        let begin_key = Included(user_key(key_range_test_key(0, 2, 0).as_slice()).to_vec());
        let end_key = Excluded(user_key(key_range_test_key(0, 7, 0).as_slice()).to_vec());

        let mut ui = UserIterator::new(mi, (begin_key, end_key));

        // ----- basic iterate -----
        ui.rewind().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- before-begin-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0, 1).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- begin-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0, 2).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- end-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0, 7).as_slice()))
            .await
            .unwrap();
        assert!(!ui.is_valid());

        // ----- after-end-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0, 8).as_slice()))
            .await
            .unwrap();
        assert!(!ui.is_valid());
    }

    // ..=right
    #[tokio::test]
    async fn test_range_to_inclusive() {
        // key=[table, idx, epoch], value
        let kv_pairs = vec![
            (0, 0, 200, HummockValue::Delete),
            (0, 0, 100, HummockValue::Put(test_value_of(0, 0))),
            (0, 1, 200, HummockValue::Put(test_value_of(0, 1))),
            (0, 1, 100, HummockValue::Delete),
            (0, 2, 300, HummockValue::Put(test_value_of(0, 2))),
            (0, 2, 200, HummockValue::Delete),
            (0, 2, 100, HummockValue::Delete),
            (0, 3, 100, HummockValue::Put(test_value_of(0, 3))),
            (0, 5, 200, HummockValue::Delete),
            (0, 5, 100, HummockValue::Put(test_value_of(0, 5))),
            (0, 6, 100, HummockValue::Put(test_value_of(0, 6))),
            (0, 7, 200, HummockValue::Delete),
            (0, 7, 100, HummockValue::Put(test_value_of(0, 7))),
            (0, 8, 100, HummockValue::Put(test_value_of(0, 8))),
        ];
        let table = add_kv_pair(kv_pairs).await;
        let iters: Vec<BoxedHummockIterator> =
            vec![Box::new(SSTableIterator::new(Arc::new(table)))];
        let mi = MergeIterator::new(iters);
        let end_key = Included(user_key(key_range_test_key(0, 7, 0).as_slice()).to_vec());

        let mut ui = UserIterator::new(mi, (Unbounded, end_key));

        // ----- basic iterate -----
        ui.rewind().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 1).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- begin-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0, 0).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 1).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- in-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0, 2).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 6).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- end-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0, 7).as_slice()))
            .await
            .unwrap();
        assert!(!ui.is_valid());

        // ----- after-end-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0, 8).as_slice()))
            .await
            .unwrap();
        assert!(!ui.is_valid());
    }

    // left..
    #[tokio::test]
    async fn test_range_from() {
        // key=[table, idx, epoch], value
        let kv_pairs = vec![
            (0, 0, 200, HummockValue::Delete),
            (0, 0, 100, HummockValue::Put(test_value_of(0, 0))),
            (0, 1, 200, HummockValue::Put(test_value_of(0, 1))),
            (0, 1, 100, HummockValue::Delete),
            (0, 2, 300, HummockValue::Put(test_value_of(0, 2))),
            (0, 2, 200, HummockValue::Delete),
            (0, 2, 100, HummockValue::Delete),
            (0, 3, 100, HummockValue::Put(test_value_of(0, 3))),
            (0, 5, 200, HummockValue::Delete),
            (0, 5, 100, HummockValue::Put(test_value_of(0, 5))),
            (0, 6, 100, HummockValue::Put(test_value_of(0, 6))),
            (0, 7, 200, HummockValue::Delete),
            (0, 7, 100, HummockValue::Put(test_value_of(0, 7))),
            (0, 8, 100, HummockValue::Put(test_value_of(0, 8))),
        ];
        let table = add_kv_pair(kv_pairs).await;
        let iters: Vec<BoxedHummockIterator> =
            vec![Box::new(SSTableIterator::new(Arc::new(table)))];
        let mi = MergeIterator::new(iters);
        let begin_key = Included(user_key(key_range_test_key(0, 2, 0).as_slice()).to_vec());

        let mut ui = UserIterator::new(mi, (begin_key, Unbounded));

        // ----- basic iterate -----
        ui.rewind().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 6).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 8).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- begin-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0, 2).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 6).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 8).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- in-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0, 2).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 2).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 3).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 6).as_slice()));
        ui.next().await.unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 8).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- end-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0, 8).as_slice()))
            .await
            .unwrap();
        assert_eq!(ui.key(), user_key(iterator_test_key_of(0, 8).as_slice()));
        ui.next().await.unwrap();
        assert!(!ui.is_valid());

        // ----- after-end-range iterate -----
        ui.seek(user_key(iterator_test_key_of(0, 9).as_slice()))
            .await
            .unwrap();
        assert!(!ui.is_valid());
    }

    // key=[table, idx, epoch], value
    async fn add_kv_pair(kv_pairs: Vec<(u64, usize, u64, HummockValue<Vec<u8>>)>) -> SSTable {
        const REMOTE_DIR: &str = "test";
        let mut b = SSTableBuilder::new(default_builder_opt_for_test());
        for kv in kv_pairs {
            b.add(key_range_test_key(kv.0, kv.1, kv.2).as_slice(), kv.3);
        }
        let (data, meta) = b.finish();
        // get remote table
        let obj_client = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
        gen_remote_sstable(obj_client, 0, data, meta, REMOTE_DIR, None)
            .await
            .unwrap()
    }

    fn key_range_test_key(table: u64, idx: usize, epoch: u64) -> Vec<u8> {
        iterator_test_key_of_epoch(table, idx, epoch)
    }
}
