// Copyright 2024 RisingWave Labs
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

use std::cmp::Ordering;
use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};

use risingwave_common::catalog::TableId;
use risingwave_common::must_match;
use risingwave_common::util::epoch::MAX_SPILL_TIMES;
use risingwave_hummock_sdk::key::{FullKey, KeyPayloadType, SetSlice, TableKeyRange, UserKey};
use risingwave_hummock_sdk::EpochWithGap;

use crate::error::StorageResult;
use crate::hummock::iterator::{
    Forward, HummockIterator, MergeIterator, UserKeyEndBoundedIterator,
};
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockResult, SstableIterator};
use crate::store::{ChangeLogValue, StateStoreReadLogItem, StateStoreReadLogItemRef};
use crate::StateStoreIter;

struct ChangeLogIteratorInner<
    NI: HummockIterator<Direction = Forward>,
    OI: HummockIterator<Direction = Forward>,
> {
    new_value_iter: NI,
    old_value_iter: OI,
    max_epoch: u64,
    min_epoch: u64,
    start_bound: Bound<UserKey<KeyPayloadType>>,

    curr_key: FullKey<Vec<u8>>,
    new_value: Vec<u8>,
    is_new_value_delete: bool,

    is_old_value_set: bool,

    is_current_pos_valid: bool,
}

impl<NI: HummockIterator<Direction = Forward>, OI: HummockIterator<Direction = Forward>>
    ChangeLogIteratorInner<NI, OI>
{
    fn new(
        (min_epoch, max_epoch): (u64, u64),
        start_bound: Bound<UserKey<KeyPayloadType>>,
        new_value_iter: NI,
        old_value_iter: OI,
    ) -> Self {
        Self {
            new_value_iter,
            old_value_iter,
            min_epoch,
            max_epoch,
            start_bound,

            curr_key: FullKey::default(),
            new_value: vec![],
            is_new_value_delete: false,
            is_old_value_set: false,
            is_current_pos_valid: false,
        }
    }

    /// Resets the iterating position to the beginning.
    pub async fn rewind(&mut self) -> HummockResult<()> {
        // Handle range scan
        match &self.start_bound {
            Included(begin_key) => {
                let full_key = FullKey {
                    user_key: begin_key.as_ref(),
                    epoch_with_gap: EpochWithGap::new(self.max_epoch, MAX_SPILL_TIMES),
                };
                self.new_value_iter.seek(full_key).await?;
                self.old_value_iter.seek(full_key).await?;
            }
            Excluded(_) => unimplemented!("excluded begin key is not supported"),
            Unbounded => {
                self.new_value_iter.rewind().await?;
                self.old_value_iter.rewind().await?;
            }
        };

        self.try_advance_to_next_change_log_value().await?;
        Ok(())
    }

    pub async fn next(&mut self) -> HummockResult<()> {
        self.try_advance_to_next_change_log_value().await
    }

    pub fn is_valid(&self) -> bool {
        self.is_current_pos_valid
    }

    pub fn log_value(&self) -> ChangeLogValue<&[u8]> {
        if self.is_new_value_delete {
            ChangeLogValue::Delete(
                self.old_value()
                    .expect("should have old value when new value is delete"),
            )
        } else {
            match self.old_value() {
                Some(old_value) => ChangeLogValue::Update {
                    new_value: self.new_value.as_slice(),
                    old_value,
                },
                None => ChangeLogValue::Insert(self.new_value.as_slice()),
            }
        }
    }

    pub fn key(&self) -> UserKey<&[u8]> {
        self.curr_key.user_key.as_ref()
    }
}

impl<NI: HummockIterator<Direction = Forward>, OI: HummockIterator<Direction = Forward>>
    ChangeLogIteratorInner<NI, OI>
{
    async fn try_advance_to_next_change_log_value(&mut self) -> HummockResult<()> {
        loop {
            self.try_advance_to_next_valid().await?;
            if !self.is_valid() {
                break;
            }
            if self.has_log_value() {
                break;
            } else {
                continue;
            }
        }
        Ok(())
    }

    async fn advance_to_valid_key(&mut self) -> HummockResult<()> {
        self.is_current_pos_valid = false;
        loop {
            if !self.new_value_iter.is_valid() {
                return Ok(());
            }
            // Handle epoch visibility
            if !self.is_valid_epoch(self.new_value_iter.key().epoch_with_gap) {
                self.new_value_iter.next().await?;
                continue;
            }
            break;
        }

        debug_assert!(self.new_value_iter.is_valid());
        debug_assert!(self.is_valid_epoch(self.new_value_iter.key().epoch_with_gap));
        self.is_current_pos_valid = true;
        self.curr_key.set(self.new_value_iter.key());
        match self.new_value_iter.value() {
            HummockValue::Put(val) => {
                self.new_value.set(val);
                self.is_new_value_delete = false;
            }
            HummockValue::Delete => {
                self.new_value.clear();
                self.is_new_value_delete = true;
            }
        }

        Ok(())
    }

    async fn advance_to_find_oldest_epoch(&mut self) -> HummockResult<EpochWithGap> {
        let mut ret = self.curr_key.epoch_with_gap;
        debug_assert!(self.is_valid_epoch(ret));
        self.new_value_iter.next().await?;
        loop {
            if !self.new_value_iter.is_valid() {
                break;
            }
            let key = self.new_value_iter.key();
            match self.curr_key.user_key.as_ref().cmp(&key.user_key) {
                Ordering::Less => {
                    // has advance to next key
                    break;
                }
                Ordering::Equal => {
                    assert!(ret > key.epoch_with_gap);
                    if !self.is_valid_epoch(key.epoch_with_gap) {
                        debug_assert!(self.min_epoch > key.epoch_with_gap.pure_epoch());
                        break;
                    }
                    ret = key.epoch_with_gap;
                    self.new_value_iter.next().await?;
                    continue;
                }
                Ordering::Greater => {
                    unreachable!(
                        "hummock iterator advance to a prev key: {:?} {:?}",
                        self.curr_key,
                        self.new_value_iter.key()
                    );
                }
            }
        }
        debug_assert!(self.is_valid_epoch(ret));

        Ok(ret)
    }

    /// Advance the two iters to a valid position. After it returns with Ok,
    /// it is possible that the position is valid but there is no change log value,
    /// because the new and old value may consume each other, such as Insert in old epoch,
    /// but then Delete in new epoch
    async fn try_advance_to_next_valid(&mut self) -> HummockResult<()> {
        // 1. advance the new_value_iter to the newest op between max and min epoch
        self.advance_to_valid_key().await?;

        if !self.is_current_pos_valid {
            return Ok(());
        }

        // 2. advance new_value_iter to out of the valid range, and save the oldest value
        let oldest_epoch = self.advance_to_find_oldest_epoch().await?;

        // 3. iterate old value iter to the oldest epoch
        self.is_old_value_set = false;
        loop {
            if !self.old_value_iter.is_valid() {
                break;
            }

            let old_value_iter_key = self.old_value_iter.key();
            match self
                .curr_key
                .user_key
                .as_ref()
                .cmp(&old_value_iter_key.user_key.as_ref())
            {
                Ordering::Less => {
                    // old value iter has advanced over the current range
                    break;
                }
                Ordering::Equal => match oldest_epoch.cmp(&old_value_iter_key.epoch_with_gap) {
                    Ordering::Less => {
                        assert!(
                                old_value_iter_key.epoch_with_gap.pure_epoch() < self.min_epoch,
                                "there should not be old value between oldest new_value and min_epoch. \
                                new value key: {:?}, oldest epoch: {:?}, min epoch: {:?}, old value epoch: {:?}",
                                self.curr_key, oldest_epoch, self.min_epoch, old_value_iter_key.epoch_with_gap
                            );
                        break;
                    }
                    Ordering::Equal => {
                        self.is_old_value_set = true;
                        break;
                    }
                    Ordering::Greater => {
                        self.old_value_iter.next().await?;
                        continue;
                    }
                },
                Ordering::Greater => {
                    self.old_value_iter.next().await?;
                    continue;
                }
            }
        }

        Ok(())
    }

    fn is_valid_epoch(&self, epoch: EpochWithGap) -> bool {
        let epoch = epoch.pure_epoch();
        self.min_epoch <= epoch && epoch <= self.max_epoch
    }

    fn old_value(&self) -> Option<&[u8]> {
        if self.is_old_value_set {
            debug_assert!(self.old_value_iter.is_valid());
            debug_assert_eq!(
                self.old_value_iter.key().user_key,
                self.curr_key.user_key.as_ref()
            );
            Some(must_match!(self.old_value_iter.value(), HummockValue::Put(val) => val))
        } else {
            None
        }
    }

    fn has_log_value(&self) -> bool {
        debug_assert!(self.is_current_pos_valid);
        !self.is_new_value_delete || self.is_old_value_set
    }
}

pub struct ChangeLogIterator {
    inner: ChangeLogIteratorInner<
        UserKeyEndBoundedIterator<MergeIterator<SstableIterator>>,
        MergeIterator<SstableIterator>,
    >,
    initial_read: bool,
}

impl ChangeLogIterator {
    pub async fn new(
        epoch_range: (u64, u64),
        (start_bound, end_bound): TableKeyRange,
        new_value_iter: MergeIterator<SstableIterator>,
        old_value_iter: MergeIterator<SstableIterator>,
        table_id: TableId,
    ) -> HummockResult<Self> {
        let make_user_key = |table_key| UserKey {
            table_id,
            table_key,
        };
        let start_bound = start_bound.map(make_user_key);
        let end_bound = end_bound.map(make_user_key);
        let new_value_iter = UserKeyEndBoundedIterator::new(new_value_iter, end_bound);
        let mut inner =
            ChangeLogIteratorInner::new(epoch_range, start_bound, new_value_iter, old_value_iter);
        inner.rewind().await?;
        Ok(Self {
            inner,
            initial_read: false,
        })
    }
}

impl StateStoreIter<StateStoreReadLogItem> for ChangeLogIterator {
    async fn try_next(&mut self) -> StorageResult<Option<StateStoreReadLogItemRef<'_>>> {
        if !self.initial_read {
            self.initial_read = true;
        } else {
            self.inner.next().await?;
        }
        if self.inner.is_valid() {
            Ok(Some((self.inner.key().table_key, self.inner.log_value())))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ops::Bound;

    use bytes::Bytes;
    use itertools::Itertools;
    use risingwave_common::catalog::TableId;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::key::{TableKey, UserKey};
    use risingwave_hummock_sdk::EpochWithGap;

    use crate::hummock::iterator::change_log::ChangeLogIteratorInner;
    use crate::hummock::iterator::test_utils::{
        iterator_test_table_key_of, iterator_test_value_of,
    };
    use crate::hummock::iterator::MergeIterator;
    use crate::mem_table::{MemTable, MemTableHummockIterator};
    use crate::store::{ChangeLogValue, OpConsistencyLevel};

    #[tokio::test]
    async fn test_basic() {
        let table_id = TableId::new(233);

        let count = 100;
        let kvs = (0..count)
            .map(|i| {
                (
                    TableKey(Bytes::from(iterator_test_table_key_of(i))),
                    Bytes::from(iterator_test_value_of(i)),
                )
            })
            .collect_vec();
        let mem_tables = kvs
            .iter()
            .map(|(key, value)| {
                let mut t = MemTable::new(OpConsistencyLevel::Inconsistent);
                t.insert(key.clone(), value.clone()).unwrap();
                t
            })
            .collect_vec();
        let epoch = EpochWithGap::new_from_epoch(test_epoch(1));
        let new_value_iter = MergeIterator::new(
            mem_tables
                .iter()
                .map(|mem_table| MemTableHummockIterator::new(&mem_table.buffer, epoch, table_id)),
        );
        let empty = BTreeMap::new();
        let old_value_iter = MemTableHummockIterator::new(&empty, epoch, table_id);
        let mut iter = ChangeLogIteratorInner::new(
            (epoch.pure_epoch(), epoch.pure_epoch()),
            Bound::Unbounded,
            new_value_iter,
            old_value_iter,
        );
        iter.rewind().await.unwrap();
        for (key, value) in kvs {
            assert!(iter.is_valid());
            assert_eq!(
                UserKey {
                    table_id,
                    table_key: key.to_ref(),
                },
                iter.key()
            );
            assert_eq!(ChangeLogValue::Insert(value.as_ref()), iter.log_value());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());
    }
}
