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
use std::ops::Bound::{Excluded, Included, Unbounded};

use risingwave_common::catalog::TableId;
use risingwave_common::must_match;
use risingwave_common::util::epoch::MAX_SPILL_TIMES;
use risingwave_hummock_sdk::key::{FullKey, SetSlice, TableKey, TableKeyRange, UserKey};
use risingwave_hummock_sdk::EpochWithGap;

use crate::error::StorageResult;
use crate::hummock::iterator::{Forward, HummockIterator, MergeIterator};
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
    key_range: TableKeyRange,
    table_id: TableId,

    curr_key: Vec<u8>,
    new_value: Vec<u8>,
    new_value_epoch: EpochWithGap,
    is_new_value_delete: bool,

    last_new_value: Vec<u8>,
    last_new_value_epoch: EpochWithGap,
    is_last_new_value_delete: bool,
    is_different_last_new_value: bool,

    last_old_value: Vec<u8>,
    last_old_value_epoch: EpochWithGap,
    is_old_value_set: bool,

    is_current_pos_valid: bool,
}

impl<NI: HummockIterator<Direction = Forward>, OI: HummockIterator<Direction = Forward>>
    ChangeLogIteratorInner<NI, OI>
{
    fn new(
        (min_epoch, max_epoch): (u64, u64),
        key_range: TableKeyRange,
        new_value_iter: NI,
        old_value_iter: OI,
        table_id: TableId,
    ) -> Self {
        Self {
            new_value_iter,
            old_value_iter,
            min_epoch,
            max_epoch,
            key_range,
            table_id,
            curr_key: vec![],
            new_value: vec![],
            new_value_epoch: Default::default(),
            is_new_value_delete: false,
            last_new_value: vec![],
            last_new_value_epoch: Default::default(),
            is_last_new_value_delete: false,
            is_different_last_new_value: false,
            last_old_value: vec![],
            last_old_value_epoch: Default::default(),
            is_old_value_set: false,
            is_current_pos_valid: false,
        }
    }

    /// Resets the iterating position to the beginning.
    pub async fn rewind(&mut self) -> HummockResult<()> {
        // Handle range scan
        match &self.key_range.0 {
            Included(begin_key) => {
                let full_key = FullKey {
                    user_key: UserKey {
                        table_id: self.table_id,
                        table_key: TableKey(begin_key.as_ref()),
                    },
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

    /// Advance the two iters to a valid position. After it returns with Ok,
    /// it is possible that the position is valid but there is no change log value,
    /// because the new and old value may consume each other, such as Insert in old epoch,
    /// but then Delete in new epoch
    async fn try_advance_to_next_valid(&mut self) -> HummockResult<()> {
        // 1. advance the new_value_iter to the newest op between max and min epoch
        self.is_current_pos_valid = false;
        loop {
            if !self.new_value_iter.is_valid() {
                return Ok(());
            }

            let full_key = self.new_value_iter.key();
            let epoch = full_key.epoch_with_gap.pure_epoch();

            // Handle epoch visibility
            if epoch < self.min_epoch || epoch > self.max_epoch {
                self.new_value_iter.next().await?;
                continue;
            }

            // A new user key is observed.
            if self.user_key_out_of_range(full_key.user_key.table_key) {
                return Ok(());
            }

            self.is_current_pos_valid = true;
            break;
        }

        debug_assert!(self.new_value_iter.is_valid());
        self.curr_key
            .set(self.new_value_iter.key().user_key.table_key.as_ref());
        self.new_value_epoch = self.new_value_iter.key().epoch_with_gap;
        match self.new_value_iter.value() {
            HummockValue::Put(val) => {
                self.is_new_value_delete = false;
                self.new_value.set(val);
            }
            HummockValue::Delete => {
                self.is_new_value_delete = true;
                self.new_value.clear();
            }
        }

        self.new_value_iter.next().await?;

        // 2. advance new_value_iter to out of the valid range, and save the oldest value
        self.is_different_last_new_value = false;
        loop {
            if !self.new_value_iter.is_valid() {
                break;
            }

            let full_key = self.new_value_iter.key();

            match self
                .curr_key
                .as_slice()
                .cmp(full_key.user_key.table_key.as_ref())
            {
                Ordering::Less => {
                    break;
                }
                Ordering::Equal => {
                    assert!(
                        self.new_value_epoch > full_key.epoch_with_gap,
                        "iterator epoch going backward: {:?} {:?}",
                        self.new_value_epoch,
                        full_key
                    );
                    if full_key.epoch_with_gap.pure_epoch() >= self.min_epoch {
                        self.is_different_last_new_value = true;
                        match self.new_value_iter.value() {
                            HummockValue::Put(val) => {
                                self.last_new_value.set(val);
                                self.is_last_new_value_delete = false;
                            }
                            HummockValue::Delete => {
                                self.last_new_value.clear();
                                self.is_last_new_value_delete = true;
                            }
                        }
                        self.last_new_value_epoch = full_key.epoch_with_gap;
                        self.new_value_iter.next().await?;
                        continue;
                    } else {
                        break;
                    }
                }
                Ordering::Greater => {
                    unreachable!(
                        "iterator going backward: {:?} {:?}",
                        self.curr_key, full_key
                    );
                }
            }
        }

        // 3. iterate old value iter to out of the valid range. save the oldest value
        self.is_old_value_set = false;
        loop {
            if !self.old_value_iter.is_valid() {
                break;
            }

            let old_value_iter_key = self.old_value_iter.key();
            match self
                .curr_key
                .as_slice()
                .cmp(old_value_iter_key.user_key.table_key.as_ref())
            {
                Ordering::Less => {
                    // old value iter has advanced over the current range
                    break;
                }
                Ordering::Equal => {
                    let epoch = old_value_iter_key.epoch_with_gap.pure_epoch();
                    if epoch > self.max_epoch {
                        self.old_value_iter.next().await?;
                        continue;
                    }
                    assert!(
                        old_value_iter_key.epoch_with_gap <= self.new_value_epoch,
                        "there should not be old value between current new value iter key and max_epoch. \
                        new value key: {:?}, max_epoch: {:?}, old value key: {:?}",
                        self.curr_key, self.max_epoch, self.new_value_epoch
                    );
                    if epoch < self.min_epoch {
                        // old value iter has advanced over the current range
                        break;
                    }
                    self.is_old_value_set = true;
                    self.last_old_value_epoch = old_value_iter_key.epoch_with_gap;
                    self.last_old_value.set(
                        must_match!(self.old_value_iter.value(), HummockValue::Put(val) => val),
                    );
                }
                Ordering::Greater => {
                    self.old_value_iter.next().await?;
                    continue;
                }
            }
        }

        if self.is_old_value_set {
            assert!(
                self.last_old_value_epoch >= self.last_new_value_epoch(),
                "last old value must not be later than last new value, {:?} {:?}",
                self.last_old_value_epoch,
                self.last_new_value_epoch()
            )
        }

        Ok(())
    }

    fn is_valid(&self) -> bool {
        self.is_current_pos_valid
    }

    // fn new_value(&self) -> HummockValue<&[u8]> {
    //     if self.is_new_value_delete {
    //         HummockValue::Delete
    //     } else {
    //         HummockValue::Put(self.new_value.as_slice())
    //     }
    // }

    // fn last_new_value(&self) -> HummockValue<&[u8]> {
    //     if self.is_different_last_new_value {
    //         if self.is_last_new_value_delete {
    //             HummockValue::Delete
    //         } else {
    //             HummockValue::Put(self.last_new_value.as_slice())
    //         }
    //     } else {
    //         self.new_value()
    //     }
    // }

    fn last_new_value_epoch(&self) -> EpochWithGap {
        if self.is_different_last_new_value {
            self.last_new_value_epoch
        } else {
            self.new_value_epoch
        }
    }

    // Validate whether the current key is already out of range.
    fn user_key_out_of_range(&self, table_key: TableKey<&[u8]>) -> bool {
        // handle range scan
        match &self.key_range.1 {
            Included(end_key) => table_key.as_ref() > end_key.as_ref(),
            Excluded(end_key) => table_key.as_ref() >= end_key.as_ref(),
            Unbounded => false,
        }
    }

    fn key(&self) -> TableKey<&[u8]> {
        TableKey(self.curr_key.as_slice())
    }

    fn old_value(&self) -> Option<&[u8]> {
        if !self.is_old_value_set || self.last_new_value_epoch() < self.last_old_value_epoch {
            None
        } else {
            Some(self.last_old_value.as_slice())
        }
    }

    fn has_log_value(&self) -> bool {
        debug_assert!(self.is_current_pos_valid);
        !self.is_new_value_delete || self.old_value().is_some()
    }

    fn log_value(&self) -> ChangeLogValue<&[u8]> {
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
}

pub struct ChangeLogIterator {
    inner: ChangeLogIteratorInner<MergeIterator<SstableIterator>, MergeIterator<SstableIterator>>,
    initial_read: bool,
}

impl ChangeLogIterator {
    pub async fn new(
        epoch_range: (u64, u64),
        key_range: TableKeyRange,
        new_value_iter: MergeIterator<SstableIterator>,
        old_value_iter: MergeIterator<SstableIterator>,
        table_id: TableId,
    ) -> HummockResult<Self> {
        let mut inner = ChangeLogIteratorInner::new(
            epoch_range,
            key_range,
            new_value_iter,
            old_value_iter,
            table_id,
        );
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
            Ok(Some((self.inner.key(), self.inner.log_value())))
        } else {
            Ok(None)
        }
    }
}
