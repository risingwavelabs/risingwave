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
use risingwave_hummock_sdk::key::{FullKey, SetSlice, TableKeyRange, UserKey, UserKeyRange};
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
    key_range: UserKeyRange,

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
        key_range: UserKeyRange,
        new_value_iter: NI,
        old_value_iter: OI,
    ) -> Self {
        Self {
            new_value_iter,
            old_value_iter,
            min_epoch,
            max_epoch,
            key_range,

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
        match &self.key_range.0 {
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

    fn user_key_out_of_range(&self, user_key: UserKey<&[u8]>) -> bool {
        // handle range scan
        match &self.key_range.1 {
            Included(end_key) => user_key > end_key.as_ref(),
            Excluded(end_key) => user_key >= end_key.as_ref(),
            Unbounded => false,
        }
    }

    async fn advance_to_valid_key(&mut self) -> HummockResult<()> {
        self.is_current_pos_valid = false;
        loop {
            if !self.new_value_iter.is_valid() {
                return Ok(());
            }

            let key = self.new_value_iter.key();

            // Handle epoch visibility
            if !self.is_valid_epoch(key.epoch_with_gap) {
                self.new_value_iter.next().await?;
                continue;
            }

            if self.user_key_out_of_range(key.user_key) {
                return Ok(());
            }

            break;
        }

        debug_assert!(self.new_value_iter.is_valid());
        debug_assert!(self.is_valid_epoch(self.new_value_iter.key().epoch_with_gap));
        debug_assert!(!self.user_key_out_of_range(self.new_value_iter.key().user_key));
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
                Ordering::Equal => match old_value_iter_key.epoch_with_gap.cmp(&oldest_epoch) {
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
    inner: ChangeLogIteratorInner<MergeIterator<SstableIterator>, MergeIterator<SstableIterator>>,
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
        let mut inner = ChangeLogIteratorInner::new(
            epoch_range,
            (start_bound, end_bound),
            new_value_iter,
            old_value_iter,
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
            Ok(Some((self.inner.key().table_key, self.inner.log_value())))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ops::Bound::Unbounded;

    use bytes::Bytes;
    use itertools::Itertools;
    use rand::{thread_rng, Rng, RngCore};
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::table_distribution::TableDistribution;
    use risingwave_common::util::epoch::{test_epoch, EpochPair};
    use risingwave_hummock_sdk::key::{TableKey, UserKey};
    use risingwave_hummock_sdk::EpochWithGap;

    use crate::hummock::iterator::change_log::ChangeLogIteratorInner;
    use crate::hummock::iterator::test_utils::{
        iterator_test_table_key_of, iterator_test_value_of,
    };
    use crate::hummock::iterator::MergeIterator;
    use crate::mem_table::{KeyOp, MemTable, MemTableHummockIterator, MemTableStore};
    use crate::memory::MemoryStateStore;
    use crate::store::{
        ChangeLogValue, InitOptions, LocalStateStore, NewLocalOptions, OpConsistencyLevel,
        ReadLogOptions, ReadOptions, SealCurrentEpochOptions, StateStoreIter, StateStoreRead,
        CHECK_BYTES_EQUAL,
    };
    use crate::StateStore;

    #[tokio::test]
    async fn test_empty() {
        let table_id = TableId::new(233);
        let epoch = EpochWithGap::new_from_epoch(test_epoch(1));
        let empty = BTreeMap::new();
        let new_value_iter = MemTableHummockIterator::new(&empty, epoch, table_id);
        let old_value_iter = MemTableHummockIterator::new(&empty, epoch, table_id);
        let mut iter = ChangeLogIteratorInner::new(
            (epoch.pure_epoch(), epoch.pure_epoch()),
            (Unbounded, Unbounded),
            new_value_iter,
            old_value_iter,
        );
        iter.rewind().await.unwrap();
        assert!(!iter.is_valid());
    }

    #[tokio::test]
    async fn test_append_only() {
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
            (Unbounded, Unbounded),
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

    #[tokio::test]
    async fn test_delete_only() {
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
        let mut new_value_memtable = MemTable::new(OpConsistencyLevel::Inconsistent);
        let mut old_value_memtable = MemTable::new(OpConsistencyLevel::Inconsistent);
        for (key, value) in &kvs {
            new_value_memtable
                .delete(key.clone(), Bytes::new())
                .unwrap();
            old_value_memtable
                .insert(key.clone(), value.clone())
                .unwrap();
        }
        let epoch = EpochWithGap::new_from_epoch(test_epoch(1));
        let new_value_iter =
            MemTableHummockIterator::new(&new_value_memtable.buffer, epoch, table_id);
        let old_value_iter =
            MemTableHummockIterator::new(&old_value_memtable.buffer, epoch, table_id);
        let mut iter = ChangeLogIteratorInner::new(
            (epoch.pure_epoch(), epoch.pure_epoch()),
            (Unbounded, Unbounded),
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
            assert_eq!(ChangeLogValue::Delete(value.as_ref()), iter.log_value());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());
    }

    async fn gen_test_data(
        table_id: TableId,
        epoch_count: usize,
        key_count: usize,
        delete_ratio: f64,
    ) -> (Vec<(u64, MemTableStore, MemTableStore)>, MemoryStateStore) {
        let state_store = MemoryStateStore::new();
        let mut rng = thread_rng();
        let mut local = state_store
            .new_local(NewLocalOptions {
                table_id,
                op_consistency_level: OpConsistencyLevel::ConsistentOldValue(
                    CHECK_BYTES_EQUAL.clone(),
                ),
                table_option: Default::default(),
                is_replicated: false,
                vnodes: TableDistribution::all_vnodes(),
            })
            .await;
        let mut logs = Vec::new();
        for epoch_idx in 1..=epoch_count {
            let epoch = test_epoch(epoch_idx as _);
            let mut new_values = MemTableStore::new();
            let mut old_values = MemTableStore::new();
            if epoch_idx == 1 {
                local
                    .init(InitOptions {
                        epoch: EpochPair::new_test_epoch(epoch),
                    })
                    .await
                    .unwrap();
            } else {
                local.flush().await.unwrap();
                local.seal_current_epoch(
                    epoch,
                    SealCurrentEpochOptions {
                        table_watermarks: None,
                        switch_op_consistency_level: None,
                    },
                );
            }
            for key_idx in 0..key_count {
                let key = TableKey(Bytes::from(iterator_test_table_key_of(key_idx)));
                if rng.gen_bool(delete_ratio) {
                    if let Some(prev_value) = local
                        .get(
                            key.clone(),
                            ReadOptions {
                                prefix_hint: None,
                                ignore_range_tombstone: false,
                                prefetch_options: Default::default(),
                                cache_policy: Default::default(),
                                retention_seconds: None,
                                table_id,
                                read_version_from_backup: false,
                            },
                        )
                        .await
                        .unwrap()
                    {
                        new_values.insert(key.clone(), KeyOp::Delete(Bytes::new()));
                        old_values.insert(key.clone(), KeyOp::Insert(prev_value.clone()));
                        local.delete(key, prev_value).unwrap();
                    }
                } else {
                    let value = Bytes::copy_from_slice(rng.next_u64().to_string().as_bytes());
                    new_values.insert(key.clone(), KeyOp::Insert(value.clone()));
                    let prev_value = local
                        .get(
                            key.clone(),
                            ReadOptions {
                                prefix_hint: None,
                                ignore_range_tombstone: false,
                                prefetch_options: Default::default(),
                                cache_policy: Default::default(),
                                retention_seconds: None,
                                table_id,
                                read_version_from_backup: false,
                            },
                        )
                        .await
                        .unwrap();
                    if let Some(prev_value) = prev_value.clone() {
                        old_values.insert(key.clone(), KeyOp::Insert(prev_value));
                    }
                    local.insert(key, value, prev_value).unwrap();
                }
            }
            logs.push((epoch, new_values, old_values));
        }
        local.flush().await.unwrap();
        local.seal_current_epoch(
            test_epoch((epoch_count + 1) as _),
            SealCurrentEpochOptions {
                table_watermarks: None,
                switch_op_consistency_level: None,
            },
        );
        (logs, state_store)
    }

    #[tokio::test]
    async fn test_random_data() {
        let table_id = TableId::new(233);
        let epoch_count = 10;
        let (logs, state_store) = gen_test_data(table_id, epoch_count, 10000, 0.2).await;
        assert_eq!(logs.len(), epoch_count);
        for start_epoch_idx in 0..epoch_count {
            for end_epoch_idx in start_epoch_idx + 1..epoch_count {
                let new_value_iter =
                    MergeIterator::new(logs.iter().map(|(epoch, new_value_memtable, _)| {
                        MemTableHummockIterator::new(
                            new_value_memtable,
                            EpochWithGap::new_from_epoch(*epoch),
                            table_id,
                        )
                    }));
                let old_value_iter =
                    MergeIterator::new(logs.iter().map(|(epoch, _, old_value_memtable)| {
                        MemTableHummockIterator::new(
                            old_value_memtable,
                            EpochWithGap::new_from_epoch(*epoch),
                            table_id,
                        )
                    }));
                let epoch_range = (logs[start_epoch_idx].0, logs[end_epoch_idx].0);
                let mut change_log_iter = ChangeLogIteratorInner::new(
                    epoch_range,
                    (Unbounded, Unbounded),
                    new_value_iter,
                    old_value_iter,
                );
                change_log_iter.rewind().await.unwrap();
                let mut expected_change_log_iter = state_store
                    .iter_log(
                        epoch_range,
                        (Unbounded, Unbounded),
                        ReadLogOptions { table_id },
                    )
                    .await
                    .unwrap();
                while let Some((key, change_log_value)) =
                    expected_change_log_iter.try_next().await.unwrap()
                {
                    assert!(change_log_iter.is_valid());
                    assert_eq!(
                        change_log_iter.key(),
                        UserKey {
                            table_id,
                            table_key: key,
                        },
                    );
                    assert_eq!(change_log_iter.log_value(), change_log_value);
                    change_log_iter.next().await.unwrap();
                }
                assert!(!change_log_iter.is_valid());
            }
        }
    }
}
