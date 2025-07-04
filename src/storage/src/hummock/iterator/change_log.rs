// Copyright 2025 RisingWave Labs
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
use risingwave_hummock_sdk::EpochWithGap;
use risingwave_hummock_sdk::key::{
    FullKey, SetSlice, TableKeyRange, UserKey, UserKeyRange, bound_table_key_range,
};

use crate::StateStoreIter;
use crate::error::StorageResult;
use crate::hummock::iterator::{Forward, HummockIterator, MergeIterator};
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockResult, SstableIterator};
use crate::monitor::IterLocalMetricsGuard;
use crate::store::{ChangeLogValue, StateStoreReadLogItem, StateStoreReadLogItemRef};

struct ChangeLogIteratorInner<
    NI: HummockIterator<Direction = Forward>,
    OI: HummockIterator<Direction = Forward>,
> {
    /// Iterator for new value. In each `next`, the iterator will iterate over all value of the current key.
    /// Therefore, we need to buffer the key and newest value in `curr_key` and `new_value`.
    ///
    /// We assume that all operation between `min_epoch` and `max_epoch` will be included in the `new_value_iter`.
    new_value_iter: NI,
    /// Iterator for old value. When `is_old_value_set` is true, its value is the old value in the change log value.
    ///
    /// We assume that each old value will have a new value of the same epoch in the `new_value_iter`. This is to say,
    /// For a specific key, we won't have an epoch that only exists in the `old_value_iter` but not exists in `new_value_iter`.
    /// `Delete` also contains a tombstone value.
    old_value_iter: OI,
    /// Inclusive max epoch
    max_epoch: u64,
    /// Inclusive min epoch
    min_epoch: u64,
    key_range: UserKeyRange,

    /// Buffer of current key
    curr_key: FullKey<Vec<u8>>,
    /// Buffer for new value. Only valid when `is_new_value_delete` is true
    new_value: Vec<u8>,
    /// Indicate whether the current new value is delete.
    is_new_value_delete: bool,

    /// Whether Indicate whether the current `old_value_iter` represents the old value in `ChangeLogValue`
    is_old_value_set: bool,

    /// Whether the iterator is currently pointing at a valid key with `ChangeLogValue`
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

    /// Advance the `new_value_iter` to a valid key and valid epoch.
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
        // The key and value will be saved in a buffer, because in the next step we will
        // continue advancing the `new_value_iter`.
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

    /// Advance the `new_value_iter` to find the oldest epoch of the current key.
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
                        // The assertion holds because we assume that for a specific key, any old value will have a new value of the same
                        // epoch in the `new_value_iter`. If the assertion is broken, it means we must have a new value of the same epoch
                        // that are valid but older than the `oldest_epoch`, which breaks the definition of `oldest_epoch`.
                        assert!(
                            old_value_iter_key.epoch_with_gap.pure_epoch() < self.min_epoch,
                            "there should not be old value between oldest new_value and min_epoch. \
                                new value key: {:?}, oldest epoch: {:?}, min epoch: {:?}, old value epoch: {:?}",
                            self.curr_key,
                            oldest_epoch,
                            self.min_epoch,
                            old_value_iter_key.epoch_with_gap
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

impl Drop for ChangeLogIterator {
    fn drop(&mut self) {
        self.inner
            .new_value_iter
            .collect_local_statistic(&mut self.stats_guard.local_stats);
        self.inner
            .old_value_iter
            .collect_local_statistic(&mut self.stats_guard.local_stats);
    }
}

pub struct ChangeLogIterator {
    inner: ChangeLogIteratorInner<MergeIterator<SstableIterator>, MergeIterator<SstableIterator>>,
    initial_read: bool,
    stats_guard: IterLocalMetricsGuard,
}

impl ChangeLogIterator {
    pub async fn new(
        epoch_range: (u64, u64),
        table_key_range: TableKeyRange,
        new_value_iter: MergeIterator<SstableIterator>,
        old_value_iter: MergeIterator<SstableIterator>,
        table_id: TableId,
        stats_guard: IterLocalMetricsGuard,
    ) -> HummockResult<Self> {
        let user_key_range_ref = bound_table_key_range(table_id, &table_key_range);
        let (start_bound, end_bound) = (
            user_key_range_ref.0.map(|key| key.cloned()),
            user_key_range_ref.1.map(|key| key.cloned()),
        );
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
            stats_guard,
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

#[cfg(any(test, feature = "test"))]
pub mod test_utils {
    use std::collections::HashMap;

    use bytes::Bytes;
    use rand::{Rng, RngCore, rng as thread_rng};
    use risingwave_common::util::epoch::{EpochPair, MAX_EPOCH, test_epoch};
    use risingwave_hummock_sdk::key::TableKey;

    use crate::hummock::iterator::test_utils::iterator_test_table_key_of;
    use crate::mem_table::KeyOp;
    use crate::store::{InitOptions, LocalStateStore, SealCurrentEpochOptions};

    pub type TestLogDataType = Vec<(u64, Vec<(TableKey<Bytes>, KeyOp)>)>;

    pub fn gen_test_data(
        epoch_count: usize,
        key_count: usize,
        skip_ratio: f64,
        delete_ratio: f64,
    ) -> TestLogDataType {
        let mut store: HashMap<TableKey<Bytes>, Bytes> = HashMap::new();
        let mut rng = thread_rng();
        let mut logs = Vec::new();
        for epoch_idx in 1..=(epoch_count - 1) {
            let mut epoch_logs = Vec::new();
            let epoch = test_epoch(epoch_idx as _);
            for key_idx in 0..key_count {
                if rng.random_bool(skip_ratio) {
                    continue;
                }
                let key = TableKey(Bytes::from(iterator_test_table_key_of(key_idx)));
                if rng.random_bool(delete_ratio) {
                    if let Some(prev_value) = store.remove(&key) {
                        epoch_logs.push((key, KeyOp::Delete(prev_value)));
                    }
                } else {
                    let value = Bytes::copy_from_slice(rng.next_u64().to_string().as_bytes());
                    let prev_value = store.get(&key);
                    if let Some(prev_value) = prev_value {
                        epoch_logs.push((
                            key.clone(),
                            KeyOp::Update((prev_value.clone(), value.clone())),
                        ));
                    } else {
                        epoch_logs.push((key.clone(), KeyOp::Insert(value.clone())));
                    }
                    store.insert(key, value);
                }
            }
            logs.push((epoch, epoch_logs));
        }
        // at the end add an epoch with only delete
        {
            let mut epoch_logs = Vec::new();
            let epoch = test_epoch(epoch_count as _);
            for (key, value) in store {
                epoch_logs.push((key, KeyOp::Delete(value)));
            }
            logs.push((epoch, epoch_logs));
        }
        logs
    }

    pub async fn apply_test_log_data(
        log_data: TestLogDataType,
        state_store: &mut impl LocalStateStore,
        try_flush_ratio: f64,
    ) {
        let mut rng = thread_rng();
        let first_epoch = log_data[0].0;
        for (epoch, epoch_logs) in log_data {
            if epoch == first_epoch {
                state_store
                    .init(InitOptions {
                        epoch: EpochPair::new_test_epoch(epoch),
                    })
                    .await
                    .unwrap();
            } else {
                state_store.flush().await.unwrap();
                state_store.seal_current_epoch(
                    epoch,
                    SealCurrentEpochOptions {
                        table_watermarks: None,
                        switch_op_consistency_level: None,
                    },
                );
            }
            for (key, op) in epoch_logs {
                match op {
                    KeyOp::Insert(value) => {
                        state_store.insert(key, value, None).unwrap();
                    }
                    KeyOp::Delete(old_value) => {
                        state_store.delete(key, old_value).unwrap();
                    }
                    KeyOp::Update((old_value, value)) => {
                        state_store.insert(key, value, Some(old_value)).unwrap();
                    }
                }
                if rng.random_bool(try_flush_ratio) {
                    state_store.try_flush().await.unwrap();
                }
            }
        }
        state_store.flush().await.unwrap();
        state_store.seal_current_epoch(
            MAX_EPOCH,
            SealCurrentEpochOptions {
                table_watermarks: None,
                switch_op_consistency_level: None,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ops::Bound::Unbounded;

    use bytes::Bytes;
    use itertools::Itertools;
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::EpochWithGap;
    use risingwave_hummock_sdk::key::{TableKey, UserKey};

    use crate::hummock::iterator::MergeIterator;
    use crate::hummock::iterator::change_log::ChangeLogIteratorInner;
    use crate::hummock::iterator::change_log::test_utils::{
        TestLogDataType, apply_test_log_data, gen_test_data,
    };
    use crate::hummock::iterator::test_utils::{
        iterator_test_table_key_of, iterator_test_value_of,
    };
    use crate::mem_table::{KeyOp, MemTable, MemTableHummockIterator, MemTableStore};
    use crate::memory::MemoryStateStore;
    use crate::store::{
        CHECK_BYTES_EQUAL, ChangeLogValue, NewLocalOptions, OpConsistencyLevel, ReadLogOptions,
        StateStoreReadLog,
    };
    use crate::{StateStore, StateStoreIter};

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

    fn gen_test_mem_table_store(
        test_log_data: TestLogDataType,
    ) -> Vec<(u64, MemTableStore, MemTableStore)> {
        let mut logs = Vec::new();
        for (epoch, epoch_logs) in test_log_data {
            let mut new_values = MemTableStore::new();
            let mut old_values = MemTableStore::new();
            for (key, op) in epoch_logs {
                new_values.insert(key.clone(), op.clone());
                if let KeyOp::Delete(old_value) | KeyOp::Update((old_value, _)) = op {
                    old_values.insert(key, KeyOp::Insert(old_value));
                }
            }
            logs.push((epoch, new_values, old_values));
        }
        logs
    }

    #[tokio::test]
    async fn test_random_data() {
        let table_id = TableId::new(233);
        let epoch_count = 10;
        let state_store = MemoryStateStore::new();
        let mut local = state_store
            .new_local(NewLocalOptions {
                table_id,
                op_consistency_level: OpConsistencyLevel::ConsistentOldValue {
                    check_old_value: CHECK_BYTES_EQUAL.clone(),
                    is_log_store: true,
                },
                table_option: Default::default(),
                is_replicated: false,
                vnodes: Bitmap::ones(VirtualNode::COUNT_FOR_TEST).into(),
                upload_on_flush: true,
            })
            .await;
        let logs = gen_test_data(epoch_count, 10000, 0.05, 0.2);
        assert_eq!(logs.len(), epoch_count);
        apply_test_log_data(logs.clone(), &mut local, 0.0).await;
        let mem_table_logs = gen_test_mem_table_store(logs.clone());
        assert_eq!(mem_table_logs.len(), epoch_count);
        for start_epoch_idx in 0..epoch_count {
            for end_epoch_idx in start_epoch_idx..epoch_count {
                let new_value_iter = MergeIterator::new(mem_table_logs.iter().map(
                    |(epoch, new_value_memtable, _)| {
                        MemTableHummockIterator::new(
                            new_value_memtable,
                            EpochWithGap::new_from_epoch(*epoch),
                            table_id,
                        )
                    },
                ));
                let old_value_iter = MergeIterator::new(mem_table_logs.iter().map(
                    |(epoch, _, old_value_memtable)| {
                        MemTableHummockIterator::new(
                            old_value_memtable,
                            EpochWithGap::new_from_epoch(*epoch),
                            table_id,
                        )
                    },
                ));
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
