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

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::ops::Bound::{Included, Unbounded};
use std::ops::RangeBounds;

use bytes::Bytes;
use risingwave_common_estimate_size::{EstimateSize, KvSize};
use risingwave_hummock_sdk::key::TableKey;
use thiserror::Error;
use thiserror_ext::AsReport;
use tracing::error;

use crate::hummock::iterator::{Backward, Forward, FromRustIterator, RustIteratorBuilder};
use crate::hummock::shared_buffer::shared_buffer_batch::{SharedBufferBatch, SharedBufferBatchId};
use crate::hummock::utils::sanity_check_enabled;
use crate::hummock::value::HummockValue;
use crate::row_serde::value_serde::ValueRowSerde;
use crate::store::*;
pub type ImmutableMemtable = SharedBufferBatch;

pub type ImmId = SharedBufferBatchId;

#[derive(Clone, Debug, EstimateSize)]
pub enum KeyOp {
    Insert(Bytes),
    Delete(Bytes),
    /// (`old_value`, `new_value`)
    Update((Bytes, Bytes)),
}

/// `MemTable` is a buffer for modify operations without encoding
#[derive(Clone)]
pub struct MemTable {
    pub(crate) buffer: MemTableStore,
    pub(crate) op_consistency_level: OpConsistencyLevel,
    pub(crate) kv_size: KvSize,
}

#[derive(Error, Debug)]
pub enum MemTableError {
    #[error("Inconsistent operation {key:?}, prev: {prev:?}, new: {new:?}")]
    InconsistentOperation {
        key: TableKey<Bytes>,
        prev: KeyOp,
        new: KeyOp,
    },
}

type Result<T> = std::result::Result<T, Box<MemTableError>>;

pub type MemTableStore = BTreeMap<TableKey<Bytes>, KeyOp>;
pub struct MemTableIteratorBuilder;
pub struct MemTableRevIteratorBuilder;

fn map_to_hummock_value<'a>(
    (key, op): (&'a TableKey<Bytes>, &'a KeyOp),
) -> (TableKey<&'a [u8]>, HummockValue<&'a [u8]>) {
    (
        TableKey(key.0.as_ref()),
        match op {
            KeyOp::Insert(value) | KeyOp::Update((_, value)) => HummockValue::Put(value),
            KeyOp::Delete(_) => HummockValue::Delete,
        },
    )
}

impl RustIteratorBuilder for MemTableRevIteratorBuilder {
    type Direction = Backward;
    type Iterable = MemTableStore;

    type RewindIter<'a> =
        impl Iterator<Item = (TableKey<&'a [u8]>, HummockValue<&'a [u8]>)> + Send + 'a;
    type SeekIter<'a> =
        impl Iterator<Item = (TableKey<&'a [u8]>, HummockValue<&'a [u8]>)> + Send + 'a;

    fn seek<'a>(iterable: &'a Self::Iterable, seek_key: TableKey<&[u8]>) -> Self::SeekIter<'a> {
        iterable
            .range::<[u8], _>((Unbounded, Included(seek_key.0)))
            .rev()
            .map(map_to_hummock_value)
    }

    fn rewind(iterable: &Self::Iterable) -> Self::RewindIter<'_> {
        iterable.iter().rev().map(map_to_hummock_value)
    }
}

impl RustIteratorBuilder for MemTableIteratorBuilder {
    type Direction = Forward;
    type Iterable = MemTableStore;

    type RewindIter<'a> =
        impl Iterator<Item = (TableKey<&'a [u8]>, HummockValue<&'a [u8]>)> + Send + 'a;
    type SeekIter<'a> =
        impl Iterator<Item = (TableKey<&'a [u8]>, HummockValue<&'a [u8]>)> + Send + 'a;

    fn seek<'a>(iterable: &'a Self::Iterable, seek_key: TableKey<&[u8]>) -> Self::SeekIter<'a> {
        iterable
            .range::<[u8], _>((Included(seek_key.0), Unbounded))
            .map(map_to_hummock_value)
    }

    fn rewind(iterable: &Self::Iterable) -> Self::RewindIter<'_> {
        iterable.iter().map(map_to_hummock_value)
    }
}

pub type MemTableHummockIterator<'a> = FromRustIterator<'a, MemTableIteratorBuilder>;
pub type MemTableHummockRevIterator<'a> = FromRustIterator<'a, MemTableRevIteratorBuilder>;

impl MemTable {
    pub fn new(op_consistency_level: OpConsistencyLevel) -> Self {
        Self {
            buffer: BTreeMap::new(),
            op_consistency_level,
            kv_size: KvSize::new(),
        }
    }

    pub fn drain(&mut self) -> Self {
        self.kv_size.set(0);
        std::mem::replace(self, Self::new(self.op_consistency_level.clone()))
    }

    pub fn is_dirty(&self) -> bool {
        !self.buffer.is_empty()
    }

    /// write methods
    pub fn insert(&mut self, pk: TableKey<Bytes>, value: Bytes) -> Result<()> {
        if let OpConsistencyLevel::Inconsistent = &self.op_consistency_level {
            let key_len = std::mem::size_of::<Bytes>() + pk.len();
            let op = KeyOp::Insert(value);
            self.kv_size.add(&pk, &op);
            let old_op = self.buffer.insert(pk, op);
            self.sub_old_op_size(old_op, key_len);
            return Ok(());
        };
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                let op = KeyOp::Insert(value);
                self.kv_size.add(e.key(), &op);
                e.insert(op);
                Ok(())
            }
            Entry::Occupied(mut e) => {
                let old_op = e.get_mut();
                self.kv_size.sub_val(old_op);

                match old_op {
                    KeyOp::Delete(ref mut old_op_old_value) => {
                        let new_op = KeyOp::Update((std::mem::take(old_op_old_value), value));
                        self.kv_size.add_val(&new_op);
                        e.insert(new_op);
                        Ok(())
                    }
                    KeyOp::Insert(_) | KeyOp::Update(_) => {
                        let new_op = KeyOp::Insert(value);
                        let err = MemTableError::InconsistentOperation {
                            key: e.key().clone(),
                            prev: e.get().clone(),
                            new: new_op.clone(),
                        };

                        if sanity_check_enabled() {
                            Err(err.into())
                        } else {
                            tracing::error!(
                                error = %err.as_report(),
                                "double insert / insert on updated, ignoring because sanity check is disabled"
                            );
                            self.kv_size.add_val(&new_op);
                            e.insert(new_op);
                            Ok(())
                        }
                    }
                }
            }
        }
    }

    pub fn delete(&mut self, pk: TableKey<Bytes>, old_value: Bytes) -> Result<()> {
        let key_len = std::mem::size_of::<Bytes>() + pk.len();
        let OpConsistencyLevel::ConsistentOldValue {
            check_old_value: value_checker,
            ..
        } = &self.op_consistency_level
        else {
            let op = KeyOp::Delete(old_value);
            self.kv_size.add(&pk, &op);
            let old_op = self.buffer.insert(pk, op);
            self.sub_old_op_size(old_op, key_len);
            return Ok(());
        };
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                let op = KeyOp::Delete(old_value);
                self.kv_size.add(e.key(), &op);
                e.insert(op);
                Ok(())
            }
            Entry::Occupied(mut e) => {
                let old_op = e.get_mut();
                self.kv_size.sub_val(old_op);

                match old_op {
                    KeyOp::Insert(old_op_new_value) => {
                        if sanity_check_enabled() && !value_checker(old_op_new_value, &old_value) {
                            return Err(Box::new(MemTableError::InconsistentOperation {
                                key: e.key().clone(),
                                prev: e.get().clone(),
                                new: KeyOp::Delete(old_value),
                            }));
                        }

                        self.kv_size.sub_size(key_len);
                        e.remove();
                        Ok(())
                    }
                    KeyOp::Delete(_) => {
                        let new_op = KeyOp::Delete(old_value);
                        let err = MemTableError::InconsistentOperation {
                            key: e.key().clone(),
                            prev: e.get().clone(),
                            new: new_op.clone(),
                        };

                        if sanity_check_enabled() {
                            Err(err.into())
                        } else {
                            tracing::error!(
                                error = %err.as_report(),
                                "double delete, ignoring because sanity check is disabled"
                            );
                            self.kv_size.add_val(&new_op);
                            e.insert(new_op);
                            Ok(())
                        }
                    }
                    KeyOp::Update((old_op_old_value, old_op_new_value)) => {
                        if sanity_check_enabled() && !value_checker(old_op_new_value, &old_value) {
                            return Err(Box::new(MemTableError::InconsistentOperation {
                                key: e.key().clone(),
                                prev: e.get().clone(),
                                new: KeyOp::Delete(old_value),
                            }));
                        }

                        let new_op = KeyOp::Delete(std::mem::take(old_op_old_value));
                        self.kv_size.add_val(&new_op);
                        e.insert(new_op);
                        Ok(())
                    }
                }
            }
        }
    }

    pub fn update(
        &mut self,
        pk: TableKey<Bytes>,
        old_value: Bytes,
        new_value: Bytes,
    ) -> Result<()> {
        let OpConsistencyLevel::ConsistentOldValue {
            check_old_value: value_checker,
            ..
        } = &self.op_consistency_level
        else {
            let key_len = std::mem::size_of::<Bytes>() + pk.len();
            let op = KeyOp::Update((old_value, new_value));
            self.kv_size.add(&pk, &op);
            let old_op = self.buffer.insert(pk, op);
            self.sub_old_op_size(old_op, key_len);
            return Ok(());
        };
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                let op = KeyOp::Update((old_value, new_value));
                self.kv_size.add(e.key(), &op);
                e.insert(op);
                Ok(())
            }
            Entry::Occupied(mut e) => {
                let old_op = e.get_mut();
                self.kv_size.sub_val(old_op);

                match old_op {
                    KeyOp::Insert(old_op_new_value) => {
                        if sanity_check_enabled() && !value_checker(old_op_new_value, &old_value) {
                            return Err(Box::new(MemTableError::InconsistentOperation {
                                key: e.key().clone(),
                                prev: e.get().clone(),
                                new: KeyOp::Update((old_value, new_value)),
                            }));
                        }

                        let new_op = KeyOp::Insert(new_value);
                        self.kv_size.add_val(&new_op);
                        e.insert(new_op);
                        Ok(())
                    }
                    KeyOp::Update((old_op_old_value, old_op_new_value)) => {
                        if sanity_check_enabled() && !value_checker(old_op_new_value, &old_value) {
                            return Err(Box::new(MemTableError::InconsistentOperation {
                                key: e.key().clone(),
                                prev: e.get().clone(),
                                new: KeyOp::Update((old_value, new_value)),
                            }));
                        }

                        let new_op = KeyOp::Update((std::mem::take(old_op_old_value), new_value));
                        self.kv_size.add_val(&new_op);
                        e.insert(new_op);
                        Ok(())
                    }
                    KeyOp::Delete(_) => {
                        let new_op = KeyOp::Update((old_value, new_value));
                        let err = MemTableError::InconsistentOperation {
                            key: e.key().clone(),
                            prev: e.get().clone(),
                            new: new_op.clone(),
                        };

                        if sanity_check_enabled() {
                            Err(err.into())
                        } else {
                            tracing::error!(
                                error = %err.as_report(),
                                "update on deleted, ignoring because sanity check is disabled"
                            );
                            self.kv_size.add_val(&new_op);
                            e.insert(new_op);
                            Ok(())
                        }
                    }
                }
            }
        }
    }

    pub fn into_parts(self) -> BTreeMap<TableKey<Bytes>, KeyOp> {
        self.buffer
    }

    pub fn iter<'a, R>(
        &'a self,
        key_range: R,
    ) -> impl Iterator<Item = (&'a TableKey<Bytes>, &'a KeyOp)>
    where
        R: RangeBounds<TableKey<Bytes>> + 'a,
    {
        self.buffer.range(key_range)
    }

    pub fn rev_iter<'a, R>(
        &'a self,
        key_range: R,
    ) -> impl Iterator<Item = (&'a TableKey<Bytes>, &'a KeyOp)>
    where
        R: RangeBounds<TableKey<Bytes>> + 'a,
    {
        self.buffer.range(key_range).rev()
    }

    fn sub_old_op_size(&mut self, old_op: Option<KeyOp>, key_len: usize) {
        if let Some(op) = old_op {
            self.kv_size.sub_val(&op);
            self.kv_size.sub_size(key_len);
        }
    }
}

impl KeyOp {
    /// Print as debug string with decoded data.
    ///
    /// # Panics
    ///
    /// The function will panic if it failed to decode the bytes with provided data types.
    pub fn debug_fmt(&self, row_deserializer: &impl ValueRowSerde) -> String {
        match self {
            Self::Insert(after) => {
                let after = row_deserializer.deserialize(after.as_ref());
                format!("Insert({:?})", &after)
            }
            Self::Delete(before) => {
                let before = row_deserializer.deserialize(before.as_ref());
                format!("Delete({:?})", &before)
            }
            Self::Update((before, after)) => {
                let after = row_deserializer.deserialize(after.as_ref());
                let before = row_deserializer.deserialize(before.as_ref());
                format!("Update({:?}, {:?})", &before, &after)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};
    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use rand::{Rng, rng as thread_rng};
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::{EpochExt, test_epoch};
    use risingwave_hummock_sdk::EpochWithGap;
    use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKey};

    use crate::hummock::iterator::HummockIterator;
    use crate::hummock::value::HummockValue;
    use crate::mem_table::{KeyOp, MemTable, MemTableHummockIterator, MemTableHummockRevIterator};
    use crate::store::{CHECK_BYTES_EQUAL, OpConsistencyLevel};

    #[tokio::test]
    async fn test_mem_table_memory_size() {
        let mut mem_table = MemTable::new(OpConsistencyLevel::ConsistentOldValue {
            check_old_value: CHECK_BYTES_EQUAL.clone(),
            is_log_store: false,
        });
        assert_eq!(mem_table.kv_size.size(), 0);

        mem_table
            .insert(TableKey("key1".into()), "value1".into())
            .unwrap();
        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key1").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value1").len()
        );

        // delete
        mem_table.drain();
        assert_eq!(mem_table.kv_size.size(), 0);
        mem_table
            .delete(TableKey("key2".into()), "value2".into())
            .unwrap();
        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key2").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value2").len()
        );
        mem_table
            .insert(TableKey("key2".into()), "value22".into())
            .unwrap();
        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key2").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value22").len()
                + Bytes::from("value2").len()
        );

        mem_table
            .delete(TableKey("key2".into()), "value22".into())
            .unwrap();

        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key2").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value2").len()
        );

        // update
        mem_table.drain();
        assert_eq!(mem_table.kv_size.size(), 0);
        mem_table
            .insert(TableKey("key3".into()), "value3".into())
            .unwrap();
        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key3").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value3").len()
        );

        // update-> insert
        mem_table
            .update(TableKey("key3".into()), "value3".into(), "value333".into())
            .unwrap();
        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key3").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value333").len()
        );

        mem_table.drain();
        mem_table
            .update(TableKey("key4".into()), "value4".into(), "value44".into())
            .unwrap();

        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key4").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value4").len()
                + Bytes::from("value44").len()
        );
        mem_table
            .update(
                TableKey("key4".into()),
                "value44".into(),
                "value4444".into(),
            )
            .unwrap();

        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key4").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value4").len()
                + Bytes::from("value4444").len()
        );
    }

    #[tokio::test]
    async fn test_mem_table_memory_size_not_consistent_op() {
        let mut mem_table = MemTable::new(OpConsistencyLevel::Inconsistent);
        assert_eq!(mem_table.kv_size.size(), 0);

        mem_table
            .insert(TableKey("key1".into()), "value1".into())
            .unwrap();

        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key1").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value1").len()
        );

        mem_table
            .insert(TableKey("key1".into()), "value111".into())
            .unwrap();
        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key1").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value111").len()
        );
        mem_table.drain();

        mem_table
            .update(TableKey("key4".into()), "value4".into(), "value44".into())
            .unwrap();

        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key4").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value4").len()
                + Bytes::from("value44").len()
        );
        mem_table
            .update(
                TableKey("key4".into()),
                "value44".into(),
                "value4444".into(),
            )
            .unwrap();

        assert_eq!(
            mem_table.kv_size.size(),
            std::mem::size_of::<Bytes>()
                + Bytes::from("key4").len()
                + std::mem::size_of::<KeyOp>()
                + Bytes::from("value44").len()
                + Bytes::from("value4444").len()
        );
    }

    #[tokio::test]
    async fn test_mem_table_hummock_iterator() {
        let mut rng = thread_rng();

        fn get_key(i: usize) -> TableKey<Bytes> {
            let mut bytes = BytesMut::new();
            bytes.put(&VirtualNode::ZERO.to_be_bytes()[..]);
            bytes.put(format!("key_{:20}", i).as_bytes());
            TableKey(bytes.freeze())
        }

        let mut ordered_test_data = (0..10000)
            .map(|i| {
                let key_op = match rng.random_range(0..=2) {
                    0 => KeyOp::Insert(Bytes::from("insert")),
                    1 => KeyOp::Delete(Bytes::from("delete")),
                    2 => KeyOp::Update((Bytes::from("old_value"), Bytes::from("new_value"))),
                    _ => unreachable!(),
                };
                (get_key(i), key_op)
            })
            .collect_vec();

        let mut test_data = ordered_test_data.clone();

        test_data.shuffle(&mut rng);
        let mut mem_table = MemTable::new(OpConsistencyLevel::ConsistentOldValue {
            check_old_value: CHECK_BYTES_EQUAL.clone(),
            is_log_store: false,
        });
        for (key, op) in test_data {
            match op {
                KeyOp::Insert(value) => {
                    mem_table.insert(key, value).unwrap();
                }
                KeyOp::Delete(value) => mem_table.delete(key, value).unwrap(),
                KeyOp::Update((old_value, new_value)) => {
                    mem_table.update(key, old_value, new_value).unwrap();
                }
            }
        }

        const TEST_TABLE_ID: TableId = TableId::new(233);
        const TEST_EPOCH: u64 = test_epoch(10);

        async fn check_data<I: HummockIterator>(
            iter: &mut I,
            test_data: &[(TableKey<Bytes>, KeyOp)],
        ) {
            let mut idx = 0;
            while iter.is_valid() {
                let key = iter.key();
                let value = iter.value();

                let (expected_key, expected_value) = test_data[idx].clone();
                assert_eq!(key.epoch_with_gap, EpochWithGap::new_from_epoch(TEST_EPOCH));
                assert_eq!(key.user_key.table_id, TEST_TABLE_ID);
                assert_eq!(
                    key.user_key.table_key.0,
                    expected_key.0.as_ref(),
                    "failed at {}, {:?} != {:?}",
                    idx,
                    String::from_utf8(key.user_key.table_key.key_part().to_vec()).unwrap(),
                    String::from_utf8(expected_key.key_part().to_vec()).unwrap(),
                );
                match expected_value {
                    KeyOp::Insert(expected_value) | KeyOp::Update((_, expected_value)) => {
                        assert_eq!(value, HummockValue::Put(expected_value.as_ref()));
                    }
                    KeyOp::Delete(_) => {
                        assert_eq!(value, HummockValue::Delete);
                    }
                }

                idx += 1;
                iter.next().await.unwrap();
            }
            assert_eq!(idx, test_data.len());
        }

        let mut iter = MemTableHummockIterator::new(
            &mem_table.buffer,
            EpochWithGap::new_from_epoch(TEST_EPOCH),
            TEST_TABLE_ID,
        );

        // Test rewind
        iter.rewind().await.unwrap();
        check_data(&mut iter, &ordered_test_data).await;

        // Test seek with a later epoch, the first key is not skipped
        let later_epoch = EpochWithGap::new_from_epoch(TEST_EPOCH.next_epoch());
        let seek_idx = 500;
        iter.seek(FullKey {
            user_key: UserKey {
                table_id: TEST_TABLE_ID,
                table_key: TableKey(&get_key(seek_idx)),
            },
            epoch_with_gap: later_epoch,
        })
        .await
        .unwrap();
        check_data(&mut iter, &ordered_test_data[seek_idx..]).await;

        // Test seek with a earlier epoch, the first key is skipped
        let early_epoch = EpochWithGap::new_from_epoch(TEST_EPOCH.prev_epoch());
        let seek_idx = 500;
        iter.seek(FullKey {
            user_key: UserKey {
                table_id: TEST_TABLE_ID,
                table_key: TableKey(&get_key(seek_idx)),
            },
            epoch_with_gap: early_epoch,
        })
        .await
        .unwrap();
        check_data(&mut iter, &ordered_test_data[seek_idx + 1..]).await;

        // Test seek to over the end
        iter.seek(FullKey {
            user_key: UserKey {
                table_id: TEST_TABLE_ID,
                table_key: TableKey(&get_key(ordered_test_data.len() + 10)),
            },
            epoch_with_gap: EpochWithGap::new_from_epoch(TEST_EPOCH),
        })
        .await
        .unwrap();
        check_data(&mut iter, &[]).await;

        // Test seek with a smaller table id
        let smaller_table_id = TableId::new(TEST_TABLE_ID.table_id() - 1);
        iter.seek(FullKey {
            user_key: UserKey {
                table_id: smaller_table_id,
                table_key: TableKey(&get_key(ordered_test_data.len() + 10)),
            },
            epoch_with_gap: EpochWithGap::new_from_epoch(TEST_EPOCH),
        })
        .await
        .unwrap();
        check_data(&mut iter, &ordered_test_data).await;

        // Test seek with a greater table id
        let greater_table_id = TableId::new(TEST_TABLE_ID.table_id() + 1);
        iter.seek(FullKey {
            user_key: UserKey {
                table_id: greater_table_id,
                table_key: TableKey(&get_key(0)),
            },
            epoch_with_gap: EpochWithGap::new_from_epoch(TEST_EPOCH),
        })
        .await
        .unwrap();
        check_data(&mut iter, &[]).await;

        // check reverse iterator
        ordered_test_data.reverse();
        drop(iter);
        let mut iter = MemTableHummockRevIterator::new(
            &mem_table.buffer,
            EpochWithGap::new_from_epoch(TEST_EPOCH),
            TEST_TABLE_ID,
        );

        // Test rewind
        iter.rewind().await.unwrap();
        check_data(&mut iter, &ordered_test_data).await;

        // Test seek with a smaller table id
        let smaller_table_id = TableId::new(TEST_TABLE_ID.table_id() - 1);
        iter.seek(FullKey {
            user_key: UserKey {
                table_id: smaller_table_id,
                table_key: TableKey(&get_key(ordered_test_data.len() + 10)),
            },
            epoch_with_gap: EpochWithGap::new_from_epoch(TEST_EPOCH),
        })
        .await
        .unwrap();
        check_data(&mut iter, &[]).await;

        // Test seek with a greater table id
        let greater_table_id = TableId::new(TEST_TABLE_ID.table_id() + 1);
        iter.seek(FullKey {
            user_key: UserKey {
                table_id: greater_table_id,
                table_key: TableKey(&get_key(0)),
            },
            epoch_with_gap: EpochWithGap::new_from_epoch(TEST_EPOCH),
        })
        .await
        .unwrap();
        check_data(&mut iter, &ordered_test_data).await;

        // Test seek with a later epoch, the first key is skipped
        let later_epoch = EpochWithGap::new_from_epoch(TEST_EPOCH.next_epoch());
        let seek_idx = 500;
        iter.seek(FullKey {
            user_key: UserKey {
                table_id: TEST_TABLE_ID,
                table_key: TableKey(&get_key(seek_idx)),
            },
            epoch_with_gap: later_epoch,
        })
        .await
        .unwrap();
        let rev_seek_idx = ordered_test_data.len() - seek_idx - 1;
        check_data(&mut iter, &ordered_test_data[rev_seek_idx + 1..]).await;

        // Test seek with a earlier epoch, the first key is not skipped
        let early_epoch = EpochWithGap::new_from_epoch(TEST_EPOCH.prev_epoch());
        let seek_idx = 500;
        iter.seek(FullKey {
            user_key: UserKey {
                table_id: TEST_TABLE_ID,
                table_key: TableKey(&get_key(seek_idx)),
            },
            epoch_with_gap: early_epoch,
        })
        .await
        .unwrap();
        let rev_seek_idx = ordered_test_data.len() - seek_idx - 1;
        check_data(&mut iter, &ordered_test_data[rev_seek_idx..]).await;

        drop(iter);
        mem_table.insert(get_key(10001), "value1".into()).unwrap();

        let mut iter = MemTableHummockRevIterator::new(
            &mem_table.buffer,
            EpochWithGap::new_from_epoch(TEST_EPOCH),
            TEST_TABLE_ID,
        );
        iter.seek(FullKey {
            user_key: UserKey {
                table_id: TEST_TABLE_ID,
                table_key: TableKey(&get_key(10000)),
            },
            epoch_with_gap: early_epoch,
        })
        .await
        .unwrap();
        assert_eq!(iter.key().user_key.table_key, get_key(9999).to_ref());

        let mut iter = MemTableHummockIterator::new(
            &mem_table.buffer,
            EpochWithGap::new_from_epoch(TEST_EPOCH),
            TEST_TABLE_ID,
        );
        iter.seek(FullKey {
            user_key: UserKey {
                table_id: TEST_TABLE_ID,
                table_key: TableKey(&get_key(10000)),
            },
            epoch_with_gap: later_epoch,
        })
        .await
        .unwrap();
        assert_eq!(iter.key().user_key.table_key, get_key(10001).to_ref());
    }
}
