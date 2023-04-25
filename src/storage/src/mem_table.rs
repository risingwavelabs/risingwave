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

use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::future::Future;
use std::ops::RangeBounds;

use bytes::Bytes;
use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::util::value_encoding::ValueRowSerde;
use risingwave_hummock_sdk::key::{FullKey, TableKey};
use risingwave_hummock_sdk::opts::{NewLocalOptions, ReadOptions, WriteOptions};
use thiserror::Error;

use crate::error::{StorageError, StorageResult};
use crate::hummock::utils::{
    do_delete_sanity_check, do_insert_sanity_check, do_update_sanity_check,
    filter_with_delete_range, ENABLE_SANITY_CHECK,
};
use crate::storage_value::StorageValue;
use crate::store::*;

#[derive(Clone, Debug)]
pub enum KeyOp {
    Insert(Bytes),
    Delete(Bytes),
    /// (old_value, new_value)
    Update((Bytes, Bytes)),
}

/// `MemTable` is a buffer for modify operations without encoding
#[derive(Clone)]
pub struct MemTable {
    pub(crate) buffer: BTreeMap<Bytes, KeyOp>,
    pub(crate) is_consistent_op: bool,
}

#[derive(Error, Debug)]
pub enum MemTableError {
    #[error("Inconsistent operation")]
    InconsistentOperation { key: Bytes, prev: KeyOp, new: KeyOp },
}

type Result<T> = std::result::Result<T, Box<MemTableError>>;

impl MemTable {
    pub fn new(is_consistent_op: bool) -> Self {
        Self {
            buffer: BTreeMap::new(),
            is_consistent_op,
        }
    }

    pub fn drain(&mut self) -> Self {
        std::mem::replace(self, Self::new(self.is_consistent_op))
    }

    pub fn is_dirty(&self) -> bool {
        !self.buffer.is_empty()
    }

    /// read methods
    pub fn get_key_op(&self, pk: &[u8]) -> Option<&KeyOp> {
        self.buffer.get(pk)
    }

    /// write methods
    pub fn insert(&mut self, pk: Bytes, value: Bytes) -> Result<()> {
        if !self.is_consistent_op {
            self.buffer.insert(pk, KeyOp::Insert(value));
            return Ok(());
        }
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(KeyOp::Insert(value));
                Ok(())
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                KeyOp::Delete(ref mut old_value) => {
                    let old_val = std::mem::take(old_value);
                    e.insert(KeyOp::Update((old_val, value)));
                    Ok(())
                }
                KeyOp::Insert(_) | KeyOp::Update(_) => Err(MemTableError::InconsistentOperation {
                    key: e.key().clone(),
                    prev: e.get().clone(),
                    new: KeyOp::Insert(value),
                }
                .into()),
            },
        }
    }

    pub fn delete(&mut self, pk: Bytes, old_value: Bytes) -> Result<()> {
        if !self.is_consistent_op {
            self.buffer.insert(pk, KeyOp::Delete(old_value));
            return Ok(());
        }
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(KeyOp::Delete(old_value));
                Ok(())
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                KeyOp::Insert(original_value) => {
                    if ENABLE_SANITY_CHECK && original_value != &old_value {
                        return Err(Box::new(MemTableError::InconsistentOperation {
                            key: e.key().clone(),
                            prev: e.get().clone(),
                            new: KeyOp::Delete(old_value),
                        }));
                    }
                    e.remove();
                    Ok(())
                }
                KeyOp::Delete(_) => Err(MemTableError::InconsistentOperation {
                    key: e.key().clone(),
                    prev: e.get().clone(),
                    new: KeyOp::Delete(old_value),
                }
                .into()),
                KeyOp::Update(value) => {
                    let (original_old_value, original_new_value) = std::mem::take(value);
                    if ENABLE_SANITY_CHECK && original_new_value != old_value {
                        return Err(Box::new(MemTableError::InconsistentOperation {
                            key: e.key().clone(),
                            prev: e.get().clone(),
                            new: KeyOp::Delete(old_value),
                        }));
                    }
                    e.insert(KeyOp::Delete(original_old_value));
                    Ok(())
                }
            },
        }
    }

    pub fn update(&mut self, pk: Bytes, old_value: Bytes, new_value: Bytes) -> Result<()> {
        if !self.is_consistent_op {
            self.buffer
                .insert(pk, KeyOp::Update((old_value, new_value)));
            return Ok(());
        }
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(KeyOp::Update((old_value, new_value)));
                Ok(())
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                KeyOp::Insert(ref mut original_new_value)
                | KeyOp::Update((_, ref mut original_new_value)) => {
                    if ENABLE_SANITY_CHECK && original_new_value != &old_value {
                        return Err(Box::new(MemTableError::InconsistentOperation {
                            key: e.key().clone(),
                            prev: e.get().clone(),
                            new: KeyOp::Update((old_value, new_value)),
                        }));
                    }
                    *original_new_value = new_value;
                    Ok(())
                }
                KeyOp::Delete(_) => Err(MemTableError::InconsistentOperation {
                    key: e.key().clone(),
                    prev: e.get().clone(),
                    new: KeyOp::Update((old_value, new_value)),
                }
                .into()),
            },
        }
    }

    pub fn into_parts(self) -> BTreeMap<Bytes, KeyOp> {
        self.buffer
    }

    pub fn iter<'a, R>(&'a self, key_range: R) -> impl Iterator<Item = (&'a Bytes, &'a KeyOp)>
    where
        R: RangeBounds<Bytes> + 'a,
    {
        self.buffer.range(key_range)
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

#[try_stream(ok = StateStoreIterItem, error = StorageError)]
pub(crate) async fn merge_stream<'a>(
    mem_table_iter: impl Iterator<Item = (&'a Bytes, &'a KeyOp)> + 'a,
    inner_stream: impl StateStoreReadIterStream,
    table_id: TableId,
    epoch: u64,
) {
    let inner_stream = inner_stream.peekable();
    pin_mut!(inner_stream);

    let mut mem_table_iter = mem_table_iter.fuse().peekable();

    loop {
        match (inner_stream.as_mut().peek().await, mem_table_iter.peek()) {
            (None, None) => break,
            // The mem table side has come to an end, return data from the shared storage.
            (Some(_), None) => {
                let (key, value) = inner_stream.next().await.unwrap()?;
                yield (key, value)
            }
            // The stream side has come to an end, return data from the mem table.
            (None, Some(_)) => {
                let (key, key_op) = mem_table_iter.next().unwrap();
                match key_op {
                    KeyOp::Insert(value) | KeyOp::Update((_, value)) => {
                        yield (
                            FullKey::new(table_id, TableKey(key.clone()), epoch),
                            value.clone(),
                        )
                    }
                    _ => {}
                }
            }
            (Some(Ok((inner_key, _))), Some((mem_table_key, _))) => {
                debug_assert_eq!(inner_key.user_key.table_id, table_id);
                match inner_key.user_key.table_key.0.cmp(mem_table_key) {
                    Ordering::Less => {
                        // yield data from storage
                        let (key, value) = inner_stream.next().await.unwrap()?;
                        yield (key, value);
                    }
                    Ordering::Equal => {
                        // both memtable and storage contain the key, so we advance both
                        // iterators and return the data in memory.

                        let (_, key_op) = mem_table_iter.next().unwrap();
                        let (key, old_value_in_inner) = inner_stream.next().await.unwrap()?;
                        match key_op {
                            KeyOp::Insert(value) => {
                                yield (key.clone(), value.clone());
                            }
                            KeyOp::Delete(_) => {}
                            KeyOp::Update((old_value, new_value)) => {
                                debug_assert!(old_value == &old_value_in_inner);

                                yield (key, new_value.clone());
                            }
                        }
                    }
                    Ordering::Greater => {
                        // yield data from mem table
                        let (key, key_op) = mem_table_iter.next().unwrap();

                        match key_op {
                            KeyOp::Insert(value) => {
                                yield (
                                    FullKey::new(table_id, TableKey(key.clone()), epoch),
                                    value.clone(),
                                );
                            }
                            KeyOp::Delete(_) => {}
                            KeyOp::Update(_) => unreachable!(
                                "memtable update should always be paired with a storage key"
                            ),
                        }
                    }
                }
            }
            (Some(Err(_)), Some(_)) => {
                // Throw the error.
                return Err(inner_stream.next().await.unwrap().unwrap_err());
            }
        }
    }
}

pub struct MemtableLocalStateStore<S: StateStoreWrite + StateStoreRead> {
    mem_table: MemTable,
    inner: S,

    epoch: Option<u64>,

    table_id: TableId,
    is_consistent_op: bool,
    table_option: TableOption,
}

impl<S: StateStoreWrite + StateStoreRead> MemtableLocalStateStore<S> {
    pub fn new(inner: S, option: NewLocalOptions) -> Self {
        Self {
            inner,
            mem_table: MemTable::new(option.is_consistent_op),
            epoch: None,
            table_id: option.table_id,
            is_consistent_op: option.is_consistent_op,
            table_option: option.table_option,
        }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: StateStoreWrite + StateStoreRead> LocalStateStore for MemtableLocalStateStore<S> {
    type FlushFuture<'a> = impl Future<Output = StorageResult<usize>> + 'a;
    type GetFuture<'a> = impl GetFutureTrait<'a>;
    type IterFuture<'a> = impl Future<Output = StorageResult<Self::IterStream<'a>>> + Send + 'a;
    type IterStream<'a> = impl StateStoreIterItemStream + 'a;

    define_local_state_store_associated_type!();

    fn may_exist(
        &self,
        _key_range: IterKeyRange,
        _read_options: ReadOptions,
    ) -> Self::MayExistFuture<'_> {
        async { Ok(true) }
    }

    fn get(&self, key: Bytes, read_options: ReadOptions) -> Self::GetFuture<'_> {
        async move {
            match self.mem_table.buffer.get(&key) {
                None => self.inner.get(key, self.epoch(), read_options).await,
                Some(op) => match op {
                    KeyOp::Insert(value) | KeyOp::Update((_, value)) => Ok(Some(value.clone())),
                    KeyOp::Delete(_) => Ok(None),
                },
            }
        }
    }

    fn iter(&self, key_range: IterKeyRange, read_options: ReadOptions) -> Self::IterFuture<'_> {
        async move {
            let stream = self
                .inner
                .iter(key_range.clone(), self.epoch(), read_options)
                .await?;
            let (l, r) = key_range;
            let key_range = (l.map(Bytes::from), r.map(Bytes::from));
            Ok(merge_stream(
                self.mem_table.iter(key_range),
                stream,
                self.table_id,
                self.epoch(),
            ))
        }
    }

    fn insert(&mut self, key: Bytes, new_val: Bytes, old_val: Option<Bytes>) -> StorageResult<()> {
        match old_val {
            None => self.mem_table.insert(key, new_val)?,
            Some(old_val) => self.mem_table.update(key, old_val, new_val)?,
        };
        Ok(())
    }

    fn delete(&mut self, key: Bytes, old_val: Bytes) -> StorageResult<()> {
        Ok(self.mem_table.delete(key, old_val)?)
    }

    fn flush(&mut self, delete_ranges: Vec<(Bytes, Bytes)>) -> Self::FlushFuture<'_> {
        async move {
            debug_assert!(delete_ranges.iter().map(|(key, _)| key).is_sorted());
            let buffer = self.mem_table.drain().into_parts();
            let mut kv_pairs = Vec::with_capacity(buffer.len());
            for (key, key_op) in filter_with_delete_range(buffer.into_iter(), delete_ranges.iter())
            {
                match key_op {
                    // Currently, some executors do not strictly comply with these semantics. As
                    // a workaround you may call disable the check by initializing the
                    // state store with `is_consistent_op=false`.
                    KeyOp::Insert(value) => {
                        if ENABLE_SANITY_CHECK && self.is_consistent_op {
                            do_insert_sanity_check(
                                key.clone(),
                                value.clone(),
                                &self.inner,
                                self.epoch(),
                                self.table_id,
                                self.table_option,
                            )
                            .await?;
                        }
                        kv_pairs.push((key, StorageValue::new_put(value)));
                    }
                    KeyOp::Delete(old_value) => {
                        if ENABLE_SANITY_CHECK && self.is_consistent_op {
                            do_delete_sanity_check(
                                key.clone(),
                                old_value,
                                &self.inner,
                                self.epoch(),
                                self.table_id,
                                self.table_option,
                            )
                            .await?;
                        }
                        kv_pairs.push((key, StorageValue::new_delete()));
                    }
                    KeyOp::Update((old_value, new_value)) => {
                        if ENABLE_SANITY_CHECK && self.is_consistent_op {
                            do_update_sanity_check(
                                key.clone(),
                                old_value,
                                new_value.clone(),
                                &self.inner,
                                self.epoch(),
                                self.table_id,
                                self.table_option,
                            )
                            .await?;
                        }
                        kv_pairs.push((key, StorageValue::new_put(new_value)));
                    }
                }
            }
            self.inner
                .ingest_batch(
                    kv_pairs,
                    delete_ranges,
                    WriteOptions {
                        epoch: self.epoch(),
                        table_id: self.table_id,
                    },
                )
                .await
        }
    }

    fn epoch(&self) -> u64 {
        self.epoch.expect("should have set the epoch")
    }

    fn is_dirty(&self) -> bool {
        self.mem_table.is_dirty()
    }

    fn init(&mut self, epoch: u64) {
        assert!(
            self.epoch.replace(epoch).is_none(),
            "local state store of table id {:?} is init for more than once",
            self.table_id
        );
    }

    fn seal_current_epoch(&mut self, next_epoch: u64) {
        assert!(!self.is_dirty());
        let prev_epoch = self
            .epoch
            .replace(next_epoch)
            .expect("should have init epoch before seal the first epoch");
        assert!(
            next_epoch > prev_epoch,
            "new epoch {} should be greater than current epoch: {}",
            next_epoch,
            prev_epoch
        );
    }
}
