// Copyright 2023 Singularity Data
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
use std::mem::swap;
use std::ops::{Bound, RangeBounds};

use bytes::Bytes;
use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::row::RowDeserializer;
use risingwave_hummock_sdk::key::{FullKey, TableKey};
use thiserror::Error;

use crate::error::{StorageError, StorageResult};
use crate::storage_value::StorageValue;
use crate::store::*;

#[derive(Clone, Debug)]
pub enum KeyOp {
    Insert(Bytes),
    Delete(Bytes),
    Update((Bytes, Bytes)),
}

/// `MemTable` is a buffer for modify operations without encoding
#[derive(Clone)]
pub struct MemTable {
    buffer: BTreeMap<Bytes, KeyOp>,
    disable_sanity_check: bool,
}

#[derive(Error, Debug)]
pub enum MemTableError {
    #[error("conflicted row operations on same key")]
    Conflict { key: Bytes, prev: KeyOp, new: KeyOp },

    #[error("update on different old value")]
    Update {
        key: Bytes,
        value: Bytes,
        old_value: Bytes,
        stored_value: Bytes,
    },

    #[error("delete on different old value")]
    Delete {
        key: Bytes,
        old_value: Bytes,
        stored_value: Bytes,
    },
}

type Result<T> = std::result::Result<T, Box<MemTableError>>;

impl MemTable {
    pub fn new(disable_sanity_check: bool) -> Self {
        Self {
            buffer: BTreeMap::new(),
            disable_sanity_check,
        }
    }

    pub fn drain(&mut self) -> Self {
        let mut temp = Self::new(self.disable_sanity_check);
        swap(&mut temp, self);
        temp
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
        if !ENABLE_SANITY_CHECK || self.disable_sanity_check {
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
                _ => Err(MemTableError::Conflict {
                    key: e.key().clone(),
                    prev: e.get().clone(),
                    new: KeyOp::Insert(value),
                }
                .into()),
            },
        }
    }

    pub fn delete(&mut self, pk: Bytes, old_value: Bytes) -> Result<()> {
        if !ENABLE_SANITY_CHECK || self.disable_sanity_check {
            self.buffer.insert(pk, KeyOp::Delete(old_value));
            return Ok(());
        }
        let entry = self.buffer.entry(pk.clone());
        match entry {
            Entry::Vacant(e) => {
                e.insert(KeyOp::Delete(old_value));
                Ok(())
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                KeyOp::Insert(original_value) => {
                    if original_value != &old_value {
                        return Err(Box::new(MemTableError::Delete {
                            key: pk,
                            stored_value: original_value.clone(),
                            old_value,
                        }));
                    }
                    e.remove();
                    Ok(())
                }
                KeyOp::Delete(_) => Err(MemTableError::Conflict {
                    key: e.key().clone(),
                    prev: e.get().clone(),
                    new: KeyOp::Delete(old_value),
                }
                .into()),
                KeyOp::Update(value) => {
                    let (original_old_value, original_new_value) = std::mem::take(value);
                    if original_new_value != old_value {
                        return Err(Box::new(MemTableError::Delete {
                            key: pk,
                            stored_value: original_new_value,
                            old_value,
                        }));
                    }
                    e.insert(KeyOp::Delete(original_old_value));
                    Ok(())
                }
            },
        }
    }

    pub fn update(&mut self, pk: Bytes, old_value: Bytes, new_value: Bytes) -> Result<()> {
        if !ENABLE_SANITY_CHECK || self.disable_sanity_check {
            self.buffer
                .insert(pk, KeyOp::Update((old_value, new_value)));
            return Ok(());
        }
        let entry = self.buffer.entry(pk.clone());
        match entry {
            Entry::Vacant(e) => {
                e.insert(KeyOp::Update((old_value, new_value)));
                Ok(())
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                KeyOp::Insert(original_value) => {
                    if original_value != &old_value {
                        return Err(Box::new(MemTableError::Update {
                            key: pk,
                            value: new_value,
                            stored_value: original_value.clone(),
                            old_value,
                        }));
                    }
                    e.insert(KeyOp::Insert(new_value));
                    Ok(())
                }
                KeyOp::Delete(_) => Err(MemTableError::Conflict {
                    key: e.key().clone(),
                    prev: e.get().clone(),
                    new: KeyOp::Update((old_value, new_value)),
                }
                .into()),
                KeyOp::Update(value) => {
                    let (original_old_value, original_new_value) = std::mem::take(value);
                    if original_new_value != old_value {
                        return Err(Box::new(MemTableError::Update {
                            key: pk,
                            value: new_value,
                            stored_value: original_old_value,
                            old_value,
                        }));
                    }
                    e.insert(KeyOp::Update((original_old_value, new_value)));
                    Ok(())
                }
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
    pub fn debug_fmt(&self, row_deserializer: &RowDeserializer) -> String {
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
async fn merge_stream<'a>(
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
    disable_sanity_check: bool,
    table_option: TableOption,
}

impl<S: StateStoreWrite + StateStoreRead> MemtableLocalStateStore<S> {
    pub fn new(inner: S, option: NewLocalOptions) -> Self {
        Self {
            inner,
            mem_table: MemTable::new(option.disable_sanity_check),
            epoch: None,
            table_id: option.table_id,
            disable_sanity_check: option.disable_sanity_check,
            table_option: option.table_option,
        }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Make sure the key to insert should not exist in storage.
    async fn do_insert_sanity_check(&self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        let read_options = ReadOptions {
            prefix_hint: None,
            retention_seconds: self.table_option.retention_seconds,
            table_id: self.table_id,
            ignore_range_tombstone: false,
            read_version_from_backup: false,
        };
        let stored_value = self.inner.get(key, self.epoch(), read_options).await?;

        if let Some(stored_value) = stored_value {
            return Err(Box::new(MemTableError::Conflict {
                key: Bytes::copy_from_slice(key),
                prev: KeyOp::Insert(stored_value),
                new: KeyOp::Insert(Bytes::copy_from_slice(value)),
            })
            .into());
        }
        Ok(())
    }

    /// Make sure that the key to delete should exist in storage and the value should be matched.
    async fn do_delete_sanity_check(&self, key: &[u8], old_value: &[u8]) -> StorageResult<()> {
        let read_options = ReadOptions {
            prefix_hint: None,
            retention_seconds: self.table_option.retention_seconds,
            table_id: self.table_id,
            ignore_range_tombstone: false,
            read_version_from_backup: false,
        };
        let stored_value = self.inner.get(key, self.epoch(), read_options).await?;

        if stored_value.is_none() || stored_value.as_ref().unwrap() != old_value {
            return Err(Box::new(MemTableError::Delete {
                key: Bytes::copy_from_slice(key),
                old_value: Bytes::copy_from_slice(old_value),
                stored_value: stored_value.unwrap(),
            })
            .into());
        }
        Ok(())
    }

    /// Make sure that the key to update should exist in storage and the value should be matched
    async fn do_update_sanity_check(
        &self,
        key: &[u8],
        old_value: &[u8],
        new_value: &[u8],
    ) -> StorageResult<()> {
        let read_options = ReadOptions {
            prefix_hint: None,
            ignore_range_tombstone: false,
            retention_seconds: self.table_option.retention_seconds,
            table_id: self.table_id,
            read_version_from_backup: false,
        };
        let stored_value = self.inner.get(key, self.epoch(), read_options).await?;

        if stored_value.is_none() || stored_value.as_ref().unwrap() != old_value {
            return Err(Box::new(MemTableError::Update {
                key: Bytes::copy_from_slice(key),
                value: Bytes::copy_from_slice(new_value),
                old_value: Bytes::copy_from_slice(old_value),
                stored_value: stored_value.unwrap(),
            })
            .into());
        }

        Ok(())
    }
}

const ENABLE_SANITY_CHECK: bool = cfg!(debug_assertions);

fn filter_with_delete_range<'a>(
    kv_iter: impl Iterator<Item = (Bytes, KeyOp)> + 'a,
    mut delete_ranges_iter: impl Iterator<Item = &'a (Bytes, Bytes)> + 'a,
) -> impl Iterator<Item = (Bytes, KeyOp)> + 'a {
    let mut range = delete_ranges_iter.next();
    if let Some((range_start, range_end)) = range {
        assert!(
            range_start <= range_end,
            "range_end {:?} smaller than range_start {:?}",
            range_start,
            range_end
        );
    }
    kv_iter.filter(move |(ref key, _)| {
        if let Some((range_start, range_end)) = range {
            if key < range_start {
                true
            } else if key < range_end {
                false
            } else {
                // Key has exceeded the current key range. Advance to the next range.
                loop {
                    range = delete_ranges_iter.next();
                    if let Some((range_start, range_end)) = range {
                        assert!(
                            range_start <= range_end,
                            "range_end {:?} smaller than range_start {:?}",
                            range_start,
                            range_end
                        );
                        if key < range_start {
                            // Not fall in the next delete range
                            break true;
                        } else if key < range_end {
                            // Fall in the next delete range
                            break false;
                        } else {
                            // Exceed the next delete range. Go to the next delete range if there is
                            // any in the next loop
                            continue;
                        }
                    } else {
                        // No more delete range.
                        break true;
                    }
                }
            }
        } else {
            true
        }
    })
}

impl<S: StateStoreWrite + StateStoreRead> LocalStateStore for MemtableLocalStateStore<S> {
    type FlushFuture<'a> = impl Future<Output = StorageResult<usize>> + 'a;
    type GetFuture<'a> = impl GetFutureTrait<'a>;
    type IterFuture<'a> = impl Future<Output = StorageResult<Self::IterStream<'a>>> + Send + 'a;
    type IterStream<'a> = impl StateStoreIterItemStream + 'a;

    fn get<'a>(&'a self, key: &'a [u8], read_options: ReadOptions) -> Self::GetFuture<'_> {
        async move {
            match self.mem_table.buffer.get(key) {
                None => self.inner.get(key, self.epoch(), read_options).await,
                Some(op) => match op {
                    KeyOp::Insert(value) | KeyOp::Update((_, value)) => Ok(Some(value.clone())),
                    KeyOp::Delete(_) => Ok(None),
                },
            }
        }
    }

    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
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
                    // state store with `disable_sanity_check=true`.
                    KeyOp::Insert(value) => {
                        if ENABLE_SANITY_CHECK && !self.disable_sanity_check {
                            self.do_insert_sanity_check(&key, &value).await?;
                        }
                        kv_pairs.push((key, StorageValue::new_put(value)));
                    }
                    KeyOp::Delete(old_value) => {
                        if ENABLE_SANITY_CHECK && !self.disable_sanity_check {
                            self.do_delete_sanity_check(&key, &old_value).await?;
                        }
                        kv_pairs.push((key, StorageValue::new_delete()));
                    }
                    KeyOp::Update((old_value, new_value)) => {
                        if ENABLE_SANITY_CHECK && !self.disable_sanity_check {
                            self.do_update_sanity_check(&key, &old_value, &new_value)
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
