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
use std::mem::swap;
use std::ops::RangeBounds;

use bytes::Bytes;
use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::TableId;
use risingwave_common::row::RowDeserializer;
use risingwave_hummock_sdk::key::{FullKey, TableKey};
use thiserror::Error;

use crate::error::StorageError;
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
    buffer: BTreeMap<Bytes, KeyOp>,
    is_consistent_op: bool,
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
        let mut temp = Self::new(self.is_consistent_op);
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
pub async fn merge_stream<'a>(
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

const ENABLE_SANITY_CHECK: bool = cfg!(debug_assertions);
