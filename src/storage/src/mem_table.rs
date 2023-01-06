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
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::mem::swap;
use std::ops::RangeBounds;

use bytes::Bytes;
use risingwave_common::row::RowDeserializer;
use thiserror::Error;

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
}

pub type MemTableIter<'a> = impl Iterator<Item = (&'a Bytes, &'a KeyOp)>;

#[derive(Error, Debug)]
pub enum MemTableError {
    #[error("conflicted row operations on same key")]
    Conflict { key: Bytes, prev: KeyOp, new: KeyOp },
}

type Result<T> = std::result::Result<T, Box<MemTableError>>;

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            buffer: BTreeMap::new(),
        }
    }

    pub fn drain(&mut self) -> Self {
        let mut temp = Self::new();
        swap(&mut temp, self);
        temp
    }

    pub fn is_dirty(&self) -> bool {
        !self.buffer.is_empty()
    }

    /// read methods
    pub fn get_row_op(&self, pk: &[u8]) -> Option<&KeyOp> {
        self.buffer.get(pk)
    }

    /// write methods
    pub fn insert(&mut self, pk: Bytes, value: Bytes) -> Result<()> {
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
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(KeyOp::Delete(old_value));
                Ok(())
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                KeyOp::Insert(original_value) => {
                    debug_assert_eq!(original_value, &old_value);
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
                    debug_assert_eq!(original_new_value, old_value);
                    e.insert(KeyOp::Delete(original_old_value));
                    Ok(())
                }
            },
        }
    }

    pub fn update(&mut self, pk: Bytes, old_value: Bytes, new_value: Bytes) -> Result<()> {
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(KeyOp::Update((old_value, new_value)));
                Ok(())
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                KeyOp::Insert(original_value) => {
                    debug_assert_eq!(original_value, &old_value);
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
                    debug_assert_eq!(original_new_value, old_value);
                    e.insert(KeyOp::Update((original_old_value, new_value)));
                    Ok(())
                }
            },
        }
    }

    pub fn into_parts(self) -> BTreeMap<Bytes, KeyOp> {
        self.buffer
    }

    pub fn iter<'a, R>(&'a self, key_range: R) -> MemTableIter<'a>
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
