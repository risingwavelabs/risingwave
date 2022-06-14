// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#![allow(dead_code)]
use std::collections::btree_map::{self, Entry};
use std::collections::BTreeMap;

use risingwave_common::array::Row;

use crate::error::StorageResult;

#[derive(Clone)]
pub enum RowOp {
    Insert(Row),
    Delete(Row),
    Update((Row, Row)),
}
/// `MemTable` is a buffer for modify operations without encoding
#[derive(Clone)]
pub struct MemTable {
    pub buffer: BTreeMap<Vec<u8>, RowOp>,
}
type MemTableIter<'a> = btree_map::Iter<'a, Row, RowOp>;

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

    /// read methods
    pub fn get_row(&self, pk: &[u8]) -> StorageResult<Option<&RowOp>> {
        Ok(self.buffer.get(pk))
    }

    /// write methods
    pub fn insert(&mut self, pk: Vec<u8>, value: Row) -> StorageResult<()> {
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(RowOp::Insert(value));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                x @ RowOp::Delete(_) => {
                    if let RowOp::Delete(ref mut old_value) = x {
                        let old_val = std::mem::take(old_value);
                        e.insert(RowOp::Update((old_val, value)));
                    } else {
                        unreachable!();
                    }
                }

                _ => {
                    panic!(
                        "invalid flush status: double insert {:?} -> {:?}",
                        e.key(),
                        value
                    );
                }
            },
        }
        Ok(())
    }

    pub fn delete(&mut self, pk: Vec<u8>, old_value: Row) -> StorageResult<()> {
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(RowOp::Delete(old_value));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                RowOp::Insert(original_value) => {
                    debug_assert_eq!(original_value, &old_value);
                    e.remove();
                }
                RowOp::Delete(_) => {
                    panic!(
                        "invalid flush status: double delete {:?} -> {:?}",
                        e.key(),
                        old_value
                    );
                }
                x @ RowOp::Update(_) => {
                    if let RowOp::Update(ref mut value) = x {
                        let (original_old_value, original_new_value) = std::mem::take(value);
                        debug_assert_eq!(original_new_value, old_value);
                        e.insert(RowOp::Delete(original_old_value));
                    }
                }
            },
        }
        Ok(())
    }

    pub fn update(&mut self, pk: Vec<u8>, old_value: Row, new_value: Row) -> StorageResult<()> {
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(RowOp::Update((old_value, new_value)));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                RowOp::Insert(original_value) => {
                    debug_assert_eq!(original_value, &old_value);
                    e.insert(RowOp::Update((old_value, new_value)));
                }
                RowOp::Delete(_) => {
                    panic!(
                        "invalid flush status: double delete {:?} -> {:?}",
                        e.key(),
                        old_value
                    );
                }
                x @ RowOp::Update(_) => {
                    if let RowOp::Update(ref mut value) = x {
                        let (original_old_value, original_new_value) = std::mem::take(value);
                        debug_assert_eq!(original_new_value, old_value);
                        e.insert(RowOp::Update((original_old_value, new_value)));
                    }
                }
            },
        }
        Ok(())
    }

    pub fn into_parts(self) -> BTreeMap<Vec<u8>, RowOp> {
        self.buffer
    }

    pub async fn iter(&self, _pk: Row) -> StorageResult<MemTableIter<'_>> {
        todo!();
    }
}
