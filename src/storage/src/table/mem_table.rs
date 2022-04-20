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
    Update(Row),
}
impl RowOp {
    pub fn is_insert(&self) -> bool {
        matches!(self, Self::Insert(_))
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, Self::Delete(_))
    }

    pub fn is_update(&self) -> bool {
        matches!(self, Self::Delete(_))
    }
}
/// `MemTable` is a buffer for modify operations without encoding
#[derive(Clone)]
pub struct MemTable {
    pub buffer: BTreeMap<Row, RowOp>,
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
    pub fn get_row(&self, pk: &Row) -> StorageResult<Option<RowOp>> {
        let res = self.buffer.get(pk);
        match res {
            Some(row_op) => Ok(Some(row_op.clone())),
            None => Ok(None),
        }
    }

    /// write methods
    pub fn insert(&mut self, pk: Row, value: Row) -> StorageResult<()> {
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(RowOp::Insert(value));
            }
            Entry::Occupied(mut e) => {
                if e.get().is_delete() {
                    e.insert(RowOp::Update(value));
                } else {
                    panic!(
                        "invalid flush status: double insert {:?} -> {:?}",
                        e.key(),
                        value
                    );
                }
            }
        }
        Ok(())
    }

    pub fn delete(&mut self, pk: Row, old_value: Row) -> StorageResult<()> {
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(RowOp::Delete(old_value));
            }
            Entry::Occupied(mut e) => {
                if e.get().is_insert() {
                    e.remove();
                } else if e.get().is_update() {
                    e.insert(RowOp::Delete(old_value));
                } else {
                    panic!(
                        "invalid flush status: double delete {:?} -> {:?}",
                        e.key(),
                        old_value
                    );
                }
            }
        }
        Ok(())
    }

    pub fn into_parts(self) -> BTreeMap<Row, RowOp> {
        self.buffer
    }

    pub fn update(&mut self, _pk: Row, _old_value: Row, _new_value: Row) -> StorageResult<()> {
        Ok(())
    }

    pub async fn iter(&self, _pk: Row) -> StorageResult<MemTableIter<'_>> {
        todo!();
    }
}
