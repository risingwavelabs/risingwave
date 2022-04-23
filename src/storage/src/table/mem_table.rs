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
#![allow(unused)]
use std::collections::{btree_map, BTreeMap};

use futures::Future;
use risingwave_common::array::Row;

use crate::error::StorageResult;

#[derive(Clone)]
pub enum RowOp {
    Insert(Row),
    Delete(Row),
    Update((Row, Row)),
}
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
    pub async fn get_row(&self, pk: &Row) -> StorageResult<Option<RowOp>> {
        todo!()
    }

    /// write methods
    pub async fn insert(&mut self, _pk: Row, _value: Row) -> StorageResult<()> {
        Ok(())
    }

    pub async fn delete(&mut self, _pk: Row, _old_value: Row) -> StorageResult<()> {
        Ok(())
    }

    pub async fn update(
        &mut self,
        _pk: Row,
        _old_value: Row,
        _new_value: Row,
    ) -> StorageResult<()> {
        Ok(())
    }

    pub async fn iter(&self, _pk: Row) -> StorageResult<MemTableIter<'_>> {
        todo!();
    }
}
