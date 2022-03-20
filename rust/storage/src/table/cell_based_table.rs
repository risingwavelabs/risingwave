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
//
#![allow(dead_code)]
#![allow(unused)]
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId, OrderedColumnDesc};
use risingwave_common::error::Result;
use risingwave_common::types::Datum;
use risingwave_common::util::ordered::*;

use super::TableIter;
use crate::{Keyspace, StateStore};

/// `CellBasedTable` is the interface accessing relational data in KV(`StateStore`) with encoding
/// format: [keyspace | pk | `column_id` (4B)] -> value.
/// if the key of the column id does not exist, it will be Null in the relation
pub struct CellBasedTable<S: StateStore> {
    keyspace: Keyspace<S>,
    pk: Vec<OrderedColumnDesc>,
    pk_serializer: OrderedRowSerializer,
}

impl<S: StateStore> CellBasedTable<S> {
    pub fn new(keyspace: Keyspace<S>, pk: Vec<OrderedColumnDesc>) -> Self {
        todo!()
    }

    pub async fn get(&self, pk: Row, column: &ColumnDesc, epoch: u64) -> Result<Option<Datum>> {
        todo!()
    }

    pub async fn get_row(&self, pk: Row, columns: &[ColumnDesc], epoch: u64) -> Result<Row> {
        todo!()
    }
}
// (st1page): May be we will have a "ChunkIter" trait which returns a chunk each time, so the name
// "RowTableIter" is reserved now
pub struct RowTableRowIter<S: StateStore> {
    keyspace: Keyspace<S>,
    epoch: u64,
    // TODO: some field will be used here to maintain the iter states
}

impl<'a, S: StateStore> RowTableRowIter<S> {
    async fn new(keyspace: Keyspace<S>, columns: Vec<ColumnDesc>, epoch: u64) -> Result<Self> {
        todo!()
    }
    pub async fn next(&mut self) -> Result<Option<Row>> {
        todo!()
    }
}
