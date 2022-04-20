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
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::array::Row;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;

use super::cell_based_table::{CellBasedTable, CellBasedTableRowIter};
use super::mem_table::MemTable;
use crate::cell_based_row_deserializer::CellBasedRowDeserializer;
use crate::error::StorageResult;
use crate::monitor::StateStoreMetrics;
use crate::{Keyspace, StateStore};

/// `CellBasedTable` is the interface accessing relational data in KV(`StateStore`) with encoding
/// format: [keyspace | pk | `column_id` (4B)] -> value.
/// if the key of the column id does not exist, it will be Null in the relation
pub struct StateTable<S: StateStore> {
    keyspace: Keyspace<S>,

    /// `ColumnDesc` contains strictly more info than `schema`.
    column_descs: Vec<ColumnDesc>,

    /// Ordering of primary key (for assertion)
    order_types: Vec<OrderType>,

    /// Serializer to serialize keys from input rows
    key_serializer: OrderedRowSerializer,

    /// buffer key/values
    mem_table: MemTable,

    /// Relation layer
    cell_based_table: CellBasedTable<S>,
}
impl<S: StateStore> StateTable<S> {
    pub fn new(
        keyspace: Keyspace<S>,
        column_descs: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
    ) -> Self {
        let cell_based_keyspace = keyspace.clone();
        let cell_based_column_descs = column_descs.clone();
        Self {
            keyspace,
            column_descs,
            order_types: order_types.clone(),
            key_serializer: OrderedRowSerializer::new(order_types),
            mem_table: MemTable::new(),
            cell_based_table: CellBasedTable::new_adhoc(
                cell_based_keyspace,
                cell_based_column_descs,
                Arc::new(StateStoreMetrics::unused()),
            ),
        }
    }

    /// read methods
    pub async fn get_row(&self, _pk: &Row, _epoch: u64) -> StorageResult<Option<Row>> {
        todo!()
    }

    pub async fn get_row_by_scan(&self, _pk: &Row, _epoch: u64) -> StorageResult<Option<Row>> {
        todo!()
    }

    /// write methods
    pub async fn insert(&mut self, _pk: Row, _value: Row) -> StorageResult<()> {
        todo!()
    }

    pub async fn delete(&mut self, _pk: Row, _old_value: Row) -> StorageResult<()> {
        todo!()
    }

    pub async fn update(
        &mut self,
        _pk: Row,
        _old_value: Row,
        _new_value: Row,
    ) -> StorageResult<()> {
        todo!()
    }

    fn commit(&mut self, _new_epoch: u64) -> StorageResult<()> {
        todo!()
    }

    pub async fn iter(&self, _pk: Row) -> StorageResult<StateTableRowIter<S>> {
        todo!()
    }
}

pub struct StateTableRowIter<S: StateStore> {
    cell_based_iter: CellBasedTableRowIter<S>,
    mem_table: MemTable,
}

impl<S: StateStore> StateTableRowIter<S> {
    async fn new(
        _keyspace: Keyspace<S>,
        _table_descs: Vec<ColumnDesc>,
        _epoch: u64,
        _stats: Arc<StateStoreMetrics>,
    ) -> StorageResult<Self> {
        todo!()
    }

    async fn next(&mut self) -> StorageResult<Option<Row>> {
        todo!()
    }
}
