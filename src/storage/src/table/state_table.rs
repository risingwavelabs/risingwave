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
use std::cmp::Ordering;
use std::collections::btree_map;
use std::iter::Peekable;
use std::sync::Arc;

use risingwave_common::array::Row;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::RwError;
use risingwave_common::util::ordered::{serialize_pk, OrderedRowSerializer};
use risingwave_common::util::sort_util::OrderType;

use super::cell_based_table::{CellBasedTable, CellBasedTableStreamingIter};
use super::mem_table::{MemTable, RowOp};
use crate::error::{StorageError, StorageResult};
use crate::monitor::StateStoreMetrics;
use crate::{Keyspace, StateStore};

/// `StateTable` is the interface accessing relational data in KV(`StateStore`) with encoding
#[derive(Clone)]
pub struct StateTable<S: StateStore> {
    keyspace: Keyspace<S>,

    column_descs: Vec<ColumnDesc>,
    // /// Ordering of primary key (for assertion)
    order_types: Vec<OrderType>,

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
        dist_key_indices: Option<Vec<usize>>,
    ) -> Self {
        let cell_based_keyspace = keyspace.clone();
        let cell_based_column_descs = column_descs.clone();
        Self {
            keyspace,
            column_descs,
            order_types: order_types.clone(),
            mem_table: MemTable::new(),
            cell_based_table: CellBasedTable::new(
                cell_based_keyspace,
                cell_based_column_descs,
                Some(OrderedRowSerializer::new(order_types)),
                Arc::new(StateStoreMetrics::unused()),
                dist_key_indices,
            ),
        }
    }

    /// read methods
    pub async fn get_row(&self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        let mem_table_res = self.mem_table.get_row(pk).map_err(err)?;
        match mem_table_res {
            Some(row_op) => match row_op {
                RowOp::Insert(row) => Ok(Some(row.clone())),
                RowOp::Delete(_) => Ok(None),
                RowOp::Update((_, new_row)) => Ok(Some(new_row.clone())),
            },
            None => self.cell_based_table.get_row(pk, epoch).await,
        }
    }

    /// write methods
    pub fn insert(&mut self, pk: Row, value: Row) -> StorageResult<()> {
        assert_eq!(self.order_types.len(), pk.size());
        self.mem_table.insert(pk, value)?;
        Ok(())
    }

    pub fn delete(&mut self, pk: Row, old_value: Row) -> StorageResult<()> {
        assert_eq!(self.order_types.len(), pk.size());
        self.mem_table.delete(pk, old_value)?;
        Ok(())
    }

    pub fn update(&mut self, _pk: Row, _old_value: Row, _new_value: Row) -> StorageResult<()> {
        todo!()
    }

    pub async fn commit(&mut self, new_epoch: u64) -> StorageResult<()> {
        let mem_table = std::mem::take(&mut self.mem_table).into_parts();
        self.cell_based_table
            .batch_write_rows(mem_table, new_epoch)
            .await?;
        Ok(())
    }

    pub async fn commit_with_value_meta(&mut self, new_epoch: u64) -> StorageResult<()> {
        let mem_table = std::mem::take(&mut self.mem_table).into_parts();
        self.cell_based_table
            .batch_write_rows_with_value_meta(mem_table, new_epoch)
            .await?;
        Ok(())
    }

    pub async fn iter(&self, epoch: u64) -> StorageResult<StateTableRowIter<'_, S>> {
        let mem_table_iter = self.mem_table.buffer.iter().peekable();
        StateTableRowIter::new(
            &self.keyspace,
            self.column_descs.clone(),
            mem_table_iter,
            &self.order_types,
            epoch,
        )
        .await
    }
}

/// `StateTableRowIter` is able to read the just written data (uncommited data).
/// It will merge the result of `mem_table_iter` and `cell_based_streaming_iter`
pub struct StateTableRowIter<'a, S: StateStore> {
    mem_table_iter: Peekable<MemTableIter<'a>>,
    cell_based_streaming_iter: CellBasedTableStreamingIter<S>,

    /// The result of the last cell_based_streaming_iter next is saved, which can avoid being
    /// discarded.
    cell_based_item: Option<(Vec<u8>, Row)>,
    pk_serializer: OrderedRowSerializer,
}
type MemTableIter<'a> = btree_map::Iter<'a, Row, RowOp>;

enum NextOutcome {
    MemTable,
    Storage,
    Both,
    End,
}
impl<'a, S: StateStore> StateTableRowIter<'a, S> {
    async fn new(
        keyspace: &Keyspace<S>,
        table_descs: Vec<ColumnDesc>,
        mem_table_iter: Peekable<MemTableIter<'a>>,
        order_types_vec: &[OrderType],
        epoch: u64,
    ) -> StorageResult<StateTableRowIter<'a, S>> {
        let mut cell_based_streaming_iter =
            CellBasedTableStreamingIter::new(keyspace, table_descs, epoch).await?;
        let pk_serializer = OrderedRowSerializer::new(order_types_vec.to_vec());
        let cell_based_item = cell_based_streaming_iter.next().await.map_err(err)?;
        let state_table_iter = Self {
            mem_table_iter,
            cell_based_streaming_iter,
            cell_based_item,
            pk_serializer,
        };

        Ok(state_table_iter)
    }

    pub async fn next(&mut self) -> StorageResult<Option<Row>> {
        loop {
            let next_flag;
            let res;
            let cell_based_item = self.cell_based_item.take();
            match (cell_based_item, self.mem_table_iter.peek()) {
                (None, None) => {
                    next_flag = NextOutcome::End;
                    res = None;
                }
                (Some((_, row)), None) => {
                    res = Some(row);
                    next_flag = NextOutcome::Storage;
                }
                (None, Some((_, row_op))) => {
                    next_flag = NextOutcome::MemTable;
                    match row_op {
                        RowOp::Insert(row) => {
                            res = Some(row.clone());
                        }
                        RowOp::Delete(_) => {
                            res = None;
                        }
                        RowOp::Update((_, new_row)) => {
                            res = Some(new_row.clone());
                        }
                    }
                }
                (Some((cell_based_pk, cell_based_row)), Some((mem_table_pk, mem_table_row_op))) => {
                    let mem_table_pk_bytes =
                        serialize_pk(mem_table_pk, &self.pk_serializer).map_err(err)?;
                    match cell_based_pk.cmp(&mem_table_pk_bytes) {
                        Ordering::Less => {
                            // cell_based_table_item will be return
                            res = Some(cell_based_row);
                            next_flag = NextOutcome::Storage;
                        }
                        Ordering::Equal => {
                            // mem_table_item will be return, while both cell_based_streaming_iter
                            // and mem_table_iter need to execute next()
                            // once.
                            next_flag = NextOutcome::Both;
                            match mem_table_row_op {
                                RowOp::Insert(row) => {
                                    res = Some(row.clone());
                                }
                                RowOp::Delete(_) => {
                                    res = None;
                                }
                                RowOp::Update((old_row, new_row)) => {
                                    debug_assert!(old_row == &cell_based_row);
                                    res = Some(new_row.clone());
                                }
                            }
                        }
                        Ordering::Greater => {
                            // mem_table_item will be return
                            next_flag = NextOutcome::MemTable;
                            match mem_table_row_op {
                                RowOp::Insert(row) => {
                                    res = Some(row.clone());
                                }
                                RowOp::Delete(_) => {
                                    res = None;
                                }
                                RowOp::Update(_) => {
                                    panic!(
                                    "There must be a record in shared storage, so this case is unreachable.",
                                );
                                }
                            }
                            self.cell_based_item = Some((cell_based_pk, cell_based_row));
                        }
                    }
                }
            }

            // If a pk exist in both shared storage(cell_based_table) and memory(mem_table), and
            // mem_table stores a delete record, state table iter need to next again.
            match next_flag {
                NextOutcome::MemTable => {
                    self.mem_table_iter.next();
                    if res.is_some() {
                        return Ok(res);
                    }
                }
                NextOutcome::Storage => {
                    self.cell_based_item =
                        self.cell_based_streaming_iter.next().await.map_err(err)?;

                    if res.is_some() {
                        return Ok(res);
                    }
                }
                NextOutcome::Both => {
                    self.mem_table_iter.next();
                    self.cell_based_item =
                        self.cell_based_streaming_iter.next().await.map_err(err)?;
                    if res.is_some() {
                        return Ok(res);
                    }
                }
                NextOutcome::End => {
                    return Ok(res);
                }
            }
        }
    }
}

fn err(rw: impl Into<RwError>) -> StorageError {
    StorageError::StateTable(rw.into())
}
