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
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::btree_map;
use std::marker::PhantomData;
use std::sync::Arc;

use aws_smithy_http::pin_mut;
use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
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

    pub async fn iter(&self, epoch: u64) -> StorageResult<impl RowStream<'_>> {
        let mem_table_iter = self.mem_table.buffer.iter();
        Ok(StateTableRowIter::into_stream(
            &self.keyspace,
            self.column_descs.clone(),
            mem_table_iter,
            &self.order_types,
            epoch,
        ))
    }
}

pub trait RowStream<'a> = Stream<Item = StorageResult<Cow<'a, Row>>> + 'a;

struct StateTableRowIter<S: StateStore> {
    _phantom: PhantomData<S>,
}

/// `StateTableRowIter` is able to read the just written data (uncommited data).
/// It will merge the result of `mem_table_iter` and `cell_based_streaming_iter`.
impl<S: StateStore> StateTableRowIter<S> {
    /// This function scans kv pairs from the `shared_storage`(`cell_based_table`) and
    /// memory(`mem_table`). If a record exist in both `cell_based_table` and `mem_table`, result
    /// `mem_table` is returned according to the operation(RowOp) on it.
    #[try_stream(ok = Cow<'a, Row>, error = StorageError)]
    async fn into_stream<'a>(
        keyspace: &'a Keyspace<S>,
        table_descs: Vec<ColumnDesc>,
        mem_table_iter: MemTableIter<'a>,
        order_types_vec: &'a [OrderType],
        epoch: u64,
    ) {
        let cell_based_table_iter: futures::stream::Peekable<_> =
            CellBasedTableStreamingIter::new(keyspace, table_descs, epoch)
                .await?
                .into_stream()
                .peekable();
        pin_mut!(cell_based_table_iter);

        let pk_serializer = OrderedRowSerializer::new(order_types_vec.to_vec());
        let mut mem_table_iter = mem_table_iter
            .map(|(k, v)| Ok::<_, StorageError>((serialize_pk(k, &pk_serializer).map_err(err)?, v)))
            .peekable();

        loop {
            match (
                cell_based_table_iter.as_mut().peek().await,
                mem_table_iter.peek(),
            ) {
                (None, None) => break,
                (Some(_), None) => {
                    let row: Row = cell_based_table_iter.next().await.unwrap()?.1;
                    yield Cow::Owned(row);
                }
                (None, Some(_)) => {
                    let row_op = mem_table_iter.next().unwrap()?.1;
                    match row_op {
                        RowOp::Insert(row) | RowOp::Update((_, row)) => {
                            yield Cow::Borrowed(row);
                        }
                        _ => {}
                    }
                }

                (
                    Some(Ok((cell_based_pk, cell_based_row))),
                    Some(Ok((mem_table_pk, _mem_table_row_op))),
                ) => {
                    match cell_based_pk.cmp(mem_table_pk) {
                        Ordering::Less => {
                            // cell_based_table_item will be return
                            let row: Row = cell_based_table_iter.next().await.unwrap()?.1;
                            yield Cow::Owned(row);
                        }
                        Ordering::Equal => {
                            // mem_table_item will be return, while both cell_based_streaming_iter
                            // and mem_table_iter need to execute next()
                            // once.
                            let row_op = mem_table_iter.next().unwrap()?.1;
                            match row_op {
                                RowOp::Insert(row) => yield Cow::Borrowed(row),
                                RowOp::Delete(_) => {}
                                RowOp::Update((old_row, new_row)) => {
                                    debug_assert!(old_row == cell_based_row);
                                    yield Cow::Borrowed(new_row);
                                }
                            }
                            let _cell_based_row = cell_based_table_iter.next().await.unwrap()?.1;
                        }
                        Ordering::Greater => {
                            // mem_table_item will be return
                            let row_op = mem_table_iter.next().unwrap()?.1;
                            match row_op {
                                RowOp::Insert(row) => yield Cow::Borrowed(row),
                                RowOp::Delete(_) => {}
                                RowOp::Update(_) => unreachable!(),
                            }
                        }
                    }
                }
                (Some(_), Some(_)) => {
                    // Throw the error.
                    let _ = cell_based_table_iter.next().await.unwrap()?;
                    let _ = mem_table_iter.next().unwrap()?;

                    unreachable!()
                }
            }
        }
    }
}
type MemTableIter<'a> = btree_map::Iter<'a, Row, RowOp>;

fn err(rw: impl Into<RwError>) -> StorageError {
    StorageError::StateTable(rw.into())
}
