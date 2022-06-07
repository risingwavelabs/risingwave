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
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::Arc;

use futures::{pin_mut, Stream, StreamExt};
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

/// `StateTable` is the interface accessing relational data in KV(`StateStore`) with encoding.
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

    _pk_indices: Vec<usize>,
}
impl<S: StateStore> StateTable<S> {
    pub fn new(
        keyspace: Keyspace<S>,
        column_descs: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        dist_key_indices: Option<Vec<usize>>,
        _pk_indices: Vec<usize>,
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
            _pk_indices,
        }
    }

    /// read methods
    pub async fn get_row(&self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        let pk_bytes =
            serialize_pk(pk, self.cell_based_table.pk_serializer.as_ref().unwrap()).map_err(err)?;
        let mem_table_res = self.mem_table.get_row(&pk_bytes).map_err(err)?;
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
    pub fn insert(&mut self, pk: &Row, value: Row) -> StorageResult<()> {
        assert_eq!(self.order_types.len(), pk.size());
        let pk_bytes =
            serialize_pk(pk, self.cell_based_table.pk_serializer.as_ref().unwrap()).map_err(err)?;
        self.mem_table.insert(pk_bytes, value)?;
        Ok(())
    }

    pub fn delete(&mut self, pk: &Row, old_value: Row) -> StorageResult<()> {
        assert_eq!(self.order_types.len(), pk.size());
        let pk_bytes =
            serialize_pk(pk, self.cell_based_table.pk_serializer.as_ref().unwrap()).map_err(err)?;
        self.mem_table.delete(pk_bytes, old_value)?;
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
            epoch,
        ))
    }

    pub async fn iter_with_bounds<R, B>(
        &self,
        pk_bounds: R,
        epoch: u64,
    ) -> StorageResult<impl RowStream<'_>>
    where
        R: RangeBounds<B> + Send + Clone + 'static,
        B: AsRef<Row> + Send + Clone + 'static,
    {
        let mem_table_iter = self.mem_table.buffer.iter();
        Ok(StateTableRowIter::into_stream_with_bounds(
            &self.keyspace,
            self.column_descs.clone(),
            mem_table_iter,
            pk_bounds,
            self.cell_based_table.pk_serializer.clone(),
            epoch,
        ))
    }

    pub async fn iter_with_prefix(
        &self,
        pk_prifix: Row,
        prifix_serializer: OrderedRowSerializer,
        epoch: u64,
    ) -> StorageResult<impl RowStream<'_>> {
        let mem_table_iter = self.mem_table.buffer.iter();
        Ok(StateTableRowIter::into_stream_with_prefix(
            &self.keyspace,
            self.column_descs.clone(),
            mem_table_iter,
            pk_prifix,
            prifix_serializer,
            epoch,
        ))
    }
}

pub trait RowStream<'a> = Stream<Item = StorageResult<Cow<'a, Row>>> + 'a;

type MemTableIter<'a> = btree_map::Iter<'a, Vec<u8>, RowOp>;

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
        epoch: u64,
    ) {
        let cell_based_table_iter: futures::stream::Peekable<_> =
            CellBasedTableStreamingIter::new(keyspace, table_descs, epoch)
                .await?
                .into_stream()
                .peekable();
        pin_mut!(cell_based_table_iter);

        let mut mem_table_iter = mem_table_iter
            .map(|(k, v)| Ok::<_, StorageError>((k, v)))
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
                            cell_based_table_iter.next().await.unwrap()?;
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
                    cell_based_table_iter.next().await.unwrap()?;
                    mem_table_iter.next().unwrap()?;

                    unreachable!()
                }
            }
        }
    }

    #[try_stream(ok = Cow<'a, Row>, error = StorageError)]
    async fn into_stream_with_bounds<'a, R, B>(
        keyspace: &'a Keyspace<S>,
        table_descs: Vec<ColumnDesc>,
        mem_table_iter: MemTableIter<'a>,
        pk_bounds: R,
        pk_serializer: Option<OrderedRowSerializer>,
        epoch: u64,
    ) where
        R: RangeBounds<B> + Send + Clone + 'static,
        B: AsRef<Row> + Send + Clone + 'static,
    {
        let cell_based_table_iter: futures::stream::Peekable<_> =
            CellBasedTableStreamingIter::new_with_bounds(
                keyspace,
                table_descs,
                pk_bounds.clone(),
                pk_serializer.clone(),
                epoch,
            )
            .await?
            .into_stream()
            .peekable();
        pin_mut!(cell_based_table_iter);

        let mut mem_table_iter = mem_table_iter
            .map(|(k, v)| Ok::<_, StorageError>((k, v)))
            .peekable();
        let pk_serializer = pk_serializer.as_ref().expect("pk_serializer is None");

        loop {
            let start_key = match pk_bounds.start_bound() {
                Included(k) => Included(serialize_pk(k.as_ref(), pk_serializer).map_err(err)?),
                Excluded(k) => Excluded(serialize_pk(k.as_ref(), pk_serializer).map_err(err)?),
                Unbounded => Unbounded,
            };
            let end_key = match pk_bounds.end_bound() {
                Included(k) => Included(serialize_pk(k.as_ref(), pk_serializer).map_err(err)?),
                Excluded(k) => Excluded(serialize_pk(k.as_ref(), pk_serializer).map_err(err)?),
                Unbounded => Unbounded,
            };
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
                    let (mem_table_pk, row_op) = mem_table_iter.next().unwrap()?;

                    if (start_key, end_key).contains(mem_table_pk) {
                        match row_op {
                            RowOp::Insert(row) | RowOp::Update((_, row)) => {
                                yield Cow::Borrowed(row);
                            }
                            _ => {}
                        }
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
                            let (mem_table_pk, row_op) = mem_table_iter.next().unwrap()?;
                            if (start_key, end_key).contains(mem_table_pk) {
                                match row_op {
                                    RowOp::Insert(row) => yield Cow::Borrowed(row),
                                    RowOp::Delete(_) => {}
                                    RowOp::Update((old_row, new_row)) => {
                                        debug_assert!(old_row == cell_based_row);
                                        yield Cow::Borrowed(new_row);
                                    }
                                }
                            }
                            cell_based_table_iter.next().await.unwrap()?;
                        }
                        Ordering::Greater => {
                            // mem_table_item will be return
                            let (mem_table_pk, row_op) = mem_table_iter.next().unwrap()?;
                            if (start_key, end_key).contains(mem_table_pk) {
                                match row_op {
                                    RowOp::Insert(row) => yield Cow::Borrowed(row),
                                    RowOp::Delete(_) => {}
                                    RowOp::Update(_) => unreachable!(),
                                }
                            }
                        }
                    }
                }
                (Some(_), Some(_)) => {
                    // Throw the error.
                    cell_based_table_iter.next().await.unwrap()?;
                    mem_table_iter.next().unwrap()?;

                    unreachable!()
                }
            }
        }
    }

    #[try_stream(ok = Cow<'a, Row>, error = StorageError)]
    async fn into_stream_with_prefix<'a>(
        keyspace: &'a Keyspace<S>,
        table_descs: Vec<ColumnDesc>,
        mem_table_iter: MemTableIter<'a>,
        pk_prefix: Row,
        prefix_serializer: OrderedRowSerializer,
        epoch: u64,
    ) {
        let cell_based_table_iter: futures::stream::Peekable<_> =
            CellBasedTableStreamingIter::new_with_prefix(
                keyspace,
                table_descs,
                pk_prefix.clone(),
                prefix_serializer.clone(),
                epoch,
            )
            .await?
            .into_stream()
            .peekable();
        pin_mut!(cell_based_table_iter);

        let mut mem_table_iter = mem_table_iter
            .map(|(k, v)| Ok::<_, StorageError>((k, v)))
            .peekable();
        let mut pk_prefix_bytes = vec![];
        prefix_serializer.serialize(&pk_prefix, &mut pk_prefix_bytes);

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
                    let (mem_table_pk, row_op) = mem_table_iter.next().unwrap()?;
                    if mem_table_pk.starts_with(&pk_prefix_bytes) {
                        match row_op {
                            RowOp::Insert(row) | RowOp::Update((_, row)) => {
                                yield Cow::Borrowed(row);
                            }
                            _ => {}
                        }
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
                            let (mem_table_pk, row_op) = mem_table_iter.next().unwrap()?;
                            if mem_table_pk.starts_with(&pk_prefix_bytes) {
                                match row_op {
                                    RowOp::Insert(row) => yield Cow::Borrowed(row),
                                    RowOp::Delete(_) => {}
                                    RowOp::Update((old_row, new_row)) => {
                                        debug_assert!(old_row == cell_based_row);
                                        yield Cow::Borrowed(new_row);
                                    }
                                }
                            }
                            cell_based_table_iter.next().await.unwrap()?;
                        }
                        Ordering::Greater => {
                            // mem_table_item will be return
                            let (mem_table_pk, row_op) = mem_table_iter.next().unwrap()?;
                            if mem_table_pk.starts_with(&pk_prefix_bytes) {
                                match row_op {
                                    RowOp::Insert(row) => yield Cow::Borrowed(row),
                                    RowOp::Delete(_) => {}
                                    RowOp::Update(_) => unreachable!(),
                                }
                            }
                        }
                    }
                }
                (Some(_), Some(_)) => {
                    // Throw the error.
                    cell_based_table_iter.next().await.unwrap()?;
                    mem_table_iter.next().unwrap()?;

                    unreachable!()
                }
            }
        }
    }
}

fn err(rw: impl Into<RwError>) -> StorageError {
    StorageError::StateTable(rw.into())
}
