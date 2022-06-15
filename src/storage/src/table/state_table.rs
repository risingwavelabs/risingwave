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
use std::marker::PhantomData;
use std::ops::{Index, RangeBounds};
use std::sync::Arc;

use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::Row;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::util::ordered::{serialize_pk, OrderedRowSerializer};
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::key::range_of_prefix;

use super::cell_based_table::{CellBasedTable, CellBasedTableStreamingIter};
use super::mem_table::{MemTable, RowOp};
use crate::cell_based_row_deserializer::{make_column_desc_index, ColumnDescMapping};
use crate::error::{StorageError, StorageResult};
use crate::{Keyspace, StateStore};

/// `StateTable` is the interface accessing relational data in KV(`StateStore`) with encoding.
#[derive(Clone)]
pub struct StateTable<S: StateStore> {
    keyspace: Keyspace<S>,
    column_mapping: Arc<ColumnDescMapping>,

    /// buffer key/values
    mem_table: MemTable,

    /// Relation layer
    cell_based_table: CellBasedTable<S>,

    pk_indices: Vec<usize>,
}

impl<S: StateStore> StateTable<S> {
    pub fn new(
        keyspace: Keyspace<S>,
        column_descs: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        dist_key_indices: Option<Vec<usize>>,
        pk_indices: Vec<usize>,
    ) -> Self {
        let cell_based_keyspace = keyspace.clone();
        let cell_based_column_descs = column_descs.clone();
        Self {
            keyspace,
            column_mapping: Arc::new(make_column_desc_index(column_descs)),
            mem_table: MemTable::new(),
            cell_based_table: CellBasedTable::new(
                cell_based_keyspace,
                cell_based_column_descs,
                Some(OrderedRowSerializer::new(order_types)),
                dist_key_indices,
            ),
            pk_indices,
        }
    }

    pub fn get_pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }

    pub fn get_mem_table(&self) -> &MemTable {
        &self.mem_table
    }

    /// read methods
    pub async fn get_row(&self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        // TODO: change to Cow to avoid unnecessary clone.
        let pk_bytes = serialize_pk(pk, self.cell_based_table.pk_serializer.as_ref().unwrap());
        let mem_table_res = self.mem_table.get_row(&pk_bytes)?;
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
    pub fn insert(&mut self, value: Row) -> StorageResult<()> {
        let mut datums = vec![];
        for pk_index in &self.pk_indices {
            datums.push(value.index(*pk_index).clone());
        }
        let pk = Row::new(datums);
        let pk_bytes = serialize_pk(&pk, self.cell_based_table.pk_serializer.as_ref().unwrap());
        self.mem_table.insert(pk_bytes, value)?;
        Ok(())
    }

    pub fn delete(&mut self, old_value: Row) -> StorageResult<()> {
        let mut datums = vec![];
        for pk_index in &self.pk_indices {
            datums.push(old_value.index(*pk_index).clone());
        }
        let pk = Row::new(datums);
        let pk_bytes = serialize_pk(&pk, self.cell_based_table.pk_serializer.as_ref().unwrap());
        self.mem_table.delete(pk_bytes, old_value)?;
        Ok(())
    }

    /// Update a row. The old and new value should have the same pk.
    pub fn update(&mut self, old_value: Row, new_value: Row) -> StorageResult<()> {
        let pk = old_value.by_indices(&self.pk_indices);
        debug_assert_eq!(pk, new_value.by_indices(&self.pk_indices));
        let pk_bytes = serialize_pk(&pk, self.cell_based_table.pk_serializer.as_ref().unwrap());
        self.mem_table.update(pk_bytes, old_value, new_value)?;
        Ok(())
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
}

/// Iterator functions.
impl<S: StateStore> StateTable<S> {
    async fn iter_with_encoded_key_bounds<R>(
        &self,
        encoded_key_bounds: R,
        epoch: u64,
    ) -> StorageResult<impl RowStream<'_>>
    where
        R: RangeBounds<Vec<u8>> + Send + Clone + 'static,
    {
        let cell_based_table_iter = CellBasedTableStreamingIter::new_with_bounds(
            &self.keyspace,
            self.column_mapping.clone(),
            encoded_key_bounds.clone(),
            epoch,
        )
        .await?
        .into_stream();

        let mem_table_iter = self.mem_table.buffer.range(encoded_key_bounds);

        Ok(StateTableRowIter::new(mem_table_iter, cell_based_table_iter).into_stream())
    }

    /// This function scans rows from the relational table.
    pub async fn iter(&self, epoch: u64) -> StorageResult<impl RowStream<'_>> {
        self.iter_with_pk_bounds::<_, Row>(.., epoch).await
    }

    /// This function scans rows from the relational table with specific `pk_bounds`.
    pub async fn iter_with_pk_bounds<R, B>(
        &self,
        pk_bounds: R,
        epoch: u64,
    ) -> StorageResult<impl RowStream<'_>>
    where
        R: RangeBounds<B> + Send + Clone + 'static,
        B: AsRef<Row> + Send + Clone + 'static,
    {
        let pk_serializer = self.cell_based_table.pk_serializer.as_ref();

        let encoded_start_key = pk_bounds
            .start_bound()
            .map(|pk| serialize_pk(pk.as_ref(), pk_serializer.unwrap()));
        let encoded_end_key = pk_bounds
            .end_bound()
            .map(|pk| serialize_pk(pk.as_ref(), pk_serializer.unwrap()));
        let encoded_key_bounds = (encoded_start_key, encoded_end_key);

        self.iter_with_encoded_key_bounds(encoded_key_bounds, epoch)
            .await
    }

    /// This function scans rows from the relational table with specific `pk_prefix`.
    pub async fn iter_with_pk_prefix(
        &self,
        pk_prefix: &Row,
        epoch: u64,
    ) -> StorageResult<impl RowStream<'_>> {
        let pk_serializer = self
                .cell_based_table
                .pk_serializer
                .clone()
                .expect("pk_serializer is None");
            let order_types = &pk_serializer.into_order_types()[0..pk_prefix.size()];
            let prefix_serializer = OrderedRowSerializer::new(order_types.into());
        let encoded_start_key = serialize_pk(pk_prefix, &prefix_serializer);
        let encoded_key_bounds = range_of_prefix(&encoded_start_key);

        self.iter_with_encoded_key_bounds(encoded_key_bounds, epoch)
            .await
    }
}

pub trait RowStream<'a> = Stream<Item = StorageResult<Cow<'a, Row>>>;

struct StateTableRowIter<'a, M, C> {
    mem_table_iter: M,
    cell_based_table_iter: C,
    _phantom: PhantomData<&'a ()>,
}

/// `StateTableRowIter` is able to read the just written data (uncommited data).
/// It will merge the result of `mem_table_iter` and `cell_based_streaming_iter`.
impl<'a, M, C> StateTableRowIter<'a, M, C>
where
    M: Iterator<Item = (&'a Vec<u8>, &'a RowOp)>,
    C: Stream<Item = StorageResult<(Vec<u8>, Row)>>,
{
    fn new(mem_table_iter: M, cell_based_table_iter: C) -> Self {
        Self {
            mem_table_iter,
            cell_based_table_iter,
            _phantom: PhantomData,
        }
    }

    /// This function scans kv pairs from the `shared_storage`(`cell_based_table`) and
    /// memory(`mem_table`) with optional pk_bounds. If pk_bounds is
    /// (Included(prefix),Excluded(next_key(prefix))), all kv pairs within corresponding prefix will
    /// be scanned. If a record exist in both `cell_based_table` and `mem_table`, result
    /// `mem_table` is returned according to the operation(RowOp) on it.
    #[try_stream(ok = Cow<'a, Row>, error = StorageError)]
    async fn into_stream(self) {
        let cell_based_table_iter = self.cell_based_table_iter.peekable();
        pin_mut!(cell_based_table_iter);

        let mut mem_table_iter = self.mem_table_iter.peekable();

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
                    let row_op = mem_table_iter.next().unwrap().1;
                    match row_op {
                        RowOp::Insert(row) | RowOp::Update((_, row)) => {
                            yield Cow::Borrowed(row);
                        }
                        _ => {}
                    }
                }

                (
                    Some(Ok((cell_based_pk, cell_based_row))),
                    Some((mem_table_pk, _mem_table_row_op)),
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
                            let row_op = mem_table_iter.next().unwrap().1;
                            match row_op {
                                RowOp::Insert(row) => {
                                    yield Cow::Borrowed(row);
                                }
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
                            let row_op = mem_table_iter.next().unwrap().1;
                            match row_op {
                                RowOp::Insert(row) => {
                                    yield Cow::Borrowed(row);
                                }
                                RowOp::Delete(_) => {}
                                RowOp::Update(_) => unreachable!(),
                            }
                        }
                    }
                }
                (Some(Err(_)), Some(_)) => {
                    // Throw the error.
                    cell_based_table_iter.next().await.unwrap()?;

                    unreachable!()
                }
            }
        }
    }
}
