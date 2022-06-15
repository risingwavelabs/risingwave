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

    /// Serializer for pk
    pk_serializer: OrderedRowSerializer,

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
        let pk_serializer = OrderedRowSerializer::new(order_types.clone());

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
            pk_serializer,
            pk_indices,
        }
    }

    // TODO: remove, should not be exposed to user
    pub fn get_pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }

    // TODO: remove, should not be exposed to user
    pub fn get_mem_table(&self) -> &MemTable {
        &self.mem_table
    }

    /// Get a single row from state table. This function will return a Cow. If the value is from
    /// memtable, it will be a [`Cow::Borrowed`]. If is from cell based table, it will be an owned
    /// value. To convert `Option<Cow<Row>>` to `Option<Row>`, just call `to_owned`.
    pub async fn get_row_ref(&self, pk: &Row, epoch: u64) -> StorageResult<Option<Cow<Row>>> {
        // TODO: change to Cow to avoid unnecessary clone.
        let pk_bytes = serialize_pk(pk, &self.pk_serializer);
        let mem_table_res = self.mem_table.get_row(&pk_bytes)?;
        match mem_table_res {
            Some(row_op) => match row_op {
                RowOp::Insert(row) => Ok(Some(Cow::Borrowed(row))),
                RowOp::Delete(_) => Ok(None),
                RowOp::Update((_, row)) => Ok(Some(Cow::Borrowed(row))),
            },
            None => Ok(self
                .cell_based_table
                .get_row(pk, epoch)
                .await?
                .map(Cow::Owned)),
        }
    }

    pub async fn get_row(&self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        Ok(self.get_row_ref(pk, epoch).await?.map(|r| r.into_owned()))
    }

    /// Insert a row into state table. Must provide a full row corresponding to the column desc of
    /// the table.
    pub fn insert(&mut self, value: Row) -> StorageResult<()> {
        let mut datums = vec![];
        for pk_index in &self.pk_indices {
            datums.push(value.index(*pk_index).clone());
        }
        let pk = Row::new(datums);
        let pk_bytes = serialize_pk(&pk, &self.pk_serializer);
        self.mem_table.insert(pk_bytes, value)?;
        Ok(())
    }

    /// Insert a row into state table. Must provide a full row of old value corresponding to the
    /// column desc of the table.
    pub fn delete(&mut self, old_value: Row) -> StorageResult<()> {
        let mut datums = vec![];
        for pk_index in &self.pk_indices {
            datums.push(old_value.index(*pk_index).clone());
        }
        let pk = Row::new(datums);
        let pk_bytes = serialize_pk(&pk, &self.pk_serializer);
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

    /// This function scans rows from the relational table with specific `pk_prefix`.
    pub async fn iter_with_pk_bounds<R, B>(
        &self,
        pk_bounds: R,
        epoch: u64,
    ) -> StorageResult<impl RowStream<'_>>
    where
        R: RangeBounds<B> + Send + Clone + 'static,
        B: AsRef<Row> + Send + Clone + 'static,
    {
        let pk_serializer = &self.pk_serializer;

        let encoded_start_key = pk_bounds
            .start_bound()
            .map(|pk| serialize_pk(pk.as_ref(), pk_serializer));
        let encoded_end_key = pk_bounds
            .end_bound()
            .map(|pk| serialize_pk(pk.as_ref(), pk_serializer));
        let encoded_key_bounds = (encoded_start_key, encoded_end_key);

        self.iter_with_encoded_key_bounds(encoded_key_bounds, epoch)
            .await
    }

    /// This function scans rows from the relational table with specific `pk_prefix`.
    pub async fn iter_with_pk_prefix(
        &self,
        pk_prefix: &Row,
        prefix_serializer: OrderedRowSerializer,
        epoch: u64,
    ) -> StorageResult<impl RowStream<'_>> {
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

        let mut mem_table_iter = self.mem_table_iter.fuse().peekable();

        loop {
            match (
                cell_based_table_iter.as_mut().peek().await,
                mem_table_iter.peek(),
            ) {
                (None, None) => break,
                // The mem table side has come to an end, return data from the shared storage.
                (Some(_), None) => {
                    let (_, row) = cell_based_table_iter.next().await.unwrap()?;
                    yield Cow::Owned(row);
                }
                // The stream side has come to an end, return data from the mem table.
                (None, Some(_)) => {
                    let (_, row_op) = mem_table_iter.next().unwrap();
                    match row_op {
                        RowOp::Insert(row) | RowOp::Update((_, row)) => {
                            yield Cow::Borrowed(row);
                        }
                        _ => {}
                    }
                }
                (Some(Ok((cell_based_pk, _))), Some((mem_table_pk, _))) => {
                    match cell_based_pk.cmp(mem_table_pk) {
                        Ordering::Less => {
                            // yield data from cell based table
                            let (_, row) = cell_based_table_iter.next().await.unwrap()?;
                            yield Cow::Owned(row);
                        }
                        Ordering::Equal => {
                            // both memtable and storage contain the key, so we advance both
                            // iterators and return the data in memory.
                            let (_, row_op) = mem_table_iter.next().unwrap();
                            let (_, old_row_in_storage) =
                                cell_based_table_iter.next().await.unwrap()?;
                            match row_op {
                                RowOp::Insert(row) => {
                                    yield Cow::Borrowed(row);
                                }
                                RowOp::Delete(_) => {}
                                RowOp::Update((old_row, new_row)) => {
                                    debug_assert!(old_row == &old_row_in_storage);
                                    yield Cow::Borrowed(new_row);
                                }
                            }
                        }
                        Ordering::Greater => {
                            // yield data from mem table
                            let (_, row_op) = mem_table_iter.next().unwrap();
                            match row_op {
                                RowOp::Insert(row) => {
                                    yield Cow::Borrowed(row);
                                }
                                RowOp::Delete(_) => {}
                                RowOp::Update(_) => unreachable!(
                                    "memtable update should always be paired with a storage key"
                                ),
                            }
                        }
                    }
                }
                (Some(Err(_)), Some(_)) => {
                    // Throw the error.
                    return Err(cell_based_table_iter.next().await.unwrap().unwrap_err());
                }
            }
        }
    }
}
