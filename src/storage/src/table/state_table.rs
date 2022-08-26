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
use std::sync::Arc;

use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::Row;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, TableId};
use risingwave_common::util::ordered::OrderedRowSerializer;
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::key::range_of_prefix;
use risingwave_pb::catalog::Table;

use super::mem_table::{MemTable, RowOp};
use super::storage_table::{StorageTableBase, READ_WRITE};
use super::Distribution;
use crate::error::{StorageError, StorageResult};
use crate::row_serde::{serialize_pk, RowBasedSerde, RowSerde};
use crate::StateStore;

/// `RowBasedStateTable` is the interface accessing relational data in KV(`StateStore`) with
/// row-based encoding.
pub type RowBasedStateTable<S> = StateTableBase<S, RowBasedSerde>;
/// `StateTableBase` is the interface accessing relational data in KV(`StateStore`) with
/// encoding, using `RowSerde` for row to KV entries.
#[derive(Clone)]
pub struct StateTableBase<S: StateStore, RS: RowSerde> {
    /// buffer row operations.
    mem_table: MemTable,

    /// write into state store.
    storage_table: StorageTableBase<S, RS, READ_WRITE>,
}

impl<S: StateStore, RS: RowSerde> StateTableBase<S, RS> {
    /// Create a state table without distribution, used for singleton executors and unit tests.
    pub fn new_without_distribution(
        store: S,
        table_id: TableId,
        columns: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
    ) -> Self {
        Self::new_with_distribution(
            store,
            table_id,
            columns,
            order_types,
            pk_indices,
            Distribution::fallback(),
        )
    }

    /// Create a state table with distribution specified with `distribution`. Should use
    /// `Distribution::fallback()` for singleton executors and tests.
    pub fn new_with_distribution(
        store: S,
        table_id: TableId,
        columns: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        distribution: Distribution,
    ) -> Self {
        Self {
            mem_table: MemTable::new(),
            storage_table: StorageTableBase::new(
                store,
                table_id,
                columns,
                order_types,
                pk_indices,
                distribution,
            ),
        }
    }

    /// Disable sanity check in this state table. Need revisit and fix behavior for all tables.
    pub fn disable_sanity_check(&mut self) {
        self.storage_table.disable_sanity_check();
    }

    /// Update the vnode bitmap of this state table, used for fragment scaling or migration.
    pub fn update_vnode_bitmap(&mut self, vnode_bitmap: Arc<Bitmap>) {
        assert!(
            !self.is_dirty(),
            "vnode bitmap should only be updated when state table is clean"
        );
        self.storage_table.update_vnode_bitmap(vnode_bitmap);
    }

    /// Get the underlying [` StorageTableBase`]. Should only be used for tests.
    pub fn storage_table(&self) -> &StorageTableBase<S, RS, READ_WRITE> {
        &self.storage_table
    }

    fn pk_serializer(&self) -> &OrderedRowSerializer {
        self.storage_table.pk_serializer()
    }

    // TODO: remove, should not be exposed to user
    pub fn pk_indices(&self) -> &[usize] {
        self.storage_table.pk_indices()
    }

    pub fn is_dirty(&self) -> bool {
        self.mem_table.is_dirty()
    }

    /// Get a single row from state table. This function will return a Cow. If the value is from
    /// memtable, it will be a [`Cow::Borrowed`]. If is from storage table, it will be an owned
    /// value. To convert `Option<Cow<Row>>` to `Option<Row>`, just call `into_owned`.
    pub async fn get_row<'a>(
        &'a self,
        pk: &'a Row,
        epoch: u64,
    ) -> StorageResult<Option<Cow<'a, Row>>> {
        let pk_bytes = serialize_pk(pk, self.pk_serializer());
        let mem_table_res = self.mem_table.get_row_op(&pk_bytes);
        match mem_table_res {
            Some(row_op) => match row_op {
                RowOp::Insert(row) => Ok(Some(Cow::Borrowed(row))),
                RowOp::Delete(_) => Ok(None),
                RowOp::Update((_, row)) => Ok(Some(Cow::Borrowed(row))),
            },
            None => Ok(self.storage_table.get_row(pk, epoch).await?.map(Cow::Owned)),
        }
    }

    pub async fn get_owned_row(&self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        Ok(self.get_row(pk, epoch).await?.map(|r| r.into_owned()))
    }

    /// Insert a row into state table. Must provide a full row corresponding to the column desc of
    /// the table.
    pub fn insert(&mut self, value: Row) -> StorageResult<()> {
        let pk = value.by_indices(self.pk_indices());
        let pk_bytes = serialize_pk(&pk, self.pk_serializer());
        self.mem_table.insert(pk_bytes, value);
        Ok(())
    }

    /// Delete a row from state table. Must provide a full row of old value corresponding to the
    /// column desc of the table.
    pub fn delete(&mut self, old_value: Row) -> StorageResult<()> {
        let pk = old_value.by_indices(self.pk_indices());
        let pk_bytes = serialize_pk(&pk, self.pk_serializer());
        self.mem_table.delete(pk_bytes, old_value);
        Ok(())
    }

    /// Update a row. The old and new value should have the same pk.
    pub fn update(&mut self, old_value: Row, new_value: Row) -> StorageResult<()> {
        let pk = old_value.by_indices(self.pk_indices());
        debug_assert_eq!(pk, new_value.by_indices(self.pk_indices()));
        let pk_bytes = serialize_pk(&pk, self.pk_serializer());
        self.mem_table.update(pk_bytes, old_value, new_value);
        Ok(())
    }

    pub async fn commit(&mut self, new_epoch: u64) -> StorageResult<()> {
        let mem_table = std::mem::take(&mut self.mem_table).into_parts();
        self.storage_table
            .batch_write_rows(mem_table, new_epoch)
            .await?;
        Ok(())
    }
}

/// Iterator functions.
impl<S: StateStore, RS: RowSerde> StateTableBase<S, RS> {
    /// This function scans rows from the relational table.
    pub async fn iter(&self, epoch: u64) -> StorageResult<RowStream<'_, S, RS>> {
        self.iter_with_pk_prefix(Row::empty(), epoch).await
    }

    /// This function scans rows from the relational table with specific `pk_prefix`.
    pub async fn iter_with_pk_prefix<'a>(
        &'a self,
        pk_prefix: &'a Row,
        epoch: u64,
    ) -> StorageResult<RowStream<'a, S, RS>> {
        let storage_table_iter = self
            .storage_table
            .streaming_iter_with_pk_bounds(epoch, pk_prefix, ..)
            .await?;

        let mem_table_iter = {
            // TODO: reuse calculated serialized key from cell-based table.
            let prefix_serializer = self.pk_serializer().prefix(pk_prefix.size());
            let encoded_prefix = serialize_pk(pk_prefix, &prefix_serializer);
            let encoded_key_range = range_of_prefix(&encoded_prefix);
            self.mem_table.iter(encoded_key_range)
        };

        Ok(StateTableRowIter::new(mem_table_iter, storage_table_iter).into_stream())
    }

    /// Create state table from table catalog and store.
    pub fn from_table_catalog(
        table_catalog: &Table,
        store: S,
        vnodes: Option<Arc<Bitmap>>,
    ) -> Self {
        Self {
            mem_table: MemTable::new(),
            storage_table: StorageTableBase::from_table_catalog(table_catalog, store, vnodes),
        }
    }
}

pub type RowStream<'a, S: StateStore, RS: RowSerde> =
    impl Stream<Item = StorageResult<Cow<'a, Row>>>;
pub type RowBasedRowStream<'a, S> = RowStream<'a, S, RowBasedSerde>;

struct StateTableRowIter<'a, M, C> {
    mem_table_iter: M,
    storage_table_iter: C,
    _phantom: PhantomData<&'a ()>,
}

/// `StateTableRowIter` is able to read the just written data (uncommited data).
/// It will merge the result of `mem_table_iter` and `storage_streaming_iter`.
impl<'a, M, C> StateTableRowIter<'a, M, C>
where
    M: Iterator<Item = (&'a Vec<u8>, &'a RowOp)>,
    C: Stream<Item = StorageResult<(Vec<u8>, Row)>>,
{
    fn new(mem_table_iter: M, storage_table_iter: C) -> Self {
        Self {
            mem_table_iter,
            storage_table_iter,
            _phantom: PhantomData,
        }
    }

    /// This function scans kv pairs from the `shared_storage`(`storage_table`) and
    /// memory(`mem_table`) with optional pk_bounds. If pk_bounds is
    /// (Included(prefix),Excluded(next_key(prefix))), all kv pairs within corresponding prefix will
    /// be scanned. If a record exist in both `storage_table` and `mem_table`, result
    /// `mem_table` is returned according to the operation(RowOp) on it.
    #[try_stream(ok = Cow<'a, Row>, error = StorageError)]
    async fn into_stream(self) {
        let storage_table_iter = self.storage_table_iter.fuse().peekable();
        pin_mut!(storage_table_iter);

        let mut mem_table_iter = self.mem_table_iter.fuse().peekable();

        loop {
            match (
                storage_table_iter.as_mut().peek().await,
                mem_table_iter.peek(),
            ) {
                (None, None) => break,
                // The mem table side has come to an end, return data from the shared storage.
                (Some(_), None) => {
                    let (_, row) = storage_table_iter.next().await.unwrap()?;
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
                (Some(Ok((storage_pk, _))), Some((mem_table_pk, _))) => {
                    match storage_pk.cmp(mem_table_pk) {
                        Ordering::Less => {
                            // yield data from storage table
                            let (_, row) = storage_table_iter.next().await.unwrap()?;
                            yield Cow::Owned(row);
                        }
                        Ordering::Equal => {
                            // both memtable and storage contain the key, so we advance both
                            // iterators and return the data in memory.
                            let (_, row_op) = mem_table_iter.next().unwrap();
                            let (_, old_row_in_storage) =
                                storage_table_iter.next().await.unwrap()?;
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
                    return Err(storage_table_iter.next().await.unwrap().unwrap_err());
                }
            }
        }
    }
}
