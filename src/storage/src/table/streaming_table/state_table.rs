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
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::Bound::{self, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::Arc;

use async_stack_trace::StackTrace;
use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::Row;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, TableId, TableOption};
use risingwave_common::error::RwError;
use risingwave_common::types::{DataType, VirtualNode};
use risingwave_common::util::ordered::OrderedRowSerializer;
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::key::{end_bound_of_prefix, prefixed_range, range_of_prefix};
use risingwave_pb::catalog::Table;
use tracing::trace;

use super::mem_table::{MemTable, RowOp};
use crate::error::{StorageError, StorageResult};
use crate::keyspace::StripPrefixIterator;
use crate::row_serde::row_serde_util::{
    serialize_pk, serialize_pk_with_vnode, serialize_value, streaming_deserialize,
};
use crate::storage_value::StorageValue;
use crate::store::{ReadOptions, WriteOptions};
use crate::table::{compute_vnode, Distribution};
use crate::{Keyspace, StateStore, StateStoreIter};

/// `StateTable` is the interface accessing relational data in KV(`StateStore`) with
/// row-based encoding.
#[derive(Clone)]
pub struct StateTable<S: StateStore> {
    /// buffer row operations.
    mem_table: MemTable,

    /// write into state store.
    keyspace: Keyspace<S>,

    /// All columns of this table. Note that this is different from the output columns in
    /// `mapping.output_columns`.
    table_columns: Vec<ColumnDesc>,

    /// Used for serializing the primary key.
    pk_serializer: OrderedRowSerializer,

    /// Datatypes of each column, used for deserializing the row.
    data_types: Vec<DataType>,

    /// Indices of primary key.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.
    pk_indices: Vec<usize>,

    /// Indices of distribution key for computing vnode.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.
    dist_key_indices: Vec<usize>,

    /// Indices of distribution key for computing vnode.
    /// Note that the index is based on the primary key columns by `pk_indices`.
    dist_key_in_pk_indices: Vec<usize>,

    /// Virtual nodes that the table is partitioned into.
    ///
    /// Only the rows whose vnode of the primary key is in this set will be visible to the
    /// executor. The table will also check whether the written rows
    /// confirm to this partition.
    vnodes: Arc<Bitmap>,

    /// Used for catalog table_properties
    table_option: TableOption,

    /// If true, sanity check is disabled on this table.
    disable_sanity_check: bool,
}

/// init Statetable
impl<S: StateStore> StateTable<S> {
    /// Create state table from table catalog and store.
    pub fn from_table_catalog(
        table_catalog: &Table,
        store: S,
        vnodes: Option<Arc<Bitmap>>,
    ) -> Self {
        let table_id = TableId::new(table_catalog.id);
        let table_columns: Vec<ColumnDesc> = table_catalog
            .columns
            .iter()
            .map(|col| col.column_desc.as_ref().unwrap().into())
            .collect();
        let order_types = table_catalog
            .order_key
            .iter()
            .map(|col_order| {
                OrderType::from_prost(
                    &risingwave_pb::plan_common::OrderType::from_i32(col_order.order_type).unwrap(),
                )
            })
            .collect();
        let dist_key_indices: Vec<usize> = table_catalog
            .distribution_key
            .iter()
            .map(|dist_index| *dist_index as usize)
            .collect();

        let pk_indices = table_catalog
            .order_key
            .iter()
            .map(|col_order| col_order.index as usize)
            .collect_vec();

        let dist_key_in_pk_indices = dist_key_indices
            .iter()
            .map(|&di| {
                pk_indices
                    .iter()
                    .position(|&pi| di == pi)
                    .unwrap_or_else(|| {
                        panic!(
                            "distribution key {:?} must be a subset of primary key {:?}",
                            dist_key_indices, pk_indices
                        )
                    })
            })
            .collect_vec();

        let keyspace = Keyspace::table_root(store, &table_id);
        let pk_serializer = OrderedRowSerializer::new(order_types);

        let data_types = table_columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect_vec();

        let Distribution {
            dist_key_indices,
            vnodes,
        } = match vnodes {
            Some(vnodes) => Distribution {
                dist_key_indices,
                vnodes,
            },
            None => Distribution::fallback(),
        };
        Self {
            mem_table: MemTable::new(),
            keyspace,
            table_columns,
            pk_serializer,
            data_types,
            pk_indices: pk_indices.to_vec(),
            dist_key_indices,
            dist_key_in_pk_indices,
            vnodes,
            table_option: TableOption::build_table_option(table_catalog.get_properties()),
            disable_sanity_check: false,
        }
    }

    /// Create a state table without distribution, used for unit tests.
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
    /// `Distribution::fallback()` for tests.
    pub fn new_with_distribution(
        store: S,
        table_id: TableId,
        table_columns: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        Distribution {
            dist_key_indices,
            vnodes,
        }: Distribution,
    ) -> Self {
        let keyspace = Keyspace::table_root(store, &table_id);

        let pk_serializer = OrderedRowSerializer::new(order_types);
        let data_types = table_columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect_vec();
        let dist_key_in_pk_indices = dist_key_indices
            .iter()
            .map(|&di| {
                pk_indices
                    .iter()
                    .position(|&pi| di == pi)
                    .unwrap_or_else(|| {
                        panic!(
                            "distribution key {:?} must be a subset of primary key {:?}",
                            dist_key_indices, pk_indices
                        )
                    })
            })
            .collect_vec();
        Self {
            mem_table: MemTable::new(),
            keyspace,
            table_columns,
            pk_serializer,
            data_types,
            pk_indices,
            dist_key_indices,
            dist_key_in_pk_indices,
            vnodes,
            table_option: Default::default(),
            disable_sanity_check: false,
        }
    }

    /// Disable sanity check on this storage table.
    pub fn disable_sanity_check(&mut self) {
        self.disable_sanity_check = true;
    }

    /// Get vnode value with given primary key.
    fn compute_vnode_by_pk(&self, pk: &Row) -> VirtualNode {
        compute_vnode(
            pk,
            &self.dist_key_in_pk_indices,
            self.vnodes.clone(),
            &self.table_columns,
            &self.dist_key_indices,
        )
    }

    // TODO: remove, should not be exposed to user
    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }

    pub fn is_dirty(&self) -> bool {
        self.mem_table.is_dirty()
    }

    fn get_read_option(&self, epoch: u64) -> ReadOptions {
        ReadOptions {
            epoch,
            table_id: Some(self.keyspace.table_id()),
            retention_seconds: self.table_option.retention_seconds,
        }
    }
}
const ENABLE_STATE_TABLE_SANITY_CHECK: bool = cfg!(debug_assertions);
/// point get
impl<S: StateStore> StateTable<S> {
    /// Get a single row from state table.
    pub async fn get_row<'a>(&'a self, pk: &'a Row, epoch: u64) -> StorageResult<Option<Row>> {
        let serialized_pk =
            serialize_pk_with_vnode(pk, &self.pk_serializer, &self.compute_vnode_by_pk(pk));
        let mem_table_res = self.mem_table.get_row_op(&serialized_pk);

        let read_options = self.get_read_option(epoch);
        match mem_table_res {
            Some(row_op) => match row_op {
                RowOp::Insert(row_bytes) => {
                    let row =
                        streaming_deserialize(&self.data_types, row_bytes.as_ref()).map_err(err)?;
                    Ok(Some(row))
                }
                RowOp::Delete(_) => Ok(None),
                RowOp::Update((_, row_bytes)) => {
                    let row =
                        streaming_deserialize(&self.data_types, row_bytes.as_ref()).map_err(err)?;
                    Ok(Some(row))
                }
            },
            None => {
                assert!(pk.size() <= self.pk_indices.len());
                let key_indices = (0..pk.size())
                    .into_iter()
                    .map(|index| self.pk_indices[index])
                    .collect_vec();
                if let Some(storage_row_bytes) = self
                    .keyspace
                    .get(
                        &serialized_pk,
                        self.dist_key_indices == key_indices,
                        read_options,
                    )
                    .await?
                {
                    let row = streaming_deserialize(&self.data_types, storage_row_bytes.as_ref())
                        .map_err(err)?;
                    Ok(Some(row))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub fn update_vnode_bitmap(&mut self, new_vnodes: Arc<Bitmap>) {
        assert!(
            !self.is_dirty(),
            "vnode bitmap should only be updated when state table is clean"
        );
        if self.dist_key_indices.is_empty() {
            assert_eq!(
                new_vnodes, self.vnodes,
                "should not update vnode bitmap for singleton table"
            );
        }
        self.vnodes = new_vnodes;
    }
}

// write
impl<S: StateStore> StateTable<S> {
    /// Insert a row into state table. Must provide a full row corresponding to the column desc of
    /// the table.
    pub fn insert(&mut self, value: Row) -> StorageResult<()> {
        let pk = value.by_indices(self.pk_indices());

        let key_bytes =
            serialize_pk_with_vnode(&pk, &self.pk_serializer, &self.compute_vnode_by_pk(&pk));
        let value_bytes = serialize_value(value).map_err(err)?;
        self.mem_table.insert(key_bytes, value_bytes);
        Ok(())
    }

    /// Delete a row from state table. Must provide a full row of old value corresponding to the
    /// column desc of the table.
    pub fn delete(&mut self, old_value: Row) -> StorageResult<()> {
        let pk = old_value.by_indices(self.pk_indices());
        let key_bytes =
            serialize_pk_with_vnode(&pk, &self.pk_serializer, &self.compute_vnode_by_pk(&pk));
        let value_bytes = serialize_value(old_value).map_err(err)?;
        self.mem_table.delete(key_bytes, value_bytes);
        Ok(())
    }

    /// Update a row. The old and new value should have the same pk.
    pub fn update(&mut self, old_value: Row, new_value: Row) -> StorageResult<()> {
        let old_pk = old_value.by_indices(self.pk_indices());
        let new_pk = new_value.by_indices(self.pk_indices());
        debug_assert_eq!(old_pk, new_pk);

        let new_key_bytes = serialize_pk_with_vnode(
            &new_pk,
            &self.pk_serializer,
            &self.compute_vnode_by_pk(&new_pk),
        );

        let old_value_bytes = serialize_value(old_value).map_err(err)?;
        let new_value_bytes = serialize_value(new_value).map_err(err)?;
        self.mem_table
            .update(new_key_bytes, old_value_bytes, new_value_bytes);
        Ok(())
    }

    pub async fn commit(&mut self, new_epoch: u64) -> StorageResult<()> {
        let mem_table = std::mem::take(&mut self.mem_table).into_parts();
        self.batch_write_rows(mem_table, new_epoch).await?;
        Ok(())
    }

    /// Write to state store.
    pub async fn batch_write_rows(
        &mut self,
        buffer: BTreeMap<Vec<u8>, RowOp>,
        epoch: u64,
    ) -> StorageResult<()> {
        let mut local = self.keyspace.start_write_batch(WriteOptions {
            epoch,
            table_id: self.keyspace.table_id(),
        });
        for (pk, row_op) in buffer {
            match row_op {
                RowOp::Insert(row) => {
                    if ENABLE_STATE_TABLE_SANITY_CHECK && !self.disable_sanity_check {
                        // If we want to insert a row, it should not exist in storage.
                        let storage_row = self
                            .keyspace
                            .get(&pk, false, self.get_read_option(epoch))
                            .await?;

                        // It's normal for some executors to fail this assert, you can use
                        // `.disable_sanity_check()` on state table to disable this check.
                        assert!(
                            storage_row.is_none(),
                            "overwriting an existing row:\nin-storage: {:?}\nto-be-written: {:?}",
                            storage_row.unwrap(),
                            row
                        );
                    }
                    local.put(pk, StorageValue::new_default_put(row));
                }
                RowOp::Delete(old_row) => {
                    if ENABLE_STATE_TABLE_SANITY_CHECK && !self.disable_sanity_check {
                        // If we want to delete a row, it should exist in storage, and should
                        // have the same old_value as recorded.
                        let storage_row = self
                            .keyspace
                            .get(&pk, false, self.get_read_option(epoch))
                            .await?;
                        // It's normal for some executors to fail this assert, you can use
                        // `.disable_sanity_check()` on state table to disable this check.
                        assert!(storage_row.is_some(), "deleting an non-existing row");
                        assert!(
                            storage_row.as_ref().unwrap() == &old_row,
                            "inconsistent deletion:\nin-storage: {:?}\nold-value: {:?}",
                            storage_row.as_ref().unwrap(),
                            old_row
                        );
                    }
                    local.delete(pk);
                }
                RowOp::Update((old_row, new_row)) => {
                    if ENABLE_STATE_TABLE_SANITY_CHECK && !self.disable_sanity_check {
                        // If we want to update a row, it should exist in storage, and should
                        // have the same old_value as recorded.
                        let storage_row = self
                            .keyspace
                            .get(&pk, false, self.get_read_option(epoch))
                            .await?;

                        // It's normal for some executors to fail this assert, you can use
                        // `.disable_sanity_check()` on state table to disable this check.
                        assert!(
                            storage_row.is_some(),
                            "update a non-existing row: {:?}",
                            old_row
                        );
                        assert!(
                            storage_row.as_ref().unwrap() == &old_row,
                            "value mismatch when updating row: {:?} != {:?}",
                            storage_row,
                            old_row
                        );
                    }
                    local.put(pk, StorageValue::new_default_put(new_row));
                }
            }
        }
        local.ingest().await?;
        Ok(())
    }
}

/// Iterator functions.
impl<S: StateStore> StateTable<S> {
    /// This function scans rows from the relational table.
    pub async fn iter(&self, epoch: u64) -> StorageResult<RowStream<'_, S>> {
        self.iter_with_pk_prefix(Row::empty(), epoch).await
    }

    /// This function scans rows from the relational table with specific `pk_prefix`.
    pub async fn iter_with_pk_prefix<'a>(
        &'a self,
        pk_prefix: &'a Row,
        epoch: u64,
    ) -> StorageResult<RowStream<'a, S>> {
        let storage_iter = self.storage_iter_with_prefix(epoch, pk_prefix).await?;

        let mem_table_iter = {
            let prefix_serializer = self.pk_serializer.prefix(pk_prefix.size());
            let encoded_prefix = serialize_pk(pk_prefix, &prefix_serializer);
            let encoded_key_range = range_of_prefix(&encoded_prefix);

            let vnode_hint = self.try_compute_vnode_by_pk_prefix(pk_prefix);
            let vnode = vnode_hint.unwrap_or(0_u8);
            let encoded_key_range_with_vnode =
                prefixed_range(encoded_key_range, &vnode.to_be_bytes());
            self.mem_table.iter(encoded_key_range_with_vnode)
        };

        Ok(
            StateTableRowIter::new(mem_table_iter, storage_iter, self.data_types.clone())
                .into_stream(),
        )
    }

    /// Iterates on the table with the given prefix of the pk in `pk_prefix`.
    async fn storage_iter_with_prefix(
        &self,
        epoch: u64,
        pk_prefix: &Row,
    ) -> StorageResult<StorageIter<S>> {
        fn serialize_pk_bound(
            pk_serializer: &OrderedRowSerializer,
            pk_prefix: &Row,
            is_start_bound: bool,
        ) -> Bound<Vec<u8>> {
            let pk_prefix_serializer = pk_serializer.prefix(pk_prefix.size());
            let serialized_pk_prefix = serialize_pk(pk_prefix, &pk_prefix_serializer);
            if pk_prefix.size() == 0 {
                Unbounded
            } else if is_start_bound {
                Included(serialized_pk_prefix)
            } else {
                end_bound_of_prefix(&serialized_pk_prefix)
            }
        }

        let start_key = serialize_pk_bound(&self.pk_serializer, pk_prefix, true);
        let end_key = serialize_pk_bound(&self.pk_serializer, pk_prefix, false);

        assert!(pk_prefix.size() <= self.pk_indices.len());
        let pk_prefix_indices = (0..pk_prefix.size())
            .into_iter()
            .map(|index| self.pk_indices[index])
            .collect_vec();
        let prefix_hint = if self.dist_key_indices.is_empty()
            || self.dist_key_indices != pk_prefix_indices
        {
            trace!(
                "storage_iter_with_prefix dist_key_indices table_id {} not match prefix pk_prefix {:?} dist_key_indices {:?} pk_prefix_indices {:?}",
                self.keyspace.table_id(),
                pk_prefix,
                self.dist_key_indices,
                pk_prefix_indices
            );
            None
        } else {
            let pk_prefix_serializer = self.pk_serializer.prefix(pk_prefix.size());
            let serialized_pk_prefix = serialize_pk(pk_prefix, &pk_prefix_serializer);
            Some(serialized_pk_prefix)
        };

        trace!(
            "storage_iter_with_prefix table_id {} prefix_hint {:?} start_key: {:?}, end_key: {:?} pk_prefix {:?} dist_key_indices {:?} pk_prefix_indices {:?}" ,
            self.keyspace.table_id(),
            prefix_hint,
            start_key,
            end_key,
            pk_prefix,
            self.dist_key_indices,
            pk_prefix_indices
        );

        let vnode_hint = self.try_compute_vnode_by_pk_prefix(pk_prefix);
        let vnode = vnode_hint.unwrap_or(0_u8);

        let raw_key_range = prefixed_range((start_key, end_key), &vnode.to_be_bytes());
        let prefix_hint = prefix_hint
            .clone()
            .map(|prefix_hint| [&vnode.to_be_bytes(), prefix_hint.as_slice()].concat());
        let read_options = self.get_read_option(epoch);
        let iter = StorageIterInner::<S>::new(
            &self.keyspace,
            prefix_hint,
            raw_key_range,
            read_options,
            self.data_types.clone(),
        )
        .await?
        .into_stream();

        Ok(iter)
    }

    /// Try getting vnode value with given primary key prefix, used for `vnode_hint` in iterators.
    /// Return `None` if the provided columns are not enough.
    fn try_compute_vnode_by_pk_prefix(&self, pk_prefix: &Row) -> Option<VirtualNode> {
        self.dist_key_in_pk_indices
            .iter()
            .all(|&d| d < pk_prefix.0.len())
            .then(|| {
                compute_vnode(
                    pk_prefix,
                    &self.dist_key_in_pk_indices,
                    self.vnodes.clone(),
                    &self.table_columns,
                    &self.dist_key_indices,
                )
            })
    }
}

pub type RowStream<'a, S: StateStore> = impl Stream<Item = StorageResult<Cow<'a, Row>>>;

pub trait PkAndRowStream = Stream<Item = StorageResult<(Vec<u8>, Row)>> + Send;

/// The row iterator of the storage table.
pub type StorageIter<S: StateStore> = impl PkAndRowStream;
struct StateTableRowIter<'a, M, C> {
    mem_table_iter: M,
    storage_iter: C,
    _phantom: PhantomData<&'a ()>,
    /// Data type of each column, used for deserializing the row.
    data_types: Vec<DataType>,
}

/// `StateTableRowIter` is able to read the just written data (uncommitted data).
/// It will merge the result of `mem_table_iter` and `state_store_iter`.
impl<'a, M, C> StateTableRowIter<'a, M, C>
where
    M: Iterator<Item = (&'a Vec<u8>, &'a RowOp)>,
    C: Stream<Item = StorageResult<(Vec<u8>, Row)>>,
{
    fn new(mem_table_iter: M, storage_iter: C, data_types: Vec<DataType>) -> Self {
        Self {
            mem_table_iter,
            storage_iter,
            _phantom: PhantomData,
            data_types,
        }
    }

    /// This function scans kv pairs from the `shared_storage` and
    /// memory(`mem_table`) with optional pk_bounds. If a record exist in both `shared_storage` and
    /// `mem_table`, result `mem_table` is returned according to the operation(RowOp) on it.
    #[try_stream(ok = Cow<'a, Row>, error = StorageError)]
    async fn into_stream(self) {
        let storage_iter = self.storage_iter.peekable();
        pin_mut!(storage_iter);

        let mut mem_table_iter = self.mem_table_iter.fuse().peekable();

        loop {
            match (storage_iter.as_mut().peek().await, mem_table_iter.peek()) {
                (None, None) => break,
                // The mem table side has come to an end, return data from the shared storage.
                (Some(_), None) => {
                    let (_, row) = storage_iter.next().await.unwrap()?;
                    yield Cow::Owned(row)
                }
                // The stream side has come to an end, return data from the mem table.
                (None, Some(_)) => {
                    let (_, row_op) = mem_table_iter.next().unwrap();
                    match row_op {
                        RowOp::Insert(row_bytes) | RowOp::Update((_, row_bytes)) => {
                            let row = streaming_deserialize(&self.data_types, row_bytes.as_ref())
                                .map_err(err)?;
                            yield Cow::Owned(row)
                        }
                        _ => {}
                    }
                }
                (Some(Ok((storage_pk, _))), Some((mem_table_pk, _))) => {
                    match storage_pk.cmp(mem_table_pk) {
                        Ordering::Less => {
                            // yield data from storage
                            let (_, row) = storage_iter.next().await.unwrap()?;
                            yield Cow::Owned(row);
                        }
                        Ordering::Equal => {
                            // both memtable and storage contain the key, so we advance both
                            // iterators and return the data in memory.

                            let (_, row_op) = mem_table_iter.next().unwrap();
                            let (_, old_row_in_storage) = storage_iter.next().await.unwrap()?;
                            match row_op {
                                RowOp::Insert(row_bytes) => {
                                    let row =
                                        streaming_deserialize(&self.data_types, row_bytes.as_ref())
                                            .map_err(err)?;
                                    yield Cow::Owned(row);
                                }
                                RowOp::Delete(_) => {}
                                RowOp::Update((old_row_bytes, new_row_bytes)) => {
                                    let old_row = streaming_deserialize(
                                        &self.data_types,
                                        old_row_bytes.as_ref(),
                                    )
                                    .map_err(err)?;

                                    let new_row = streaming_deserialize(
                                        &self.data_types,
                                        new_row_bytes.as_ref(),
                                    )
                                    .map_err(err)?;
                                    debug_assert!(old_row == old_row_in_storage);

                                    yield Cow::Owned(new_row);
                                }
                            }
                        }
                        Ordering::Greater => {
                            // yield data from mem table
                            let (_, row_op) = mem_table_iter.next().unwrap();

                            match row_op {
                                RowOp::Insert(row_bytes) => {
                                    let row =
                                        streaming_deserialize(&self.data_types, row_bytes.as_ref())
                                            .map_err(err)?;

                                    yield Cow::Owned(row);
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
                    return Err(storage_iter.next().await.unwrap().unwrap_err());
                }
            }
        }
    }
}

struct StorageIterInner<S: StateStore> {
    /// An iterator that returns raw bytes from storage.
    iter: StripPrefixIterator<S::Iter>,
    /// Data type of each column, used for deserializing the row.
    data_types: Vec<DataType>,
}

impl<S: StateStore> StorageIterInner<S> {
    async fn new<R, B>(
        keyspace: &Keyspace<S>,
        prefix_hint: Option<Vec<u8>>,
        raw_key_range: R,
        read_options: ReadOptions,
        data_types: Vec<DataType>,
    ) -> StorageResult<Self>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        let iter = keyspace
            .iter_with_range(prefix_hint, raw_key_range, read_options)
            .await?;
        let iter = Self { iter, data_types };
        Ok(iter)
    }

    /// Yield a row with its primary key.
    #[try_stream(ok = (Vec<u8>, Row), error = StorageError)]
    async fn into_stream(mut self) {
        while let Some((key, value)) = self
            .iter
            .next()
            .stack_trace("storage_table_iter_next")
            .await?
        {
            let row = streaming_deserialize(&self.data_types, value.as_ref()).map_err(err)?;
            yield (key.to_vec(), row)
        }
    }
}

fn err(rw: impl Into<RwError>) -> StorageError {
    StorageError::StateTable(rw.into())
}
