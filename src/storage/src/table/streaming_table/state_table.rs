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
use std::ops::RangeBounds;
use std::sync::Arc;

use async_stack_trace::StackTrace;
use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::{izip, Itertools};
use risingwave_common::array::{Op, Row, StreamChunk, Vis};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, TableId, TableOption};
use risingwave_common::error::RwError;
use risingwave_common::types::VirtualNode;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::ordered::{OrderedRowDeserializer, OrderedRowSerializer};
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::key::{prefixed_range, range_of_prefix};
use risingwave_pb::catalog::Table;
use tracing::trace;

use super::mem_table::{MemTable, MemTableIter, RowOp};
use crate::error::{StorageError, StorageResult};
use crate::keyspace::StripPrefixIterator;
use crate::row_serde::row_serde_util::{
    deserialize_pk_with_vnode, serialize_pk, serialize_pk_with_vnode, streaming_deserialize,
};
use crate::storage_value::StorageValue;
use crate::store::{ReadOptions, WriteOptions};
use crate::table::streaming_table::mem_table::MemTableError;
use crate::table::{compute_chunk_vnode, compute_vnode, DataTypes, Distribution};
use crate::{Keyspace, StateStore, StateStoreIter};

/// `StateTable` is the interface accessing relational data in KV(`StateStore`) with
/// row-based encoding.
#[derive(Clone)]
pub struct StateTable<S: StateStore> {
    /// buffer row operations.
    mem_table: MemTable,

    /// write into state store.
    keyspace: Keyspace<S>,

    /// Used for serializing the primary key.
    pk_serializer: OrderedRowSerializer,

    /// Used for deserializing the primary key. Debug-only for now.
    pk_deserializer: OrderedRowDeserializer,

    /// Datatypes of each column, used for deserializing the row.
    data_types: DataTypes,

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

    /// an optional column index which is the vnode of each row computed by the table's consistent
    /// hash distribution
    pub vnode_col_idx_in_pk: Option<usize>,

    value_indices: Vec<usize>,

    /// the epoch flush to the state store last time
    epoch: Option<EpochPair>,
}

// initialize
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
        let order_types: Vec<OrderType> = table_catalog
            .pk
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
            .pk
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
        let pk_serializer = OrderedRowSerializer::new(order_types.clone());

        let pk_data_types = pk_indices
            .iter()
            .map(|i| table_columns[*i].data_type.clone())
            .collect();
        let pk_deserializer = OrderedRowDeserializer::new(pk_data_types, order_types);

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

        let vnode_col_idx_in_pk = table_catalog
            .vnode_col_idx
            .as_ref()
            .and_then(|vnode_col_idx| {
                let vnode_col_idx = vnode_col_idx.index as usize;
                pk_indices.iter().position(|&i| vnode_col_idx == i)
            });
        let value_indices = table_catalog
            .value_indices
            .iter()
            .map(|val| *val as usize)
            .collect_vec();

        let data_types = value_indices
            .iter()
            .map(|idx| table_columns[*idx].data_type.clone())
            .collect();
        Self {
            mem_table: MemTable::new(),
            keyspace,
            pk_serializer,
            pk_deserializer,
            data_types,
            pk_indices: pk_indices.to_vec(),
            dist_key_indices,
            dist_key_in_pk_indices,
            vnodes,
            table_option: TableOption::build_table_option(table_catalog.get_properties()),
            disable_sanity_check: false,
            vnode_col_idx_in_pk,
            value_indices,
            epoch: None,
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
        let value_indices = (0..columns.len()).collect_vec();
        Self::new_with_distribution(
            store,
            table_id,
            columns,
            order_types,
            pk_indices,
            Distribution::fallback(),
            value_indices,
        )
    }

    /// Create a state table with given `value_indices`, used for unit tests.
    pub fn new_without_distribution_partial(
        store: S,
        table_id: TableId,
        columns: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        value_indices: Vec<usize>,
    ) -> Self {
        Self::new_with_distribution(
            store,
            table_id,
            columns,
            order_types,
            pk_indices,
            Distribution::fallback(),
            value_indices,
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
        value_indices: Vec<usize>,
    ) -> Self {
        let keyspace = Keyspace::table_root(store, &table_id);

        let pk_serializer = OrderedRowSerializer::new(order_types.clone());

        let pk_data_types = pk_indices
            .iter()
            .map(|i| table_columns[*i].data_type.clone())
            .collect();
        let pk_deserializer = OrderedRowDeserializer::new(pk_data_types, order_types);

        let data_types = value_indices
            .iter()
            .map(|idx| table_columns[*idx].data_type.clone())
            .collect();
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
            pk_serializer,
            pk_deserializer,
            data_types,
            pk_indices,
            dist_key_indices,
            dist_key_in_pk_indices,
            vnodes,
            table_option: Default::default(),
            disable_sanity_check: false,
            vnode_col_idx_in_pk: None,
            value_indices,
            epoch: None,
        }
    }

    /// Disable sanity check on this storage table.
    pub fn disable_sanity_check(&mut self) {
        self.disable_sanity_check = true;
    }

    fn table_id(&self) -> TableId {
        self.keyspace.table_id()
    }

    /// get the newest epoch of the state store and panic if the `init_epoch()` has never be called
    pub fn init_epoch(&mut self, epoch: EpochPair) {
        match self.epoch {
            Some(prev_epoch) => {
                panic!(
                    "init the state table's epoch twice, table_id: {}, prev_epoch: {}, new_epoch: {}",
                    self.table_id(),
                    prev_epoch.curr,
                    epoch.curr
                )
            }
            None => {
                self.epoch = Some(epoch);
            }
        }
    }

    /// get the newest epoch of the state store and panic if the `init_epoch()` has never be called
    pub fn epoch(&self) -> u64 {
        self.epoch.unwrap_or_else(|| panic!("try to use state table's epoch, but the init_epoch() has not been called, table_id: {}", self.table_id())).curr
    }

    /// get the newest epoch of the state store and panic if the `init_epoch()` has never be called
    pub fn prev_epoch(&self) -> u64 {
        self.epoch.unwrap_or_else(|| panic!("try to use state table's epoch, but the init_epoch() has not been called, table_id: {}", self.table_id())).prev
    }

    /// Get the vnode value with given (prefix of) primary key
    fn compute_vnode(&self, pk_prefix: &Row) -> VirtualNode {
        let prefix_len = pk_prefix.0.len();
        if let Some(vnode_col_idx_in_pk) = self.vnode_col_idx_in_pk {
            let vnode = pk_prefix.0.get(vnode_col_idx_in_pk).unwrap();
            vnode.clone().unwrap().into_int16() as _
        } else {
            // For streaming, the given prefix must be enough to calculate the vnode
            assert!(self.dist_key_in_pk_indices.iter().all(|&d| d < prefix_len));
            compute_vnode(pk_prefix, &self.dist_key_in_pk_indices, &self.vnodes)
        }
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
            table_id: Some(self.table_id()),
            retention_seconds: self.table_option.retention_seconds,
        }
    }
}

const ENABLE_SANITY_CHECK: bool = cfg!(debug_assertions);

// point get
impl<S: StateStore> StateTable<S> {
    /// Get a single row from state table.
    pub async fn get_row<'a>(&'a self, pk: &'a Row) -> StorageResult<Option<Row>> {
        let serialized_pk =
            serialize_pk_with_vnode(pk, &self.pk_serializer, self.compute_vnode(pk));
        let mem_table_res = self.mem_table.get_row_op(&serialized_pk);

        let read_options = self.get_read_option(self.epoch());
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
    fn handle_mem_table_error(&self, e: MemTableError) {
        match e {
            MemTableError::Conflict { key, prev, new } => {
                let (vnode, key) = deserialize_pk_with_vnode(&key, &self.pk_deserializer).unwrap();
                panic!(
                    "mem-table operation conflicts! table_id: {}, vnode: {}, key: {:?}, prev: {}, new: {}",
                    self.table_id(),
                    vnode,
                    &key,
                    prev.debug_fmt(self.data_types.as_ref()),
                    new.debug_fmt(self.data_types.as_ref()),
                )
            }
        }
    }

    /// Insert a row into state table. Must provide a full row corresponding to the column desc of
    /// the table.
    pub fn insert(&mut self, value: Row) {
        let pk = value.by_indices(self.pk_indices());

        let key_bytes = serialize_pk_with_vnode(&pk, &self.pk_serializer, self.compute_vnode(&pk));
        let value_bytes = value.serialize(&self.value_indices);
        self.mem_table
            .insert(key_bytes, value_bytes)
            .unwrap_or_else(|e| self.handle_mem_table_error(e));
    }

    /// Delete a row from state table. Must provide a full row of old value corresponding to the
    /// column desc of the table.
    pub fn delete(&mut self, old_value: Row) {
        let pk = old_value.by_indices(self.pk_indices());
        let key_bytes = serialize_pk_with_vnode(&pk, &self.pk_serializer, self.compute_vnode(&pk));
        let value_bytes = old_value.serialize(&self.value_indices);
        self.mem_table
            .delete(key_bytes, value_bytes)
            .unwrap_or_else(|e| self.handle_mem_table_error(e));
    }

    /// Update a row. The old and new value should have the same pk.
    pub fn update(&mut self, old_value: Row, new_value: Row) {
        let old_pk = old_value.by_indices(self.pk_indices());
        let new_pk = new_value.by_indices(self.pk_indices());
        debug_assert_eq!(old_pk, new_pk);

        let new_key_bytes =
            serialize_pk_with_vnode(&new_pk, &self.pk_serializer, self.compute_vnode(&new_pk));

        self.mem_table
            .update(
                new_key_bytes,
                old_value.serialize(&self.value_indices),
                new_value.serialize(&self.value_indices),
            )
            .unwrap_or_else(|e| self.handle_mem_table_error(e));
    }

    /// Write batch with a `StreamChunk` which should have the same schema with the table.
    // allow(izip, which use zip instead of zip_eq)
    #[allow(clippy::disallowed_methods)]
    pub fn write_chunk(&mut self, chunk: StreamChunk) {
        let (chunk, op) = chunk.into_parts();

        let mut vnode_and_pks = vec![vec![]; chunk.capacity()];

        compute_chunk_vnode(&chunk, &self.dist_key_indices, &self.vnodes)
            .into_iter()
            .zip_eq(vnode_and_pks.iter_mut())
            .for_each(|(vnode, vnode_and_pk)| vnode_and_pk.extend(vnode.to_be_bytes()));
        let values = chunk.serialize();

        let chunk = chunk.reorder_columns(self.pk_indices());
        chunk
            .rows_with_holes()
            .zip_eq(vnode_and_pks.iter_mut())
            .for_each(|(r, vnode_and_pk)| {
                if let Some(r) = r {
                    self.pk_serializer.serialize_ref(r, vnode_and_pk);
                }
            });

        let (_, vis) = chunk.into_parts();
        match vis {
            Vis::Bitmap(vis) => {
                for ((op, key, value), vis) in izip!(op, vnode_and_pks, values).zip_eq(vis.iter()) {
                    if vis {
                        match op {
                            Op::Insert | Op::UpdateInsert => self.mem_table.insert(key, value),
                            Op::Delete | Op::UpdateDelete => self.mem_table.delete(key, value),
                        }
                        .unwrap_or_else(|e| self.handle_mem_table_error(e))
                    }
                }
            }
            Vis::Compact(_) => {
                for (op, key, value) in izip!(op, vnode_and_pks, values) {
                    match op {
                        Op::Insert | Op::UpdateInsert => self.mem_table.insert(key, value),
                        Op::Delete | Op::UpdateDelete => self.mem_table.delete(key, value),
                    }
                    .unwrap_or_else(|e| self.handle_mem_table_error(e))
                }
            }
        }
    }

    fn update_epoch(&mut self, new_epoch: EpochPair) {
        assert!(
            self.epoch() <= new_epoch.curr,
            "state table commit a committed epoch, table_id: {}, prev_epoch: {}, new_epoch: {}",
            self.table_id(),
            self.epoch(),
            new_epoch.curr
        );
        self.epoch = Some(new_epoch);
    }

    pub async fn commit(&mut self, new_epoch: EpochPair) -> StorageResult<()> {
        assert_eq!(self.epoch(), new_epoch.prev);
        let mem_table = std::mem::take(&mut self.mem_table).into_parts();
        self.batch_write_rows(mem_table, new_epoch.prev).await?;
        self.update_epoch(new_epoch);
        Ok(())
    }

    /// used for unit test, and do not need to assert epoch.
    pub async fn commit_for_test(&mut self, new_epoch: EpochPair) -> StorageResult<()> {
        let mem_table = std::mem::take(&mut self.mem_table).into_parts();
        self.batch_write_rows(mem_table, new_epoch.prev).await?;
        self.update_epoch(new_epoch);
        Ok(())
    }

    // TODO(st1page): maybe we should extract a pub struct to do it
    /// just specially used by those state table read-only and after the call the data
    /// in the epoch will be visible
    pub fn commit_no_data_expected(&mut self, new_epoch: EpochPair) {
        assert!(!self.is_dirty());
        self.update_epoch(new_epoch);
    }

    /// Write to state store.
    async fn batch_write_rows(
        &mut self,
        buffer: BTreeMap<Vec<u8>, RowOp>,
        epoch: u64,
    ) -> StorageResult<()> {
        let mut local = self.keyspace.start_write_batch(WriteOptions {
            epoch,
            table_id: self.table_id(),
        });
        for (pk, row_op) in buffer {
            match row_op {
                RowOp::Insert(row) => {
                    if ENABLE_SANITY_CHECK && !self.disable_sanity_check {
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
                    local.put(pk, StorageValue::new_put(row));
                }
                RowOp::Delete(old_row) => {
                    if ENABLE_SANITY_CHECK && !self.disable_sanity_check {
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
                    if ENABLE_SANITY_CHECK && !self.disable_sanity_check {
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
                    local.put(pk, StorageValue::new_put(new_row));
                }
            }
        }
        local.ingest().await?;
        Ok(())
    }
}

// Iterator functions
impl<S: StateStore> StateTable<S> {
    /// This function scans rows from the relational table.
    pub async fn iter(&self) -> StorageResult<RowStream<'_, S>> {
        self.iter_with_pk_prefix(Row::empty(), false).await
    }

    /// This function scans rows from the relational table with specific `pk_prefix`.
    pub async fn iter_with_pk_prefix<'a>(
        &'a self,
        pk_prefix: &'a Row,
        use_curr_epoch: bool,
    ) -> StorageResult<RowStream<'a, S>> {
        let (mem_table_iter, storage_iter_stream) = match use_curr_epoch {
            true => self.iter_inner(pk_prefix, self.epoch()).await?,
            false => self.iter_inner(pk_prefix, self.prev_epoch()).await?,
        };

        let storage_iter = storage_iter_stream.into_stream();
        Ok(
            StateTableRowIter::new(mem_table_iter, storage_iter, self.data_types.clone())
                .into_stream()
                .map(|pk_row| pk_row.map(|(_, row)| row)),
        )
    }

    /// This function scans rows from the relational table with specific `pk_prefix`, return both
    /// key and value.
    pub async fn iter_key_and_val<'a>(
        &'a self,
        pk_prefix: &'a Row,
    ) -> StorageResult<RowStreamWithPk<'a, S>> {
        let (mem_table_iter, storage_iter_stream) =
            self.iter_inner(pk_prefix, self.epoch()).await?;
        let storage_iter = storage_iter_stream.into_stream();

        Ok(
            StateTableRowIter::new(mem_table_iter, storage_iter, self.data_types.clone())
                .into_stream(),
        )
    }

    async fn iter_inner<'a>(
        &'a self,
        pk_prefix: &'a Row,
        epoch: u64,
    ) -> StorageResult<(MemTableIter<'_>, StorageIterInner<S>)> {
        let prefix_serializer = self.pk_serializer.prefix(pk_prefix.size());
        let encoded_prefix = serialize_pk(pk_prefix, &prefix_serializer);
        let encoded_key_range = range_of_prefix(&encoded_prefix);

        // We assume that all usages of iterating the state table only access a single vnode.
        // If this assertion fails, then something must be wrong with the operator implementation or
        // the distribution derivation from the optimizer.
        let vnode = self.compute_vnode(pk_prefix).to_be_bytes();
        let encoded_key_range_with_vnode = prefixed_range(encoded_key_range, &vnode);

        // Mem table iterator.
        let mem_table_iter = self.mem_table.iter(encoded_key_range_with_vnode.clone());

        // Storage iterator.
        let storage_iter = {
            // Construct prefix hint for prefix bloom filter.
            let pk_prefix_indices = &self.pk_indices[..pk_prefix.size()];
            let prefix_hint = {
                if self.dist_key_indices.is_empty() || self.dist_key_indices != pk_prefix_indices {
                    None
                } else {
                    Some([&vnode, &encoded_prefix[..]].concat())
                }
            };

            trace!(
                table_id = ?self.table_id(),
                ?prefix_hint, ?encoded_key_range_with_vnode, ?pk_prefix,
                dist_key_indices = ?self.dist_key_indices, ?pk_prefix_indices,
                "storage_iter_with_prefix"
            );

            StorageIterInner::<S>::new(
                &self.keyspace,
                prefix_hint,
                encoded_key_range_with_vnode,
                self.get_read_option(epoch),
                self.data_types.clone(),
            )
            .await?
        };
        Ok((mem_table_iter, storage_iter))
    }
}

pub type RowStream<'a, S: StateStore> = impl Stream<Item = StorageResult<Cow<'a, Row>>>;
pub type RowStreamWithPk<'a, S: StateStore> =
    impl Stream<Item = StorageResult<(Cow<'a, Vec<u8>>, Cow<'a, Row>)>>;

/// `StateTableRowIter` is able to read the just written data (uncommitted data).
/// It will merge the result of `mem_table_iter` and `state_store_iter`.
struct StateTableRowIter<'a, M, C> {
    mem_table_iter: M,
    storage_iter: C,
    _phantom: PhantomData<&'a ()>,
    /// Data type of each column, used for deserializing the row.
    data_types: DataTypes,
}

impl<'a, M, C> StateTableRowIter<'a, M, C>
where
    M: Iterator<Item = (&'a Vec<u8>, &'a RowOp)>,
    C: Stream<Item = StorageResult<(Vec<u8>, Row)>>,
{
    fn new(mem_table_iter: M, storage_iter: C, data_types: DataTypes) -> Self {
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
    #[try_stream(ok = (Cow<'a, Vec<u8>>, Cow<'a, Row>), error = StorageError)]
    async fn into_stream(self) {
        let storage_iter = self.storage_iter.peekable();
        pin_mut!(storage_iter);

        let mut mem_table_iter = self.mem_table_iter.fuse().peekable();

        loop {
            match (storage_iter.as_mut().peek().await, mem_table_iter.peek()) {
                (None, None) => break,
                // The mem table side has come to an end, return data from the shared storage.
                (Some(_), None) => {
                    let (pk, row) = storage_iter.next().await.unwrap()?;
                    yield (Cow::Owned(pk), Cow::Owned(row))
                }
                // The stream side has come to an end, return data from the mem table.
                (None, Some(_)) => {
                    let (pk, row_op) = mem_table_iter.next().unwrap();
                    match row_op {
                        RowOp::Insert(row_bytes) | RowOp::Update((_, row_bytes)) => {
                            let row = streaming_deserialize(&self.data_types, row_bytes.as_ref())
                                .map_err(err)?;

                            yield (Cow::Borrowed(pk), Cow::Owned(row))
                        }
                        _ => {}
                    }
                }
                (Some(Ok((storage_pk, _))), Some((mem_table_pk, _))) => {
                    match storage_pk.cmp(mem_table_pk) {
                        Ordering::Less => {
                            // yield data from storage
                            let (pk, row) = storage_iter.next().await.unwrap()?;
                            yield (Cow::Owned(pk), Cow::Owned(row));
                        }
                        Ordering::Equal => {
                            // both memtable and storage contain the key, so we advance both
                            // iterators and return the data in memory.

                            let (pk, row_op) = mem_table_iter.next().unwrap();
                            let (_, old_row_in_storage) = storage_iter.next().await.unwrap()?;
                            match row_op {
                                RowOp::Insert(row_bytes) => {
                                    let row =
                                        streaming_deserialize(&self.data_types, row_bytes.as_ref())
                                            .map_err(err)?;

                                    yield (Cow::Borrowed(pk), Cow::Owned(row));
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

                                    yield (Cow::Borrowed(pk), Cow::Owned(new_row));
                                }
                            }
                        }
                        Ordering::Greater => {
                            // yield data from mem table
                            let (pk, row_op) = mem_table_iter.next().unwrap();

                            match row_op {
                                RowOp::Insert(row_bytes) => {
                                    let row =
                                        streaming_deserialize(&self.data_types, row_bytes.as_ref())
                                            .map_err(err)?;

                                    yield (Cow::Borrowed(pk), Cow::Owned(row));
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
    data_types: DataTypes,
}

impl<S: StateStore> StorageIterInner<S> {
    async fn new<R, B>(
        keyspace: &Keyspace<S>,
        prefix_hint: Option<Vec<u8>>,
        raw_key_range: R,
        read_options: ReadOptions,
        data_types: DataTypes,
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

            yield (key.to_vec(), row);
        }
    }
}

fn err(rw: impl Into<RwError>) -> StorageError {
    StorageError::StateTable(rw.into())
}
