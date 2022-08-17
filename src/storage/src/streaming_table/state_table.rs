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
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::BufMut;
use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use log::trace;
use risingwave_common::array::Row;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, TableId, TableOption};
use risingwave_common::error::RwError;
use risingwave_common::types::VirtualNode;
use risingwave_common::util::hash_util::CRC32FastBuilder;
use risingwave_common::util::ordered::OrderedRowSerializer;
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::key::range_of_prefix;
use risingwave_pb::catalog::Table;

use super::mem_table::{MemTable, RowOp};
use crate::{error::{StorageError, StorageResult}, keyspace::StripPrefixIterator};
use crate::row_serde::row_serde_util::{deserialize, serialize, serialize_pk};
use crate::row_serde::ColumnDescMapping;
use crate::storage_value::StorageValue;
use crate::store::{ReadOptions, WriteOptions};
use crate::table::Distribution;
use crate::{Keyspace, StateStore};

/// `StateTable` is the interface accessing relational data in KV(`StateStore`) with
/// row-based encoding.
///
/// For tables without distribution (singleton), the `DEFAULT_VNODE` is encoded.
pub const DEFAULT_VNODE: VirtualNode = 0;
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

    /// Mapping from column id to column index. Used for deserializing the row.
    mapping: Arc<ColumnDescMapping>,

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
    /// executor. The table will also check whether the writed rows
    /// confirm to this partition.
    vnodes: Arc<Bitmap>,

    /// Used for catalog table_properties
    table_option: TableOption,
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
        let column_ids = table_columns.iter().map(|c| c.column_id).collect_vec();

        let mapping = ColumnDescMapping::new_partial(&table_columns, &column_ids);

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
            mapping,
            pk_indices: pk_indices.to_vec(),
            dist_key_indices,
            dist_key_in_pk_indices,
            vnodes,
            table_option: TableOption::build_table_option(table_catalog.get_properties()),
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

        let column_ids = table_columns.iter().map(|c| c.column_id).collect_vec();
        let pk_serializer = OrderedRowSerializer::new(order_types);
        let mapping = ColumnDescMapping::new_partial(&table_columns, &column_ids);
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
            mapping,
            pk_indices,
            dist_key_indices,
            dist_key_in_pk_indices,
            vnodes,
            table_option: Default::default(),
        }
    }

    /// Get vnode value with `indices` on the given `row`. Should not be used directly.
    fn compute_vnode(&self, row: &Row, indices: &[usize]) -> VirtualNode {
        let vnode = if indices.is_empty() {
            DEFAULT_VNODE
        } else {
            row.hash_by_indices(indices, &CRC32FastBuilder {})
                .to_vnode()
        };

        tracing::trace!(target: "events::storage::storage_table", "compute vnode: {:?} key {:?} => {}", row, indices, vnode);

        // FIXME: temporary workaround for local agg, may not needed after we have a vnode builder
        if !indices.is_empty() {
            self.check_vnode_is_set(vnode);
        }
        vnode
    }

    /// Check whether the given `vnode` is set in the `vnodes` of this table.
    fn check_vnode_is_set(&self, vnode: VirtualNode) {
        let is_set = self.vnodes.is_set(vnode as usize).unwrap();
        assert!(
            is_set,
            "vnode {} should not be accessed by this table: {:#?}, dist key {:?}",
            vnode, self.table_columns, self.dist_key_indices
        );
    }

    /// Get vnode value with given primary key.
    fn compute_vnode_by_pk(&self, pk: &Row) -> VirtualNode {
        self.compute_vnode(pk, &self.dist_key_in_pk_indices)
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

/// point get
impl<S: StateStore> StateTable<S> {
    /// Get a single row from state table.
    pub async fn get_row<'a>(&'a self, pk: &'a Row, epoch: u64) -> StorageResult<Option<Row>> {
        let serialized_pk = self.serialize_pk_with_vnode(pk);
        let mem_table_res = self.mem_table.get_row_op(&serialized_pk);

        let read_options = self.get_read_option(epoch);
        match mem_table_res {
            Some(row_op) => match row_op {
                RowOp::Insert(row_bytes) => {
                    let row = deserialize(self.mapping.clone(), row_bytes).map_err(err)?;
                    Ok(Some(row))
                }
                RowOp::Delete(_) => Ok(None),
                RowOp::Update((_, row_bytes)) => {
                    let row = deserialize(self.mapping.clone(), row_bytes).map_err(err)?;
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
                    let row = deserialize(self.mapping.clone(), storage_row_bytes).map_err(err)?;
                    Ok(Some(row))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// `vnode | pk`
    fn serialize_pk_with_vnode(&self, pk: &Row) -> Vec<u8> {
        let mut output = Vec::new();
        output.put_slice(&self.compute_vnode_by_pk(pk).to_be_bytes());
        self.pk_serializer.serialize(pk, &mut output);
        output
    }
}

// write
impl<S: StateStore> StateTable<S> {
    /// Insert a row into state table. Must provide a full row corresponding to the column desc of
    /// the table.
    pub fn insert(&mut self, value: Row) -> StorageResult<()> {
        let pk = value.by_indices(self.pk_indices());
        let key_bytes = self.serialize_pk_with_vnode(&pk);
        let value_bytes = serialize(value).map_err(err)?;
        self.mem_table.insert(key_bytes, value_bytes);
        Ok(())
    }

    /// Delete a row from state table. Must provide a full row of old value corresponding to the
    /// column desc of the table.
    pub fn delete(&mut self, old_value: Row) -> StorageResult<()> {
        let pk = old_value.by_indices(self.pk_indices());
        let key_bytes = self.serialize_pk_with_vnode(&pk);
        let value_bytes = serialize(old_value).map_err(err)?;
        self.mem_table.delete(key_bytes, value_bytes);
        Ok(())
    }

    /// Update a row. The old and new value should have the same pk.
    pub fn update(&mut self, old_value: Row, new_value: Row) -> StorageResult<()> {
        let old_pk = old_value.by_indices(self.pk_indices());
        let new_pk = new_value.by_indices(self.pk_indices());
        debug_assert_eq!(old_pk, new_pk);

        let new_key_bytes = self.serialize_pk_with_vnode(&new_pk);

        let old_value_bytes = serialize(old_value).map_err(err)?;
        let new_value_bytes = serialize(new_value).map_err(err)?;
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
        let mut batch = self.keyspace.state_store().start_write_batch(WriteOptions {
            epoch,
            table_id: self.keyspace.table_id(),
        });
        let mut local = batch.prefixify(&self.keyspace);
        for (pk, row_op) in buffer {
            match row_op {
                RowOp::Insert(row) => {
                    local.put(pk, StorageValue::new_default_put(row));
                }
                RowOp::Delete(_) => {
                    local.delete(pk);
                }
                RowOp::Update((_, new_row)) => {
                    local.put(pk, StorageValue::new_default_put(new_row));
                }
            }
        }
        batch.ingest().await?;
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
        let read_options = self.get_read_option(epoch);
        let pk_prefix_indices = (0..pk_prefix.size())
            .into_iter()
            .map(|index| self.pk_indices[index])
            .collect_vec();
        let prefix_hint = if self.dist_key_indices.is_empty()
            || self.dist_key_indices != pk_prefix_indices
        {
            trace!(
                "iter_with_pk_bounds dist_key_indices table_id {} not match prefix pk_prefix {:?} dist_key_indices {:?} pk_prefix_indices {:?}",
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
        let storage_table_iter =  self.keyspace
            .iter_with_range(prefix_hint, .., read_options)
            .await?;
  
        let mem_table_iter = {
            let prefix_serializer = self.pk_serializer.prefix(pk_prefix.size());
            let encoded_prefix = serialize_pk(pk_prefix, &prefix_serializer);
            let encoded_key_range = range_of_prefix(&encoded_prefix);
            self.mem_table.iter(encoded_key_range)
        };

        Ok(StateTableRowIter::new(mem_table_iter, storage_table_iter, self.mapping).into_stream())
    }

    fn get_read_option(&self, epoch: u64) -> ReadOptions {
        ReadOptions {
            epoch,
            table_id: Some(self.keyspace.table_id()),
            retention_seconds: self.table_option.retention_seconds,
        }
    }
}

pub type RowStream<'a, S: StateStore> = impl Stream<Item = StorageResult<Row>>;
pub type RowBasedRowStream<'a, S> = RowStream<'a, S>;

struct StateTableRowIter<M, C> {
    mem_table_iter: M,
    state_store_iter: C,
    
    _phantom: PhantomData<&'a ()>,
    /// Mapping from column id to column index. Used for deserializing the row.
    mapping: Arc<ColumnDescMapping>,
}

/// `StateTableRowIter` is able to read the just written data (uncommited data).
/// It will merge the result of `mem_table_iter` and `storage_streaming_iter`.
impl<'a, M, C> StateTableRowIter<'a,  M, C>
where
    M: Iterator<Item = (&'a Vec<u8>, &'a RowOp)>,
    C: Stream<Item = StorageResult<(Vec<u8>, Row)>>,
{
    fn new(mem_table_iter: M, state_store_iter: StripPrefixIterator<S::Iter>, mapping: Arc<ColumnDescMapping>) -> Self {
        Self {
            mem_table_iter,
            state_store_iter,
            _phantom: PhantomData,
            mapping,
        }
    }

    /// This function scans kv pairs from the `shared_storage`(`storage_table`) and
    /// memory(`mem_table`) with optional pk_bounds. If pk_bounds is
    /// (Included(prefix),Excluded(next_key(prefix))), all kv pairs within corresponding prefix will
    /// be scanned. If a record exist in both `storage_table` and `mem_table`, result
    /// `mem_table` is returned according to the operation(RowOp) on it.
    #[try_stream(ok =  Row, error = StorageError)]
    async fn into_stream(self) {
        let state_store_iter = self.state_store_iter.fuse().peekable();
        pin_mut!(state_store_iter);

        let mut mem_table_iter = self.mem_table_iter.fuse().peekable();

        loop {
            match (
                state_store_iter.as_mut().peek().await,
                mem_table_iter.peek(),
            ) {
                (None, None) => break,
                // The mem table side has come to an end, return data from the shared storage.
                (Some(_), None) => {
                    let (_, row) = state_store_iter.next().await.unwrap()?;
                    yield row
                }
                // The stream side has come to an end, return data from the mem table.
                (None, Some(_)) => {
                    let (_, row_op) = mem_table_iter.next().unwrap();
                    match row_op {
                        RowOp::Insert(row_bytes) | RowOp::Update((_, row_bytes)) => {
                            let row = deserialize(self.mapping.clone(), row_bytes).map_err(err)?;
                            yield row
                        }
                        _ => {}
                    }
                }
                (Some(Ok((storage_pk, _))), Some((mem_table_pk, _))) => {
                    match storage_pk.cmp(mem_table_pk) {
                        Ordering::Less => {
                            // yield data from storage table
                            let (_, row) = state_store_iter.next().await.unwrap()?;
                            yield row;
                        }
                        Ordering::Equal => {
                            // both memtable and storage contain the key, so we advance both
                            // iterators and return the data in memory.
                            let (_, row_op) = mem_table_iter.next().unwrap();
                            let (_, old_row_in_storage) =
                            state_store_iter.next().await.unwrap()?;
                            match row_op {
                                RowOp::Insert(row_bytes) => {
                                    let row = deserialize(self.mapping.clone(), row_bytes)
                                        .map_err(err)?;
                                    yield row;
                                }
                                RowOp::Delete(_) => {}
                                RowOp::Update((old_row_bytes, new_row_bytes)) => {
                                    let old_row = deserialize(self.mapping.clone(), old_row_bytes)
                                        .map_err(err)?;
                                    let new_row = deserialize(self.mapping.clone(), new_row_bytes)
                                        .map_err(err)?;
                                    debug_assert!(old_row == old_row_in_storage);
                                    yield new_row;
                                }
                            }
                        }
                        Ordering::Greater => {
                            // yield data from mem table
                            let (_, row_op) = mem_table_iter.next().unwrap();
                            match row_op {
                                RowOp::Insert(row_bytes) => {
                                    let row = deserialize(self.mapping.clone(), row_bytes)
                                        .map_err(err)?;
                                    yield row;
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
                    return Err(state_store_iter.next().await.unwrap().unwrap_err());
                }
            }
        }
    }
}

fn err(rw: impl Into<RwError>) -> StorageError {
    StorageError::StateTable(rw.into())
}
