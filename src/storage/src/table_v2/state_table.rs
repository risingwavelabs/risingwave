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
use std::ops::Index;
use std::sync::Arc;

use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::Row;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, TableId};
use risingwave_common::error::RwError;
use risingwave_common::types::VirtualNode;
use risingwave_common::util::hash_util::CRC32FastBuilder;
use risingwave_common::util::ordered::OrderedRowSerializer;
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::key::range_of_prefix;
use risingwave_pb::catalog::Table;

use super::mem_table::{MemTable, RowOp};
use super::storage_table::{StorageTableBase, READ_WRITE};
use super::Distribution;
use crate::error::{StorageError, StorageResult};
use crate::row_serde::{serialize_pk, ColumnDescMapping, RowBasedSerde, RowSerde};
use crate::table_v2::storage_table::DEFAULT_VNODE;
use crate::{Keyspace, StateStore};

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
    keyspace: Keyspace<S>,

    /// All columns of this table. Note that this is different from the output columns in
    /// `mapping.output_columns`.
    table_columns: Vec<ColumnDesc>,

    /// Used for serializing the primary key.
    pk_serializer: OrderedRowSerializer,

    /// Used for serializing the row.
    row_serializer: RS::Serializer,

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


    /// Create state table from table catalog and store.
    pub fn from_table_catalog(
        table_catalog: &Table,
        store: S,
        vnodes: Option<Arc<Bitmap>>,
    ) -> Self {

    
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
        let dist_key_indices = table_catalog
            .distribution_key
            .iter()
            .map(|dist_index| *dist_index as usize)
            .collect();
        let pk_indices = table_catalog
            .order_key
            .iter()
            .map(|col_order| col_order.index as usize)
            .collect();
        let distribution = match vnodes {
            Some(vnodes) => Distribution {
                dist_key_indices,
                vnodes,
            },
            None => Distribution::fallback(),
        };

        let keyspace = Keyspace::table_root(store, &table_id);
        let pk_serializer = OrderedRowSerializer::new(order_types);
        let column_ids = table_columns.iter().map(|c| c.column_id).collect();

        let row_serializer = RS::create_serializer(&pk_indices, &table_columns, column_ids);
        let mapping = ColumnDescMapping::new_partial(&table_columns, column_ids);
        Self {
            mem_table: MemTable::new(),
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

    fn compute_vnode_by_row(&self, row: &Row) -> VirtualNode {
        // With `READ_WRITE`, the output columns should be exactly same with the table columns, so
        // we can directly index into the row with indices to the table columns.
        self.compute_vnode(row, &self.dist_key_indices)
    }

    /// Get vnode value with given primary key.
    fn compute_vnode_by_pk(&self, pk: &Row) -> VirtualNode {
        self.compute_vnode(pk, &self.dist_key_in_pk_indices)
    }

    fn pk_serializer(&self) -> &OrderedRowSerializer {
        &self.pk_serializer
    }

    // TODO: remove, should not be exposed to user
    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
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
        let mut datums = vec![];
        for pk_index in self.pk_indices() {
            datums.push(value.index(*pk_index).clone());
        }
        let pk = Row::new(datums);
        let pk_bytes = serialize_pk(&pk, self.pk_serializer());
        let vnode = self.compute_vnode_by_row(&value);
        let value = self
            .row_serializer
            .serialize(vnode, &pk, value)
            .map_err(err)?;
        self.mem_table.insert(pk_bytes, value);
        Ok(())
    }

    /// Delete a row from state table. Must provide a full row of old value corresponding to the
    /// column desc of the table.
    pub fn delete(&mut self, old_value: Row) -> StorageResult<()> {
        let mut datums = vec![];
        for pk_index in self.pk_indices() {
            datums.push(old_value.index(*pk_index).clone());
        }
        let pk = Row::new(datums);
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
fn err(rw: impl Into<RwError>) -> StorageError {
    StorageError::StateTable(rw.into())
}
