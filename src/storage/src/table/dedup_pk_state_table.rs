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
use std::ops::RangeBounds;

use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, OrderedColumnDesc};
use risingwave_common::util::ordered::OrderedRowSerializer;
use risingwave_common::util::sort_util::OrderType;

use super::state_table::{RowStream, StateTable};
use crate::error::StorageResult;

use crate::{Keyspace, StateStore};

pub struct DedupPkStateTable<S: StateStore> {
    inner: StateTable<S>,
    order_key: Vec<OrderedColumnDesc>,
}

impl <S: StateStore> DedupPkStateTable<S> {
    pub fn new(
        keyspace: Keyspace<S>,
        column_descs: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        dist_key_indices: Option<Vec<usize>>,
        _pk_indices: Vec<usize>,
    ) -> Self {
        // create a new state table, but only with partial decs
        let order_key = vec![]; // TODO: construct from fields
        let partial_column_descs = column_descs; // TODO: update this
        let inner = StateTable::new(keyspace, partial_column_descs, order_types, dist_key_indices, _pk_indices);
        Self {
            order_key,
            inner,
        }
    }

    /// Use order key to remove duplicate pks.
    fn dedup_pk_in_row(&self, row: Row) -> Row {
        return row;
    }

    /// TODO: read methods
    /// 1) get partial row first
    /// 2) reconstruct original row
    /// a. Layer on top will be able to just call this public api, decode after.
    /// b. If call get_row_with pk, it is just inserting changes into this.
    pub async fn get_row(&self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        self.inner.get_row(pk, epoch).await
    }

    /// TODO: write methods
    /// Layer on top will do transform,
    /// call lower layer to insert.
    /// For upper layer, when constructing cell based column descs,
    /// we need to change cell_based_column_descs to partial repr.
    pub fn insert(&mut self, pk: &Row, value: Row) -> StorageResult<()> {
        Ok(())
    }

    pub fn delete(&mut self, pk: &Row, old_value: Row) -> StorageResult<()> {
        Ok(())
    }

    pub fn update(&mut self, _pk: Row, _old_value: Row, _new_value: Row) -> StorageResult<()> {
        self.inner.update(_pk, _old_value, _new_value)
    }

    // At the Batch write phase, mem_table should ALREADY be in encoded fmt?
    // that means insert needs to encode accordingly.
    pub async fn commit(&mut self, new_epoch: u64) -> StorageResult<()> {
        Ok(())
    }

    pub async fn commit_with_value_meta(&mut self, new_epoch: u64) -> StorageResult<()> {
        // TODO: map on the stream
        Ok(())
    }

    /// TODO: iter need another layer too
    /// This function scans rows from the relational table.
    pub async fn iter(&self, epoch: u64) -> StorageResult<impl RowStream<'_>> {
        // TODO: map on the stream
        self.inner.iter(epoch).await
    }

    /// TODO: as above.
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
        // TODO: map on the stream
        self.inner.iter_with_pk_bounds(pk_bounds, epoch).await
    }

    /// TODO: as above.
    /// This function scans rows from the relational table with specific `pk_prefix`.
    pub async fn iter_with_pk_prefix(
        &self,
        pk_prefix: Row,
        prefix_serializer: OrderedRowSerializer,
        epoch: u64,
    ) -> StorageResult<impl RowStream<'_>> {
        // TODO: map on the stream
        self.inner.iter_with_pk_prefix(pk_prefix, prefix_serializer, epoch).await
    }
}
