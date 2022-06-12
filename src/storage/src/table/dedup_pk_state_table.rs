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
use std::ops::RangeBounds;

use risingwave_common::array::Row;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::util::ordered::{OrderedRowSerializer, OrderedRowDeserializer};
use risingwave_common::util::sort_util::OrderType;

use super::state_table::{KeyAndRowStream, RawKey, RowStream, StateTable};
use crate::error::{StorageError, StorageResult};
use crate::{Keyspace, StateStore};

/// `DedupPkStateTable` is the interface which
/// transforms input Rows into Rows w/o public key cells
/// to reduce storage cost.
/// Trade-off is that every access and retrieve involves ser/de, which is expensive.
pub struct DedupPkStateTable<S: StateStore> {
    inner: StateTable<S>,
    pk_decoder: OrderedRowDeserializer,
}

impl<S: StateStore> DedupPkStateTable<S> {
    pub fn new(
        keyspace: Keyspace<S>,
        column_descs: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        dist_key_indices: Option<Vec<usize>>,
        pk_indices: Vec<usize>,
    ) -> Self {
        // create a new state table, but only with partial decs
        let data_types = pk_indices.iter().map(|i| column_descs[*i].data_type.clone()).collect();
        let pk_decoder = OrderedRowDeserializer::new(data_types, order_types.clone());
        let partial_column_descs = column_descs; // TODO: update this
        let inner = StateTable::new(
            keyspace,
            partial_column_descs,
            order_types,
            dist_key_indices,
            pk_indices,
        );
        Self { inner, pk_decoder }
    }

    fn raw_key_to_dedup_pk_row(&self, pk: &RawKey) -> StorageResult<Row> {
        let ordered_row = self.pk_decoder.deserialize(pk).map_err(|e| StorageError::DedupPkStateTable(e.into()))?;
        Ok(ordered_row.into_row())
    }

    /// Use order key to remove duplicate pk datums
    fn row_to_dedup_pk_row(&self, row: Row) -> Row {
        row
    }

    /// Use order key to replace deduped pk datums
    fn dedup_pk_row_to_row(&self, _pk: &Row, dedup_pk_row: Row) -> Row {
        dedup_pk_row
    }

    fn dedup_pk_row_and_raw_key_to_row(&self, pk: &RawKey, dedup_pk_row: Row) -> StorageResult<Row> {
        let pk_row = self.raw_key_to_dedup_pk_row(pk)?;
        Ok(self.dedup_pk_row_to_row(&pk_row, dedup_pk_row))
    }

    pub async fn get_row(&self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        let dedup_pk_row = self.inner.get_row(pk, epoch).await?;
        Ok(dedup_pk_row.map(|r| self.dedup_pk_row_to_row(pk, r)))
    }

    pub fn insert(&mut self, pk: &Row, value: Row) -> StorageResult<()> {
        let dedup_pk_value = self.row_to_dedup_pk_row(value);
        self.inner.insert(pk, dedup_pk_value)
    }

    pub fn delete(&mut self, pk: &Row, old_value: Row) -> StorageResult<()> {
        let dedup_pk_old_value = self.row_to_dedup_pk_row(old_value);
        self.inner.delete(pk, dedup_pk_old_value)
    }

    pub fn update(&mut self, pk: Row, old_value: Row, new_value: Row) -> StorageResult<()> {
        let dedup_pk_old_value = self.row_to_dedup_pk_row(old_value);
        let dedup_pk_new_value = self.row_to_dedup_pk_row(new_value);
        self.inner
            .update(pk, dedup_pk_old_value, dedup_pk_new_value)
    }

    pub async fn commit(&mut self, new_epoch: u64) -> StorageResult<()> {
        self.inner.commit(new_epoch).await
    }

    pub async fn commit_with_value_meta(&mut self, new_epoch: u64) -> StorageResult<()> {
        self.inner.commit_with_value_meta(new_epoch).await
    }

    pub async fn iter(&self, epoch: u64) -> StorageResult<impl RowStream<'_>> {
        let stream = self.inner.iter_key_and_row(epoch).await?;
        Ok(stream
           .map(|r| match r {
               Ok((k, v)) => {
                    let row = self.dedup_pk_row_and_raw_key_to_row(&k, v.into_owned())?;
                    let row = Cow::Owned(row);
                    Ok(row)
               },
               Err(e) => Err(e)
           }))
    }

    pub async fn iter_with_pk_bounds<R, B>(
        &self,
        pk_bounds: R,
        epoch: u64,
    ) -> StorageResult<impl RowStream<'_>>
    where
        R: RangeBounds<B> + Send + Clone + 'static,
        B: AsRef<Row> + Send + Clone + 'static,
    {
        let stream = self
            .inner
            .iter_key_and_row_with_pk_bounds(pk_bounds, epoch)
            .await?;
        Ok(stream
           .map(|r| match r {
               Ok((k, v)) => {
                    let row = self.dedup_pk_row_and_raw_key_to_row(&k, v.into_owned())?;
                    let row = Cow::Owned(row);
                    Ok(row)
               },
               Err(e) => Err(e)
           }))
    }

    pub async fn iter_with_pk_prefix(
        &self,
        pk_prefix: Option<&Row>,
        prefix_serializer: OrderedRowSerializer,
        epoch: u64,
    ) -> StorageResult<impl RowStream<'_>> {
        let stream = self
            .inner
            .iter_key_and_row_with_pk_prefix(pk_prefix, prefix_serializer, epoch)
            .await?;
        Ok(stream
           .map(|r| match r {
               Ok((k, v)) => {
                    let row = self.dedup_pk_row_and_raw_key_to_row(&k, v.into_owned())?;
                    let row = Cow::Owned(row);
                    Ok(row)
               },
               Err(e) => Err(e)
           }))

    }
}
