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
//
#![allow(dead_code)]
#![allow(unused)]

use std::collections::HashMap;
use std::ops::Index;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, Row};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, OrderedColumnDesc, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::Datum;
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;

use super::TableIter;
use crate::cell_based_row_deserializer::CellBasedRowDeserializer;
use crate::cell_based_row_serializer::CellBasedRowSerializer;
use crate::{Keyspace, StateStore};

/// `CellBasedTable` is the interface accessing relational data in KV(`StateStore`) with encoding
/// format: [keyspace | pk | `column_id` (4B)] -> value.
/// if the key of the column id does not exist, it will be Null in the relation
#[derive(Clone)]
pub struct CellBasedTable<S: StateStore> {
    /// The keyspace that the pk and value of the original table has
    keyspace: Keyspace<S>,

    /// The schema of this table viewed by some source executor, e.g. RowSeqScanExecutor.
    schema: Schema,

    /// ColumnDesc contains strictly more info than `schema`.
    column_descs: Vec<ColumnDesc>,

    /// Mapping from column Id to column index
    column_id_to_column_index: HashMap<ColumnId, usize>,

    pk_serializer: Option<OrderedRowSerializer>,

    cell_based_row_serializer: CellBasedRowSerializer,

    column_ids: Vec<ColumnId>,
}

impl<S: StateStore> std::fmt::Debug for CellBasedTable<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellBasedTable")
            .field("column_descs", &self.column_descs)
            .finish()
    }
}
impl<S: StateStore> CellBasedTable<S> {
    pub fn new(
        keyspace: Keyspace<S>,
        column_descs: Vec<ColumnDesc>,
        ordered_row_serializer: Option<OrderedRowSerializer>,
    ) -> Self {
        let schema = Schema::new(
            column_descs
                .iter()
                .map(|cd| Field::with_name(cd.data_type.clone(), cd.name.clone()))
                .collect_vec(),
        );
        let column_id_to_column_index = generate_column_id_to_column_index_mapping(&column_descs);
        let column_ids = generate_column_id(&column_descs);
        Self {
            keyspace,
            schema,
            column_descs,
            column_id_to_column_index,

            pk_serializer: ordered_row_serializer,
            cell_based_row_serializer: CellBasedRowSerializer::new(),
            column_ids,
        }
    }

    pub fn new_for_test(
        keyspace: Keyspace<S>,
        column_descs: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
    ) -> Self {
        Self::new(
            keyspace,
            column_descs,
            Some(OrderedRowSerializer::new(order_types)),
        )
    }

    /// Create an "adhoc" [`CellBasedTable`] with specified columns.
    pub fn new_adhoc(keyspace: Keyspace<S>, column_descs: Vec<ColumnDesc>) -> Self {
        Self::new(keyspace, column_descs, None)
    }

    // cell-based interface
    pub async fn get(&self, pk: Row, column: &ColumnDesc, epoch: u64) -> Result<Option<Datum>> {
        let arrange_key_buf = serialize_pk(&pk, self.pk_serializer.as_ref().unwrap())?;
        let key = Bytes::from(arrange_key_buf);
        let state_store_get_res = self
            .keyspace
            .state_store()
            .get(&key.clone(), epoch)
            .await
            .unwrap();
        let column_id = column.column_id.get_id() as usize;
        let mut cell_based_row_deserializer = CellBasedRowDeserializer::new(vec![column.clone()]);
        let pk_and_row =
            cell_based_row_deserializer.deserialize(&key, &state_store_get_res.unwrap())?;
        match pk_and_row {
            Some(pk_row) => {
                return Ok(Some(pk_row.1.index(column_id).clone()));
            }
            None => Ok(None),
        }
    }

    pub async fn get_row(&self, pk: Row, column: &[ColumnDesc], epoch: u64) -> Result<Row> {
        let arrange_key_buf = serialize_pk(&pk, self.pk_serializer.as_ref().unwrap())?;
        let key = Bytes::from(arrange_key_buf);
        let state_store_get_res = self
            .keyspace
            .state_store()
            .get(&key.clone(), epoch)
            .await
            .unwrap();
        let mut cell_based_row_deserializer = CellBasedRowDeserializer::new(column.to_vec());
        let pk_and_row =
            cell_based_row_deserializer.deserialize(&key, &state_store_get_res.unwrap())?;
        Ok(pk_and_row.map(|(_pk, row)| row).unwrap())
    }

    pub async fn insert_row(
        &mut self,
        pk: Row,
        cell_value: Option<Row>,
        column_descs: &[ColumnDesc],
        epoch: u64,
    ) -> Result<()> {
        let mut batch = self.keyspace.state_store().start_write_batch();
        let mut local = batch.prefixify(&self.keyspace);
        let arrange_key_buf = serialize_pk(&pk, self.pk_serializer.as_ref().unwrap())?;
        let column_ids = generate_column_id(column_descs);
        let bytes = self
            .cell_based_row_serializer
            .serialize(&arrange_key_buf, cell_value, column_ids)
            .unwrap();
        for (key, value) in bytes {
            match value {
                Some(val) => local.put(key, val),
                None => local.delete(key),
            }
        }
        batch.ingest(epoch).await?;
        Ok(())
    }

    pub async fn delete_row(&mut self, pk: Row, epoch: u64) -> Result<()> {
        let mut batch = self.keyspace.state_store().start_write_batch();
        let mut local = batch.prefixify(&self.keyspace);
        let arrange_key_buf = serialize_pk(&pk, self.pk_serializer.as_ref().unwrap())?;
        let column_ids = self.column_ids.clone();
        let cell_value = None;
        let bytes = self
            .cell_based_row_serializer
            .serialize(&arrange_key_buf, cell_value, column_ids)
            .unwrap();
        for (key, value) in bytes {
            local.delete(key);
        }
        batch.ingest(epoch).await?;
        Ok(())
    }

    pub async fn update_row(
        &mut self,
        pk: Row,
        cell_value: Option<Row>,
        column_descs: &[ColumnDesc],
        epoch: u64,
    ) -> Result<()> {
        let mut batch = self.keyspace.state_store().start_write_batch();
        let mut local = batch.prefixify(&self.keyspace);
        let arrange_key_buf = serialize_pk(&pk, self.pk_serializer.as_ref().unwrap())?;
        let column_ids = generate_column_id(column_descs);
        let bytes = self
            .cell_based_row_serializer
            .serialize(&arrange_key_buf, cell_value, column_ids)
            .unwrap();
        // delete original kv_pairs in state_store
        for (key, value) in bytes.clone() {
            local.delete(key);
        }
        // write updated kv_pairs in state_store
        for (key, value) in bytes {
            match value {
                Some(val) => local.put(key, val),
                None => local.delete(key),
            }
        }
        batch.ingest(epoch).await?;
        Ok(())
    }

    pub async fn batch_insert_row(
        &mut self,
        rows: Vec<(Row, Option<Row>)>,
        column_descs: &[ColumnDesc],
        epoch: u64,
    ) -> Result<()> {
        let mut batch = self.keyspace.state_store().start_write_batch();
        let mut local = batch.prefixify(&self.keyspace);
        let column_ids = generate_column_id(column_descs);
        for (pk, cell_values) in rows {
            let arrange_key_buf = serialize_pk(&pk, self.pk_serializer.as_ref().unwrap())?;
            let bytes = self
                .cell_based_row_serializer
                .serialize(&arrange_key_buf, cell_values, column_ids.clone())
                .unwrap();
            for (key, value) in bytes {
                match value {
                    Some(val) => local.put(key, val),
                    None => local.delete(key),
                }
            }
        }
        batch.ingest(epoch).await?;
        Ok(())
    }

    // The returned iterator will iterate data from a snapshot corresponding to the given `epoch`
    pub async fn iter(&self, epoch: u64) -> Result<CellBasedTableRowIter<S>> {
        CellBasedTableRowIter::new(self.keyspace.clone(), self.column_descs.clone(), epoch).await
    }

    pub async fn get_for_test(&self, pk: Row, column_id: i32, epoch: u64) -> Result<Option<Datum>> {
        assert!(
            self.pk_serializer.is_some(),
            "this table is adhoc and there's no sort key serializer"
        );

        let column_id = ColumnId::new(column_id);

        let column_index = self.column_id_to_column_index.get(&column_id).unwrap();
        // TODO(MrCroxx): More efficient encoding is needed.

        let buf = self
            .keyspace
            .get(
                &[
                    &serialize_pk(&pk, self.pk_serializer.as_ref().unwrap())?[..],
                    &serialize_column_id(&column_id)?,
                ]
                .concat(),
                epoch,
            )
            .await
            .map_err(|err| ErrorCode::InternalError(err.to_string()))?;

        if let Some(buf) = buf {
            let mut de = memcomparable::Deserializer::new(buf);
            let cell = deserialize_cell(&mut de, &self.schema.fields[*column_index].data_type)?;
            Ok(Some(cell))
        } else {
            Ok(None)
        }
    }
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn generate_column_id_to_column_index_mapping(
    column_descs: &[ColumnDesc],
) -> HashMap<ColumnId, usize> {
    let mut mapping = HashMap::with_capacity(column_descs.len());
    for (index, column_desc) in column_descs.iter().enumerate() {
        mapping.insert(column_desc.column_id, index);
    }
    mapping
}

fn generate_column_id(column_descs: &[ColumnDesc]) -> Vec<ColumnId> {
    let mut column_ids = Vec::with_capacity(column_descs.len());
    for (index, column_desc) in column_descs.iter().enumerate() {
        column_ids.push(column_desc.column_id);
    }
    column_ids
}
// (st1page): May be we will have a "ChunkIter" trait which returns a chunk each time, so the name
// "RowTableIter" is reserved now
pub struct CellBasedTableRowIter<S: StateStore> {
    keyspace: Keyspace<S>,
    /// A buffer to store prefetched kv pairs from state store
    buf: Vec<(Bytes, Bytes)>,
    /// The idx into `buf` for the next item
    next_idx: usize,
    /// A bool to indicate whether there are more data to fetch from state store
    done: bool,
    /// Cached error messages after the iteration completes or fails
    err_msg: Option<String>,
    /// A epoch representing the read snapshot
    epoch: u64,
    /// Cell-based row deserializer
    cell_based_row_deserializer: CellBasedRowDeserializer,
}

impl<S: StateStore> CellBasedTableRowIter<S> {
    const SCAN_LIMIT: usize = 1024;

    async fn new(keyspace: Keyspace<S>, table_descs: Vec<ColumnDesc>, epoch: u64) -> Result<Self> {
        keyspace.state_store().wait_epoch(epoch).await;

        let cell_based_row_deserializer = CellBasedRowDeserializer::new(table_descs);

        let iter = Self {
            keyspace,
            buf: vec![],
            next_idx: 0,
            done: false,
            err_msg: None,
            epoch,
            cell_based_row_deserializer,
        };
        Ok(iter)
    }
    async fn consume_more(&mut self) -> Result<()> {
        assert_eq!(self.next_idx, self.buf.len());

        if self.buf.is_empty() {
            self.buf = self
                .keyspace
                .scan(Some(Self::SCAN_LIMIT), self.epoch)
                .await?;
        } else {
            let last_key = self.buf.last().unwrap().0.clone();
            let buf = self
                .keyspace
                .scan_with_start_key(last_key.to_vec(), Some(Self::SCAN_LIMIT), self.epoch)
                .await?;
            assert!(!buf.is_empty());
            assert_eq!(buf.first().as_ref().unwrap().0, last_key);
            self.buf = buf[1..].to_vec();
        }

        self.next_idx = 0;

        Ok(())
    }

    pub async fn collect_data_chunk(
        &mut self,
        cell_based_table: &CellBasedTable<S>,
        chunk_size: Option<usize>,
    ) -> Result<Option<DataChunk>> {
        let schema = &cell_based_table.schema;
        let mut builders = schema.create_array_builders(chunk_size.unwrap_or(0))?;

        let mut row_count = 0;
        for _ in 0..chunk_size.unwrap_or(usize::MAX) {
            match self.next().await? {
                Some(row) => {
                    for (datum, builder) in row.0.into_iter().zip_eq(builders.iter_mut()) {
                        builder.append_datum(&datum)?;
                    }
                    row_count += 1;
                }
                None => break,
            }
        }

        let chunk = if schema.is_empty() {
            // Generate some dummy data to ensure a correct cardinality, which might be used by
            // count(*).
            DataChunk::new_dummy(row_count)
        } else {
            let columns: Vec<Column> = builders
                .into_iter()
                .map(|builder| builder.finish().map(|a| Column::new(Arc::new(a))))
                .try_collect()?;
            DataChunk::builder().columns(columns).build()
        };

        if chunk.cardinality() == 0 {
            Ok(None)
        } else {
            Ok(Some(chunk))
        }
    }
}

#[async_trait::async_trait]
impl<S: StateStore> TableIter for CellBasedTableRowIter<S> {
    async fn next(&mut self) -> Result<Option<Row>> {
        if self.done {
            match &self.err_msg {
                Some(e) => return Err(ErrorCode::InternalError(e.clone()).into()),
                None => return Ok(None),
            }
        }

        loop {
            let (key, value) = match self.buf.get(self.next_idx) {
                Some(kv) => kv,
                None => {
                    // Need to consume more from state store
                    self.consume_more().await?;
                    if let Some(item) = self.buf.first() {
                        item
                    } else {
                        let pk_and_row = self.cell_based_row_deserializer.take();
                        self.done = true;
                        return Ok(pk_and_row.map(|(_pk, row)| row));
                    }
                }
            };
            tracing::trace!(
                target: "events::storage::CellBasedTable::scan",
                "CellBasedTable scanned key = {:?}, value = {:?}",
                bytes::Bytes::copy_from_slice(key),
                bytes::Bytes::copy_from_slice(value)
            );

            // there is no need to deserialize pk in cell-based table
            if key.len() < self.keyspace.key().len() + 4 {
                return Err(ErrorCode::InternalError("corrupted key".to_owned()).into());
            }

            let pk_and_row = self.cell_based_row_deserializer.deserialize(key, value)?;
            self.next_idx += 1;
            match pk_and_row {
                Some(_) => return Ok(pk_and_row.map(|(_pk, row)| row)),
                None => {}
            }
        }
    }
}
