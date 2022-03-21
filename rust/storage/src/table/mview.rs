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
use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, Row};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::Datum;
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;

use crate::cell_based_row_deserializer::CellBasedRowDeserializer;
use crate::table::TableIter;
use crate::{Keyspace, StateStore};

/// `MViewTable` provides a readable cell-based row table interface,
/// so that data can be queried by AP engine.
///
/// Note that `MViewTable` is sort of a view on the original table.
/// `MViewTable` only return certain columns to its callers instead of all the columns that
/// associated with the keyspace.
#[derive(Clone)]
pub struct MViewTable<S: StateStore> {
    /// The keyspace that the pk and value of the original table has
    keyspace: Keyspace<S>,

    /// The schema of this table viewed by some source executor, e.g. RowSeqScanExecutor.
    schema: Schema,

    /// ColumnDesc contains strictly more info than `schema`.
    column_descs: Vec<ColumnDesc>,

    /// Mapping from column Id to column index
    column_id_to_column_index: HashMap<ColumnId, usize>,

    /// This is used for testing only.
    ordered_row_serializer: Option<OrderedRowSerializer>,
}

impl<S: StateStore> std::fmt::Debug for MViewTable<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MViewTable")
            .field("column_descs", &self.column_descs)
            .finish()
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

impl<S: StateStore> MViewTable<S> {
    fn new(
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
        Self {
            keyspace,
            schema,
            column_descs,
            column_id_to_column_index,
            ordered_row_serializer,
        }
    }

    /// Create a [`MViewTable`] for materialized view.
    // TODO: refactor into `RowTable`.
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

    /// Create an "adhoc" [`MViewTable`] with specified columns.
    // TODO: refactor into `RowTable`.
    pub fn new_adhoc(keyspace: Keyspace<S>, column_descs: Vec<ColumnDesc>) -> Self {
        Self::new(keyspace, column_descs, None)
    }

    // The returned iterator will iterate data from a snapshot corresponding to the given `epoch`
    pub async fn iter(&self, epoch: u64) -> Result<MViewTableIter<S>> {
        MViewTableIter::new(self.keyspace.clone(), self.column_descs.clone(), epoch).await
    }

    // TODO(MrCroxx): More interfaces are needed besides cell get.
    // The returned Datum is from a snapshot corresponding to the given `epoch`
    // TODO(eric): remove this...
    // TODO(bugen): remove this...
    pub async fn get_for_test(&self, pk: Row, column_id: i32, epoch: u64) -> Result<Option<Datum>> {
        assert!(
            self.ordered_row_serializer.is_some(),
            "this table is adhoc and there's no sort key serializer"
        );

        let column_id = ColumnId::new(column_id);

        let column_index = self.column_id_to_column_index.get(&column_id).unwrap();
        // TODO(MrCroxx): More efficient encoding is needed.

        let buf = self
            .keyspace
            .get(
                &[
                    &serialize_pk(&pk, self.ordered_row_serializer.as_ref().unwrap())?[..],
                    &serialize_column_id(&column_id)?,
                ]
                .concat(),
                epoch,
            )
            .await
            .map_err(|err| ErrorCode::InternalError(err.to_string()))?;

        if let Some(buf) = buf {
            Ok(Some(deserialize_cell(
                &buf[..],
                &self.schema.fields[*column_index].data_type,
            )?))
        } else {
            Ok(None)
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

pub struct MViewTableIter<S: StateStore> {
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

impl<S: StateStore> MViewTableIter<S> {
    // TODO: adjustable limit
    const SCAN_LIMIT: usize = 1024;

    async fn new(keyspace: Keyspace<S>, table_descs: Vec<ColumnDesc>, epoch: u64) -> Result<Self> {
        keyspace.state_store().wait_epoch(epoch).await?;

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
            // TODO: remove the unnecessary clone here
            self.buf = buf[1..].to_vec();
        }

        self.next_idx = 0;

        Ok(())
    }

    pub async fn collect_data_chunk(
        &mut self,
        mview_table: &MViewTable<S>,
        chunk_size: Option<usize>,
    ) -> Result<Option<DataChunk>> {
        let schema = &mview_table.schema;
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
impl<S: StateStore> TableIter for MViewTableIter<S> {
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
                target: "events::stream::mview::scan",
                "mview scanned key = {:?}, value = {:?}",
                bytes::Bytes::copy_from_slice(key),
                bytes::Bytes::copy_from_slice(value)
            );

            // there is no need to deserialize pk in mview
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
