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
#[derive(Clone)]
pub struct MViewTable<S: StateStore> {
    keyspace: Keyspace<S>,

    schema: Schema,

    column_descs: Vec<ColumnDesc>,

    // TODO(bugen): this field is redundant since table should be scan-only
    pk_columns: Vec<usize>,

    // TODO(bugen): this field is redundant since table should be scan-only
    sort_key_serializer: OrderedRowSerializer,
}

impl<S: StateStore> std::fmt::Debug for MViewTable<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MViewTable")
            .field("schema", &self.schema)
            .field("pk_columns", &self.pk_columns)
            .finish()
    }
}

impl<S: StateStore> MViewTable<S> {
    /// Create a [`MViewTable`] for materialized view.
    // TODO(bugen): remove this...
    pub fn new_for_test(
        keyspace: Keyspace<S>,
        schema: Schema,
        pk_columns: Vec<usize>,
        order_types: Vec<OrderType>,
    ) -> Self {
        let column_descs = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(column_index, f)| {
                // For mview, column id is exactly the index, so we perform conversion here.
                let column_id = ColumnId::from(column_index as i32);
                ColumnDesc::unnamed(column_id, f.data_type.clone())
            })
            .collect_vec();

        Self {
            keyspace,
            schema,
            column_descs,
            pk_columns,
            sort_key_serializer: OrderedRowSerializer::new(order_types),
        }
    }

    /// Create an "adhoc" [`MViewTable`] with specified columns.
    // TODO: remove this and refactor into `RowTable`.
    pub fn new_adhoc(keyspace: Keyspace<S>, column_ids: &[ColumnId], fields: &[Field]) -> Self {
        let schema = Schema::new(fields.to_vec());
        let column_descs = column_ids
            .iter()
            .zip_eq(fields.iter())
            .map(|(column_id, field)| ColumnDesc::unnamed(*column_id, field.data_type.clone()))
            .collect();

        Self {
            keyspace,
            schema,
            column_descs,
            pk_columns: vec![],
            sort_key_serializer: OrderedRowSerializer::new(vec![]),
        }
    }

    // The returned iterator will iterate data from a snapshot corresponding to the given `epoch`
    pub async fn iter(&self, epoch: u64) -> Result<MViewTableIter<S>> {
        MViewTableIter::new(self.keyspace.clone(), self.column_descs.clone(), epoch).await
    }

    // TODO(MrCroxx): More interfaces are needed besides cell get.
    // The returned Datum is from a snapshot corresponding to the given `epoch`
    // TODO(eric): remove this...
    // TODO(bugen): remove this...
    pub async fn get_for_test(
        &self,
        pk: Row,
        cell_idx: usize,
        epoch: u64,
    ) -> Result<Option<Datum>> {
        assert!(
            !self.pk_columns.is_empty(),
            "this table is adhoc and there's no pk information"
        );

        debug_assert!(cell_idx < self.schema.len());
        // TODO(MrCroxx): More efficient encoding is needed.

        let buf = self
            .keyspace
            .get(
                &[
                    &serialize_pk(&pk, &self.sort_key_serializer)?[..],
                    &serialize_column_id(&ColumnId::from(cell_idx as i32))?
                    // &serialize_cell_idx(cell_idx as i32)?[..],
                ]
                .concat(),
                epoch,
            )
            .await
            .map_err(|err| ErrorCode::InternalError(err.to_string()))?;

        if let Some(buf) = buf {
            Ok(Some(deserialize_cell(
                &buf[..],
                &self.schema.fields[cell_idx].data_type,
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
            // TODO: remove the unnecessary clone here
            self.buf = buf[1..].to_vec();
        }

        self.next_idx = 0;

        Ok(())
    }

    pub async fn collect_datachunk_from_iter(
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

        let chunk = if schema.len() == 0 {
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
