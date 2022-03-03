use std::borrow::Cow;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnId, Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::Datum;
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;

use super::TableIterRef;
use crate::cell_based_row_deserializer::CellBasedRowDeserializer;
use crate::table::{ScannableTable, TableIter};
use crate::{Keyspace, StateStore, TableColumnDesc};

/// `MViewTable` provides a readable cell-based row table interface,
/// so that data can be queried by AP engine.
pub struct MViewTable<S: StateStore> {
    keyspace: Keyspace<S>,

    schema: Schema,

    column_descs: Vec<TableColumnDesc>,

    pk_columns: Vec<usize>,

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
    pub fn new(
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
                TableColumnDesc::unnamed(column_id, f.data_type.clone())
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

    /// Create a [`MViewTable`] for batch table.
    pub fn new_batch(keyspace: Keyspace<S>, column_descs: Vec<TableColumnDesc>) -> Self {
        let schema = {
            let fields = column_descs
                .iter()
                .map(|c| Field::with_name(c.data_type.clone(), c.name.clone()))
                .collect();
            Schema::new(fields)
        };

        // row id will be inserted at first column in `InsertExecutor`
        // FIXME: should we check `is_primary` in pb `ColumnDesc` after we support pk?
        let pk_columns = vec![0];
        let order_types = vec![OrderType::Ascending];

        Self {
            keyspace,
            schema,
            column_descs,
            pk_columns,
            sort_key_serializer: OrderedRowSerializer::new(order_types),
        }
    }

    // TODO(MrCroxx): remove me after iter is impled.
    pub fn storage(&self) -> S {
        self.keyspace.state_store()
    }

    // TODO(MrCroxx): Refactor this after statestore iter is finished.
    // The returned iterator will iterate data from a snapshot corresponding to the given `epoch`
    async fn iter(&self, epoch: u64) -> Result<MViewTableIter<S>> {
        MViewTableIter::new(
            self.keyspace.clone(),
            self.column_descs.clone(),
            self.schema.clone(),
            self.pk_columns.clone(),
            epoch,
        )
        .await
    }

    // TODO(MrCroxx): More interfaces are needed besides cell get.
    // The returned Datum is from a snapshot corresponding to the given `epoch`
    // TODO(eric): remove this...
    pub async fn get(&self, pk: Row, cell_idx: usize, epoch: u64) -> Result<Option<Datum>> {
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
}

pub struct MViewTableIter<S: StateStore> {
    keyspace: Keyspace<S>,
    #[allow(dead_code)]
    // TODO: MViewTableIter will be discarded later?
    schema: Schema,
    // TODO: why pk_columns is not used??
    #[allow(dead_code)]
    pk_columns: Vec<usize>,
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

impl<'a, S: StateStore> MViewTableIter<S> {
    // TODO: adjustable limit
    const SCAN_LIMIT: usize = 1024;

    async fn new(
        keyspace: Keyspace<S>,
        table_descs: Vec<TableColumnDesc>,
        schema: Schema,
        pk_columns: Vec<usize>,
        epoch: u64,
    ) -> Result<Self> {
        keyspace.state_store().wait_epoch(epoch).await;

        let cell_based_row_deserializer = CellBasedRowDeserializer::new(table_descs);

        let iter = Self {
            keyspace,
            schema,
            pk_columns,
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

#[async_trait::async_trait]
impl<S> ScannableTable for MViewTable<S>
where
    S: StateStore,
{
    async fn iter(&self, epoch: u64) -> Result<TableIterRef> {
        Ok(Box::new(self.iter(epoch).await?))
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Sync + Send> {
        self
    }

    fn schema(&self) -> Cow<Schema> {
        Cow::Borrowed(&self.schema)
    }

    fn column_descs(&self) -> Cow<[TableColumnDesc]> {
        Cow::Borrowed(&self.column_descs)
    }

    fn is_shared_storage(&self) -> bool {
        true
    }
}
