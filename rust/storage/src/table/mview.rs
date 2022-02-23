use std::borrow::Cow;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::array::Row;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::Datum;
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;

use super::TableIterRef;
use crate::table::{ScannableTable, TableIter};
use crate::{Keyspace, StateStore, TableColumnDesc};

/// `MViewTable` provides a readable cell-based row table interface,
/// so that data can be queried by AP engine.
pub struct MViewTable<S: StateStore> {
    keyspace: Keyspace<S>,

    schema: Schema,

    column_descs: Vec<TableColumnDesc>,

    pk_columns: Vec<usize>,

    sort_key_serializer: OrderedRowsSerializer,
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
        orderings: Vec<OrderType>,
    ) -> Self {
        let order_pairs = orderings
            .into_iter()
            .zip_eq(pk_columns.clone().into_iter())
            .collect::<Vec<_>>();

        let column_descs = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(column_index, f)| {
                // For mview, column id is exactly the index, so we perform conversion here.
                TableColumnDesc::new_without_name(column_index as i32, f.data_type.clone())
            })
            .collect_vec();

        Self {
            keyspace,
            schema,
            column_descs,
            pk_columns,
            sort_key_serializer: OrderedRowsSerializer::new(order_pairs),
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
        let order_pairs = vec![(OrderType::Ascending, 0)];

        Self {
            keyspace,
            schema,
            column_descs,
            pk_columns,
            sort_key_serializer: OrderedRowsSerializer::new(order_pairs),
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
            self.schema.clone(),
            self.pk_columns.clone(),
            epoch,
        )
        .await
    }

    // TODO(MrCroxx): More interfaces are needed besides cell get.
    // The returned Datum is from a snapshot corresponding to the given `epoch`
    pub async fn get(&self, pk: Row, cell_idx: usize, epoch: u64) -> Result<Option<Datum>> {
        debug_assert!(cell_idx < self.schema.len());
        // TODO(MrCroxx): More efficient encoding is needed.

        let buf = self
            .keyspace
            .get(
                &[
                    &serialize_pk(&pk, &self.sort_key_serializer)?[..],
                    &serialize_cell_idx(cell_idx as u32)?[..],
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
}

impl<'a, S: StateStore> MViewTableIter<S> {
    // TODO: adjustable limit
    const SCAN_LIMIT: usize = 1024;

    async fn new(
        keyspace: Keyspace<S>,
        schema: Schema,
        pk_columns: Vec<usize>,
        epoch: u64,
    ) -> Result<Self> {
        let iter = Self {
            keyspace,
            schema,
            pk_columns,
            buf: vec![],
            next_idx: 0,
            done: false,
            err_msg: None,
            epoch,
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

        let mut pk_buf = vec![];
        let mut restored = 0;
        let mut row = vec![];
        loop {
            let (key, value) = match self.buf.get(self.next_idx) {
                Some(kv) => kv,
                None => {
                    // Need to consume more from state store
                    self.consume_more().await?;
                    if let Some(item) = self.buf.first() {
                        item
                    } else if restored == 0 {
                        // No more items
                        self.done = true;
                        return Ok(None);
                    } else {
                        // current item is incomplete
                        self.done = true;
                        self.err_msg = Some(String::from("incomplete item"));
                        return Err(ErrorCode::InternalError(
                            self.err_msg.as_ref().unwrap().clone(),
                        )
                        .into());
                    }
                }
            };

            self.next_idx += 1;

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

            let cur_pk_buf = &key[self.keyspace.key().len()..key.len() - 4];
            if restored == 0 {
                pk_buf = cur_pk_buf.to_owned();
            } else if pk_buf != cur_pk_buf {
                return Err(ErrorCode::InternalError("primary key incorrect".to_owned()).into());
            }

            let datum = deserialize_cell(&value[..], &self.schema.data_types()[restored])?;
            row.push(datum);

            restored += 1;
            if restored == self.schema.len() {
                break;
            }
        }
        Ok(Some(Row::new(row)))
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
