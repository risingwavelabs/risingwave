use std::borrow::Cow;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::{Row, RowDeserializer};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{deserialize_datum_from, Datum};
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;

use super::TableIterRef;
use crate::table::{ScannableTable, TableIter};
use crate::{Keyspace, StateStore, StateStoreIter, TableColumnDesc};

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
                // For materialized view, column id is exactly the index, so we perform conversion
                // here.
                TableColumnDesc::new_without_name(column_index as i32, f.data_type)
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
                .map(|c| Field::with_name(c.data_type, c.name.clone()))
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

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    // TODO(MrCroxx): remove me after iter is impled.
    pub fn storage(&self) -> S {
        self.keyspace.state_store()
    }

    // TODO(MrCroxx): Refactor this after statestore iter is finished.
    pub async fn iter(&self) -> Result<MViewTableIter<S>> {
        Ok(MViewTableIter::new(
            self.keyspace.iter().await?,
            self.keyspace.key().to_owned(),
            self.schema.clone(),
            self.pk_columns.clone(),
        ))
    }

    // TODO(MrCroxx): More interfaces are needed besides cell get.
    pub async fn get(&self, pk: Row, cell_idx: usize) -> Result<Option<Datum>> {
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
            )
            .await
            .map_err(|err| ErrorCode::InternalError(err.to_string()))?;

        match buf {
            Some(buf) => {
                let mut deserializer = memcomparable::Deserializer::new(buf);
                let datum = deserialize_datum_from(
                    &self.schema.fields[cell_idx].data_type,
                    &mut deserializer,
                )?;
                Ok(Some(datum))
            }
            None => Ok(None),
        }
    }
}

pub struct MViewTableIter<S: StateStore> {
    inner: S::Iter,
    prefix: Vec<u8>,
    schema: Schema,
    // TODO: why pk_columns is not used??
    #[allow(dead_code)]
    pk_columns: Vec<usize>,
}

impl<S: StateStore> MViewTableIter<S> {
    fn new(inner: S::Iter, prefix: Vec<u8>, schema: Schema, pk_columns: Vec<usize>) -> Self {
        Self {
            inner,
            prefix,
            schema,
            pk_columns,
        }
    }
}

#[async_trait::async_trait]
impl<S: StateStore> TableIter for MViewTableIter<S> {
    async fn next(&mut self) -> Result<Option<Row>> {
        let mut pk_buf = vec![];
        let mut restored = 0;
        let mut row_bytes = vec![];
        loop {
            match self.inner.next().await? {
                Some((key, value)) => {
                    log::trace!(
                        "scanned key = {:?}, value = {:?}",
                        bytes::Bytes::copy_from_slice(&key),
                        bytes::Bytes::copy_from_slice(&value)
                    );

                    // there is no need to deserialize pk in mview
                    if key.len() < self.prefix.len() + 4 {
                        return Err(ErrorCode::InternalError("corrupted key".to_owned()).into());
                    }

                    let cur_pk_buf = &key[self.prefix.len()..key.len() - 4];
                    if restored == 0 {
                        pk_buf = cur_pk_buf.to_owned();
                    } else if pk_buf != cur_pk_buf {
                        return Err(
                            ErrorCode::InternalError("primary key incorrect".to_owned()).into()
                        );
                    }

                    row_bytes.extend_from_slice(&value);

                    restored += 1;
                    if restored == self.schema.len() {
                        break;
                    }

                    // continue loop
                }
                // no more item
                None if restored == 0 => return Ok(None),
                // current item is incomplete
                None => {
                    return Err(ErrorCode::InternalError("incomplete item".to_owned()).into());
                }
            }
        }
        let row_deserializer = RowDeserializer::new(self.schema.data_types());
        let row = row_deserializer.deserialize(&row_bytes)?;
        Ok(Some(row))
    }
}

#[async_trait::async_trait]
impl<S> ScannableTable for MViewTable<S>
where
    S: StateStore,
{
    async fn iter(&self) -> Result<TableIterRef> {
        Ok(Box::new(self.iter().await?))
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
}
