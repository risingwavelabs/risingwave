use std::sync::Arc;

use risingwave_common::array::{Row, RowDeserializer};
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{deserialize_datum_from, Datum};
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;

use super::TableIterRef;
use crate::table::{ScannableTable, TableIter};
use crate::{Keyspace, StateStore, StateStoreIter};

/// `MViewTable` provides a readable cell-based row table interface,
/// so that data can be queried by AP engine.
pub struct MViewTable<S: StateStore> {
    keyspace: Keyspace<S>,
    schema: Schema,
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
    pub fn new(
        keyspace: Keyspace<S>,
        schema: Schema,
        pk_columns: Vec<usize>,
        orderings: Vec<OrderType>,
    ) -> Self {
        let order_pairs = orderings
            .into_iter()
            .zip(pk_columns.clone().into_iter())
            .collect::<Vec<_>>();
        Self {
            keyspace,
            schema,
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
                    &self.schema.fields[cell_idx].data_type.data_type_kind(),
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
                    // there is no need to deserialize pk in mview

                    if key.len() < self.prefix.len() + 4 {
                        return Err(ErrorCode::InternalError("corrupted key".to_owned()).into());
                    }

                    let cur_pk_buf = &key[self.prefix.len()..key.len() - 4];
                    if restored == 0 {
                        pk_buf = cur_pk_buf.to_owned();
                    } else if pk_buf != cur_pk_buf {
                        // previous item is incomplete
                        return Err(ErrorCode::InternalError("incomplete item".to_owned()).into());
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
                None => return Err(ErrorCode::InternalError("incomplete item".to_owned()).into()),
            }
        }
        let schema = self
            .schema
            .data_types_clone()
            .into_iter()
            .map(|data_type| data_type.data_type_kind())
            .collect::<Vec<_>>();
        let row_deserializer = RowDeserializer::new(schema);
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

    fn schema(&self) -> Schema {
        self.schema.clone()
    }
}
