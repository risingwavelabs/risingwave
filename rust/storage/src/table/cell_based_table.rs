#![allow(dead_code)]
#![allow(unused)]
use bytes::Bytes;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId, OrderedColumnDesc};
use risingwave_common::error::Result;
use risingwave_common::types::Datum;
use risingwave_common::util::ordered::*;

use super::TableIter;
use crate::cell_based_row_deserializer::CellBasedRowDeserializer;
use crate::cell_based_row_serializer::CellBasedRowSerializer;
use crate::{Keyspace, StateStore};

/// `CellBasedTable` is the interface accessing relational data in KV(`StateStore`) with encoding
/// format: [keyspace | pk | `column_id` (4B)] -> value.
/// if the key of the column id does not exist, it will be Null in the relation
pub struct CellBasedTable<S: StateStore> {
    keyspace: Keyspace<S>,
    pk: Vec<OrderedColumnDesc>,
    pk_serializer: OrderedRowSerializer,
    cell_based_row_serializer: CellBasedRowSerializer,
    column_ids: Vec<ColumnId>,
}

impl<S: StateStore> CellBasedTable<S> {
    pub fn new(
        keyspace: Keyspace<S>,
        pk: Vec<OrderedColumnDesc>,
        pk_serializer: OrderedRowSerializer,
        column_ids: Vec<ColumnId>,
    ) -> Self {
        Self {
            keyspace,
            pk,
            pk_serializer,
            cell_based_row_serializer: CellBasedRowSerializer::new(column_ids.clone()),
            column_ids,
        }
    }

    pub async fn get(&self, pk: Row, column: &ColumnDesc, epoch: u64) -> Result<Option<Datum>> {
        todo!()
    }

    pub async fn get_row(&self, pk: Row, column: &[ColumnDesc], epoch: u64) -> Result<Row> {
        let arrange_key_buf = serialize_pk(&pk, &self.pk_serializer)?;
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
        cell_values: Option<Row>,
        epoch: u64,
    ) -> Result<()> {
        let mut batch = self.keyspace.state_store().start_write_batch();
        let mut local = batch.prefixify(&self.keyspace);
        let arrange_key_buf = serialize_pk(&pk, &self.pk_serializer)?;
        let column_ids = self.column_ids.clone();
        let bytes = self
            .cell_based_row_serializer
            .serialize(&arrange_key_buf, cell_values, column_ids)
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

    pub async fn batch_insert_row(
        &mut self,
        rows: Vec<(Row, Option<Row>)>,
        epoch: u64,
    ) -> Result<()> {
        let mut batch = self.keyspace.state_store().start_write_batch();
        let mut local = batch.prefixify(&self.keyspace);
        for (pk, cell_values) in rows {
            let arrange_key_buf = serialize_pk(&pk, &self.pk_serializer)?;
            let column_ids = self.column_ids.clone();
            let bytes = self
                .cell_based_row_serializer
                .serialize(&arrange_key_buf, cell_values, column_ids)
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
}
// (st1page): May be we will have a "ChunkIter" trait which returns a chunk each time, so the name
// "RowTableIter" is reserved now
pub struct CellBasedTableRowIter<S: StateStore> {
    keyspace: Keyspace<S>,
    epoch: u64,
    // TODO: some field will be used here to maintain the iter states
}

impl<'a, S: StateStore> CellBasedTableRowIter<S> {
    async fn new(keyspace: Keyspace<S>, columns: Vec<ColumnDesc>, epoch: u64) -> Result<Self> {
        todo!()
    }
    pub async fn next(&mut self) -> Result<Option<Row>> {
        todo!()
    }
}
