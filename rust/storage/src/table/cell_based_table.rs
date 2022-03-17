#![allow(dead_code)]
#![allow(unused)]

use std::ops::Index;

use bytes::Bytes;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId, OrderedColumnDesc};
use risingwave_common::error::{ErrorCode, Result};
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
            cell_based_row_serializer: CellBasedRowSerializer::new(),
            column_ids,
        }
    }

    pub async fn get(&self, pk: Row, column: &ColumnDesc, epoch: u64) -> Result<Option<Datum>> {
        let arrange_key_buf = serialize_pk(&pk, &self.pk_serializer)?;
        let key = Bytes::from(arrange_key_buf);
        let state_store_get_res = self
            .keyspace
            .state_store()
            .get(&key.clone(), epoch)
            .await
            .unwrap();
        let ax = column;
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

    pub async fn delete_row(&mut self, pk: Row, epoch: u64) -> Result<()> {
        let mut batch = self.keyspace.state_store().start_write_batch();
        let mut local = batch.prefixify(&self.keyspace);
        let arrange_key_buf = serialize_pk(&pk, &self.pk_serializer)?;
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

    pub async fn update_row(&mut self, pk: Row, cell_value: Option<Row>, epoch: u64) -> Result<()> {
        let mut batch = self.keyspace.state_store().start_write_batch();
        let mut local = batch.prefixify(&self.keyspace);
        let arrange_key_buf = serialize_pk(&pk, &self.pk_serializer)?;
        let column_ids = self.column_ids.clone();
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

    // The returned iterator will iterate data from a snapshot corresponding to the given `epoch`
    pub async fn iter(&self, epoch: u64) -> Result<CellBasedTableRowIter<S>> {
        CellBasedTableRowIter::new(
            self.keyspace.clone(),
            vec![self.pk[0].column_desc.clone()],
            epoch,
        )
        .await
    }
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
