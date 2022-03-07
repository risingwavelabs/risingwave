#![allow(dead_code)]
#![allow(unused)]
use risingwave_common::array::Row;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::Result;
use risingwave_common::types::Datum;
use risingwave_common::util::ordered::*;

use super::TableIter;
use crate::{ColumnDesc, IndexDesc, Keyspace, StateStore};

/// `RowTable` is the interface accessing relational data in KV(`StateStore`) with encoding format:
/// [keyspace | pk | `column_id` (4B)] -> value.
/// if the key of the column id does not exist, it will be Null in the relation
pub struct RowTable<S: StateStore> {
    keyspace: Keyspace<S>,
    pk: Vec<IndexDesc>,
    pk_serializer: OrderedRowSerializer,
}

impl<S: StateStore> RowTable<S> {
    pub fn new(keyspace: Keyspace<S>, pk: Vec<IndexDesc>) -> Self {
        todo!()
    }

    pub async fn get(&self, pk: Row, column: ColumnDesc, epoch: u64) -> Result<Option<Datum>> {
        todo!()
    }

    pub async fn get_row(&self, pk: Row, columns: Vec<ColumnDesc>, epoch: u64) -> Result<Row> {
        todo!()
    }
}
// (st1page): May be we will have a "ChunkIter" trait which returns a chunk each time, so the name
// "RowTableIter" is reserved now
pub struct RowTableRowIter<S: StateStore> {
    keyspace: Keyspace<S>,
    epoch: u64,
    // TODO: some field will be used here to maintain the iter states
}

impl<'a, S: StateStore> RowTableRowIter<S> {
    async fn new(keyspace: Keyspace<S>, columns: Vec<ColumnDesc>, epoch: u64) -> Result<Self> {
        todo!()
    }
    pub async fn next(&mut self) -> Result<Option<Row>> {
        todo!()
    }
}
