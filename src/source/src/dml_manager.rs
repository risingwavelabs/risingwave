use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{ColumnDesc, TableId};
use risingwave_common::error::Result;
use tokio::sync::oneshot;

use crate::{TableSource, TableSourceRef};

pub type DmlManagerRef = Arc<DmlManager>;

#[derive(Default)]
pub struct DmlManager {
    senders: Mutex<HashMap<TableId, TableSourceRef>>,
}

impl DmlManager {
    pub fn new() -> Self {
        Self {
            senders: Mutex::new(HashMap::new()),
        }
    }

    pub fn register_reader(
        &self,
        table_id: &TableId,
        column_descs: &[ColumnDesc],
    ) -> TableSourceRef {
        let mut senders = self.senders.lock();
        if !senders.contains_key(table_id) {
            let sender = Arc::new(TableSource::new(column_descs.to_vec()));
            senders.insert(*table_id, sender);
        }
        senders.get(table_id).unwrap().clone()
    }

    pub fn write_chunk(
        &self,
        table_id: &TableId,
        chunk: StreamChunk,
    ) -> Result<oneshot::Receiver<usize>> {
        let senders = self.senders.lock();
        senders.get(table_id).unwrap().write_chunk(chunk)
    }
}
