use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{ColumnDesc, TableId};
use risingwave_common::error::Result;
use tokio::sync::oneshot;

use crate::{TableSource, TableSourceRef};

pub type DmlManagerRef = Arc<DmlManager>;

/// [`DmlManager`] manages the communication between batch data manipulation and streaming
/// processing.
/// NOTE: `TableSource` is used here as an out-of-the-box solution. It should be renamed
/// as `BatchDml` later. We should further optimize its implementation (e.g. directly expose a
/// channel instead of offering a `write_chunk` interface).
#[derive(Default, Debug)]
pub struct DmlManager {
    batch_dmls: Mutex<HashMap<TableId, TableSourceRef>>,
}

impl DmlManager {
    pub fn new() -> Self {
        Self {
            batch_dmls: Mutex::new(HashMap::new()),
        }
    }

    pub fn register_reader(
        &self,
        table_id: &TableId,
        column_descs: &[ColumnDesc],
    ) -> TableSourceRef {
        let mut batch_dmls = self.batch_dmls.lock();
        if !batch_dmls.contains_key(table_id) {
            let batch_dml = Arc::new(TableSource::new(column_descs.to_vec()));
            batch_dmls.insert(*table_id, batch_dml);
        }
        batch_dmls.get(table_id).unwrap().clone()
    }

    pub fn write_chunk(
        &self,
        table_id: &TableId,
        chunk: StreamChunk,
    ) -> Result<oneshot::Receiver<usize>> {
        let batch_dmls = self.batch_dmls.lock();
        batch_dmls.get(table_id).unwrap().write_chunk(chunk)
    }
}
