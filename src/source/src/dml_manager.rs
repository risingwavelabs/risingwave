// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::RwLock;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{ColumnDesc, TableId};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use tokio::sync::oneshot;

use crate::{TableDmlHandle, TableDmlHandleRef};

pub type DmlManagerRef = Arc<DmlManager>;

/// [`DmlManager`] manages the communication between batch data manipulation and streaming
/// processing.
/// NOTE: `TableDmlHandle` is used here as an out-of-the-box solution. We should further optimize
/// its implementation (e.g. directly expose a channel instead of offering a `write_chunk`
/// interface).
#[derive(Default, Debug)]
pub struct DmlManager {
    table_readers: RwLock<HashMap<TableId, Weak<TableDmlHandle>>>,
}

impl DmlManager {
    pub fn new() -> Self {
        Self {
            table_readers: RwLock::new(HashMap::new()),
        }
    }

    pub fn register_reader(
        &self,
        table_id: TableId,
        column_descs: &[ColumnDesc],
    ) -> Result<TableDmlHandleRef> {
        let mut table_readers = self.table_readers.write();

        // Clear invalid table readers.
        table_readers.drain_filter(|_, weak_ref| weak_ref.strong_count() == 0);

        match table_readers.entry(table_id) {
            Entry::Occupied(o) => o.get().upgrade().ok_or_else(|| {
                InternalError(format!(
                    "fail to register reader for table with id {:?}",
                    table_id.table_id
                ))
                .into()
            }),
            Entry::Vacant(v) => {
                let reader = Arc::new(TableDmlHandle::new(column_descs.to_vec()));
                v.insert(Arc::downgrade(&reader));
                Ok(reader)
            }
        }
    }

    pub fn write_chunk(
        &self,
        table_id: &TableId,
        chunk: StreamChunk,
    ) -> Result<oneshot::Receiver<usize>> {
        let table_readers = self.table_readers.read();
        let writer = table_readers
            .get(table_id)
            .ok_or_else(|| {
                InternalError(format!(
                    "no reader for dml in table with id {:?}",
                    table_id.table_id
                ))
            })?
            .upgrade()
            .ok_or_else(|| {
                InternalError(format!(
                    "no reader for dml in table with id {:?}",
                    table_id.table_id
                ))
            })?;
        writer.write_chunk(chunk)
    }

    pub fn clear(&self) {
        self.table_readers.write().clear()
    }
}
