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

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

use anyhow::Context;
use parking_lot::RwLock;
use risingwave_common::array::StreamChunk;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, TableId, TableVersionId};
use risingwave_common::error::Result;
use tokio::sync::oneshot;

use crate::{TableDmlHandle, TableDmlHandleRef};

pub type DmlManagerRef = Arc<DmlManager>;

#[derive(Debug)]
struct TableReader {
    version_id: TableVersionId,
    handle: Weak<TableDmlHandle>,
}

/// [`DmlManager`] manages the communication between batch data manipulation and streaming
/// processing.
/// NOTE: `TableDmlHandle` is used here as an out-of-the-box solution. We should further optimize
/// its implementation (e.g. directly expose a channel instead of offering a `write_chunk`
/// interface).
#[derive(Default, Debug)]
pub struct DmlManager {
    table_readers: RwLock<HashMap<TableId, TableReader>>,
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
        table_version_id: TableVersionId,
        column_descs: &[ColumnDesc],
    ) -> Result<TableDmlHandleRef> {
        let mut table_readers = self.table_readers.write();

        // Clear invalid table readers.
        table_readers.drain_filter(|_, r| r.handle.strong_count() == 0);

        macro_rules! new_handle {
            ($entry:ident) => {{
                let handle = Arc::new(TableDmlHandle::new(column_descs.to_vec()));
                $entry.insert(TableReader {
                    version_id: table_version_id,
                    handle: Arc::downgrade(&handle),
                });
                handle
            }};
        }

        let handle = match table_readers.entry(table_id) {
            // Create a new reader. This happens when the first `DmlExecutor` of this table is
            // activated on this compute node.
            Entry::Vacant(v) => new_handle!(v),

            Entry::Occupied(mut o) => {
                let TableReader { version_id, handle } = o.get();

                match table_version_id.cmp(version_id) {
                    // This should never happen as the schema change is guaranteed to happen after a
                    // table is successfully created and all the readers are registered.
                    Ordering::Less => unreachable!("table version `{table_version_id}` expired"),

                    // Register with the correct version. This happens when the following
                    // `DmlExecutor`s of this table is activated on this compute
                    // node.
                    Ordering::Equal => handle.upgrade().with_context(|| {
                        format!("fail to register reader for table with key `{table_id:?}`")
                    })?,

                    // A new version of the table is activated, overwrite the old reader.
                    Ordering::Greater => new_handle!(o),
                }
            }
        };

        Ok(handle)
    }

    pub fn write_chunk(
        &self,
        table_id: TableId,
        table_version_id: TableVersionId,
        chunk: StreamChunk,
    ) -> Result<oneshot::Receiver<usize>> {
        let handle = {
            let table_readers = self.table_readers.read();

            match table_readers.get(&table_id) {
                Some(TableReader { version_id, handle }) => {
                    match table_version_id.cmp(version_id) {
                        // A new version of the table is activated, but the DML request is still on
                        // the old version.
                        Ordering::Less => {
                            bail!("schema changed for table `{table_id:?}`, please retry later")
                        }

                        // Write the chunk of correct version to the table.
                        Ordering::Equal => handle.upgrade(),

                        // This should never happen as the notifaction of the new version is
                        // guaranteed to happen after all new readers are activated.
                        Ordering::Greater => {
                            unreachable!("table version `{table_version_id} not registered")
                        }
                    }
                }
                None => None,
            }
        }
        .with_context(|| format!("no reader for dml in table `{table_id:?}`"))?;

        handle.write_chunk(chunk)
    }

    pub fn clear(&self) {
        self.table_readers.write().clear()
    }
}
