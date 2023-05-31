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
use risingwave_common::transaction::transaction_message::TxnMsg;
use risingwave_common::transaction::TxnId;
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

    /// Register a new DML reader for a table. If the reader for this version of the table already
    /// exists, returns a reference to the existing reader.
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
                    Ordering::Equal => handle
                        .upgrade()
                        .inspect(|handle| {
                            assert_eq!(
                                handle.column_descs(),
                                column_descs,
                                "dml handler registers with same version but different schema"
                            )
                        })
                        .with_context(|| {
                            format!("fail to register reader for table with key `{table_id:?}`")
                        })?,

                    // A new version of the table is activated, overwrite the old reader.
                    Ordering::Greater => new_handle!(o),
                }
            }
        };

        Ok(handle)
    }

    pub async fn write_txn_msg(
        &self,
        table_id: TableId,
        table_version_id: TableVersionId,
        txn_msg: TxnMsg,
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

                        // This should never happen as the notification of the new version is
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

        handle.write_txn_msg(txn_msg).await
    }

    pub fn clear(&self) {
        self.table_readers.write().clear()
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use risingwave_common::catalog::INITIAL_TABLE_VERSION_ID;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;

    use super::*;

    #[easy_ext::ext(DmlManagerTestExt)]
    impl DmlManager {
        /// Write a chunk and assert that the chunk channel is not blocking.
        pub fn write_chunk_ready(
            &self,
            table_id: TableId,
            table_version_id: TableVersionId,
            chunk: StreamChunk,
        ) -> Result<oneshot::Receiver<usize>> {
            const TEST_TRANSACTION_ID: TxnId = 1;
            self.write_txn_msg(
                table_id,
                table_version_id,
                TxnMsg::Begin(TEST_TRANSACTION_ID),
            )
            .now_or_never()
            .unwrap()?;
            let result = self
                .write_txn_msg(
                    table_id,
                    table_version_id,
                    TxnMsg::Data(TEST_TRANSACTION_ID, chunk),
                )
                .now_or_never()
                .unwrap();
            self.write_txn_msg(table_id, table_version_id, TxnMsg::End(TEST_TRANSACTION_ID))
                .now_or_never()
                .unwrap()?;
            result
        }
    }

    #[test]
    fn test_register_and_drop() {
        let dml_manager = DmlManager::new();
        let table_id = TableId::new(1);
        let table_version_id = INITIAL_TABLE_VERSION_ID;
        let column_descs = vec![ColumnDesc::unnamed(100.into(), DataType::Float64)];
        let chunk = || StreamChunk::from_pretty("F\n+ 1");

        let h1 = dml_manager
            .register_reader(table_id, table_version_id, &column_descs)
            .unwrap();
        let h2 = dml_manager
            .register_reader(table_id, table_version_id, &column_descs)
            .unwrap();

        // They should be the same handle.
        assert!(Arc::ptr_eq(&h1, &h2));

        // Start reading.
        let r1 = h1.stream_reader();
        let r2 = h2.stream_reader();

        // Should be able to write to the table.
        dml_manager
            .write_chunk_ready(table_id, table_version_id, chunk())
            .unwrap();

        // After dropping one reader, the other one should still be able to write.
        // This is to simulate the scale-in of DML executors.
        drop(r1);
        dml_manager
            .write_chunk_ready(table_id, table_version_id, chunk())
            .unwrap();

        // After dropping the last reader, no more writes are allowed.
        // This is to simulate the dropping of the table.
        drop(r2);
        dml_manager
            .write_chunk_ready(table_id, table_version_id, chunk())
            .unwrap_err();
    }

    #[test]
    fn test_versioned() {
        let dml_manager = DmlManager::new();
        let table_id = TableId::new(1);

        let old_version_id = INITIAL_TABLE_VERSION_ID;
        let old_column_descs = vec![ColumnDesc::unnamed(100.into(), DataType::Float64)];
        let old_chunk = || StreamChunk::from_pretty("F\n+ 1");

        let new_version_id = old_version_id + 1;
        let new_column_descs = vec![
            ColumnDesc::unnamed(100.into(), DataType::Float64),
            ColumnDesc::unnamed(101.into(), DataType::Float64),
        ];
        let new_chunk = || StreamChunk::from_pretty("F F\n+ 1 2");

        // Start reading.
        let old_h = dml_manager
            .register_reader(table_id, old_version_id, &old_column_descs)
            .unwrap();
        let _old_r = old_h.stream_reader();

        // Should be able to write to the table.
        dml_manager
            .write_chunk_ready(table_id, old_version_id, old_chunk())
            .unwrap();

        // Start reading the new version.
        let new_h = dml_manager
            .register_reader(table_id, new_version_id, &new_column_descs)
            .unwrap();
        let _new_r = new_h.stream_reader();

        // Should not be able to write to the old version.
        dml_manager
            .write_chunk_ready(table_id, old_version_id, old_chunk())
            .unwrap_err();
        // Should be able to write to the new version.
        dml_manager
            .write_chunk_ready(table_id, new_version_id, new_chunk())
            .unwrap();
    }

    #[test]
    #[should_panic]
    fn test_bad_schema() {
        let dml_manager = DmlManager::new();
        let table_id = TableId::new(1);
        let table_version_id = INITIAL_TABLE_VERSION_ID;

        let column_descs = vec![ColumnDesc::unnamed(100.into(), DataType::Float64)];
        let other_column_descs = vec![ColumnDesc::unnamed(101.into(), DataType::Float64)];

        let _h = dml_manager
            .register_reader(table_id, table_version_id, &column_descs)
            .unwrap();

        // Should panic as the schema is different.
        let _h = dml_manager
            .register_reader(table_id, table_version_id, &other_column_descs)
            .unwrap();
    }
}
