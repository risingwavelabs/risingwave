// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::Context;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use rand::seq::IteratorRandom;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::error::Result;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
struct TableSourceCore {
    /// The senders of the changes channel.
    ///
    /// When a `StreamReader` is created, a channel will be created and the sender will be
    /// saved here. The insert statement will take one channel randomly.
    changes_txs: Vec<mpsc::UnboundedSender<(StreamChunk, oneshot::Sender<usize>)>>,
}

/// [`TableSource`] is a special internal source to handle table updates from user,
/// including insert/delete/update statements via SQL interface.
///
/// Changed rows will be send to the associated "materialize" streaming task, then be written to the
/// state store. Therefore, [`TableSource`] can be simply be treated as a channel without side
/// effects.
#[derive(Debug)]
pub struct TableSource {
    core: RwLock<TableSourceCore>,

    /// All columns in this table.
    column_descs: Vec<ColumnDesc>,
}

impl TableSource {
    pub fn new(column_descs: Vec<ColumnDesc>) -> Self {
        let core = TableSourceCore {
            changes_txs: vec![],
        };

        Self {
            core: RwLock::new(core),
            column_descs,
        }
    }

    /// Asynchronously write stream chunk into table. Changes written here will be simply passed to
    /// the associated streaming task via channel, and then be materialized to storage there.
    ///
    /// Returns an oneshot channel which will be notified when the chunk is taken by some reader,
    /// and the `usize` represents the cardinality of this chunk.
    pub fn write_chunk(&self, mut chunk: StreamChunk) -> Result<oneshot::Receiver<usize>> {
        loop {
            let core = self.core.upgradable_read();

            // The `changes_txs` should not be empty normally, since we ensured that the channels
            // between the `TableSource` and the `SourceExecutor`s are ready before we making the
            // table catalog visible to the users. However, when we're recovering, it's possible
            // that the streaming executors are not ready when the frontend is able to schedule DML
            // tasks to the compute nodes, so this'll be temporarily unavailable, so we throw an
            // error instead of asserting here.
            // TODO: may reject DML when streaming executors are not recovered.
            let (index, tx) = core
                .changes_txs
                .iter()
                .enumerate()
                .choose(&mut rand::thread_rng())
                .context("no available table reader in streaming source executors")?;

            #[cfg(debug_assertions)]
            risingwave_common::util::schema_check::schema_check(
                self.column_descs.iter().map(|c| &c.data_type),
                chunk.columns(),
            )
            .expect("table source write chunk schema check failed");

            let (notifier_tx, notifier_rx) = oneshot::channel();

            match tx.send((chunk, notifier_tx)) {
                Ok(_) => return Ok(notifier_rx),

                // It's possible that the source executor is scaled in or migrated, so the channel
                // is closed. In this case, we should remove the closed channel and retry.
                Err(SendError((chunk_, _))) => {
                    tracing::info!("find one closed table source channel, remove it and retry");

                    chunk = chunk_;
                    RwLockUpgradableReadGuard::upgrade(core)
                        .changes_txs
                        .swap_remove(index);
                }
            }
        }
    }
}

/// [`TableStreamReader`] reads changes from a certain table continuously.
/// This struct should be only used for associated materialize task, thus the reader should be
/// created only once. Further streaming task relying on this table source should follow the
/// structure of "`MView` on `MView`".
#[derive(Debug)]
pub struct TableStreamReader {
    /// The receiver of the changes channel.
    rx: mpsc::UnboundedReceiver<(StreamChunk, oneshot::Sender<usize>)>,

    /// Mappings from the source column to the column to be read.
    column_indices: Vec<usize>,
}

impl TableStreamReader {
    pub async fn next(&mut self) -> Result<StreamChunk> {
        let (chunk, notifier) = self
            .rx
            .recv()
            .await
            .expect("TableSource dropped before associated streaming task terminated");

        // Caveats: this function is an arm of `tokio::select`. We should ensure there's no `await`
        // after here.

        let (ops, columns, bitmap) = chunk.into_inner();

        let selected_columns = self
            .column_indices
            .iter()
            .map(|i| columns[*i].clone())
            .collect();
        let chunk = StreamChunk::new(ops, selected_columns, bitmap);

        // Notify about that we've taken the chunk.
        notifier.send(chunk.cardinality()).ok();

        Ok(chunk)
    }
}

impl TableSource {
    /// Create a new stream reader.
    #[expect(clippy::unused_async)]
    pub async fn stream_reader(&self, column_ids: Vec<ColumnId>) -> Result<TableStreamReader> {
        let column_indices = column_ids
            .into_iter()
            .map(|id| {
                self.column_descs
                    .iter()
                    .position(|c| c.column_id == id)
                    .expect("column id not exists")
            })
            .collect();

        let mut core = self.core.write();
        let (tx, rx) = mpsc::unbounded_channel();
        core.changes_txs.push(tx);

        Ok(TableStreamReader { rx, column_indices })
    }
}

pub mod test_utils {
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema};
    use risingwave_pb::catalog::{ColumnIndex, TableSourceInfo};
    use risingwave_pb::plan_common::ColumnCatalog;
    use risingwave_pb::stream_plan::source_node::Info as ProstSourceInfo;

    pub fn create_table_info(
        schema: &Schema,
        row_id_index: Option<u64>,
        pk_column_ids: Vec<i32>,
    ) -> ProstSourceInfo {
        ProstSourceInfo::TableSource(TableSourceInfo {
            row_id_index: row_id_index.map(|index| ColumnIndex { index }),
            columns: schema
                .fields
                .iter()
                .enumerate()
                .map(|(i, f)| ColumnCatalog {
                    column_desc: Some(
                        ColumnDesc {
                            data_type: f.data_type.clone(),
                            column_id: ColumnId::from(i as i32), // use column index as column id
                            name: f.name.clone(),
                            field_descs: vec![],
                            type_name: "".to_string(),
                        }
                        .to_protobuf(),
                    ),
                    is_hidden: false,
                })
                .collect(),
            pk_column_ids,
            properties: Default::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use itertools::Itertools;
    use risingwave_common::array::{Array, I64Array, Op};
    use risingwave_common::column_nonnull;
    use risingwave_common::types::DataType;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::Keyspace;

    use super::*;

    fn new_source() -> TableSource {
        let store = MemoryStateStore::new();
        let _keyspace = Keyspace::table_root(store, &Default::default());

        TableSource::new(vec![ColumnDesc::unnamed(
            ColumnId::from(0),
            DataType::Int64,
        )])
    }

    #[tokio::test]
    async fn test_table_source() -> Result<()> {
        let source = Arc::new(new_source());
        let mut reader = source.stream_reader(vec![ColumnId::from(0)]).await?;

        macro_rules! write_chunk {
            ($i:expr) => {{
                let source = source.clone();
                let chunk = StreamChunk::new(
                    vec![Op::Insert],
                    vec![column_nonnull!(I64Array, [$i])],
                    None,
                );
                source.write_chunk(chunk).unwrap();
            }};
        }

        write_chunk!(0);

        macro_rules! check_next_chunk {
            ($i: expr) => {
                assert_matches!(reader.next().await?, chunk => {
                    assert_eq!(chunk.columns()[0].array_ref().as_int64().iter().collect_vec(), vec![Some($i)]);
                });
            }
        }

        check_next_chunk!(0);

        write_chunk!(1);
        check_next_chunk!(1);

        Ok(())
    }
}
