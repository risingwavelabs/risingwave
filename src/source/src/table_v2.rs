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
struct TableSourceV2Core {
    /// The senders of the changes channel.
    ///
    /// When a `StreamReader` is created, a channel will be created and the sender will be
    /// saved here. The insert statement will take one channel randomly.
    changes_txs: Vec<mpsc::UnboundedSender<(StreamChunk, oneshot::Sender<usize>)>>,
}

/// [`TableSourceV2`] is a special internal source to handle table updates from user,
/// including insert/delete/update statements via SQL interface.
///
/// Changed rows will be send to the associated "materialize" streaming task, then be written to the
/// state store. Therefore, [`TableSourceV2`] can be simply be treated as a channel without side
/// effects.
#[derive(Debug)]
pub struct TableSourceV2 {
    core: RwLock<TableSourceV2Core>,

    /// All columns in this table.
    column_descs: Vec<ColumnDesc>,
}

impl TableSourceV2 {
    pub fn new(column_descs: Vec<ColumnDesc>) -> Self {
        let core = TableSourceV2Core {
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
            // between the `TableSourceV2` and the `SourceExecutor`s are ready before we making the
            // table catalog visible to the users.
            // However, when we're recovering, it's possible that the streaming executors are not
            // ready when the frontend is able to schedule DML tasks to the compute nodes, so
            // this'll be temporarily unavailable, so we throw an error instead of asserting here.
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

/// [`TableV2StreamReader`] reads changes from a certain table continuously.
/// This struct should be only used for associated materialize task, thus the reader should be
/// created only once. Further streaming task relying on this table source should follow the
/// structure of "`MView` on `MView`".
#[derive(Debug)]
pub struct TableV2StreamReader {
    /// The receiver of the changes channel.
    rx: mpsc::UnboundedReceiver<(StreamChunk, oneshot::Sender<usize>)>,

    /// Mappings from the source column to the column to be read.
    column_indices: Vec<usize>,
}

impl TableV2StreamReader {
    pub async fn next(&mut self) -> Result<StreamChunk> {
        let (chunk, notifier) = self
            .rx
            .recv()
            .await
            .expect("TableSourceV2 dropped before associated streaming task terminated");

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

impl TableSourceV2 {
    /// Create a new stream reader.
    #[expect(clippy::unused_async)]
    pub async fn stream_reader(&self, column_ids: Vec<ColumnId>) -> Result<TableV2StreamReader> {
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

        Ok(TableV2StreamReader { rx, column_indices })
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

    fn new_source() -> TableSourceV2 {
        let store = MemoryStateStore::new();
        let _keyspace = Keyspace::table_root(store, &Default::default());

        TableSourceV2::new(vec![ColumnDesc::unnamed(
            ColumnId::from(0),
            DataType::Int64,
        )])
    }

    #[tokio::test]
    async fn test_table_source_v2() -> Result<()> {
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
