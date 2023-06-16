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

use std::sync::Arc;

use anyhow::{anyhow, Context};
use futures_async_stream::try_stream;
use parking_lot::RwLock;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::{Result, RwError};
use risingwave_common::hash::ActorId;
use risingwave_common::transaction::transaction_id::TxnId;
use risingwave_common::transaction::transaction_message::TxnMsg;
use risingwave_connector::source::StreamChunkWithState;
use tokio::sync::{mpsc, oneshot};

pub type TableDmlHandleRef = Arc<TableDmlHandle>;

#[derive(Debug)]
pub struct ChangesSender {
    pub actor_id: ActorId,
    pub tx: mpsc::UnboundedSender<(TxnMsg, oneshot::Sender<usize>)>,
}

#[derive(Debug)]
pub struct TableDmlHandleCore {
    /// The senders of the changes channel.
    ///
    /// When a `StreamReader` is created, a channel will be created and the sender will be
    /// saved here. The insert statement will take one channel randomly.
    pub changes_txs: Vec<ChangesSender>,
}

/// [`TableDmlHandle`] is a special internal source to handle table updates from user,
/// including insert/delete/update statements via SQL interface.
///
/// Changed rows will be send to the associated "materialize" streaming task, then be written to the
/// state store. Therefore, [`TableDmlHandle`] can be simply be treated as a channel without side
/// effects.
#[derive(Debug)]
pub struct TableDmlHandle {
    pub core: RwLock<TableDmlHandleCore>,

    /// All columns in this table.
    pub column_descs: Vec<ColumnDesc>,
}

impl TableDmlHandle {
    pub fn new(column_descs: Vec<ColumnDesc>) -> Self {
        let core = TableDmlHandleCore {
            changes_txs: vec![],
        };

        Self {
            core: RwLock::new(core),
            column_descs,
        }
    }

    pub fn stream_reader(&self, actor_id: ActorId) -> TableStreamReader {
        let mut core = self.core.write();
        // TODO: use an unbounded channel with permits to limit the buffer size for data chunks.
        let (tx, rx) = mpsc::unbounded_channel();
        core.changes_txs.push(ChangesSender { actor_id, tx });

        TableStreamReader { rx }
    }

    pub fn write_handle(&self, txn_id: TxnId) -> Result<WriteHandle> {
        // The `changes_txs` should not be empty normally, since we ensured that the channels
        // between the `TableDmlHandle` and the `SourceExecutor`s are ready before we making the
        // table catalog visible to the users. However, when we're recovering, it's possible
        // that the streaming executors are not ready when the frontend is able to schedule DML
        // tasks to the compute nodes, so this'll be temporarily unavailable, so we throw an
        // error instead of asserting here.
        // TODO: may reject DML when streaming executors are not recovered.
        let guard = self.core.read();
        if guard.changes_txs.is_empty() {
            return Err(RwError::from(anyhow!(
                "no available table reader in streaming source executors"
            )));
        }
        let len = guard.changes_txs.len();
        let tx = guard
            .changes_txs
            .get((txn_id % len as u64) as usize)
            .context("no available table reader in streaming source executors")?
            .tx
            .clone();

        Ok(WriteHandle::new(txn_id, tx))
    }

    /// Get the reference of all columns in this table.
    pub(super) fn column_descs(&self) -> &[ColumnDesc] {
        self.column_descs.as_ref()
    }

    pub fn check_chunk_schema(&self, chunk: &StreamChunk) {
        risingwave_common::util::schema_check::schema_check(
            self.column_descs
                .iter()
                .filter_map(|c| (!c.is_generated()).then_some(&c.data_type)),
            chunk.columns(),
        )
        .expect("table source write txn_msg schema check failed");
    }
}

#[derive(Debug, PartialEq)]
enum TxnState {
    Init,
    Begin,
    Committed,
    Rollback,
}

/// [`WriteHandle`] writes its data into a table in a transactional way.
///
/// First, it needs to call `begin()` and then write chunks by calling `write_chunk()`.
///
/// Finally call `end()` to commit the transaction or `rollback()` to rollback the transaction.
///
/// If the [`WriteHandle`] is dropped with a `Begin` transaction state, it will automatically
/// rollback the transaction.
pub struct WriteHandle {
    txn_id: TxnId,
    tx: mpsc::UnboundedSender<(TxnMsg, oneshot::Sender<usize>)>,
    // Indicate whether `TxnMsg::End` or `TxnMsg::Rollback` have been sent to the write channel.
    txn_state: TxnState,
}

impl Drop for WriteHandle {
    fn drop(&mut self) {
        if self.txn_state == TxnState::Begin {
            let _ = self.rollback();
        }
    }
}

impl WriteHandle {
    pub fn new(txn_id: TxnId, tx: mpsc::UnboundedSender<(TxnMsg, oneshot::Sender<usize>)>) -> Self {
        Self {
            txn_id,
            tx,
            txn_state: TxnState::Init,
        }
    }

    pub fn begin(&mut self) -> Result<oneshot::Receiver<usize>> {
        assert_eq!(self.txn_state, TxnState::Init);
        self.txn_state = TxnState::Begin;
        self.write_txn_msg(TxnMsg::Begin(self.txn_id))
    }

    pub fn write_chunk(&self, chunk: StreamChunk) -> Result<oneshot::Receiver<usize>> {
        self.write_txn_msg(TxnMsg::Data(self.txn_id, chunk))
    }

    pub fn end(&mut self) -> Result<oneshot::Receiver<usize>> {
        assert_eq!(self.txn_state, TxnState::Begin);
        self.txn_state = TxnState::Committed;
        self.write_txn_msg(TxnMsg::End(self.txn_id))
    }

    pub fn rollback(&mut self) -> Result<oneshot::Receiver<usize>> {
        assert_eq!(self.txn_state, TxnState::Begin);
        self.txn_state = TxnState::Rollback;
        self.write_txn_msg(TxnMsg::Rollback(self.txn_id))
    }

    /// Asynchronously write txn messages into table. Changes written here will be simply passed to
    /// the associated streaming task via channel, and then be materialized to storage there.
    ///
    /// Returns an oneshot channel which will be notified when the chunk is taken by some reader,
    /// and the `usize` represents the cardinality of this chunk.
    fn write_txn_msg(&self, txn_msg: TxnMsg) -> Result<oneshot::Receiver<usize>> {
        assert_eq!(self.txn_id, txn_msg.txn_id());
        let (notifier_tx, notifier_rx) = oneshot::channel();
        match self.tx.send((txn_msg, notifier_tx)) {
            Ok(_) => Ok(notifier_rx),

            // It's possible that the source executor is scaled in or migrated, so the channel
            // is closed. To guarantee the transactional atomicity, bail out.
            Err(_) => Err(RwError::from("write txn_msg channel closed".to_string())),
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
    rx: mpsc::UnboundedReceiver<(TxnMsg, oneshot::Sender<usize>)>,
}

impl TableStreamReader {
    #[try_stream(boxed, ok = StreamChunkWithState, error = RwError)]
    pub async fn into_stream_for_source_reader_test(mut self) {
        while let Some((txn_msg, notifier)) = self.rx.recv().await {
            // Notify about that we've taken the chunk.
            match txn_msg {
                TxnMsg::Begin(_) | TxnMsg::End(_) | TxnMsg::Rollback(_) => {
                    _ = notifier.send(0);
                }
                TxnMsg::Data(_, chunk) => {
                    _ = notifier.send(chunk.cardinality());
                    yield chunk.into();
                }
            }
        }
    }

    #[try_stream(boxed, ok = TxnMsg, error = RwError)]
    pub async fn into_stream(mut self) {
        while let Some((txn_msg, notifier)) = self.rx.recv().await {
            // Notify about that we've taken the chunk.
            match &txn_msg {
                TxnMsg::Begin(_) | TxnMsg::End(_) | TxnMsg::Rollback(_) => {
                    _ = notifier.send(0);
                    yield txn_msg;
                }
                TxnMsg::Data(_, chunk) => {
                    _ = notifier.send(chunk.cardinality());
                    yield txn_msg;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::array::{Array, I64Array, Op, StreamChunk};
    use risingwave_common::catalog::ColumnId;
    use risingwave_common::transaction::transaction_id::TxnId;
    use risingwave_common::types::DataType;

    use super::*;

    const TEST_TRANSACTION_ID: TxnId = 0;
    const ACTOR_ID1: ActorId = 1;

    fn new_table_dml_handle() -> TableDmlHandle {
        TableDmlHandle::new(vec![ColumnDesc::unnamed(
            ColumnId::from(0),
            DataType::Int64,
        )])
    }

    #[tokio::test]
    async fn test_table_dml_handle() -> Result<()> {
        let table_dml_handle = Arc::new(new_table_dml_handle());
        let mut reader = table_dml_handle.stream_reader(ACTOR_ID1).into_stream();
        let mut write_handle = table_dml_handle.write_handle(TEST_TRANSACTION_ID).unwrap();
        write_handle.begin().unwrap();

        assert_matches!(reader.next().await.unwrap()?, TxnMsg::Begin(_));

        macro_rules! write_chunk {
            ($i:expr) => {{
                let chunk = StreamChunk::new(
                    vec![Op::Insert],
                    vec![I64Array::from_iter([$i]).into_ref()],
                    None,
                );
                write_handle.write_chunk(chunk).unwrap();
            }};
        }

        write_chunk!(0);

        macro_rules! check_next_chunk {
            ($i: expr) => {
                assert_matches!(reader.next().await.unwrap()?, txn_msg => {
                    let chunk = txn_msg.as_stream_chunk().unwrap();
                    assert_eq!(chunk.columns()[0].as_int64().iter().collect_vec(), vec![Some($i)]);
                });
            }
        }

        check_next_chunk!(0);

        write_chunk!(1);
        check_next_chunk!(1);

        write_handle.end().unwrap();

        assert_matches!(reader.next().await.unwrap()?, TxnMsg::End(_));

        Ok(())
    }

    #[tokio::test]
    async fn test_write_handle_rollback_on_drop() -> Result<()> {
        let table_dml_handle = Arc::new(new_table_dml_handle());
        let mut reader = table_dml_handle.stream_reader(ACTOR_ID1).into_stream();
        let mut write_handle = table_dml_handle.write_handle(TEST_TRANSACTION_ID).unwrap();
        write_handle.begin().unwrap();

        assert_matches!(reader.next().await.unwrap()?, TxnMsg::Begin(_));

        let chunk = StreamChunk::new(
            vec![Op::Insert],
            vec![I64Array::from_iter([1]).into_ref()],
            None,
        );
        write_handle.write_chunk(chunk).unwrap();

        assert_matches!(reader.next().await.unwrap()?, txn_msg => {
            let chunk = txn_msg.as_stream_chunk().unwrap();
            assert_eq!(chunk.columns()[0].as_int64().iter().collect_vec(), vec![Some(1)]);
        });

        // Rollback on drop
        drop(write_handle);
        assert_matches!(reader.next().await.unwrap()?, TxnMsg::Rollback(_));

        Ok(())
    }
}
