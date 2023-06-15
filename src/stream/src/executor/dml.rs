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

use std::collections::BTreeMap;

use either::Either;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{ColumnDesc, Schema, TableId, TableVersionId};
use risingwave_common::transaction::transaction_id::TxnId;
use risingwave_common::transaction::transaction_message::TxnMsg;
use risingwave_source::dml_manager::DmlManagerRef;

use super::error::StreamExecutorError;
use super::{
    expect_first_barrier, BoxedExecutor, BoxedMessageStream, Executor, Message, Mutation,
    PkIndices, PkIndicesRef,
};
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::ActorId;

/// [`DmlExecutor`] accepts both stream data and batch data for data manipulation on a specific
/// table. The two streams will be merged into one and then sent to downstream.
pub struct DmlExecutor {
    upstream: BoxedExecutor,

    schema: Schema,

    pk_indices: PkIndices,

    identity: String,

    /// Stores the information of batch data channels.
    dml_manager: DmlManagerRef,

    // Id of the table on which DML performs.
    table_id: TableId,

    // Version of the table on which DML performs.
    table_version_id: TableVersionId,

    // Column descriptions of the table.
    column_descs: Vec<ColumnDesc>,

    actor_id: ActorId,
}

/// If a transaction's data is less than `MAX_CHUNK_FOR_ATOMICITY` * `CHUNK_SIZE`, we can provide
/// atomicity. Otherwise, it is possible that part of transaction's data is sent to the downstream
/// without barrier boundaries. There are some cases that could cause non-atomicity for large
/// transaction. 1. The system crashes.
/// 2. Actor scale-in or migration.
/// 3. Dml's batch query error occurs at the middle of its execution. (e.g. Remove UDF function
/// server become unavailable).
const MAX_CHUNK_FOR_ATOMICITY: usize = 32;

#[derive(Debug, Default)]
struct TxnBuffer {
    vec: Vec<StreamChunk>,
    // When vec size exceeds `MAX_CHUNK_FOR_ATOMICITY`, set true to `overflow`.
    overflow: bool,
}

impl DmlExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        upstream: BoxedExecutor,
        schema: Schema,
        pk_indices: PkIndices,
        executor_id: u64,
        dml_manager: DmlManagerRef,
        table_id: TableId,
        table_version_id: TableVersionId,
        column_descs: Vec<ColumnDesc>,
        actor_id: ActorId,
    ) -> Self {
        Self {
            upstream,
            schema,
            pk_indices,
            identity: format!("DmlExecutor {:X}", executor_id),
            dml_manager,
            table_id,
            table_version_id,
            column_descs,
            actor_id,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self: Box<Self>) {
        let mut upstream = self.upstream.execute();

        // The first barrier message should be propagated.
        let barrier = expect_first_barrier(&mut upstream).await?;

        // Construct the reader of batch data (DML from users). We must create a variable to hold
        // this `Arc<TableDmlHandle>` here, or it will be dropped due to the `Weak` reference in
        // `DmlManager`.
        //
        // Note(bugen): Only register after the first barrier message is received, which means the
        // current executor is activated. This avoids the new reader overwriting the old one during
        // the preparation of schema change.
        let batch_reader = self
            .dml_manager
            .register_reader(self.table_id, self.table_version_id, &self.column_descs)
            .map_err(StreamExecutorError::connector_error)?;
        let batch_reader = batch_reader.stream_reader(self.actor_id).into_stream();

        // Merge the two streams using `StreamReaderWithPause` because when we receive a pause
        // barrier, we should stop receiving the data from DML. We poll data from the two streams in
        // a round robin way.
        let mut stream = StreamReaderWithPause::<false, TxnMsg>::new(upstream, batch_reader);

        // If the first barrier is configuration change, then the DML executor must be newly
        // created, and we should start with the paused state.
        if barrier.is_update() {
            stream.pause_stream();
        }

        yield Message::Barrier(barrier);

        // Active transactions: txn_id -> (the epoch of `Begin`, transaction chunks).
        let mut active_txn_map: BTreeMap<TxnId, TxnBuffer> = Default::default();

        while let Some(input_msg) = stream.next().await {
            match input_msg? {
                Either::Left(msg) => {
                    // Stream messages.
                    if let Message::Barrier(barrier) = &msg {
                        // We should handle barrier messages here to pause or resume the data from
                        // DML.
                        if let Some(mutation) = barrier.mutation.as_deref() {
                            match mutation {
                                Mutation::Pause => stream.pause_stream(),
                                Mutation::Resume => stream.resume_stream(),
                                _ => {}
                            }
                        }

                        // Stop barrier. It could be issued by scaling or DDLs(e.g. add column).
                        if barrier.is_stop(self.actor_id) {
                            self.dml_manager
                                .unregister_changes_sender(self.table_id, self.actor_id);
                        }
                    }
                    yield msg;
                }
                Either::Right(txn_msg) => {
                    // Batch data.
                    match txn_msg {
                        TxnMsg::Begin(txn_id) => {
                            active_txn_map
                                .try_insert(txn_id, TxnBuffer::default())
                                .expect("Transaction id collision.");
                        }
                        TxnMsg::End(txn_id) => {
                            let mut txn_buffer = active_txn_map.remove(&txn_id).expect(&format!("Receive an unexpected transaction end message. Active transaction map doesn't contain this transaction txn_id = {}.", txn_id));
                            for chunk in txn_buffer.vec.drain(..) {
                                yield Message::Chunk(chunk);
                            }
                        }
                        TxnMsg::Rollback(txn_id) => {
                            let txn_buffer = active_txn_map.remove(&txn_id).expect(&format!("Receive an unexpected transaction rollback message. Active transaction map doesn't contain this transaction txn_id = {}.", txn_id));
                            if txn_buffer.overflow {
                                tracing::warn!("txn_id={} large transaction tries to rollback, but part of its data has already been sent to the downstream.", txn_id);
                            }
                        }
                        TxnMsg::Data(txn_id, chunk) => {
                            match active_txn_map.get_mut(&txn_id) {
                                Some(txn_buffer) => {
                                    // This transaction is too large, we can't provide atomicity,
                                    // so yield chunk ASAP.
                                    if txn_buffer.overflow {
                                        yield Message::Chunk(chunk);
                                        continue;
                                    }
                                    txn_buffer.vec.push(chunk);
                                    if txn_buffer.vec.len() > MAX_CHUNK_FOR_ATOMICITY {
                                        // Too many chunks for atomicity. Drain and yield them.
                                        tracing::warn!("txn_id={} Too many chunks for atomicity. Sent them to the downstream anyway.", txn_id);
                                        for chunk in txn_buffer.vec.drain(..) {
                                            yield Message::Chunk(chunk);
                                        }
                                        txn_buffer.overflow = true;
                                    }
                                }
                                None => unreachable!("Receive an unexpected transaction data message. Active transaction map doesn't contain this transaction txn_id = {}.", txn_id),
                            };
                        }
                    }
                }
            }
        }
    }
}

impl Executor for DmlExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnId, Field, INITIAL_TABLE_VERSION_ID};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::transaction::transaction_id::TxnId;
    use risingwave_common::transaction::transaction_message::TxnMsg;
    use risingwave_common::types::DataType;
    use risingwave_source::dml_manager::DmlManager;

    use super::*;
    use crate::executor::test_utils::MockSource;
    use crate::task::WorkerNodeId;

    const TEST_TRANSACTION_ID: TxnId = 0;
    const ACTOR_ID1: ActorId = 1;

    #[tokio::test]
    async fn test_dml_executor() {
        let table_id = TableId::default();
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Int64),
        ]);
        let column_descs = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
        ];
        let pk_indices = vec![0];
        let dml_manager = Arc::new(DmlManager::new(WorkerNodeId::default()));

        let (mut tx, source) = MockSource::channel(schema.clone(), pk_indices.clone());

        let dml_executor = Box::new(DmlExecutor::new(
            Box::new(source),
            schema,
            pk_indices,
            1,
            dml_manager.clone(),
            table_id,
            INITIAL_TABLE_VERSION_ID,
            column_descs,
            ACTOR_ID1,
        ));
        let mut dml_executor = dml_executor.execute();

        let stream_chunk1 = StreamChunk::from_pretty(
            " I I
            + 1 1
            + 2 2
            + 3 6",
        );
        let stream_chunk2 = StreamChunk::from_pretty(
            " I I
            + 88 43",
        );
        let stream_chunk3 = StreamChunk::from_pretty(
            " I I
            + 199 40
            + 978 72
            + 134 41
            + 398 98",
        );
        let batch_chunk = StreamChunk::from_pretty(
            "  I I
            U+ 1 11
            U+ 2 22",
        );

        // The first barrier
        tx.push_barrier(1, false);
        dml_executor.next().await.unwrap().unwrap();

        // Messages from upstream streaming executor
        tx.push_chunk(stream_chunk1);
        tx.push_chunk(stream_chunk2);
        tx.push_chunk(stream_chunk3);

        let table_dml_handle = dml_manager
            .table_dml_handle(table_id, INITIAL_TABLE_VERSION_ID)
            .unwrap();
        let write_handle = table_dml_handle.write_handle(TEST_TRANSACTION_ID).unwrap();

        // Message from batch
        write_handle
            .write_txn_msg(TxnMsg::Begin(TEST_TRANSACTION_ID))
            .await
            .unwrap();
        write_handle
            .write_txn_msg(TxnMsg::Data(TEST_TRANSACTION_ID, batch_chunk))
            .await
            .unwrap();
        write_handle
            .write_txn_msg(TxnMsg::End(TEST_TRANSACTION_ID))
            .await
            .unwrap();

        // Consume the 1st message from upstream executor
        let msg = dml_executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 1 1
                + 2 2
                + 3 6",
            )
        );

        // Consume the message from batch (because dml executor selects from the streams in a round
        // robin way)

        // TxnMsg::Begin is consumed implicitly

        // Consume the 2nd message from upstream executor
        let msg = dml_executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 88 43",
            )
        );

        // TxnMsg::Data is buffed

        // Consume the 3rd message from upstream executor
        let msg = dml_executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 199 40
                + 978 72
                + 134 41
                + 398 98",
            )
        );

        // After TxnMsg::End, we can consume dml data
        let msg = dml_executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I
                U+ 1 11
                U+ 2 22",
            )
        );
    }
}
