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

use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row;
use risingwave_common::types::{DataType, Datum};
use risingwave_storage::StateStore;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{
    Barrier, BoxedMessageStream, Executor, Message, Mutation, PkIndices, PkIndicesRef,
    StreamExecutorError, Watermark,
};
use crate::common::table::state_table::StateTable;

pub struct NowExecutor<S: StateStore> {
    /// Receiver of barrier channel.
    barrier_receiver: UnboundedReceiver<Barrier>,

    pk_indices: PkIndices,
    identity: String,
    schema: Schema,
    state_table: StateTable<S>,
}

impl<S: StateStore> NowExecutor<S> {
    pub fn new(
        barrier_receiver: UnboundedReceiver<Barrier>,
        executor_id: u64,
        state_table: StateTable<S>,
    ) -> Self {
        let schema = Schema::new(vec![Field {
            data_type: DataType::Timestamptz,
            name: String::from("now"),
            sub_fields: vec![],
            type_name: String::default(),
        }]);

        Self {
            barrier_receiver,
            pk_indices: vec![],
            identity: format!("NowExecutor {:X}", executor_id),
            schema,
            state_table,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(self) {
        let Self {
            mut barrier_receiver,
            mut state_table,
            schema,
            ..
        } = self;

        // Whether the executor is paused.
        let mut paused = false;
        // The last timestamp **sent** to the downstream.
        let mut last_timestamp: Datum = None;
        // Whether the first barrier is handled and `last_timestamp` is initialized.
        let mut initialized = false;

        while let Some(barrier) = barrier_receiver.recv().await {
            if !initialized {
                // Handle the first barrier.
                state_table.init_epoch(barrier.epoch);

                let state_row = {
                    let data_iter = state_table.iter_row(Default::default()).await?;
                    pin_mut!(data_iter);
                    if let Some(keyed_row) = data_iter.next().await {
                        Some(keyed_row?)
                    } else {
                        None
                    }
                };
                last_timestamp = state_row.and_then(|row| row[0].clone());
                initialized = true;
            } else if paused {
                // Assert that no data is updated.
                state_table.commit_no_data_expected(barrier.epoch);
            } else {
                state_table.commit(barrier.epoch).await?;
            }

            // Extract timestamp from the current epoch.
            let timestamp = Some(barrier.get_curr_epoch().as_scalar());

            // Update paused state.
            if let Some(mutation) = barrier.mutation.as_deref() {
                match mutation {
                    Mutation::Pause | Mutation::Update { .. } => paused = true,
                    Mutation::Resume => paused = false,
                    _ => {}
                }
            }

            yield Message::Barrier(barrier.clone());

            // Do not yield any messages if paused.
            if paused {
                continue;
            }

            let stream_chunk = if last_timestamp.is_some() {
                let last_row = row::once(&last_timestamp);
                let row = row::once(&timestamp);
                state_table.update(last_row, row);

                StreamChunk::from_rows(
                    &[(Op::Delete, last_row), (Op::Insert, row)],
                    &schema.data_types(),
                )
            } else {
                let row = row::once(&timestamp);
                state_table.insert(row);

                StreamChunk::from_rows(&[(Op::Insert, row)], &schema.data_types())
            };

            yield Message::Chunk(stream_chunk);

            yield Message::Watermark(Watermark::new(
                0,
                DataType::Timestamptz,
                timestamp.clone().unwrap(),
            ));

            last_timestamp = timestamp;
        }
    }
}

impl<S: StateStore> Executor for NowExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

    use super::NowExecutor;
    use crate::common::table::state_table::StateTable;
    use crate::executor::test_utils::StreamExecutorTestExt;
    use crate::executor::{
        Barrier, BoxedMessageStream, Executor, Mutation, PkIndices, StreamExecutorResult, Watermark,
    };

    #[tokio::test]
    async fn test_now() -> StreamExecutorResult<()> {
        let state_store = create_state_store();
        let (tx, mut now_executor) = create_executor(&state_store).await;

        // Init barrier
        tx.send(Barrier::with_prev_epoch_for_test(1 << 16, 1))
            .unwrap();

        // Consume the barrier
        now_executor.next_unwrap_ready_barrier()?;

        // Consume the data chunk
        let chunk_msg = now_executor.next_unwrap_ready_chunk()?;

        assert_eq!(
            chunk_msg.compact(),
            StreamChunk::from_pretty(
                " TZ
                + 2021-04-01T00:00:00.001Z"
            )
        );

        // Consume the watermark
        let watermark = now_executor.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:00.001Z".parse().unwrap())
            )
        );

        tx.send(Barrier::with_prev_epoch_for_test(2 << 16, 1 << 16))
            .unwrap();

        // Consume the barrier
        now_executor.next_unwrap_ready_barrier()?;

        // Consume the data chunk
        let chunk_msg = now_executor.next_unwrap_ready_chunk()?;

        assert_eq!(
            chunk_msg.compact(),
            StreamChunk::from_pretty(
                " TZ
                - 2021-04-01T00:00:00.001Z
                + 2021-04-01T00:00:00.002Z"
            )
        );

        // Consume the watermark
        let watermark = now_executor.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:00.002Z".parse().unwrap())
            )
        );

        // No more messages until the next barrier
        now_executor.next_unwrap_pending();

        // Recovery
        drop((tx, now_executor));
        let (tx, mut now_executor) = create_executor(&state_store).await;
        tx.send(Barrier::with_prev_epoch_for_test(3 << 16, 1 << 16))
            .unwrap();

        // Consume the barrier
        now_executor.next_unwrap_ready_barrier()?;

        // Consume the data chunk
        let chunk_msg = now_executor.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk_msg.compact(),
            StreamChunk::from_pretty(
                " TZ
                - 2021-04-01T00:00:00.001Z
                + 2021-04-01T00:00:00.003Z"
            )
        );

        // Consume the watermark
        let watermark = now_executor.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:00.003Z".parse().unwrap())
            )
        );

        // Recovery with paused
        drop((tx, now_executor));
        let (tx, mut now_executor) = create_executor(&state_store).await;
        tx.send(Barrier::new_test_barrier(4 << 16).with_mutation(Mutation::Pause))
            .unwrap();

        // Consume the barrier
        now_executor.next_unwrap_ready_barrier()?;

        // There should be no messages until `Resume`
        now_executor.next_unwrap_pending();

        // Resume barrier
        tx.send(
            Barrier::with_prev_epoch_for_test(5 << 16, 4 << 16).with_mutation(Mutation::Resume),
        )
        .unwrap();

        // Consume the barrier
        now_executor.next_unwrap_ready_barrier()?;

        // Consume the data chunk
        let chunk_msg = now_executor.next_unwrap_ready_chunk()?;
        assert_eq!(
            chunk_msg.compact(),
            StreamChunk::from_pretty(
                " TZ
                - 2021-04-01T00:00:00.001Z
                + 2021-04-01T00:00:00.005Z"
            )
        );

        // Consume the watermark
        let watermark = now_executor.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:00.005Z".parse().unwrap())
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_now_start_with_paused() -> StreamExecutorResult<()> {
        let state_store = create_state_store();
        let (tx, mut now_executor) = create_executor(&state_store).await;

        // Init barrier
        tx.send(Barrier::with_prev_epoch_for_test(1 << 16, 1).with_mutation(Mutation::Pause))
            .unwrap();

        // Consume the barrier
        now_executor.next_unwrap_ready_barrier()?;

        // There should be no messages until `Resume`
        now_executor.next_unwrap_pending();

        // Resume barrier
        tx.send(
            Barrier::with_prev_epoch_for_test(2 << 16, 1 << 16).with_mutation(Mutation::Resume),
        )
        .unwrap();

        // Consume the barrier
        now_executor.next_unwrap_ready_barrier()?;

        // Consume the data chunk
        let chunk_msg = now_executor.next_unwrap_ready_chunk()?;

        assert_eq!(
            chunk_msg.compact(),
            StreamChunk::from_pretty(
                " TZ
                + 2021-04-01T00:00:00.002Z" // <- the timestamp is extracted from the current epoch
            )
        );

        // Consume the watermark
        let watermark = now_executor.next_unwrap_ready_watermark()?;

        assert_eq!(
            watermark,
            Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:00.002Z".parse().unwrap())
            )
        );

        // No more messages until the next barrier
        now_executor.next_unwrap_pending();

        Ok(())
    }

    #[inline]
    fn create_pk_indices() -> PkIndices {
        vec![]
    }

    #[inline]
    fn create_order_types() -> Vec<OrderType> {
        vec![]
    }

    fn create_state_store() -> MemoryStateStore {
        MemoryStateStore::new()
    }

    async fn create_executor(
        state_store: &MemoryStateStore,
    ) -> (UnboundedSender<Barrier>, BoxedMessageStream) {
        let table_id = TableId::new(1);
        let column_descs = vec![ColumnDesc::unnamed(ColumnId::new(0), DataType::Timestamptz)];
        let order_types = create_order_types();
        let pk_indices = create_pk_indices();
        let state_table = StateTable::new_without_distribution(
            state_store.clone(),
            table_id,
            column_descs,
            order_types,
            pk_indices,
        )
        .await;

        let (sender, barrier_receiver) = unbounded_channel();
        let now_executor = NowExecutor::new(barrier_receiver, 1, state_table);
        (sender, Box::new(now_executor).execute())
    }
}
