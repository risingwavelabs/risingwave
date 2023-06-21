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

use await_tree::InstrumentAwait;
use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::{DataChunk, Op, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row;
use risingwave_common::types::{DataType, ToDatumRef};
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

        // Consume the first barrier message and initialize state table.
        let barrier = barrier_receiver
            .recv()
            .instrument_await("now_executor_recv_first_barrier")
            .await
            .unwrap();
        let mut is_pausing = barrier.is_pause() || barrier.is_update();

        state_table.init_epoch(barrier.epoch);

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);

        let state_row = {
            let data_iter = state_table.iter(Default::default()).await?;
            pin_mut!(data_iter);
            if let Some(state_row) = data_iter.next().await {
                Some(state_row?)
            } else {
                None
            }
        };

        let mut last_timestamp = state_row.and_then(|row| row[0].clone());

        while let Some(barrier) = barrier_receiver.recv().await {
            if !is_pausing {
                let timestamp = Some(barrier.get_curr_epoch().as_scalar());

                let stream_chunk = if last_timestamp.is_some() {
                    let data_chunk = DataChunk::from_rows(
                        &[
                            row::once(last_timestamp.to_datum_ref()),
                            row::once(timestamp.to_datum_ref()),
                        ],
                        &schema.data_types(),
                    );
                    let ops = vec![Op::Delete, Op::Insert];

                    StreamChunk::from_parts(ops, data_chunk)
                } else {
                    let data_chunk = DataChunk::from_rows(
                        &[row::once(timestamp.to_datum_ref())],
                        &schema.data_types(),
                    );
                    let ops = vec![Op::Insert];

                    StreamChunk::from_parts(ops, data_chunk)
                };

                yield Message::Chunk(stream_chunk);

                yield Message::Watermark(Watermark::new(
                    0,
                    DataType::Timestamptz,
                    timestamp.as_ref().unwrap().clone(),
                ));

                if last_timestamp.is_some() {
                    state_table.delete(row::once(last_timestamp));
                }
                state_table.insert(row::once(timestamp.to_datum_ref()));
                last_timestamp = timestamp;

                state_table.commit(barrier.epoch).await?;
            } else {
                state_table.commit_no_data_expected(barrier.epoch);
            }
            if let Some(mutation) = barrier.mutation.as_deref() {
                match mutation {
                    Mutation::Pause | Mutation::Update { .. } => is_pausing = true,
                    Mutation::Resume => is_pausing = false,
                    _ => {}
                }
            }

            yield Message::Barrier(barrier);
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
    use futures::StreamExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

    use super::NowExecutor;
    use crate::common::table::state_table::StateTable;
    use crate::executor::{Barrier, BoxedMessageStream, Executor, Message, PkIndices, Watermark};

    #[tokio::test]
    async fn test_now() {
        let state_table = create_state_table().await;
        let (tx, mut now_executor) = create_executor(state_table);

        // Init barrier
        tx.send(Barrier::new_test_barrier(1)).unwrap();

        // Consume the barrier
        now_executor.next().await.unwrap().unwrap();

        tx.send(Barrier::with_prev_epoch_for_test(1 << 16, 1))
            .unwrap();

        // Consume the data chunk
        let chunk_msg = now_executor.next().await.unwrap().unwrap();

        assert_eq!(
            chunk_msg.into_chunk().unwrap().compact(),
            StreamChunk::from_pretty(
                " TZ
                + 2021-04-01T00:00:00.001Z"
            )
        );

        // Consume the watermark
        let watermark = now_executor.next().await.unwrap().unwrap();

        assert_eq!(
            watermark,
            Message::Watermark(Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:00.001Z".parse().unwrap())
            ))
        );

        // Consume the barrier
        now_executor.next().await.unwrap().unwrap();

        tx.send(Barrier::with_prev_epoch_for_test(2 << 16, 1 << 16))
            .unwrap();

        // Consume the data chunk
        let chunk_msg = now_executor.next().await.unwrap().unwrap();

        assert_eq!(
            chunk_msg.into_chunk().unwrap().compact(),
            StreamChunk::from_pretty(
                " TZ
                - 2021-04-01T00:00:00.001Z
                + 2021-04-01T00:00:00.002Z"
            )
        );

        // Consume the watermark
        let watermark = now_executor.next().await.unwrap().unwrap();

        assert_eq!(
            watermark,
            Message::Watermark(Watermark::new(
                0,
                DataType::Timestamptz,
                ScalarImpl::Timestamptz("2021-04-01T00:00:00.002Z".parse().unwrap())
            ))
        );

        // Consume the barrier
        now_executor.next().await.unwrap().unwrap();
    }

    #[inline]
    fn create_pk_indices() -> PkIndices {
        vec![]
    }

    #[inline]
    fn create_order_types() -> Vec<OrderType> {
        vec![]
    }

    async fn create_state_table() -> StateTable<MemoryStateStore> {
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        let column_descs = vec![ColumnDesc::unnamed(ColumnId::new(0), DataType::Timestamptz)];
        let order_types = create_order_types();
        let pk_indices = create_pk_indices();
        StateTable::new_without_distribution(
            memory_state_store,
            table_id,
            column_descs,
            order_types,
            pk_indices,
        )
        .await
    }

    fn create_executor(
        state_table: StateTable<MemoryStateStore>,
    ) -> (UnboundedSender<Barrier>, BoxedMessageStream) {
        let (sender, barrier_receiver) = unbounded_channel();
        let now_executor = NowExecutor::new(barrier_receiver, 1, state_table);
        (sender, Box::new(now_executor).execute())
    }
}
