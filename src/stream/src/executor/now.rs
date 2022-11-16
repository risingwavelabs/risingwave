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

use async_stack_trace::StackTrace;
use chrono::NaiveDateTime;
use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, NaiveDateTimeWrapper, ScalarImpl};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::epoch::Epoch;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{
    Barrier, BoxedMessageStream, Executor, Message, PkIndices, PkIndicesRef, StreamExecutorError,
    Watermark,
};

pub struct NowExecutor<S: StateStore> {
    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

    pk_indices: PkIndices,
    identity: String,
    schema: Schema,
    state_table: StateTable<S>,
}

impl<S: StateStore> NowExecutor<S> {
    #[allow(dead_code)]
    pub fn new(
        barrier_receiver: UnboundedReceiver<Barrier>,
        executor_id: u64,
        state_table: StateTable<S>,
    ) -> Self {
        let schema = Schema::new(vec![Field {
            data_type: DataType::Timestamp,
            name: String::from("now"),
            sub_fields: vec![],
            type_name: String::default(),
        }]);
        Self {
            barrier_receiver: Some(barrier_receiver),
            pk_indices: vec![0],
            identity: format!("NowExecutor {:X}", executor_id),
            schema,
            state_table,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();

        // Consume the first barrier message and initialize state table.
        let barrier = barrier_receiver
            .recv()
            .stack_trace("now_executor_recv_first_barrier")
            .await
            .unwrap();
        self.state_table.init_epoch(barrier.epoch);

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);

        let state_row = {
            let data_iter = self.state_table.iter().await?;
            pin_mut!(data_iter);
            if let Some(state_row) = data_iter.next().await {
                Some(state_row?)
            } else {
                None
            }
        };

        let mut last_timestamp = state_row.and_then(|row| row[0].clone());

        while let Some(barrier) = barrier_receiver.recv().await {
            if !barrier.is_update() {
                let time_millis = Epoch::from(barrier.epoch.curr).as_unix_millis();
                let timestamp = Some(ScalarImpl::NaiveDateTime(NaiveDateTimeWrapper::new(
                    NaiveDateTime::from_timestamp(
                        (time_millis / 1000) as i64,
                        (time_millis % 1000 * 1_000_000) as u32,
                    ),
                )));

                let mut data_chunk_builder = DataChunkBuilder::new(
                    self.schema().data_types(),
                    if last_timestamp.is_some() { 2 } else { 1 },
                );
                if last_timestamp.is_some() {
                    let chunk_popped = data_chunk_builder
                        .append_one_row_from_datums([&last_timestamp].into_iter());
                    debug_assert!(chunk_popped.is_none());
                }
                let data_chunk = data_chunk_builder
                    .append_one_row_from_datums([&timestamp].into_iter())
                    .unwrap();
                let mut ops = if last_timestamp.is_some() {
                    vec![Op::Delete]
                } else {
                    vec![]
                };
                ops.push(Op::Insert);
                let stream_chunk = StreamChunk::from_parts(ops, data_chunk);
                yield Message::Chunk(stream_chunk);

                yield Message::Watermark(Watermark::new(0, timestamp.as_ref().unwrap().clone()));

                if last_timestamp.is_some() {
                    self.state_table.delete(Row::new(vec![last_timestamp]));
                }
                self.state_table.insert(Row::new(vec![timestamp.clone()]));
                last_timestamp = timestamp;

                self.state_table.commit(barrier.epoch).await?;
            } else {
                self.state_table.commit_no_data_expected(barrier.epoch);
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
