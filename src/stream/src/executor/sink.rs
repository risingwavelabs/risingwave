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
use std::time::Instant;

use futures::stream::select;
use futures::{FutureExt, StreamExt};
use futures_async_stream::try_stream;
use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::Histogram;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_connector::sink::catalog::SinkType;
use risingwave_connector::sink::{Sink, SinkConfig, SinkImpl};
use risingwave_connector::{dispatch_sink, ConnectorParams};

use super::error::{StreamExecutorError, StreamExecutorResult};
use super::{BoxedExecutor, Executor, Message};
use crate::common::log_store::{LogReader, LogStoreFactory, LogStoreReadItem, LogWriter};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{expect_first_barrier, ActorContextRef, BoxedMessageStream, PkIndices};

pub struct SinkExecutor<F: LogStoreFactory> {
    input: BoxedExecutor,
    metrics: Arc<StreamingMetrics>,
    sink: SinkImpl,
    config: SinkConfig,
    identity: String,
    schema: Schema,
    pk_indices: Vec<usize>,
    sink_type: SinkType,
    actor_context: ActorContextRef,
    log_reader: F::Reader,
    log_writer: F::Writer,
}

struct SinkMetrics {
    sink_commit_duration_metrics: Histogram,
    sink_throughput_metrics: GenericCounter<AtomicU64>,
}

async fn build_sink(
    config: SinkConfig,
    schema: Schema,
    pk_indices: PkIndices,
    connector_params: ConnectorParams,
    sink_type: SinkType,
    sink_id: u64,
) -> StreamExecutorResult<SinkImpl> {
    Ok(SinkImpl::new(
        config,
        schema,
        pk_indices,
        connector_params,
        sink_type,
        sink_id,
    )
    .await?)
}

// Drop all the DELETE messages in this chunk and convert UPDATE INSERT into INSERT.
fn force_append_only(chunk: StreamChunk, data_types: Vec<DataType>) -> Option<StreamChunk> {
    let mut builder = DataChunkBuilder::new(data_types, chunk.cardinality() + 1);
    for (op, row_ref) in chunk.rows() {
        if op == Op::Insert || op == Op::UpdateInsert {
            let finished = builder.append_one_row(row_ref.into_owned_row());
            assert!(finished.is_none());
        }
    }
    builder.consume_all().map(|data_chunk| {
        let ops = vec![Op::Insert; data_chunk.capacity()];
        StreamChunk::from_parts(ops, data_chunk)
    })
}

impl<F: LogStoreFactory> SinkExecutor<F> {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        materialize_executor: BoxedExecutor,
        metrics: Arc<StreamingMetrics>,
        config: SinkConfig,
        executor_id: u64,
        connector_params: ConnectorParams,
        schema: Schema,
        pk_indices: Vec<usize>,
        sink_type: SinkType,
        sink_id: u64,
        actor_context: ActorContextRef,
        log_store_factory: F,
    ) -> StreamExecutorResult<Self> {
        let (log_reader, log_writer) = log_store_factory.build().await;
        let sink = build_sink(
            config.clone(),
            schema.clone(),
            pk_indices.clone(),
            connector_params,
            sink_type,
            sink_id,
        )
        .await?;
        Ok(Self {
            input: materialize_executor,
            metrics,
            sink,
            config,
            identity: format!("SinkExecutor {:X?}", executor_id),
            schema,
            sink_type,
            pk_indices,
            actor_context,
            log_reader,
            log_writer,
        })
    }

    fn execute_inner(self) -> BoxedMessageStream {
        let sink_commit_duration_metrics = self
            .metrics
            .sink_commit_duration
            .with_label_values(&[self.identity.as_str(), self.config.get_connector()]);
        let sink_throughput_metrics = self
            .metrics
            .sink_output_row_count
            .with_label_values(&[self.identity.as_str(), self.config.get_connector()]);

        let sink_metrics = SinkMetrics { sink_commit_duration_metrics, sink_throughput_metrics };

        let write_log_stream = Self::execute_write_log(
            self.input,
            self.log_writer,
            self.schema,
            self.sink_type,
            self.actor_context,
        );

        dispatch_sink!(self.sink, sink, {
            let consume_log_stream = Self::execute_consume_log(sink, self.log_reader, sink_metrics);
            select(consume_log_stream.into_stream(), write_log_stream).boxed()
        })
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_write_log(
        input: BoxedExecutor,
        mut log_writer: impl LogWriter,
        schema: Schema,
        sink_type: SinkType,
        actor_context: ActorContextRef,
    ) {
        let data_types = schema.data_types();
        let mut input = input.execute();

        let barrier = expect_first_barrier(&mut input).await?;

        let epoch_pair = barrier.epoch;

        log_writer.init(epoch_pair.curr).await?;

        // Propagate the first barrier
        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            match msg? {
                Message::Watermark(w) => yield Message::Watermark(w),
                Message::Chunk(chunk) => {
                    let visible_chunk = if sink_type == SinkType::ForceAppendOnly {
                        // Force append-only by dropping UPDATE/DELETE messages. We do this when the
                        // user forces the sink to be append-only while it is actually not based on
                        // the frontend derivation result.
                        force_append_only(chunk.clone(), data_types.clone())
                    } else {
                        Some(chunk.clone().compact())
                    };

                    if let Some(chunk) = visible_chunk {
                        // NOTE: We start the txn here because a force-append-only sink might
                        // receive a data chunk full of DELETE messages and then drop all of them.
                        // At this point (instead of the point above when we receive the upstream
                        // data chunk), we make sure that we do have data to send out, and we can
                        // thus mark the txn as started.
                        log_writer.write_chunk(chunk.clone()).await?;

                        yield Message::Chunk(chunk);
                    }
                }
                Message::Barrier(barrier) => {
                    log_writer
                        .flush_current_epoch(barrier.epoch.curr, barrier.checkpoint)
                        .await?;
                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(actor_context.id) {
                        log_writer.update_vnode_bitmap(vnode_bitmap);
                    }
                    yield Message::Barrier(barrier);
                }
            }
        }
    }

    async fn execute_consume_log<S: Sink, R: LogReader>(
        mut sink: S,
        mut log_reader: R,
        sink_metrics: SinkMetrics,
    ) -> StreamExecutorResult<Message> {
        log_reader.init().await?;

        enum LogConsumerState {
            /// Mark that the log consumer is not initialized yet
            Uninitialized,

            /// Mark that there is some data written in this checkpoint.
            Writing { curr_epoch: u64 },

            /// Mark that the consumer has been checkpointed and there is no new data written after
            /// the checkpoint
            Checkpointed { prev_epoch: u64 },
        }

        let mut state = LogConsumerState::Uninitialized;

        loop {
            let (epoch, item): (u64, LogStoreReadItem) = log_reader.next_item().await?;
            match item {
                LogStoreReadItem::StreamChunk(chunk) => {
                    state = match state {
                        LogConsumerState::Uninitialized => {
                            sink.begin_epoch(epoch).await?;
                            LogConsumerState::Writing { curr_epoch: epoch }
                        }
                        LogConsumerState::Writing { curr_epoch } => {
                            assert!(
                                epoch >= curr_epoch,
                                "new epoch {} should not be below the current epoch {}",
                                epoch,
                                curr_epoch
                            );
                            LogConsumerState::Writing { curr_epoch: epoch }
                        }
                        LogConsumerState::Checkpointed { prev_epoch } => {
                            assert!(
                                epoch > prev_epoch,
                                "new epoch {} should be greater than prev epoch {}",
                                epoch,
                                prev_epoch
                            );
                            sink.begin_epoch(epoch).await?;
                            LogConsumerState::Writing { curr_epoch: epoch }
                        }
                    };

                    if let Err(e) = sink.write_batch(chunk.clone()).await {
                        sink.abort().await?;
                        return Err(e.into());
                    }
                    sink_metrics
                        .sink_throughput_metrics
                        .inc_by(chunk.cardinality() as u64);
                }
                LogStoreReadItem::Barrier { is_checkpoint } => {
                    state = match state {
                        LogConsumerState::Uninitialized => {
                            LogConsumerState::Checkpointed { prev_epoch: epoch }
                        }
                        LogConsumerState::Writing { curr_epoch } => {
                            assert!(
                                epoch >= curr_epoch,
                                "barrier epoch {} should not be below current epoch {}",
                                epoch,
                                curr_epoch
                            );
                            if is_checkpoint {
                                let start_time = Instant::now();
                                sink.commit().await?;
                                sink_metrics
                                    .sink_commit_duration_metrics
                                    .observe(start_time.elapsed().as_millis() as f64);
                                LogConsumerState::Checkpointed { prev_epoch: epoch }
                            } else {
                                LogConsumerState::Writing { curr_epoch: epoch }
                            }
                        }
                        LogConsumerState::Checkpointed { prev_epoch } => {
                            assert!(
                                epoch > prev_epoch,
                                "checkpoint epoch {} should be greater than prev checkpoint epoch: {}",
                                epoch,
                                prev_epoch
                            );
                            LogConsumerState::Checkpointed { prev_epoch: epoch }
                        }
                    };
                    if is_checkpoint {
                        log_reader.truncate().await?;
                    }
                }
            }
        }
    }
}

impl<F: LogStoreFactory> Executor for SinkExecutor<F> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner()
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn pk_indices(&self) -> super::PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::common::log_store::BoundedInMemLogStoreFactory;
    use crate::executor::test_utils::*;
    use crate::executor::ActorContext;

    #[tokio::test]
    async fn test_force_append_only_sink() {
        use risingwave_common::array::stream_chunk::StreamChunk;
        use risingwave_common::array::StreamChunkTestExt;
        use risingwave_common::catalog::Field;
        use risingwave_common::types::DataType;

        use crate::executor::Barrier;

        let properties = maplit::hashmap! {
            "connector".into() => "blackhole".into(),
            "type".into() => "append-only".into(),
            "force_append_only".into() => "true".into()
        };
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int64, "v1"),
            Field::with_name(DataType::Int64, "v2"),
        ]);
        let pk = vec![0];

        let mock = MockSource::with_messages(
            schema.clone(),
            pk.clone(),
            vec![
                Message::Barrier(Barrier::new_test_barrier(1)),
                Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
                    " I I
                    + 3 2",
                ))),
                Message::Barrier(Barrier::new_test_barrier(2)),
                Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
                    "  I I
                    U- 3 2
                    U+ 3 4
                     + 5 6",
                ))),
                Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
                    " I I
                    - 5 6",
                ))),
            ],
        );

        let config = SinkConfig::from_hashmap(properties).unwrap();
        let sink_executor = SinkExecutor::new(
            Box::new(mock),
            Arc::new(StreamingMetrics::unused()),
            config,
            0,
            Default::default(),
            schema.clone(),
            pk.clone(),
            SinkType::ForceAppendOnly,
            0,
            ActorContext::create(0),
            BoundedInMemLogStoreFactory::new(1),
        )
        .await
        .unwrap();

        let mut executor = SinkExecutor::execute(Box::new(sink_executor));

        // Barrier message.
        executor.next().await.unwrap().unwrap();

        let chunk_msg = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk_msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 3 2",
            )
        );

        // Barrier message.
        executor.next().await.unwrap().unwrap();

        let chunk_msg = executor.next().await.unwrap().unwrap();
        assert_eq!(
            chunk_msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 3 4
                + 5 6",
            )
        );

        // Should not receive the third stream chunk message because the force-append-only sink
        // executor will drop all DELETE messages.

        // The last barrier message.
        executor.next().await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_empty_barrier_sink() {
        use risingwave_common::catalog::Field;
        use risingwave_common::types::DataType;

        use crate::executor::Barrier;

        let properties = maplit::hashmap! {
            "connector".into() => "blackhole".into(),
            "type".into() => "append-only".into(),
            "force_append_only".into() => "true".into()
        };
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int64, "v1"),
            Field::with_name(DataType::Int64, "v2"),
        ]);
        let pk = vec![0];

        let mock = MockSource::with_messages(
            schema.clone(),
            pk.clone(),
            vec![
                Message::Barrier(Barrier::new_test_barrier(1)),
                Message::Barrier(Barrier::new_test_barrier(2)),
                Message::Barrier(Barrier::new_test_barrier(3)),
            ],
        );

        let config = SinkConfig::from_hashmap(properties).unwrap();
        let sink_executor = SinkExecutor::new(
            Box::new(mock),
            Arc::new(StreamingMetrics::unused()),
            config,
            0,
            Default::default(),
            schema.clone(),
            pk.clone(),
            SinkType::ForceAppendOnly,
            0,
            ActorContext::create(0),
            BoundedInMemLogStoreFactory::new(1),
        )
        .await
        .unwrap();

        let mut executor = SinkExecutor::execute(Box::new(sink_executor));

        // Barrier message.
        assert_eq!(
            executor.next().await.unwrap().unwrap(),
            Message::Barrier(Barrier::new_test_barrier(1))
        );

        // Barrier message.
        assert_eq!(
            executor.next().await.unwrap().unwrap(),
            Message::Barrier(Barrier::new_test_barrier(2))
        );

        // The last barrier message.
        assert_eq!(
            executor.next().await.unwrap().unwrap(),
            Message::Barrier(Barrier::new_test_barrier(3))
        );
    }
}
