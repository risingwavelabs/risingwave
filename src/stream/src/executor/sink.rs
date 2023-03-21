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
use futures::{FutureExt, Stream, StreamExt};
use futures_async_stream::try_stream;
use prometheus::Histogram;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_connector::sink::catalog::SinkType;
use risingwave_connector::sink::{Sink, SinkConfig, SinkImpl};
use risingwave_connector::ConnectorParams;

use super::error::{StreamExecutorError, StreamExecutorResult};
use super::{BoxedExecutor, Executor, Message};
use crate::common::log_store::{
    BoundedInMemLogStoreFactory, LogReader, LogStoreFactory, LogStoreReadItem, LogWriter,
};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{expect_first_barrier, PkIndices};

pub struct SinkExecutor {
    input: BoxedExecutor,
    metrics: Arc<StreamingMetrics>,
    config: SinkConfig,
    identity: String,
    connector_params: ConnectorParams,
    schema: Schema,
    pk_indices: Vec<usize>,
    sink_type: SinkType,
}

async fn build_sink(
    config: SinkConfig,
    schema: Schema,
    pk_indices: PkIndices,
    connector_params: ConnectorParams,
    sink_type: SinkType,
) -> StreamExecutorResult<SinkImpl> {
    Ok(SinkImpl::new(config, schema, pk_indices, connector_params, sink_type).await?)
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

impl SinkExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        materialize_executor: BoxedExecutor,
        metrics: Arc<StreamingMetrics>,
        config: SinkConfig,
        executor_id: u64,
        connector_params: ConnectorParams,
        schema: Schema,
        pk_indices: Vec<usize>,
        sink_type: SinkType,
    ) -> Self {
        Self {
            input: materialize_executor,
            metrics,
            config,
            identity: format!("SinkExecutor_{:?}", executor_id),
            pk_indices,
            schema,
            connector_params,
            sink_type,
        }
    }

    fn execute_inner(
        self,
        log_store_factory: impl LogStoreFactory,
    ) -> impl Stream<Item = StreamExecutorResult<Message>> {
        let config = self.config.clone();
        let schema = self.schema.clone();

        let (log_reader, log_writer) = log_store_factory.build();

        let metrics = self
            .metrics
            .sink_commit_duration
            .with_label_values(&[self.identity.as_str(), self.config.get_connector()]);
        let consume_log_stream = Self::execute_consume_log(
            config,
            schema,
            self.pk_indices,
            self.connector_params,
            self.sink_type,
            log_reader,
            metrics,
        );

        let write_log_stream =
            Self::execute_write_log(self.input, log_writer, self.schema, self.sink_type);

        select(consume_log_stream.into_stream(), write_log_stream)
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_write_log(
        input: BoxedExecutor,
        mut log_writer: impl LogWriter,
        schema: Schema,
        sink_type: SinkType,
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
                    yield Message::Barrier(barrier);
                }
            }
        }
    }

    async fn execute_consume_log<R: LogReader>(
        config: SinkConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        connector_params: ConnectorParams,
        sink_type: SinkType,
        mut log_reader: R,
        sink_commit_duration_metrics: Histogram,
    ) -> StreamExecutorResult<Message> {
        let mut sink = build_sink(config, schema, pk_indices, connector_params, sink_type).await?;

        let mut epoch = log_reader.init().await?;

        // the flag is required because kafka transaction requires at least one
        // message, so we should abort the transaction if the flag is true.
        let mut empty_checkpoint_flag = true;
        let mut in_transaction = false;

        loop {
            let item: LogStoreReadItem = log_reader.next_item().await?;
            match item {
                LogStoreReadItem::StreamChunk(chunk) => {
                    // NOTE: We start the txn here because a force-append-only sink might
                    // receive a data chunk full of DELETE messages and then drop all of them.
                    // At this point (instead of the point above when we receive the upstream
                    // data chunk), we make sure that we do have data to send out, and we can
                    // thus mark the txn as started.
                    if !in_transaction {
                        sink.begin_epoch(epoch).await?;
                        in_transaction = true;
                    }

                    if let Err(e) = sink.write_batch(chunk.clone()).await {
                        sink.abort().await?;
                        return Err(e.into());
                    }
                    empty_checkpoint_flag = false;
                }
                LogStoreReadItem::Barrier {
                    next_epoch,
                    is_checkpoint,
                } => {
                    assert!(next_epoch > epoch);
                    if is_checkpoint {
                        if in_transaction {
                            if empty_checkpoint_flag {
                                sink.abort().await?;
                                tracing::debug!(
                                    "transaction abort due to empty epoch, epoch: {:?}",
                                    epoch
                                );
                            } else {
                                let start_time = Instant::now();
                                sink.commit().await?;
                                sink_commit_duration_metrics
                                    .observe(start_time.elapsed().as_millis() as f64);
                            }
                        }
                        log_reader.truncate().await?;
                        in_transaction = false;
                        empty_checkpoint_flag = true;
                    }
                    epoch = next_epoch;
                }
            }
        }
    }
}

impl Executor for SinkExecutor {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        // TODO: dispatch in enum
        let bounded_log_store_factory = BoundedInMemLogStoreFactory::new(1);
        self.execute_inner(bounded_log_store_factory).boxed()
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
    use crate::executor::test_utils::*;

    #[ignore]
    #[tokio::test]
    async fn test_mysqlsink() {
        use risingwave_common::array::stream_chunk::StreamChunk;
        use risingwave_common::array::StreamChunkTestExt;
        use risingwave_common::catalog::Field;
        use risingwave_common::types::DataType;

        use crate::executor::Barrier;

        let properties = maplit::hashmap! {
        "connector".into() => "mysql".into(),
        "endpoint".into() => "127.0.0.1:3306".into(),
        "database".into() => "db".into(),
        "table".into() => "t".into(),
        "user".into() => "root".into()
        };
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
            Field::with_name(DataType::Int32, "v3"),
        ]);
        let pk = vec![];

        // Mock `child`
        let mock = MockSource::with_messages(
            schema.clone(),
            pk.clone(),
            vec![
                Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
                    " I I I
            +  3 2 1",
                ))),
                Message::Barrier(Barrier::new_test_barrier(1)),
                Message::Chunk(std::mem::take(&mut StreamChunk::from_pretty(
                    " I I I
            +  6 5 4",
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
            SinkType::AppendOnly,
        );

        let mut executor = SinkExecutor::execute(Box::new(sink_executor));

        executor.next().await.unwrap().unwrap();
        executor.next().await.unwrap().unwrap();
        executor.next().await.unwrap().unwrap();
        executor.next().await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_force_append_only_sink() {
        use risingwave_common::array::stream_chunk::StreamChunk;
        use risingwave_common::array::StreamChunkTestExt;
        use risingwave_common::catalog::Field;
        use risingwave_common::types::DataType;

        use crate::executor::Barrier;

        let properties = maplit::hashmap! {
            "connector".into() => "console".into(),
            "format".into() => "append_only".into(),
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
        );

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
}
