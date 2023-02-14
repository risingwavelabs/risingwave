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

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;
use risingwave_connector::sink::{Sink, SinkConfig, SinkImpl};
use risingwave_connector::ConnectorParams;

use super::error::{StreamExecutorError, StreamExecutorResult};
use super::{BoxedExecutor, Executor, Message};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::PkIndices;

pub struct SinkExecutor {
    input: BoxedExecutor,
    metrics: Arc<StreamingMetrics>,
    config: SinkConfig,
    identity: String,
    connector_params: ConnectorParams,
    schema: Schema,
    pk_indices: Vec<usize>,
}

async fn build_sink(
    config: SinkConfig,
    schema: Schema,
    pk_indices: PkIndices,
    connector_params: ConnectorParams,
) -> StreamExecutorResult<Box<SinkImpl>> {
    Ok(Box::new(
        SinkImpl::new(config, schema, pk_indices, connector_params).await?,
    ))
}

impl SinkExecutor {
    pub fn new(
        materialize_executor: BoxedExecutor,
        metrics: Arc<StreamingMetrics>,
        config: SinkConfig,
        executor_id: u64,
        connector_params: ConnectorParams,
        schema: Schema,
        pk_indices: Vec<usize>,
    ) -> Self {
        Self {
            input: materialize_executor,
            metrics,
            config,
            identity: format!("SinkExecutor_{:?}", executor_id),
            pk_indices,
            schema,
            connector_params,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        // the flag is required because kafka transaction requires at least one
        // message, so we should abort the transaction if the flag is true.
        let mut empty_epoch_flag = true;
        let mut in_transaction = false;
        let mut epoch = 0;

        let mut sink = build_sink(
            self.config.clone(),
            self.schema,
            self.pk_indices,
            self.connector_params,
        )
        .await?;

        let input = self.input.execute();

        #[for_await]
        for msg in input {
            match msg? {
                Message::Watermark(_) => {
                    todo!("https://github.com/risingwavelabs/risingwave/issues/6042")
                }
                Message::Chunk(chunk) => {
                    if !in_transaction {
                        sink.begin_epoch(epoch).await?;
                        in_transaction = true;
                    }

                    let visible_chunk = chunk.clone().compact();
                    if let Err(e) = sink.write_batch(visible_chunk).await {
                        sink.abort().await?;
                        return Err(e.into());
                    }
                    empty_epoch_flag = false;

                    yield Message::Chunk(chunk);
                }
                Message::Barrier(barrier) => {
                    if in_transaction {
                        if empty_epoch_flag {
                            sink.abort().await?;
                            tracing::debug!(
                                "transaction abort due to empty epoch, epoch: {:?}",
                                epoch
                            );
                        } else if barrier.checkpoint {
                            let start_time = Instant::now();
                            sink.commit().await?;
                            self.metrics
                                .sink_commit_duration
                                .with_label_values(&[
                                    self.identity.as_str(),
                                    self.config.get_connector(),
                                ])
                                .observe(start_time.elapsed().as_millis() as f64);
                            in_transaction = false;
                        }
                    }
                    empty_epoch_flag = true;
                    epoch = barrier.epoch.curr;
                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

impl Executor for SinkExecutor {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
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
        );

        let mut executor = SinkExecutor::execute(Box::new(sink_executor));

        executor.next().await.unwrap().unwrap();
        executor.next().await.unwrap().unwrap();
        executor.next().await.unwrap().unwrap();
        executor.next().await.unwrap().unwrap();
    }
}
