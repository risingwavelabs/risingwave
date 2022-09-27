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

use core::default::Default;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;
use risingwave_connector::sink::{Sink, SinkConfig, SinkImpl};
use risingwave_storage::StateStore;

use super::error::{StreamExecutorError, StreamExecutorResult};
use super::{BoxedExecutor, Executor, Message};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::PkIndices;

pub struct SinkExecutor<S: StateStore> {
    input: BoxedExecutor,
    _store: S,
    metrics: Arc<StreamingMetrics>,
    properties: HashMap<String, String>,
    identity: String,
    pk_indices: PkIndices,
}

async fn build_sink(config: SinkConfig) -> StreamExecutorResult<Box<SinkImpl>> {
    Ok(Box::new(SinkImpl::new(config).await?))
}

impl<S: StateStore> SinkExecutor<S> {
    pub fn new(
        materialize_executor: BoxedExecutor,
        _store: S,
        metrics: Arc<StreamingMetrics>,
        mut properties: HashMap<String, String>,
        executor_id: u64,
    ) -> Self {
        // This field can be used to distinguish a specific actor in parallelism to prevent
        // transaction execution errors
        properties.insert("identifier".to_string(), format!("sink-{:?}", executor_id));
        Self {
            input: materialize_executor,
            _store,
            metrics,
            properties,
            identity: format!("SinkExecutor_{:?}", executor_id),
            pk_indices: Default::default(), // todo
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let sink_config = SinkConfig::from_hashmap(self.properties.clone())?;
        let mut sink = build_sink(sink_config.clone()).await?;

        // the flag is required because kafka transaction requires at least one
        // message, so we should abort the transaction if the flag is true.
        let mut empty_epoch_flag = true;
        let mut in_transaction = false;
        let mut epoch = 0;

        let schema = self.schema().clone();

        // prepare the external sink before writing if needed.
        if sink.needs_preparation() {
            sink.prepare(&schema).await?;
        }

        let input = self.input.execute();

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    if !in_transaction {
                        sink.begin_epoch(epoch).await?;
                        in_transaction = true;
                    }

                    let visible_chunk = chunk.clone().compact()?;
                    if let Err(e) = sink.write_batch(visible_chunk, &schema).await {
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
                        } else {
                            let start_time = Instant::now();
                            sink.commit().await?;
                            self.metrics
                                .sink_commit_duration
                                .with_label_values(&[
                                    self.identity.as_str(),
                                    sink_config.get_connector(),
                                ])
                                .observe(start_time.elapsed().as_millis() as f64);
                        }
                    }
                    in_transaction = false;
                    empty_epoch_flag = true;
                    epoch = barrier.epoch.curr;
                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

impl<S: StateStore> Executor for SinkExecutor<S> {
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
        use risingwave_storage::memory::MemoryStateStore;

        use crate::executor::Barrier;

        let properties = maplit::hashmap! {
        "connector".into() => "mysql".into(),
        "endpoint".into() => "127.0.0.1:3306".into(),
        "database".into() => "db".into(),
        "table".into() => "t".into(),
        "user".into() => "root".into()
        };

        // Mock `child`
        let mock = MockSource::with_messages(
            Schema::new(vec![
                Field::with_name(DataType::Int32, "v1"),
                Field::with_name(DataType::Int32, "v2"),
                Field::with_name(DataType::Int32, "v3"),
            ]),
            PkIndices::new(),
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

        let sink_executor = SinkExecutor::new(
            Box::new(mock),
            MemoryStateStore::new(),
            Arc::new(StreamingMetrics::unused()),
            properties,
            0,
        );

        let mut executor = SinkExecutor::execute(Box::new(sink_executor));

        executor.next().await.unwrap().unwrap();
        executor.next().await.unwrap().unwrap();
        executor.next().await.unwrap().unwrap();
        executor.next().await.unwrap().unwrap();
    }
}
