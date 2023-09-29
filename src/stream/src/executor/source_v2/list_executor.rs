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

use anyhow::anyhow;
use either::Either;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::Op;
use risingwave_common::catalog::Schema;
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_connector::source::filesystem::FsPage;
use risingwave_connector::source::{BoxTryStream, SourceCtrlOpts};
use risingwave_connector::ConnectorParams;
use risingwave_source::source_desc::{SourceDesc, SourceDescBuilder};
use risingwave_storage::StateStore;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::executor::*;

#[allow(dead_code)]
pub struct FsListExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    identity: String,

    schema: Schema,

    pk_indices: PkIndices,

    /// Streaming source for external
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Metrics for monitor.
    metrics: Arc<StreamingMetrics>,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

    /// System parameter reader to read barrier interval
    system_params: SystemParamsReaderRef,

    // control options for connector level
    source_ctrl_opts: SourceCtrlOpts,

    // config for the connector node
    connector_params: ConnectorParams,
}

impl<S: StateStore> FsListExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        schema: Schema,
        pk_indices: PkIndices,
        stream_source_core: Option<StreamSourceCore<S>>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        system_params: SystemParamsReaderRef,
        executor_id: u64,
        source_ctrl_opts: SourceCtrlOpts,
        connector_params: ConnectorParams,
    ) -> Self {
        Self {
            actor_ctx,
            identity: format!("FsListExecutor {:X}", executor_id),
            schema,
            pk_indices,
            stream_source_core,
            metrics,
            barrier_receiver: Some(barrier_receiver),
            system_params,
            source_ctrl_opts,
            connector_params,
        }
    }

    async fn build_chunked_paginate_stream(
        &self,
        source_desc: &SourceDesc,
    ) -> StreamExecutorResult<BoxTryStream<StreamChunk>> {
        let stream = source_desc
            .source
            .get_source_list()
            .await
            .map_err(StreamExecutorError::connector_error)?;

        Ok(stream
            .map(|item| item.map(Self::map_fs_page_to_chunk))
            .boxed())
    }

    fn map_fs_page_to_chunk(page: FsPage) -> StreamChunk {
        let rows = page
            .into_iter()
            .map(|split| {
                (
                    Op::Insert,
                    OwnedRow::new(vec![Some(ScalarImpl::Utf8(split.name.into_boxed_str()))]),
                )
            })
            .collect::<Vec<_>>();
        StreamChunk::from_rows(&rows, &[DataType::Varchar])
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let barrier = barrier_receiver
            .recv()
            .instrument_await("source_recv_first_barrier")
            .await
            .ok_or_else(|| {
                anyhow!(
                    "failed to receive the first barrier, actor_id: {:?}, source_id: {:?}",
                    self.actor_ctx.id,
                    self.stream_source_core.as_ref().unwrap().source_id
                )
            })?;

        let mut core = self.stream_source_core.unwrap();

        // Build source description from the builder.
        let source_desc_builder: SourceDescBuilder = core.source_desc_builder.take().unwrap();
        let source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;

        // Return the ownership of `stream_source_core` to the source executor.
        self.stream_source_core = Some(core);

        let chunked_paginate_stream = self.build_chunked_paginate_stream(&source_desc).await?;

        let barrier_stream = barrier_to_message_stream(barrier_receiver).boxed();
        let mut stream =
            StreamReaderWithPause::<true, _>::new(barrier_stream, chunked_paginate_stream);

        if barrier.is_pause_on_startup() {
            stream.pause_stream();
        }

        yield Message::Barrier(barrier);

        while let Some(msg) = stream.next().await {
            match msg {
                Err(_) => (),
                Ok(msg) => match msg {
                    // Barrier arrives.
                    Either::Left(msg) => match &msg {
                        Message::Barrier(barrier) => {
                            if let Some(mutation) = barrier.mutation.as_deref() {
                                match mutation {
                                    Mutation::Pause => stream.pause_stream(),
                                    Mutation::Resume => stream.resume_stream(),
                                    _ => (),
                                }
                            }

                            // Propagate the barrier.
                            yield msg;
                        }
                        // Only barrier can be received.
                        _ => unreachable!(),
                    },
                    // Chunked FsPage arrives.
                    Either::Right(chunk) => {
                        yield Message::Chunk(chunk);
                    }
                },
            }
        }
    }
}

impl<S: StateStore> Executor for FsListExecutor<S> {
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
    use maplit::{convert_args, hashmap};
    use risingwave_common::catalog::{ColumnId, Field, Schema, TableId};
    use risingwave_common::system_param::local_manager::LocalSystemParamsManager;
    use risingwave_common::types::DataType;
    use risingwave_pb::catalog::StreamSourceInfo;
    use risingwave_pb::plan_common::PbRowFormatType;
    use risingwave_source::connector_test_utils::create_source_desc_builder;
    use risingwave_storage::memory::MemoryStateStore;
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;
    use crate::executor::ActorContext;

    const MOCK_SOURCE_NAME: &str = "mock_source";

    #[ignore]
    #[tokio::test]
    async fn test_fs_list_executor() {
        let table_id = TableId::default();
        let schema = Schema {
            fields: vec![
                Field::with_name(DataType::Varchar, "filename"),
                Field::with_name(DataType::Timestamp, "timestamp"),
                Field::with_name(DataType::Int64, "size"),
            ],
        };
        let row_id_index = None;
        let pk_indices = vec![0];
        let source_info = StreamSourceInfo {
            row_format: PbRowFormatType::Native as i32,
            ..Default::default()
        };
        let (barrier_tx, barrier_rx) = unbounded_channel::<Barrier>();
        let column_ids = vec![0].into_iter().map(ColumnId::from).collect();

        let properties: HashMap<String, String> = convert_args!(hashmap!(
            "connector" => "s3",
            "s3.region_name" => "us-east-1",
            "s3.endpoint_url" => "http://[::1]:9090",
            "s3.bucket_name" => "test",
            "s3.credentials.access" => "any",
            "s3.credentials.secret" => "any",
        ));
        let source_desc_builder =
            create_source_desc_builder(&schema, row_id_index, source_info, properties, vec![]);
        let split_state_store = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            MemoryStateStore::new(),
        )
        .await;
        let core = StreamSourceCore::<MemoryStateStore> {
            source_id: table_id,
            column_ids,
            source_desc_builder: Some(source_desc_builder),
            stream_source_splits: HashMap::new(),
            split_state_store,
            state_cache: HashMap::new(),
            source_name: MOCK_SOURCE_NAME.to_string(),
        };

        let system_params_manager = LocalSystemParamsManager::for_test();

        let executor = FsListExecutor::new(
            ActorContext::create(0),
            schema,
            pk_indices,
            Some(core),
            Arc::new(StreamingMetrics::unused()),
            barrier_rx,
            system_params_manager.get_params(),
            1,
            SourceCtrlOpts::default(),
            ConnectorParams::default(),
        );
        let mut executor = Box::new(executor).execute();

        let init_barrier = Barrier::new_test_barrier(1).with_mutation(Mutation::Add {
            adds: HashMap::new(),
            added_actors: HashSet::new(),
            splits: hashmap! {
                ActorId::default() => vec![],
            },
            pause: false,
        });
        barrier_tx.send(init_barrier).unwrap();

        // Consume init barrier.
        executor.next().await.unwrap().unwrap();

        // Consume the second barrier.
        let barrier = Barrier::new_test_barrier(2);
        barrier_tx.send(barrier).unwrap();
        let msg = executor.next().await.unwrap().unwrap(); // page chunk
        executor.next().await.unwrap().unwrap(); // barrier

        println!("barrier 2: {:#?}", msg);

        // Consume the third barrier.
        let barrier = Barrier::new_test_barrier(3);
        barrier_tx.send(barrier).unwrap();
        let msg = executor.next().await.unwrap().unwrap(); // page chunk
        executor.next().await.unwrap().unwrap(); // barrier

        println!("barrier 3: {:#?}", msg);
    }
}
