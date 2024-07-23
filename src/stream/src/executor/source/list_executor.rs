// Copyright 2024 RisingWave Labs
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

use anyhow::anyhow;
use either::Either;
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::Op;
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_connector::source::reader::desc::{SourceDesc, SourceDescBuilder};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{barrier_to_message_stream, StreamSourceCore};
use crate::executor::prelude::*;
use crate::executor::stream_reader::StreamReaderWithPause;

const CHUNK_SIZE: usize = 1024;

#[allow(dead_code)]
pub struct FsListExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Streaming source for external
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Metrics for monitor.
    metrics: Arc<StreamingMetrics>,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

    /// System parameter reader to read barrier interval
    system_params: SystemParamsReaderRef,

    /// Rate limit in rows/s.
    rate_limit_rps: Option<u32>,
}

impl<S: StateStore> FsListExecutor<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: Option<StreamSourceCore<S>>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        system_params: SystemParamsReaderRef,
        rate_limit_rps: Option<u32>,
    ) -> Self {
        Self {
            actor_ctx,
            stream_source_core,
            metrics,
            barrier_receiver: Some(barrier_receiver),
            system_params,
            rate_limit_rps,
        }
    }

    #[allow(clippy::disallowed_types)]
    fn build_chunked_paginate_stream(
        &self,
        source_desc: &SourceDesc,
    ) -> StreamExecutorResult<impl Stream<Item = StreamExecutorResult<StreamChunk>>> {
        let stream = source_desc
            .source
            .get_source_list()
            .map_err(StreamExecutorError::connector_error)?
            .map_err(StreamExecutorError::connector_error);

        // Group FsPageItem stream into chunks of size 1024.
        let chunked_stream = stream.chunks(CHUNK_SIZE).map(|chunk| {
            let rows = chunk
                .into_iter()
                .map(|item| match item {
                    Ok(page_item) => Ok((
                        Op::Insert,
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Utf8(page_item.name.into_boxed_str())),
                            Some(ScalarImpl::Timestamptz(page_item.timestamp)),
                            Some(ScalarImpl::Int64(page_item.size)),
                        ]),
                    )),
                    Err(e) => {
                        tracing::error!(error = %e.as_report(), "Connector fail to list item");
                        Err(e)
                    }
                })
                .collect::<Vec<_>>();

            let res: Vec<(Op, OwnedRow)> = rows.into_iter().flatten().collect();
            Ok(StreamChunk::from_rows(
                &res,
                &[DataType::Varchar, DataType::Timestamptz, DataType::Int64],
            ))
        });

        Ok(chunked_stream)
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

        let chunked_paginate_stream = self.build_chunked_paginate_stream(&source_desc)?;

        let barrier_stream = barrier_to_message_stream(barrier_receiver).boxed();
        let mut stream =
            StreamReaderWithPause::<true, _>::new(barrier_stream, chunked_paginate_stream);

        if barrier.is_pause_on_startup() {
            stream.pause_stream();
        }

        yield Message::Barrier(barrier);

        loop {
            // a list file stream never ends, keep list to find if there is any new file.
            while let Some(msg) = stream.next().await {
                match msg {
                    Err(e) => {
                        tracing::warn!(error = %e.as_report(), "encountered an error, recovering");
                        stream
                            .replace_data_stream(self.build_chunked_paginate_stream(&source_desc)?);
                    }
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

            stream.replace_data_stream(
                self.build_chunked_paginate_stream(&source_desc)
                    .map_err(StreamExecutorError::from)?,
            );
        }
    }
}

impl<S: StateStore> Execute for FsListExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for FsListExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("FsListExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .finish()
        } else {
            f.debug_struct("FsListExecutor").finish()
        }
    }
}
