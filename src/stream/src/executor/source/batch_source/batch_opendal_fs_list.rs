// Copyright 2026 RisingWave Labs
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
use futures_async_stream::try_stream;
use parking_lot::RwLock;
use risingwave_common::array::Op;
use risingwave_common::id::TableId;
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::filesystem::opendal_source::{OpendalEnumerator, OpendalS3};
use risingwave_connector::source::reader::desc::{SourceDesc, SourceDescBuilder};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::prelude::*;
use crate::executor::source::{StreamSourceCore, barrier_to_message_stream};
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::LocalBarrierManager;

pub struct BatchOpendalFsListExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Streaming source for external
    stream_source_core: StreamSourceCore<S>,

    /// Metrics for monitor.
    #[expect(dead_code)]
    metrics: Arc<StreamingMetrics>,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

    /// System parameter reader to read barrier interval.
    #[expect(dead_code)]
    system_params: SystemParamsReaderRef,

    /// Rate limit in rows/s.
    #[expect(dead_code)]
    rate_limit_rps: Option<u32>,

    /// Local barrier manager for reporting list finished.
    barrier_manager: LocalBarrierManager,

    associated_table_id: TableId,
}

impl<S: StateStore> BatchOpendalFsListExecutor<S> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        system_params: SystemParamsReaderRef,
        rate_limit_rps: Option<u32>,
        barrier_manager: LocalBarrierManager,
        associated_table_id: Option<TableId>,
    ) -> Self {
        assert!(associated_table_id.is_some());
        Self {
            actor_ctx,
            stream_source_core,
            metrics,
            barrier_receiver: Some(barrier_receiver),
            system_params,
            rate_limit_rps,
            barrier_manager,
            associated_table_id: associated_table_id.unwrap(),
        }
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn build_chunked_paginate_stream(
        source_desc: SourceDesc,
        is_list_finished: Arc<RwLock<bool>>,
    ) {
        let ConnectorProperties::OpendalS3(properties) = source_desc.source.config.clone() else {
            unreachable!("BatchOpendalFsListExecutor must be used with S3 connector")
        };
        let lister: OpendalEnumerator<OpendalS3> = OpendalEnumerator::new_s3_source(
            &properties.s3_properties,
            properties.assume_role,
            properties.fs_common.compression_format,
        )
        .map_err(StreamExecutorError::connector_error)?;
        let matcher = lister.get_matcher().clone();
        let mut object_metadata_iter = lister
            .list()
            .await
            .map_err(StreamExecutorError::connector_error)?;

        while let Some(item) = object_metadata_iter.next().await {
            let page_item = item.map_err(StreamExecutorError::connector_error)?;
            if !matcher
                .as_ref()
                .map(|m| m.matches(&page_item.name))
                .unwrap_or(true)
            {
                continue;
            }
            let row = (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(ScalarImpl::Utf8(page_item.name.into_boxed_str())),
                    Some(ScalarImpl::Timestamptz(page_item.timestamp)),
                    Some(ScalarImpl::Int64(page_item.size)),
                ]),
            );
            yield StreamChunk::from_rows(
                &[row],
                &[DataType::Varchar, DataType::Timestamptz, DataType::Int64],
            );
        }

        *is_list_finished.write() = true;
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let first_barrier = barrier_receiver
            .recv()
            .instrument_await("source_recv_first_barrier")
            .await
            .ok_or_else(|| {
                anyhow!(
                    "failed to receive the first barrier, actor_id: {:?}, source_id: {:?}",
                    self.actor_ctx.id,
                    self.stream_source_core.source_id
                )
            })?;

        let mut core = self.stream_source_core;

        // Build source description from the builder.
        let source_desc_builder: SourceDescBuilder = core.source_desc_builder.take().unwrap();
        let source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;

        // Return the ownership of `stream_source_core` to the source executor.
        self.stream_source_core = core;

        yield Message::Barrier(first_barrier);

        let barrier_stream = barrier_to_message_stream(barrier_receiver).boxed();
        let mut stream = StreamReaderWithPause::<true, _>::new(
            barrier_stream,
            futures::stream::pending().boxed(),
        );

        let mut is_refreshing = false;
        let is_list_finished = Arc::new(RwLock::new(false));

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::error!(
                        error = %e.as_report(),
                        "encountered an error in batch opendal fs list"
                    );
                    return Err(e);
                }
                Ok(msg) => match msg {
                    Either::Left(msg) => match msg {
                        Message::Barrier(barrier) => {
                            if let Some(mutation) = barrier.mutation.as_deref() {
                                match mutation {
                                    Mutation::Pause => stream.pause_stream(),
                                    Mutation::Resume => stream.resume_stream(),
                                    Mutation::RefreshStart {
                                        associated_source_id,
                                        ..
                                    } if associated_source_id
                                        == &self.stream_source_core.source_id =>
                                    {
                                        tracing::info!(
                                            ?barrier.epoch,
                                            actor_id = %self.actor_ctx.id,
                                            source_id = %self.stream_source_core.source_id,
                                            table_id = %self.associated_table_id,
                                            "RefreshStart triggered OpenDAL file re-listing"
                                        );
                                        is_refreshing = true;
                                        *is_list_finished.write() = false;
                                        stream.replace_data_stream(
                                            Self::build_chunked_paginate_stream(
                                                source_desc.clone(),
                                                is_list_finished.clone(),
                                            )
                                            .boxed(),
                                        );
                                    }
                                    _ => (),
                                }
                            }

                            if is_refreshing && *is_list_finished.read() && barrier.is_checkpoint()
                            {
                                tracing::info!(
                                    ?barrier.epoch,
                                    source_id = %self.stream_source_core.source_id,
                                    table_id = %self.associated_table_id,
                                    "reporting batch OpenDAL list finished"
                                );
                                self.barrier_manager.report_source_list_finished(
                                    barrier.epoch,
                                    self.actor_ctx.id,
                                    self.associated_table_id,
                                    self.stream_source_core.source_id,
                                );
                                is_refreshing = false;
                            }

                            yield Message::Barrier(barrier);
                        }
                        _ => unreachable!(),
                    },
                    Either::Right(chunk) => {
                        yield Message::Chunk(chunk);
                    }
                },
            }
        }
    }
}

impl<S: StateStore> Execute for BatchOpendalFsListExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for BatchOpendalFsListExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchOpendalFsListExecutor")
            .field("source_id", &self.stream_source_core.source_id)
            .field("column_ids", &self.stream_source_core.column_ids)
            .finish()
    }
}
