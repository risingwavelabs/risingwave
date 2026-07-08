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

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

use either::Either;
use futures::TryStreamExt;
use futures::stream::{self, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::ColumnId;
use risingwave_common::id::TableId;
use risingwave_connector::source::filesystem::OpendalFsSplit;
use risingwave_connector::source::filesystem::opendal_source::OpendalSource;
use risingwave_connector::source::reader::desc::SourceDesc;
use risingwave_connector::source::{
    BoxStreamingFileSourceChunkStream, SourceContext, SourceCtrlOpts, SplitImpl,
};
use thiserror_ext::AsReport;

use crate::common::rate_limit::limited_chunk_size;
use crate::executor::prelude::*;
use crate::executor::source::{
    StreamSourceCore, apply_rate_limit_with_for_streaming_file_source_reader,
    get_split_offset_col_idx, prune_additional_cols, source_reader_event_to_chunk_stream,
};
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::LocalBarrierManager;

const BATCH_SIZE: usize = 1000;

pub struct BatchOpendalFsFetchExecutor<S: StateStore, Src: OpendalSource>
where
    SplitImpl: From<OpendalFsSplit<Src>>,
{
    actor_ctx: ActorContextRef,

    /// Core component for managing external streaming source state.
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Upstream list executor that provides file assignments.
    upstream: Option<Executor>,

    /// Optional rate limit in rows/s to control data ingestion speed.
    rate_limit_rps: Option<u32>,

    /// Local barrier manager for reporting load finished.
    barrier_manager: LocalBarrierManager,

    associated_table_id: TableId,

    _marker: PhantomData<Src>,
}

impl<S: StateStore, Src: OpendalSource> BatchOpendalFsFetchExecutor<S, Src>
where
    SplitImpl: From<OpendalFsSplit<Src>>,
{
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        upstream: Executor,
        rate_limit_rps: Option<u32>,
        barrier_manager: LocalBarrierManager,
        associated_table_id: Option<TableId>,
    ) -> Self {
        assert!(associated_table_id.is_some());
        Self {
            actor_ctx,
            stream_source_core: Some(stream_source_core),
            upstream: Some(upstream),
            rate_limit_rps,
            barrier_manager,
            associated_table_id: associated_table_id.unwrap(),
            _marker: PhantomData,
        }
    }

    fn build_source_ctx(
        actor_ctx: &ActorContextRef,
        source_desc: &SourceDesc,
        core: &StreamSourceCore<S>,
        rate_limit_rps: Option<u32>,
    ) -> SourceContext {
        SourceContext::new(
            actor_ctx.id,
            core.source_id,
            actor_ctx.fragment_id,
            core.source_name.clone(),
            source_desc.metrics.clone(),
            SourceCtrlOpts {
                chunk_size: limited_chunk_size(rate_limit_rps),
                split_txn: rate_limit_rps.is_some(),
            },
            source_desc.source.config.clone(),
            None,
        )
    }

    async fn build_single_file_stream_reader(
        column_ids: Vec<ColumnId>,
        source_ctx: SourceContext,
        source_desc: &SourceDesc,
        split: OpendalFsSplit<Src>,
        rate_limit_rps: Option<u32>,
    ) -> StreamExecutorResult<BoxStreamingFileSourceChunkStream> {
        let (stream, _) = source_desc
            .source
            .build_stream(
                Some(vec![SplitImpl::from(split)]),
                column_ids,
                Arc::new(source_ctx),
                false,
            )
            .await
            .map_err(StreamExecutorError::connector_error)?;
        let optional_stream: BoxStreamingFileSourceChunkStream =
            source_reader_event_to_chunk_stream(stream)
                .boxed()
                .map(|item| item.map(Some))
                .chain(stream::once(async { Ok(None) }))
                .boxed();
        Ok(
            apply_rate_limit_with_for_streaming_file_source_reader(optional_stream, rate_limit_rps)
                .boxed(),
        )
    }

    async fn replace_with_new_batch_reader<const BIASED: bool>(
        files_in_progress: &mut usize,
        file_queue: &mut VecDeque<OpendalFsSplit<Src>>,
        stream: &mut StreamReaderWithPause<BIASED, Option<StreamChunk>>,
        column_ids: Vec<ColumnId>,
        source_ctx: SourceContext,
        source_desc: &SourceDesc,
        rate_limit_rps: Option<u32>,
    ) -> StreamExecutorResult<()> {
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        for _ in 0..BATCH_SIZE {
            let Some(split) = file_queue.pop_front() else {
                break;
            };
            batch.push(split);
        }

        if batch.is_empty() {
            stream.replace_data_stream(stream::pending().boxed());
        } else {
            *files_in_progress += batch.len();
            let mut merged_stream =
                stream::empty::<StreamExecutorResult<Option<StreamChunk>>>().boxed();
            for split in batch {
                let single_file_stream = Self::build_single_file_stream_reader(
                    column_ids.clone(),
                    source_ctx.clone(),
                    source_desc,
                    split,
                    rate_limit_rps,
                )
                .await?
                .map_err(StreamExecutorError::connector_error);
                merged_stream = merged_stream.chain(single_file_stream).boxed();
            }
            stream.replace_data_stream(merged_stream);
        }

        Ok(())
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut upstream = self.upstream.take().unwrap().execute();
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let is_pause_on_startup = first_barrier.is_pause_on_startup();
        yield Message::Barrier(first_barrier);

        let mut core = self.stream_source_core.take().unwrap();
        let source_desc = core
            .source_desc_builder
            .take()
            .unwrap()
            .build()
            .map_err(StreamExecutorError::connector_error)?;

        let (Some(split_idx), Some(offset_idx), _) = get_split_offset_col_idx(&source_desc.columns)
        else {
            unreachable!("Partition and offset columns must be set.");
        };

        let mut files_in_progress = 0;
        let mut file_queue = VecDeque::new();
        let mut list_finished = false;
        let mut is_refreshing = false;
        let mut stream = StreamReaderWithPause::<true, Option<StreamChunk>>::new(
            upstream,
            stream::pending().boxed(),
        );

        if is_pause_on_startup {
            stream.pause_stream();
        }

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::error!(error = %e.as_report(), "Batch OpenDAL fetch error");
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
                                    } if associated_source_id == &core.source_id => {
                                        tracing::info!(
                                            ?barrier.epoch,
                                            actor_id = %self.actor_ctx.id,
                                            source_id = %core.source_id,
                                            table_id = %self.associated_table_id,
                                            queue_len = file_queue.len(),
                                            files_in_progress,
                                            "RefreshStart: clearing batch OpenDAL fetch state"
                                        );
                                        file_queue.clear();
                                        files_in_progress = 0;
                                        list_finished = false;
                                        is_refreshing = true;
                                        stream.replace_data_stream(stream::pending().boxed());
                                    }
                                    Mutation::ListFinish {
                                        associated_source_id,
                                    } if associated_source_id == &core.source_id => {
                                        tracing::info!(
                                            ?barrier.epoch,
                                            actor_id = %self.actor_ctx.id,
                                            source_id = %core.source_id,
                                            table_id = %self.associated_table_id,
                                            "received ListFinish mutation"
                                        );
                                        list_finished = true;
                                    }
                                    _ => (),
                                }
                            }

                            if files_in_progress == 0
                                && file_queue.is_empty()
                                && list_finished
                                && is_refreshing
                                && barrier.is_checkpoint()
                            {
                                tracing::info!(
                                    ?barrier.epoch,
                                    actor_id = %self.actor_ctx.id,
                                    source_id = %core.source_id,
                                    table_id = %self.associated_table_id,
                                    "Reporting batch OpenDAL source load finished"
                                );
                                self.barrier_manager.report_source_load_finished(
                                    barrier.epoch,
                                    self.actor_ctx.id,
                                    self.associated_table_id,
                                    core.source_id,
                                );
                                list_finished = false;
                                is_refreshing = false;
                            }

                            yield Message::Barrier(barrier);

                            if files_in_progress == 0 && !file_queue.is_empty() && is_refreshing {
                                let source_ctx = Self::build_source_ctx(
                                    &self.actor_ctx,
                                    &source_desc,
                                    &core,
                                    self.rate_limit_rps,
                                );
                                Self::replace_with_new_batch_reader(
                                    &mut files_in_progress,
                                    &mut file_queue,
                                    &mut stream,
                                    core.column_ids.clone(),
                                    source_ctx,
                                    &source_desc,
                                    self.rate_limit_rps,
                                )
                                .await?;
                            }
                        }
                        Message::Chunk(chunk) => {
                            for row in chunk.data_chunk().rows() {
                                let filename = row.datum_at(0).unwrap().into_utf8();
                                let size = row.datum_at(2).unwrap().into_int64();

                                if size > 0 {
                                    file_queue.push_back(OpendalFsSplit::<Src>::new(
                                        filename.to_owned(),
                                        0,
                                        size as usize,
                                    ));
                                }
                            }

                            tracing::debug!(
                                actor_id = %self.actor_ctx.id,
                                source_id = %core.source_id,
                                queue_len = file_queue.len(),
                                "Added OpenDAL file assignments to batch fetch queue"
                            );
                        }
                        Message::Watermark(_) => unreachable!(),
                    },
                    Either::Right(optional_chunk) => match optional_chunk {
                        Some(chunk) => {
                            let chunk = prune_additional_cols(
                                &chunk,
                                &[split_idx, offset_idx],
                                &source_desc.columns,
                            );
                            yield Message::Chunk(chunk);
                        }
                        None => {
                            files_in_progress = files_in_progress.saturating_sub(1);
                        }
                    },
                },
            }
        }
    }
}

impl<S: StateStore, Src: OpendalSource> Execute for BatchOpendalFsFetchExecutor<S, Src>
where
    SplitImpl: From<OpendalFsSplit<Src>>,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore, Src: OpendalSource> Debug for BatchOpendalFsFetchExecutor<S, Src>
where
    SplitImpl: From<OpendalFsSplit<Src>>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("BatchOpendalFsFetchExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .finish()
        } else {
            f.debug_struct("BatchOpendalFsFetchExecutor").finish()
        }
    }
}
