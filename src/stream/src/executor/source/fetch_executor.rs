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

use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::Bound;
use std::sync::Arc;

use either::Either;
use futures::stream::{self, StreamExt};
use futures::{pin_mut, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{ColumnId, Schema, TableId};
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{ScalarRef, ScalarRefImpl};
use risingwave_connector::source::filesystem::opendal_source::{
    OpendalGcs, OpendalPosixFs, OpendalS3, OpendalSource,
};
use risingwave_connector::source::filesystem::OpendalFsSplit;
use risingwave_connector::source::reader::desc::SourceDesc;
use risingwave_connector::source::{
    BoxChunkSourceStream, SourceContext, SourceCtrlOpts, SplitImpl, SplitMetaData,
};
use risingwave_connector::ConnectorParams;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;
use thiserror_ext::AsReport;

use crate::executor::stream_reader::StreamReaderWithPause;
use crate::executor::*;

const SPLIT_BATCH_SIZE: usize = 1000;

type SplitBatch = Option<Vec<SplitImpl>>;

pub struct FsFetchExecutor<S: StateStore, Src: OpendalSource> {
    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Streaming source for external
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Upstream list executor.
    upstream: Option<BoxedExecutor>,

    // control options for connector level
    source_ctrl_opts: SourceCtrlOpts,

    // config for the connector node
    connector_params: ConnectorParams,

    _marker: PhantomData<Src>,
}

impl<S: StateStore, Src: OpendalSource> FsFetchExecutor<S, Src> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        info: ExecutorInfo,
        stream_source_core: StreamSourceCore<S>,
        upstream: BoxedExecutor,
        source_ctrl_opts: SourceCtrlOpts,
        connector_params: ConnectorParams,
    ) -> Self {
        Self {
            actor_ctx,
            info,
            stream_source_core: Some(stream_source_core),
            upstream: Some(upstream),
            source_ctrl_opts,
            connector_params,
            _marker: PhantomData,
        }
    }

    async fn replace_with_new_batch_reader<const BIASED: bool>(
        splits_on_fetch: &mut usize,
        state_store_handler: &SourceStateTableHandler<S>,
        column_ids: Vec<ColumnId>,
        source_ctx: SourceContext,
        source_desc: &SourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, StreamChunk>,
    ) -> StreamExecutorResult<()> {
        let mut batch = Vec::with_capacity(SPLIT_BATCH_SIZE);
        'vnodes: for vnode in state_store_handler.state_store.vnodes().iter_vnodes() {
            let table_iter = state_store_handler
                .state_store
                .iter_with_vnode(
                    vnode,
                    &(Bound::<OwnedRow>::Unbounded, Bound::<OwnedRow>::Unbounded),
                    // This usage is similar with `backfill`. So we only need to fetch a large data rather than establish a connection for a whole object.
                    PrefetchOptions::prefetch_for_small_range_scan(),
                )
                .await?;
            pin_mut!(table_iter);
            let properties = source_desc.source.config.clone();
            while let Some(item) = table_iter.next().await {
                let row = item?;
                let split = match row.datum_at(1) {
                    Some(ScalarRefImpl::Jsonb(jsonb_ref)) => match properties {
                        risingwave_connector::source::ConnectorProperties::Gcs(_) => {
                            let split: OpendalFsSplit<OpendalGcs> =
                                OpendalFsSplit::restore_from_json(jsonb_ref.to_owned_scalar())?;
                            SplitImpl::from(split)
                        }
                        risingwave_connector::source::ConnectorProperties::OpendalS3(_) => {
                            let split: OpendalFsSplit<OpendalS3> =
                                OpendalFsSplit::restore_from_json(jsonb_ref.to_owned_scalar())?;
                            SplitImpl::from(split)
                        }
                        risingwave_connector::source::ConnectorProperties::PosixFs(_) => {
                            let split: OpendalFsSplit<OpendalPosixFs> =
                                OpendalFsSplit::restore_from_json(jsonb_ref.to_owned_scalar())?;
                            SplitImpl::from(split)
                        }
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                };
                batch.push(split);

                if batch.len() >= SPLIT_BATCH_SIZE {
                    break 'vnodes;
                }
            }
        }
        if batch.is_empty() {
            stream.replace_data_stream(stream::pending().boxed());
        } else {
            *splits_on_fetch += batch.len();
            let batch_reader =
                Self::build_batched_stream_reader(column_ids, source_ctx, source_desc, Some(batch))
                    .await?
                    .map_err(StreamExecutorError::connector_error);
            stream.replace_data_stream(batch_reader);
        }

        Ok(())
    }

    async fn build_batched_stream_reader(
        column_ids: Vec<ColumnId>,
        source_ctx: SourceContext,
        source_desc: &SourceDesc,
        batch: SplitBatch,
    ) -> StreamExecutorResult<BoxChunkSourceStream> {
        source_desc
            .source
            .to_stream(batch, column_ids, Arc::new(source_ctx))
            .await
            .map_err(StreamExecutorError::connector_error)
    }

    fn build_source_ctx(
        &self,
        source_desc: &SourceDesc,
        source_id: TableId,
        source_name: &str,
    ) -> SourceContext {
        SourceContext::new_with_suppressor(
            self.actor_ctx.id,
            source_id,
            self.actor_ctx.fragment_id,
            source_desc.metrics.clone(),
            self.source_ctrl_opts.clone(),
            self.connector_params.connector_client.clone(),
            self.actor_ctx.error_suppressor.clone(),
            source_desc.source.config.clone(),
            source_name.to_owned(),
        )
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut upstream = self.upstream.take().unwrap().execute();
        let barrier = expect_first_barrier(&mut upstream).await?;

        let mut core = self.stream_source_core.take().unwrap();
        let mut state_store_handler = core.split_state_store;

        // Build source description from the builder.
        let source_desc_builder = core.source_desc_builder.take().unwrap();

        let source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;

        let (Some(split_idx), Some(offset_idx)) = get_split_offset_col_idx(&source_desc.columns)
        else {
            unreachable!("Partition and offset columns must be set.");
        };

        // Initialize state table.
        state_store_handler.init_epoch(barrier.epoch);

        let mut splits_on_fetch: usize = 0;
        let mut stream =
            StreamReaderWithPause::<true, StreamChunk>::new(upstream, stream::pending().boxed());

        if barrier.is_pause_on_startup() {
            stream.pause_stream();
        }

        // If it is a recovery startup,
        // there can be file assignments in the state table.
        // Hence we try building a reader first.
        Self::replace_with_new_batch_reader(
            &mut splits_on_fetch,
            &state_store_handler,
            core.column_ids.clone(),
            self.build_source_ctx(&source_desc, core.source_id, &core.source_name),
            &source_desc,
            &mut stream,
        )
        .await?;

        yield Message::Barrier(barrier);

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::error!(error = %e.as_report(), "Fetch Error");
                    splits_on_fetch = 0;
                }
                Ok(msg) => {
                    match msg {
                        // This branch will be preferred.
                        Either::Left(msg) => {
                            match &msg {
                                Message::Barrier(barrier) => {
                                    if let Some(mutation) = barrier.mutation.as_deref() {
                                        match mutation {
                                            Mutation::Pause => stream.pause_stream(),
                                            Mutation::Resume => stream.resume_stream(),
                                            _ => (),
                                        }
                                    }

                                    state_store_handler
                                        .state_store
                                        .commit(barrier.epoch)
                                        .await?;

                                    if let Some(vnode_bitmap) =
                                        barrier.as_update_vnode_bitmap(self.actor_ctx.id)
                                    {
                                        // if _cache_may_stale, we must rebuild the stream to adjust vnode mappings
                                        let (_prev_vnode_bitmap, cache_may_stale) =
                                            state_store_handler
                                                .state_store
                                                .update_vnode_bitmap(vnode_bitmap);

                                        if cache_may_stale {
                                            splits_on_fetch = 0;
                                        }
                                    }

                                    if splits_on_fetch == 0 {
                                        Self::replace_with_new_batch_reader(
                                            &mut splits_on_fetch,
                                            &state_store_handler,
                                            core.column_ids.clone(),
                                            self.build_source_ctx(
                                                &source_desc,
                                                core.source_id,
                                                &core.source_name,
                                            ),
                                            &source_desc,
                                            &mut stream,
                                        )
                                        .await?;
                                    }

                                    // Propagate the barrier.
                                    yield msg;
                                }
                                // Receiving file assignments from upstream list executor,
                                // store into state table.
                                Message::Chunk(chunk) => {
                                    let file_assignment = chunk
                                        .data_chunk()
                                        .rows()
                                        .map(|row| {
                                            let filename = row.datum_at(0).unwrap().into_utf8();
                                            let size = row.datum_at(2).unwrap().into_int64();
                                            OpendalFsSplit::<Src>::new(
                                                filename.to_owned(),
                                                0,
                                                size as usize,
                                            )
                                        })
                                        .collect();
                                    state_store_handler.take_snapshot(file_assignment).await?;
                                    state_store_handler.state_store.try_flush().await?;
                                }
                                _ => unreachable!(),
                            }
                        }
                        // StreamChunk from FsSourceReader, and the reader reads only one file.
                        Either::Right(chunk) => {
                            let mapping =
                                get_split_offset_mapping_from_chunk(&chunk, split_idx, offset_idx)
                                    .unwrap();
                            debug_assert_eq!(mapping.len(), 1);
                            if let Some((split_id, offset)) = mapping.into_iter().next() {
                                let row = state_store_handler
                                    .get(split_id.clone())
                                    .await?
                                    .expect("The fs_split should be in the state table.");
                                let fs_split = match row.datum_at(1) {
                                    Some(ScalarRefImpl::Jsonb(jsonb_ref)) => {
                                        OpendalFsSplit::<Src>::restore_from_json(
                                            jsonb_ref.to_owned_scalar(),
                                        )?
                                    }
                                    _ => unreachable!(),
                                };

                                if offset.parse::<usize>().unwrap() >= fs_split.size {
                                    splits_on_fetch -= 1;
                                    state_store_handler.delete(split_id).await?;
                                } else {
                                    state_store_handler
                                        .set(split_id, fs_split.encode_to_json())
                                        .await?;
                                }
                            }

                            let chunk = prune_additional_cols(
                                &chunk,
                                split_idx,
                                offset_idx,
                                &source_desc.columns,
                            );
                            yield Message::Chunk(chunk);
                        }
                    }
                }
            }
        }
    }
}

impl<S: StateStore, Src: OpendalSource> Executor for FsFetchExecutor<S, Src> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

impl<S: StateStore, Src: OpendalSource> Debug for FsFetchExecutor<S, Src> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("FsFetchExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .field("pk_indices", &self.info.pk_indices)
                .finish()
        } else {
            f.debug_struct("FsFetchExecutor").finish()
        }
    }
}
