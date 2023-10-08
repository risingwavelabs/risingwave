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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::ops::Bound;
use std::pin::Pin;
use std::sync::Arc;

use either::Either;
use futures::stream::{self, SelectAll, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::{ColumnId, Schema, TableId};
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{ScalarRef, ScalarRefImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::select_all;
use risingwave_common::util::value_encoding::BasicSerde;
use risingwave_connector::source::filesystem::FsSplit;
use risingwave_connector::source::{
    BoxSourceWithStateStream, SourceContext, SourceCtrlOpts, SplitId, SplitImpl, SplitMetaData,
    StreamChunkWithState,
};
use risingwave_connector::ConnectorParams;
use risingwave_source::source_desc::SourceDesc;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use crate::common::table::state_table::KeyedRowStream;
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::executor::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor, Message,
    Mutation, PkIndices, PkIndicesRef, SourceStateTableHandler, StreamExecutorError,
    StreamExecutorResult, StreamSourceCore,
};

type StateTableIter<'a, S> = SelectAll<Pin<Box<KeyedRowStream<'a, S, BasicSerde>>>>;

pub struct FsFetchExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    identity: String,

    schema: Schema,

    pk_indices: PkIndices,

    /// Streaming source for external
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Upstream list executor.
    upstream: Option<BoxedExecutor>,

    // control options for connector level
    source_ctrl_opts: SourceCtrlOpts,

    // config for the connector node
    connector_params: ConnectorParams,
}

impl<S: StateStore> FsFetchExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        schema: Schema,
        pk_indices: PkIndices,
        stream_source_core: StreamSourceCore<S>,
        executor_id: u64,
        upstream: BoxedExecutor,
        source_ctrl_opts: SourceCtrlOpts,
        connector_params: ConnectorParams,
    ) -> Self {
        Self {
            actor_ctx,
            identity: format!("FsFetchExecutor {:X}", executor_id),
            schema,
            pk_indices,
            stream_source_core: Some(stream_source_core),
            upstream: Some(upstream),
            source_ctrl_opts,
            connector_params,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_replace_with_new_reader<'a, const BIASED: bool>(
        is_datastream_empty: &mut bool,
        _state_store_handler: &'a SourceStateTableHandler<S>,
        state_cache: &mut HashMap<SplitId, SplitImpl>,
        column_ids: Vec<ColumnId>,
        source_ctx: SourceContext,
        source_desc: &SourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, StreamChunkWithState>,
        state_table_iter: &mut StateTableIter<'a, S>,
    ) -> StreamExecutorResult<()> {
        let fs_split = state_cache // Peek into state cache first.
            .iter()
            .find(|(_, split)| {
                let fs_split = split.as_fs().unwrap();
                fs_split.offset < fs_split.size
            })
            .map(|(_, split)| split.as_fs().unwrap().to_owned())
            .or(loop {
                // Otherwise find the next assignment in the state table.
                if let Some(item) = state_table_iter.next().await {
                    let row = item?;
                    let split_id = match row.datum_at(0) {
                        Some(ScalarRefImpl::Utf8(split_id)) => split_id,
                        _ => unreachable!(),
                    };

                    // The state cache holds the latest status of the split.
                    // Entering this branch means in the state cache offset >= size.
                    // Hence we skip this split in the state table.
                    if state_cache.contains_key(split_id) {
                        continue;
                    }

                    let fs_split = match row.datum_at(1) {
                        Some(ScalarRefImpl::Jsonb(jsonb_ref)) => {
                            SplitImpl::restore_from_json(jsonb_ref.to_owned_scalar())?
                                .as_fs()
                                .unwrap()
                                .to_owned()
                        }
                        _ => unreachable!(),
                    };

                    // Cache the assignment retrieved from state table.
                    state_cache.insert(split_id.into(), fs_split.clone().into());
                    break Some(fs_split);
                }

                break None;
            });

        if let Some(fs_split) = fs_split {
            stream.replace_data_stream(
                Self::build_stream_source_reader(column_ids, source_ctx, source_desc, fs_split)
                    .await?,
            );
            *is_datastream_empty = false;
        } else {
            stream.replace_data_stream(stream::pending().boxed());
            *is_datastream_empty = true;
        };

        Ok(())
    }

    async fn take_snapshot_and_flush(
        state_store_handler: &mut SourceStateTableHandler<S>,
        state_cache: &mut HashMap<SplitId, SplitImpl>,
        epoch: EpochPair,
    ) -> StreamExecutorResult<()> {
        let mut to_flush = Vec::new();
        let mut to_delete = Vec::new();
        state_cache.iter().for_each(|(_, split)| {
            let fs_split = split.as_fs().unwrap();
            if fs_split.offset >= fs_split.size {
                // If read out, try delete in the state table
                to_delete.push(split.to_owned());
            } else {
                // Otherwise, flush to state table
                to_flush.push(split.to_owned());
            }
        });
        state_cache.clear();

        if !to_flush.is_empty() {
            state_store_handler.take_snapshot(to_flush).await?;
        }
        if !to_delete.is_empty() {
            state_store_handler.trim_state(&to_delete).await?;
        }
        state_store_handler.state_store.commit(epoch).await?;
        Ok(())
    }

    async fn build_stream_source_reader(
        column_ids: Vec<ColumnId>,
        source_ctx: SourceContext,
        source_desc: &SourceDesc,
        split: FsSplit,
    ) -> StreamExecutorResult<BoxSourceWithStateStream> {
        source_desc
            .source
            .fs_stream_reader(column_ids, Arc::new(source_ctx), split)
            .await
            .map_err(StreamExecutorError::connector_error)
    }

    fn build_source_ctx(&self, source_desc: &SourceDesc, source_id: TableId) -> SourceContext {
        SourceContext::new_with_suppressor(
            self.actor_ctx.id,
            source_id,
            self.actor_ctx.fragment_id,
            source_desc.metrics.clone(),
            self.source_ctrl_opts.clone(),
            self.connector_params.connector_client.clone(),
            self.actor_ctx.error_suppressor.clone(),
        )
    }

    async fn build_state_table_iter(
        state_store_handler: &SourceStateTableHandler<S>,
    ) -> StreamExecutorResult<StateTableIter<'_, S>> {
        Ok(select_all({
            let mut store_iter_collect =
                Vec::with_capacity(state_store_handler.state_store.vnodes().len());
            for vnodes in state_store_handler.state_store.vnodes().iter_vnodes() {
                store_iter_collect.push(Box::pin(
                    state_store_handler
                        .state_store
                        .iter_row_with_pk_range(
                            &(Bound::<OwnedRow>::Unbounded, Bound::<OwnedRow>::Unbounded),
                            vnodes,
                            PrefetchOptions::new_for_exhaust_iter(),
                        )
                        .await?,
                ))
            }
            store_iter_collect
        }))
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut upstream = self.upstream.take().unwrap().execute();
        let barrier = expect_first_barrier(&mut upstream).await?;

        let mut core = self.stream_source_core.take().unwrap();
        let mut state_store_handler = core.split_state_store;
        let mut state_cache = core.state_cache;

        // Build source description from the builder.
        let source_desc_builder = core.source_desc_builder.take().unwrap();

        let source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;

        // Initialize state table.
        state_store_handler.init_epoch(barrier.epoch);

        let mut is_datastream_empty = true;
        let mut stream = StreamReaderWithPause::<true, StreamChunkWithState>::new(
            upstream,
            stream::pending().boxed(),
        );

        if barrier.is_pause_on_startup() {
            stream.pause_stream();
        }

        let mut state_table_iter = Self::build_state_table_iter(&state_store_handler).await?;

        // If it is a recovery startup,
        // there can be file assignments in the state table.
        // Hence we try to build a reader first.
        Self::try_replace_with_new_reader(
            &mut is_datastream_empty,
            &state_store_handler,
            &mut state_cache,
            core.column_ids.clone(),
            self.build_source_ctx(&source_desc, core.source_id),
            &source_desc,
            &mut stream,
            &mut state_table_iter,
        )
        .await?;

        yield Message::Barrier(barrier);

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::error!("Fetch error: {:?}", e);
                    todo!()
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

                                    drop(state_table_iter);
                                    Self::take_snapshot_and_flush(
                                        &mut state_store_handler,
                                        &mut state_cache,
                                        barrier.epoch,
                                    )
                                    .await?;

                                    if let Some(vnode_bitmap) =
                                        barrier.as_update_vnode_bitmap(self.actor_ctx.id)
                                    {
                                        // if _cache_may_stale, we must rebuild the stream to adjust vnode mappings
                                        let (_prev_vnode_bitmap, _cache_may_stale) =
                                            state_store_handler
                                                .state_store
                                                .update_vnode_bitmap(vnode_bitmap);
                                    }

                                    // Rebuild state table iterator.
                                    state_table_iter =
                                        Self::build_state_table_iter(&state_store_handler).await?;

                                    // Propagate the barrier.
                                    yield msg;
                                }
                                // Receiving file assignments from upstream list executor,
                                // store FsSplit into the cache.
                                Message::Chunk(chunk) => {
                                    let file_assignment = chunk.data_chunk().rows().map(|row| {
                                        let filename = row.datum_at(0).unwrap().into_utf8();
                                        let size = row.datum_at(2).unwrap().into_int64();
                                        (
                                            Arc::<str>::from(filename),
                                            FsSplit::new(filename.to_owned(), 0, size as usize)
                                                .into(),
                                        )
                                    });
                                    state_cache.extend(file_assignment);

                                    // When both of state cache and state table are empty,
                                    // the right arm of stream is a pending stream,
                                    // and is_datastream_empty is set to true,
                                    // and a new reader should be built.
                                    if is_datastream_empty {
                                        Self::try_replace_with_new_reader(
                                            &mut is_datastream_empty,
                                            &state_store_handler,
                                            &mut state_cache,
                                            core.column_ids.clone(),
                                            self.build_source_ctx(&source_desc, core.source_id),
                                            &source_desc,
                                            &mut stream,
                                            &mut state_table_iter,
                                        )
                                        .await?;
                                    }
                                }
                                _ => unreachable!(),
                            }
                        }
                        // StreamChunk from FsSourceReader, and the reader reads only one file.
                        // If the file read out, replace with a new file reader.
                        Either::Right(StreamChunkWithState {
                            chunk,
                            split_offset_mapping,
                        }) => {
                            let mapping = split_offset_mapping.unwrap();
                            debug_assert_eq!(mapping.len(), 1);

                            let (split_id, offset) = mapping.iter().nth(0).unwrap();
                            if !state_cache.contains_key(split_id) {
                                // The data chunk may haven't been produced by reader yet when
                                // a barrier arrives and the state cache is flushed and cleared.
                                // Here we expect the fs_split lies in the state store and
                                // store it into the state cache.
                                let row = state_store_handler
                                    .get(split_id.to_owned())
                                    .await?
                                    .expect(&format!(
                                        "FsSplit with id {} should be in the state table.",
                                        split_id
                                    ));
                                if let Some(ScalarRefImpl::Jsonb(jsonb_ref)) = row.datum_at(1) {
                                    let split =
                                        SplitImpl::restore_from_json(jsonb_ref.to_owned_scalar())?;
                                    state_cache.insert(split_id.to_owned(), split);
                                }
                            }

                            // Get FsSplit in the state cache.
                            let mut cache_entry = match state_cache.entry(split_id.to_owned()) {
                                Entry::Occupied(entry) => entry,
                                Entry::Vacant(_) => unreachable!(),
                            };

                            // Update the offset in the state cache.
                            // If offset == size, the entry
                            // will be deleted after the next barrier.
                            let offset = offset.parse().unwrap();
                            let mut fs_split = cache_entry.get().to_owned().into_fs().unwrap();
                            let fs_split_size = fs_split.size;
                            fs_split.offset = offset;
                            cache_entry.insert(fs_split.into());

                            // The file is read out, build a new reader.
                            if offset >= fs_split_size {
                                debug_assert_eq!(offset, fs_split_size);
                                Self::try_replace_with_new_reader(
                                    &mut is_datastream_empty,
                                    &state_store_handler,
                                    &mut state_cache,
                                    core.column_ids.clone(),
                                    self.build_source_ctx(&source_desc, core.source_id),
                                    &source_desc,
                                    &mut stream,
                                    &mut state_table_iter,
                                )
                                .await?;
                            }

                            yield Message::Chunk(chunk);
                        }
                    }
                }
            }
        }
    }
}

impl<S: StateStore> Executor for FsFetchExecutor<S> {
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

impl<S: StateStore> Debug for FsFetchExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("FsFetchExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .field("pk_indices", &self.pk_indices)
                .finish()
        } else {
            f.debug_struct("FsFetchExecutor").finish()
        }
    }
}
