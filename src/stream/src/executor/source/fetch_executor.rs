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
use std::pin::Pin;
use std::sync::Arc;

use either::Either;
use futures::stream::{self, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::{ColumnId, Schema, TableId};
use risingwave_common::row::Row;
use risingwave_common::types::{ScalarRef, ScalarRefImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::value_encoding::BasicSerde;
use risingwave_connector::source::filesystem::FsSplit;
use risingwave_connector::source::{
    BoxSourceWithStateStream, SourceContext, SourceCtrlOpts, SplitId, SplitImpl, SplitMetaData,
    StreamChunkWithState,
};
use risingwave_connector::ConnectorParams;
use risingwave_source::source_desc::{SourceDesc, SourceDescBuilder};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use crate::common::table::state_table::KeyedRowStream;
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::executor::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor, Message,
    Mutation, PkIndices, PkIndicesRef, SourceStateTableHandler, StreamExecutorError,
    StreamExecutorResult, StreamSourceCore,
};

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

    async fn try_replace_with_new_reader<'a, const BIASED: bool>(
        is_datastream_empty: &mut bool,
        _state_store_handler: &'a SourceStateTableHandler<S>,
        state_cache: &mut HashMap<SplitId, SplitImpl>,
        column_ids: Vec<ColumnId>,
        source_ctx: SourceContext,
        source_desc: &SourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, StreamChunkWithState>,
        store_iter: &mut Pin<Box<KeyedRowStream<'a, S, BasicSerde>>>,
    ) -> StreamExecutorResult<()> {
        let fs_split = if let Some(item) = store_iter.next().await {
            // Find the next assignment in state store.
            let row = item?;
            let split_id = match row.datum_at(0) {
                Some(ScalarRefImpl::Utf8(split_id)) => split_id,
                _ => unreachable!(),
            };
            let fs_split = match row.datum_at(1) {
                Some(ScalarRefImpl::Jsonb(jsonb_ref)) => {
                    SplitImpl::restore_from_json(jsonb_ref.to_owned_scalar())?
                        .as_fs()
                        .unwrap()
                        .to_owned()
                }
                _ => unreachable!(),
            };

            // Cache the assignment retrieved from state store.
            state_cache.insert(split_id.into(), fs_split.clone().into());
            Some(fs_split)
        } else {
            // Find uncompleted assignment in state cache.
            state_cache
                .iter()
                .find(|(_, split)| {
                    let fs_split = split.as_fs().unwrap();
                    fs_split.offset < fs_split.size
                })
                .map(|(_, split)| split.as_fs().unwrap().to_owned())
        };

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
                // If read out, try delete in the state store
                to_delete.push(split.to_owned());
            } else {
                // Otherwise, flush to state store
                to_flush.push(split.to_owned());
            }
        });

        state_store_handler.take_snapshot(to_flush).await?;
        state_store_handler.trim_state(&to_delete).await?;
        state_store_handler.state_store.commit(epoch).await?;
        state_cache.clear();
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
            .source_reader(column_ids, Arc::new(source_ctx), split)
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

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut upstream = self.upstream.take().unwrap().execute();
        let barrier = expect_first_barrier(&mut upstream).await?;

        let mut core = self.stream_source_core.take().unwrap();
        let mut state_store_handler = core.split_state_store;
        let mut state_cache = core.state_cache;

        // Build source description from the builder.
        let source_desc_builder: SourceDescBuilder = core.source_desc_builder.take().unwrap();

        let source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;

        // Initialize state store.
        state_store_handler.init_epoch(barrier.epoch);

        let mut is_datastream_empty = true;
        let mut stream = StreamReaderWithPause::<true, StreamChunkWithState>::new(
            upstream,
            stream::pending().boxed(),
        );

        if barrier.is_pause_on_startup() {
            stream.pause_stream();
        }

        let mut store_iter = Box::pin(
            state_store_handler
                .state_store
                .iter_row(PrefetchOptions::new_with_exhaust_iter(false))
                .await?,
        );

        // If it is a recovery startup,
        // there can be file assignments in state store.
        // Hence we try to build a reader first.
        Self::try_replace_with_new_reader(
            &mut is_datastream_empty,
            &state_store_handler,
            &mut state_cache,
            core.column_ids.clone(),
            self.build_source_ctx(&source_desc, core.source_id),
            &source_desc,
            &mut stream,
            &mut store_iter,
        )
        .await?;

        yield Message::Barrier(barrier);

        while let Some(msg) = stream.next().await {
            match msg {
                Err(_) => {
                    todo!()
                }
                Ok(msg) => {
                    match msg {
                        // This branch will be preferred.
                        Either::Left(msg) => match &msg {
                            Message::Barrier(barrier) => {
                                if let Some(mutation) = barrier.mutation.as_deref() {
                                    match mutation {
                                        Mutation::Pause => stream.pause_stream(),
                                        Mutation::Resume => stream.resume_stream(),
                                        _ => (),
                                    }
                                }

                                drop(store_iter);
                                Self::take_snapshot_and_flush(
                                    &mut state_store_handler,
                                    &mut state_cache,
                                    barrier.epoch,
                                )
                                .await?;

                                // Rebuild state store iterator.
                                store_iter = Box::pin(
                                    state_store_handler
                                        .state_store
                                        .iter_row(PrefetchOptions::new_with_exhaust_iter(false))
                                        .await?,
                                );

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
                                        FsSplit::new(filename.to_owned(), 0, size as usize).into(),
                                    )
                                });
                                state_cache.extend(file_assignment);

                                // When both of state cache and state store are empty,
                                // the right arm of stream is a pending stream,
                                // and is_datastream_empty is set to true.
                                // The new
                                if is_datastream_empty {
                                    Self::try_replace_with_new_reader(
                                        &mut is_datastream_empty,
                                        &state_store_handler,
                                        &mut state_cache,
                                        core.column_ids.clone(),
                                        self.build_source_ctx(&source_desc, core.source_id),
                                        &source_desc,
                                        &mut stream,
                                        &mut store_iter,
                                    )
                                    .await?;
                                }
                            }
                            _ => unreachable!(),
                        },
                        // StreamChunk from FsSourceReader, and the reader reads only one file.
                        // If the file read out, replace with a new file reader.
                        Either::Right(StreamChunkWithState {
                            chunk,
                            split_offset_mapping,
                        }) => {
                            let mapping = split_offset_mapping.unwrap();
                            debug_assert_eq!(mapping.len(), 1);

                            // Get FsSplit in state cache.
                            let (split_id, offset) = mapping.iter().nth(0).unwrap();
                            let mut cache_entry = match state_cache.entry(split_id.to_owned()) {
                                Entry::Occupied(entry) => entry,
                                Entry::Vacant(_) => unreachable!(),
                            };

                            // Update the offset in the state cache.
                            // If offset is equal to size, the entry
                            // will be deleted after the next barrier.
                            let offset = offset.parse().unwrap();
                            let mut fs_split = cache_entry.get().to_owned().into_fs().unwrap();
                            let fs_split_size = fs_split.size;
                            fs_split.offset = offset;
                            cache_entry.insert(fs_split.into());

                            // The file is read out, build a new reader.
                            if fs_split_size <= offset {
                                debug_assert_eq!(fs_split_size, offset);
                                Self::try_replace_with_new_reader(
                                    &mut is_datastream_empty,
                                    &state_store_handler,
                                    &mut state_cache,
                                    core.column_ids.clone(),
                                    self.build_source_ctx(&source_desc, core.source_id),
                                    &source_desc,
                                    &mut stream,
                                    &mut store_iter,
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
