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
use std::ops::Bound;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use either::Either;
use futures::stream::{self, StreamExt};
use futures_async_stream::{for_await, try_stream};
use risingwave_common::catalog::{ColumnId, Schema, TableId};
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::Row;
use risingwave_common::types::{ScalarRef, ScalarRefImpl, ToDatumRef};
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
use risingwave_storage::table::KeyedRow;
use risingwave_storage::StateStore;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

use crate::common::table::state_table::{KeyedRowStream, StateTable};
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

    // async fn try_replace_with_new_reader<'a, const BIASED: bool>(
    //     is_datastream_empty: &mut bool,
    //     _state_store_handler: &'a SourceStateTableHandler<S>,
    //     state_cache: &mut HashMap<SplitId, SplitImpl>,
    //     column_ids: Vec<ColumnId>,
    //     source_ctx: SourceContext,
    //     source_desc: &SourceDesc,
    //     stream: &mut StreamReaderWithPause<BIASED, StreamChunkWithState>,
    //     store_iter: &mut Pin<Box<KeyedRowStream<'a, S, BasicSerde>>>,
    // ) -> StreamExecutorResult<()> {
    //     let fs_split = if let Some(item) = store_iter.next().await {
    //         // Find the next assignment in state store.
    //         let row = item?;
    //         let split_id = match row.datum_at(0) {
    //             Some(ScalarRefImpl::Utf8(split_id)) => split_id,
    //             _ => unreachable!(),
    //         };
    //         let fs_split = match row.datum_at(1) {
    //             Some(ScalarRefImpl::Jsonb(jsonb_ref)) => {
    //                 SplitImpl::restore_from_json(jsonb_ref.to_owned_scalar())?
    //                     .as_fs()
    //                     .unwrap()
    //                     .to_owned()
    //             }
    //             _ => unreachable!(),
    //         };
    //
    //         // Cache the assignment retrieved from state store.
    //         state_cache.insert(split_id.into(), fs_split.clone().into());
    //         Some(fs_split)
    //     } else {
    //         // Find uncompleted assignment in state cache.
    //         state_cache
    //             .iter()
    //             .find(|(_, split)| {
    //                 let fs_split = split.as_fs().unwrap();
    //                 fs_split.offset < fs_split.size
    //             })
    //             .map(|(_, split)| split.as_fs().unwrap().to_owned())
    //     };
    //
    //     if let Some(fs_split) = fs_split {
    //         stream.replace_data_stream(
    //             Self::build_stream_source_reader(column_ids, source_ctx, source_desc, fs_split)
    //                 .await?,
    //         );
    //         *is_datastream_empty = false;
    //     } else {
    //         stream.replace_data_stream(stream::pending().boxed());
    //         *is_datastream_empty = true;
    //     };
    //
    //     Ok(())
    // }

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
        split_channel: Receiver<FsSplit>,
    ) -> StreamExecutorResult<BoxSourceWithStateStream> {
        source_desc
            .source
            .fs_stream_reader(column_ids, Arc::new(source_ctx), split_channel)
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
        let mut fs_split_sender_handle: JoinHandle<StreamExecutorResult<()>>;
        let need_rebuild_stream = Arc::new(AtomicBool::new(false));

        // Build source description from the builder.
        let source_desc_builder: SourceDescBuilder = core.source_desc_builder.take().unwrap();

        let source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;

        // Initialize state store.
        state_store_handler.init_epoch(barrier.epoch);

        let build_filename_stream =
            |state_table: &StateTable<S>,
             need_rebuild: Arc<AtomicBool>|
             -> (JoinHandle<StreamExecutorResult<()>>, Receiver<FsSplit>) {
                let (producer, receiver) = tokio::sync::mpsc::channel(0);
                let handle = tokio::spawn(async move {
                    // iter rows in state table according to vnode mapping
                    for vnode in state_table.vnodes().iter_vnodes() {
                        while let Some(row) = state_table
                            .iter_row_with_pk_range(
                                &(Bound::Unbounded, Bound::Unbounded),
                                vnode,
                                PrefetchOptions::new_for_exhaust_iter(),
                            )
                            .await?
                        {
                            let fs_split = match row.datum_at(1) {
                                Some(ScalarRefImpl::Jsonb(jsonb_ref)) => {
                                    SplitImpl::restore_from_json(jsonb_ref.to_owned_scalar())?
                                        .as_fs()
                                        .unwrap()
                                        .to_owned()
                                }
                                _ => unreachable!(),
                            };
                            tracing::trace!("send split {:?} to pipe", fs_split);
                            producer.send(fs_split).await.unwrap()
                        }
                    }
                    // read to the end means require for another iter
                    need_rebuild.store(true, Ordering::Relaxed);
                    Ok(())
                });
                (handle, receiver)
            };

        let new_reader_stream = || async {
            let (fs_stream_handle, fs_split_recv) = build_filename_stream(
                &state_store_handler.state_store,
                need_rebuild_stream.clone(),
            );
            let reader_stream = Self::build_stream_source_reader(
                core.column_ids.clone(),
                self.build_source_ctx(&source_desc, core.source_id),
                &source_desc,
                fs_split_recv,
            )
            .await?;
            (reader_stream, fs_stream_handle)
        };
        let (reader_stream, fs_stream_handle) = new_reader_stream().await;
        fs_split_sender_handle = fs_stream_handle;
        let mut stream =
            StreamReaderWithPause::<true, StreamChunkWithState>::new(upstream, reader_stream);

        if barrier.is_pause_on_startup() {
            stream.pause_stream();
        }

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
                                Self::take_snapshot_and_flush(
                                    &mut state_store_handler,
                                    &mut state_cache,
                                    barrier.epoch,
                                )
                                .await?;

                                if let Some(new_bitmap) =
                                    barrier.as_update_vnode_bitmap(self.actor_ctx.id)
                                {
                                    let (_, is_updated) = state_store_handler
                                        .state_store
                                        .update_vnode_bitmap(new_bitmap);
                                    need_rebuild_stream.store(is_updated, Ordering::Relaxed);
                                }
                                if need_rebuild_stream.load(Ordering::Relaxed) {
                                    // Rebuild state store iterator.
                                }

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
                                // Writing to state store is enough here.
                                // Once reaching the end of state table, it will rebuild the stream
                                // when the next barrier comes.
                                state_cache.extend(file_assignment);
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
