// Copyright 2025 RisingWave Labs
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

use std::marker::PhantomData;
use std::ops::Bound;

use either::Either;
use futures::TryStreamExt;
use futures::stream::{self, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::{ColumnId, TableId};
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::types::ScalarRef;
use risingwave_connector::source::filesystem::OpendalFsSplit;
use risingwave_connector::source::filesystem::opendal_source::{
    OpendalAzblob, OpendalGcs, OpendalPosixFs, OpendalS3, OpendalSource,
};
use risingwave_connector::source::reader::desc::SourceDesc;
use risingwave_connector::source::{
    BoxStreamingFileSourceChunkStream, SourceContext, SourceCtrlOpts, SplitImpl, SplitMetaData,
};
use risingwave_storage::store::PrefetchOptions;
use thiserror_ext::AsReport;

use super::{
    SourceStateTableHandler, StreamSourceCore,
    apply_rate_limit_with_for_streaming_file_source_reader, get_split_offset_col_idx,
    get_split_offset_mapping_from_chunk, prune_additional_cols,
};
use crate::common::rate_limit::limited_chunk_size;
use crate::executor::prelude::*;
use crate::executor::stream_reader::StreamReaderWithPause;

const SPLIT_BATCH_SIZE: usize = 1000;

type SplitBatch = Option<Vec<SplitImpl>>;

pub struct FsFetchExecutor<S: StateStore, Src: OpendalSource> {
    actor_ctx: ActorContextRef,

    /// Streaming source for external
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Upstream list executor.
    upstream: Option<Executor>,

    /// Rate limit in rows/s.
    rate_limit_rps: Option<u32>,

    _marker: PhantomData<Src>,
}

impl<S: StateStore, Src: OpendalSource> FsFetchExecutor<S, Src> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        upstream: Executor,
        rate_limit_rps: Option<u32>,
    ) -> Self {
        Self {
            actor_ctx,
            stream_source_core: Some(stream_source_core),
            upstream: Some(upstream),
            rate_limit_rps,
            _marker: PhantomData,
        }
    }

    async fn replace_with_new_batch_reader<const BIASED: bool>(
        splits_on_fetch: &mut usize,
        state_store_handler: &SourceStateTableHandler<S>,
        column_ids: Vec<ColumnId>,
        source_ctx: SourceContext,
        source_desc: &SourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, Option<StreamChunk>>,
        rate_limit_rps: Option<u32>,
    ) -> StreamExecutorResult<()> {
        let mut batch = Vec::with_capacity(SPLIT_BATCH_SIZE);
        let state_table = state_store_handler.state_table();
        'vnodes: for vnode in state_table.vnodes().iter_vnodes() {
            let table_iter = state_table
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
                        risingwave_connector::source::ConnectorProperties::Azblob(_) => {
                            let split: OpendalFsSplit<OpendalAzblob> =
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

            let mut merged_stream =
                stream::empty::<StreamExecutorResult<Option<StreamChunk>>>().boxed();
            // Change the previous implementation where multiple files shared a single SourceReader
            // to a new approach where each SourceReader reads only one file.
            // Then, merge the streams of multiple files serially here.
            for split in batch {
                let single_file_stream = Self::build_single_file_stream_reader(
                    column_ids.clone(),
                    source_ctx.clone(),
                    source_desc,
                    Some(vec![split]),
                    rate_limit_rps,
                )
                .await?
                .map_err(StreamExecutorError::connector_error);
                let single_file_stream = single_file_stream.map(|reader| reader);
                merged_stream = merged_stream.chain(single_file_stream).boxed();
            }

            stream.replace_data_stream(merged_stream);
        }

        Ok(())
    }

    // Note: This change applies only to the file source.
    //
    // Each SourceReader (for the streaming file source, this is the `OpendalReader` struct)
    // reads only one file. After the chunk stream returned by the SourceReader,
    // chain a None to indicate that the file has been fully read.
    async fn build_single_file_stream_reader(
        column_ids: Vec<ColumnId>,
        source_ctx: SourceContext,
        source_desc: &SourceDesc,
        batch: SplitBatch,
        rate_limit_rps: Option<u32>,
    ) -> StreamExecutorResult<BoxStreamingFileSourceChunkStream> {
        let (stream, _) = source_desc
            .source
            .build_stream(batch, column_ids, Arc::new(source_ctx), false)
            .await
            .map_err(StreamExecutorError::connector_error)?;
        let optional_stream: BoxStreamingFileSourceChunkStream = stream
            .map(|item| item.map(Some))
            .chain(stream::once(async { Ok(None) }))
            .boxed();
        Ok(
            apply_rate_limit_with_for_streaming_file_source_reader(optional_stream, rate_limit_rps)
                .boxed(),
        )
    }

    fn build_source_ctx(
        &self,
        source_desc: &SourceDesc,
        source_id: TableId,
        source_name: &str,
    ) -> SourceContext {
        SourceContext::new(
            self.actor_ctx.id,
            source_id,
            self.actor_ctx.fragment_id,
            source_name.to_owned(),
            source_desc.metrics.clone(),
            SourceCtrlOpts {
                chunk_size: limited_chunk_size(self.rate_limit_rps),
                split_txn: self.rate_limit_rps.is_some(), // when rate limiting, we may split txn
            },
            source_desc.source.config.clone(),
            None,
        )
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut upstream = self.upstream.take().unwrap().execute();
        let barrier = expect_first_barrier(&mut upstream).await?;
        let first_epoch = barrier.epoch;
        let is_pause_on_startup = barrier.is_pause_on_startup();
        yield Message::Barrier(barrier);

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
        state_store_handler.init_epoch(first_epoch).await?;

        let mut splits_on_fetch: usize = 0;
        let mut stream = StreamReaderWithPause::<true, Option<StreamChunk>>::new(
            upstream,
            stream::pending().boxed(),
        );
        if is_pause_on_startup {
            stream.pause_stream();
        }

        // If it is a recovery startup,
        // there can be file assignments in the state table.
        // Hence we try building a reader first.
        Self::replace_with_new_batch_reader(
            &mut splits_on_fetch,
            &state_store_handler, // move into the function
            core.column_ids.clone(),
            self.build_source_ctx(&source_desc, core.source_id, &core.source_name),
            &source_desc,
            &mut stream,
            self.rate_limit_rps,
        )
        .await?;
        let mut reading_file: Option<Arc<str>> = None;

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
                            match msg {
                                Message::Barrier(barrier) => {
                                    if let Some(mutation) = barrier.mutation.as_deref() {
                                        match mutation {
                                            Mutation::Pause => stream.pause_stream(),
                                            Mutation::Resume => stream.resume_stream(),
                                            Mutation::Throttle(actor_to_apply) => {
                                                if let Some(new_rate_limit) =
                                                    actor_to_apply.get(&self.actor_ctx.id)
                                                    && *new_rate_limit != self.rate_limit_rps
                                                {
                                                    tracing::info!(
                                                        "updating rate limit from {:?} to {:?}",
                                                        self.rate_limit_rps,
                                                        *new_rate_limit
                                                    );
                                                    self.rate_limit_rps = *new_rate_limit;
                                                    splits_on_fetch = 0;
                                                }
                                            }
                                            _ => (),
                                        }
                                    }

                                    let post_commit = state_store_handler
                                        .commit_may_update_vnode_bitmap(barrier.epoch)
                                        .await?;

                                    let update_vnode_bitmap =
                                        barrier.as_update_vnode_bitmap(self.actor_ctx.id);
                                    // Propagate the barrier.
                                    yield Message::Barrier(barrier);

                                    if let Some((_, cache_may_stale)) =
                                        post_commit.post_yield_barrier(update_vnode_bitmap).await?
                                    {
                                        // if cache_may_stale, we must rebuild the stream to adjust vnode mappings
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
                                            self.rate_limit_rps,
                                        )
                                        .await?;
                                    }
                                }
                                // Receiving file assignments from upstream list executor,
                                // store into state table.
                                Message::Chunk(chunk) => {
                                    // For Parquet encoding, the offset indicates the current row being read.
                                    let file_assignment = chunk
                                        .data_chunk()
                                        .rows()
                                        .map(|row| {
                                            let filename = row.datum_at(0).unwrap().into_utf8();
                                            reading_file = Some(filename.into());
                                            let size = row.datum_at(2).unwrap().into_int64();
                                            OpendalFsSplit::<Src>::new(
                                                filename.to_owned(),
                                                0,
                                                size as usize,
                                            )
                                        })
                                        .collect();
                                    state_store_handler.set_states(file_assignment).await?;
                                    state_store_handler.try_flush().await?;
                                }
                                Message::Watermark(_) => unreachable!(),
                            }
                        }
                        // StreamChunk from FsSourceReader, and the reader reads only one file.
                        // Motivation for the changes:
                        //
                        // Previously, the fetch executor determined whether a file was fully read by checking if the
                        // offset reached the size of the file. However, this approach had some issues related to
                        // maintaining the offset:
                        //
                        // 1. For files compressed with gzip, the size reported corresponds to the original uncompressed
                        //    size, not the actual size of the compressed file.
                        //
                        // 2. For Parquet files, the offset represents the number of rows. Therefore, when listing each
                        //    file, it was necessary to read the metadata to obtain the total number of rows in the file,
                        //    which is an expensive operation.
                        //
                        // To address these issues, we changes the approach to determining whether a file is fully
                        // read by moving the check outside the reader. The fetch executor's right stream has been
                        // changed from a chunk stream to an `Option<Chunk>` stream. When a file is completely read,
                        // a `None` value is added at the end of the file stream to signal to the fetch executor that the
                        // file has been fully read. Upon encountering `None`, the file is deleted.
                        Either::Right(optional_chunk) => match optional_chunk {
                            Some(chunk) => {
                                let mapping = get_split_offset_mapping_from_chunk(
                                    &chunk, split_idx, offset_idx,
                                )
                                .unwrap();
                                debug_assert_eq!(mapping.len(), 1);
                                if let Some((split_id, _offset)) = mapping.into_iter().next() {
                                    reading_file = Some(split_id.clone());
                                    let row = state_store_handler.get(&split_id).await?
                                        .unwrap_or_else(|| {
                                            panic!("The fs_split (file_name) {:?} should be in the state table.",
                                        split_id)
                                        });
                                    let fs_split = match row.datum_at(1) {
                                        Some(ScalarRefImpl::Jsonb(jsonb_ref)) => {
                                            OpendalFsSplit::<Src>::restore_from_json(
                                                jsonb_ref.to_owned_scalar(),
                                            )?
                                        }
                                        _ => unreachable!(),
                                    };

                                    state_store_handler
                                        .set(&split_id, fs_split.encode_to_json())
                                        .await?;
                                }
                                let chunk = prune_additional_cols(
                                    &chunk,
                                    split_idx,
                                    offset_idx,
                                    &source_desc.columns,
                                );
                                yield Message::Chunk(chunk);
                            }
                            None => {
                                if let Some(ref delete_file_name) = reading_file {
                                    splits_on_fetch -= 1;
                                    state_store_handler.delete(delete_file_name).await?;
                                }
                            }
                        },
                    }
                }
            }
        }
    }
}

impl<S: StateStore, Src: OpendalSource> Execute for FsFetchExecutor<S, Src> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore, Src: OpendalSource> Debug for FsFetchExecutor<S, Src> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("FsFetchExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .finish()
        } else {
            f.debug_struct("FsFetchExecutor").finish()
        }
    }
}
