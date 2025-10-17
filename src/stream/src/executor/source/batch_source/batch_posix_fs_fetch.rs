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

use std::io::BufRead;
use std::ops::Bound;
use std::path::Path;

use either::Either;
use futures::stream::{self, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::types::{JsonbVal, ScalarRef};
use risingwave_connector::parser::{ByteStreamSourceParserImpl, CommonParserConfig, ParserConfig};
use risingwave_connector::source::filesystem::OpendalFsSplit;
use risingwave_connector::source::filesystem::opendal_source::OpendalPosixFs;
use risingwave_connector::source::reader::desc::SourceDesc;
use risingwave_connector::source::{
    ConnectorProperties, SourceContext, SourceCtrlOpts, SourceMessage, SourceMeta, SplitMetaData,
};
use risingwave_storage::store::PrefetchOptions;
use thiserror_ext::AsReport;
use tokio::fs;

use crate::common::rate_limit::limited_chunk_size;
use crate::executor::prelude::*;
use crate::executor::source::{SourceStateTableHandler, StreamSourceCore};
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::LocalBarrierManager;

const SPLIT_BATCH_SIZE: usize = 1000;

pub struct BatchPosixFsFetchExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Core component for managing external streaming source state
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Upstream list executor that provides the list of files to read.
    upstream: Option<Executor>,

    /// Optional rate limit in rows/s to control data ingestion speed
    rate_limit_rps: Option<u32>,

    /// Local barrier manager for reporting load finished
    barrier_manager: LocalBarrierManager,
}

/// Fetched data from a file, along with file path for state tracking
struct FileData {
    /// The actual data chunks read from the file
    chunks: Vec<StreamChunk>,

    /// Path to the data file, used for deletion from state table
    file_path: String,
}

impl<S: StateStore> BatchPosixFsFetchExecutor<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        upstream: Executor,
        rate_limit_rps: Option<u32>,
        barrier_manager: LocalBarrierManager,
    ) -> Self {
        Self {
            actor_ctx,
            stream_source_core: Some(stream_source_core),
            upstream: Some(upstream),
            rate_limit_rps,
            barrier_manager,
        }
    }

    /// Replace the current batch reader with a new one that reads files from the state table
    async fn replace_with_new_batch_reader<const BIASED: bool>(
        splits_on_fetch: &mut usize,
        state_store_handler: &SourceStateTableHandler<S>,
        _source_desc: &SourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, FileData>,
        properties: ConnectorProperties,
        parser_config: ParserConfig,
        source_ctx: SourceContext,
    ) -> StreamExecutorResult<()> {
        let mut batch = Vec::with_capacity(SPLIT_BATCH_SIZE);
        let state_table = state_store_handler.state_table();

        'vnodes: for vnode in state_table.vnodes().iter_vnodes() {
            let table_iter = state_table
                .iter_with_vnode(
                    vnode,
                    &(Bound::<OwnedRow>::Unbounded, Bound::<OwnedRow>::Unbounded),
                    PrefetchOptions::prefetch_for_small_range_scan(),
                )
                .await?;
            pin_mut!(table_iter);

            while let Some(item) = table_iter.next().await {
                let row = item?;
                let split = match row.datum_at(1) {
                    Some(ScalarRefImpl::Jsonb(jsonb_ref)) => {
                        OpendalFsSplit::<OpendalPosixFs>::restore_from_json(
                            jsonb_ref.to_owned_scalar(),
                        )?
                    }
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
                Self::build_batched_stream_reader(batch, properties, parser_config, source_ctx);
            stream.replace_data_stream(batch_reader.boxed());
        }

        Ok(())
    }

    /// Build a stream reader that reads multiple files in sequence
    #[try_stream(ok = FileData, error = StreamExecutorError)]
    async fn build_batched_stream_reader(
        batch: Vec<OpendalFsSplit<OpendalPosixFs>>,
        properties: ConnectorProperties,
        parser_config: ParserConfig,
        source_ctx: SourceContext,
    ) {
        let ConnectorProperties::BatchPosixFs(batch_posix_fs_properties) = properties else {
            unreachable!()
        };

        let root_path = batch_posix_fs_properties.root.clone();

        for split in batch {
            let file_path = split.name.clone();
            let full_path = Path::new(&root_path).join(&file_path);

            // Read the entire file
            let content = match fs::read(&full_path).await {
                Ok(content) => content,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        file_path = %full_path.display(),
                        "Failed to read file"
                    );
                    continue;
                }
            };

            if content.is_empty() {
                // Empty file, just delete it from state
                yield FileData {
                    chunks: vec![],
                    file_path,
                };
                continue;
            }

            let mut chunks = vec![];

            // Process the file line by line (similar to BatchPosixFsReader)
            for line in content.lines() {
                let line =
                    line.map_err(|e| StreamExecutorError::connector_error(anyhow::Error::from(e)))?;
                // Create a message for each line
                let message = SourceMessage {
                    key: None,
                    payload: Some(line.as_bytes().to_vec()),
                    offset: "0".to_owned(),
                    split_id: split.id(),
                    meta: SourceMeta::Empty,
                };

                // Parse the content
                let parser = ByteStreamSourceParserImpl::create(
                    parser_config.clone(),
                    Arc::new(source_ctx.clone()),
                )
                .await?;
                let chunk_stream = parser
                    .parse_stream(Box::pin(futures::stream::once(async { Ok(vec![message]) })));

                #[for_await]
                for chunk in chunk_stream {
                    chunks.push(chunk?);
                }
            }

            yield FileData { chunks, file_path };
        }
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
                split_txn: self.rate_limit_rps.is_some(),
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

        let properties = source_desc.source.config.clone();
        let parser_config = ParserConfig {
            common: CommonParserConfig {
                rw_columns: source_desc.columns.clone(),
            },
            specific: source_desc.source.parser_config.clone(),
        };

        // Initialize state table.
        state_store_handler.init_epoch(first_epoch).await?;

        let mut splits_on_fetch: usize = 0;
        let mut stream =
            StreamReaderWithPause::<true, FileData>::new(upstream, stream::pending().boxed());

        if is_pause_on_startup {
            stream.pause_stream();
        }

        // If it is a recovery startup, there can be file assignments in the state table.
        // Hence we try building a reader first.
        Self::replace_with_new_batch_reader(
            &mut splits_on_fetch,
            &state_store_handler,
            &source_desc,
            &mut stream,
            properties.clone(),
            parser_config.clone(),
            self.build_source_ctx(&source_desc, core.source_id, &core.source_name),
        )
        .await?;

        let mut list_finished = false;

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::error!(error = %e.as_report(), "Fetch Error");
                    splits_on_fetch = 0;
                }
                Ok(msg) => match msg {
                    // This branch will be preferred.
                    Either::Left(msg) => match msg {
                        Message::Barrier(barrier) => {
                            let mut need_rebuild_reader = false;

                            if let Some(mutation) = barrier.mutation.as_deref() {
                                match mutation {
                                    Mutation::Pause => stream.pause_stream(),
                                    Mutation::Resume => stream.resume_stream(),
                                    Mutation::ListFinish {
                                        associated_source_id,
                                    } => {
                                        // Check if this ListFinish is for our source
                                        if associated_source_id.table_id()
                                            == core.source_id.table_id()
                                        {
                                            tracing::info!(
                                                ?barrier.epoch,
                                                actor_id = self.actor_ctx.id,
                                                source_id = %core.source_id,
                                                "received ListFinish mutation"
                                            );
                                            list_finished = true;
                                        }
                                    }
                                    Mutation::Throttle(actor_to_apply) => {
                                        if let Some(new_rate_limit) =
                                            actor_to_apply.get(&self.actor_ctx.id)
                                            && *new_rate_limit != self.rate_limit_rps
                                        {
                                            tracing::debug!(
                                                "updating rate limit from {:?} to {:?}",
                                                self.rate_limit_rps,
                                                *new_rate_limit
                                            );
                                            self.rate_limit_rps = *new_rate_limit;
                                            need_rebuild_reader = true;
                                        }
                                    }
                                    _ => (),
                                }
                            }

                            let epoch = barrier.epoch;
                            let post_commit = state_store_handler
                                .commit_may_update_vnode_bitmap(epoch)
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

                            // Report load finished when:
                            // 1. All files have been read (splits_on_fetch == 0)
                            // 2. ListFinish mutation has been received
                            if splits_on_fetch == 0 && list_finished {
                                tracing::info!(
                                    ?epoch,
                                    actor_id = self.actor_ctx.id,
                                    source_id = %core.source_id,
                                    "reporting source load finished"
                                );
                                self.barrier_manager.report_source_load_finished(
                                    epoch,
                                    self.actor_ctx.id,
                                    core.source_id.table_id(),
                                    core.source_id.table_id(),
                                );
                                // Reset the flag to avoid duplicate reports
                                list_finished = false;
                            }

                            if splits_on_fetch == 0 || need_rebuild_reader {
                                Self::replace_with_new_batch_reader(
                                    &mut splits_on_fetch,
                                    &state_store_handler,
                                    &source_desc,
                                    &mut stream,
                                    properties.clone(),
                                    parser_config.clone(),
                                    self.build_source_ctx(
                                        &source_desc,
                                        core.source_id,
                                        &core.source_name,
                                    ),
                                )
                                .await?;
                            }
                        }
                        // Receiving file assignments from upstream list executor,
                        // store into state table.
                        Message::Chunk(chunk) => {
                            let file_assignments: Vec<(String, JsonbVal)> = chunk
                                .data_chunk()
                                .rows()
                                .map(|row| {
                                    let file_name = row.datum_at(0).unwrap().into_utf8();
                                    let split = row.datum_at(1).unwrap().into_jsonb();
                                    (file_name.to_owned(), split.to_owned_scalar())
                                })
                                .collect();
                            state_store_handler
                                .set_states_json(file_assignments)
                                .await?;
                            state_store_handler.try_flush().await?;
                        }
                        Message::Watermark(_) => unreachable!(),
                    },
                    // Data from file reader
                    Either::Right(FileData { chunks, file_path }) => {
                        // Delete the file from state table after reading
                        splits_on_fetch -= 1;
                        state_store_handler.delete(&file_path).await?;

                        // Yield all chunks
                        for chunk in chunks {
                            yield Message::Chunk(chunk);
                        }
                    }
                },
            }
        }
    }
}

impl<S: StateStore> Execute for BatchPosixFsFetchExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for BatchPosixFsFetchExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("BatchPosixFsFetchExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .finish()
        } else {
            f.debug_struct("BatchPosixFsFetchExecutor").finish()
        }
    }
}
