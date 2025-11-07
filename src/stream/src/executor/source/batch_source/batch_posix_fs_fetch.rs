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

use std::collections::VecDeque;
use std::io::BufRead;
use std::path::Path;

use either::Either;
use futures::stream::{self, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::id::TableId;
use risingwave_common::types::{JsonbVal, ScalarRef};
use risingwave_connector::parser::{ByteStreamSourceParserImpl, CommonParserConfig, ParserConfig};
use risingwave_connector::source::filesystem::OpendalFsSplit;
use risingwave_connector::source::filesystem::opendal_source::OpendalPosixFs;
use risingwave_connector::source::{
    ConnectorProperties, SourceContext, SourceCtrlOpts, SourceMessage, SourceMeta, SplitMetaData,
};
use thiserror_ext::AsReport;
use tokio::fs;

use crate::common::rate_limit::limited_chunk_size;
use crate::executor::prelude::*;
use crate::executor::source::{StreamSourceCore, get_split_offset_col_idx, prune_additional_cols};
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::LocalBarrierManager;

/// Maximum number of files to process in a single batch
const BATCH_SIZE: usize = 1000;

/// Executor for fetching and processing files in batch mode for refreshable tables.
///
/// This executor receives file assignments from an upstream list executor,
/// reads the files, parses their contents, and emits stream chunks.
///
/// Key characteristics:
/// - Uses **ephemeral in-memory state** (no persistent state table)
/// - State is cleared on recovery and `RefreshStart` mutations
/// - Suitable for refreshable materialized views
pub struct BatchPosixFsFetchExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Core component for managing external streaming source state
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Upstream list executor that provides the list of files to read
    upstream: Option<Executor>,

    /// Optional rate limit in rows/s to control data ingestion speed
    rate_limit_rps: Option<u32>,

    /// Local barrier manager for reporting load finished
    barrier_manager: LocalBarrierManager,

    /// In-memory queue of file assignments to process (`file_path`, `split_json`).
    /// This is ephemeral and cleared on recovery and `RefreshStart` mutations.
    file_queue: VecDeque<(String, JsonbVal)>,

    /// Associated table ID for reporting load finished
    associated_table_id: TableId,
}

/// Fetched data from a file, along with file path for logging
struct FileData {
    /// The actual data chunks read from the file
    chunks: Vec<StreamChunk>,

    /// Path to the data file
    file_path: String,
}

impl<S: StateStore> BatchPosixFsFetchExecutor<S> {
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
            file_queue: VecDeque::new(),
            associated_table_id: associated_table_id.unwrap(),
        }
    }

    /// Pop files from the in-memory queue and create a batch reader for them.
    /// Processes up to `BATCH_SIZE` files in parallel.
    fn replace_with_new_batch_reader<const BIASED: bool>(
        files_in_progress: &mut usize,
        file_queue: &mut VecDeque<(String, JsonbVal)>,
        stream: &mut StreamReaderWithPause<BIASED, FileData>,
        properties: ConnectorProperties,
        parser_config: ParserConfig,
        source_ctx: Arc<SourceContext>,
    ) -> StreamExecutorResult<()> {
        // Pop up to BATCH_SIZE files from the queue to process
        let mut batch = Vec::with_capacity(BATCH_SIZE);

        for _ in 0..BATCH_SIZE {
            if let Some((_file_path, split_json)) = file_queue.pop_front() {
                let split = OpendalFsSplit::<OpendalPosixFs>::restore_from_json(split_json)?;
                batch.push(split);
            } else {
                break;
            }
        }

        if batch.is_empty() {
            // No files to process, set stream to pending
            stream.replace_data_stream(stream::pending().boxed());
        } else {
            *files_in_progress += batch.len();
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
        source_ctx: Arc<SourceContext>,
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
                        error = %e.as_report(),
                        file_path = %full_path.display(),
                        "Failed to read file"
                    );
                    continue;
                }
            };

            if content.is_empty() {
                // Empty file, skip it
                yield FileData {
                    chunks: vec![],
                    file_path,
                };
                continue;
            }

            let mut chunks = vec![];

            // Process the file line by line
            for line in content.lines() {
                let line =
                    line.map_err(|e| StreamExecutorError::connector_error(anyhow::Error::from(e)))?;

                let message = SourceMessage {
                    key: None,
                    payload: Some(line.as_bytes().to_vec()),
                    offset: "0".to_owned(),
                    split_id: split.id(),
                    meta: SourceMeta::Empty,
                };

                // TODO(tab): avoid rebuilding ByteStreamSourceParserImpl for each file
                // Parser is created per line because it's consumed by parse_stream
                let parser =
                    ByteStreamSourceParserImpl::create(parser_config.clone(), source_ctx.clone())
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

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut upstream = self.upstream.take().unwrap().execute();
        let barrier = expect_first_barrier(&mut upstream).await?;
        let is_pause_on_startup = barrier.is_pause_on_startup();
        yield Message::Barrier(barrier);

        let mut core = self.stream_source_core.take().unwrap();

        // Build source description from the builder.
        let source_desc_builder = core.source_desc_builder.take().unwrap();

        let source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;
        let (Some(split_idx), Some(offset_idx)) = get_split_offset_col_idx(&source_desc.columns)
        else {
            unreachable!("Partition and offset columns must be set.");
        };

        let properties = source_desc.source.config.clone();
        let parser_config = ParserConfig {
            common: CommonParserConfig {
                rw_columns: source_desc.columns.clone(),
            },
            specific: source_desc.source.parser_config.clone(),
        };

        let mut files_in_progress: usize = 0;
        let mut stream =
            StreamReaderWithPause::<true, FileData>::new(upstream, stream::pending().boxed());

        if is_pause_on_startup {
            stream.pause_stream();
        }

        // For refreshable tables, always start fresh on recovery - no state restoration
        // File queue is empty by default (no restoration from persistent state)

        let mut list_finished = false;
        let mut is_refreshing = false;
        let mut file_queue = self.file_queue;

        // Extract fields we'll need later
        let actor_ctx = self.actor_ctx.clone();
        let barrier_manager = self.barrier_manager.clone();
        let rate_limit_rps = &mut self.rate_limit_rps;

        let source_ctx = Arc::new(SourceContext::new(
            actor_ctx.id,
            core.source_id,
            actor_ctx.fragment_id,
            core.source_name.clone(),
            source_desc.metrics.clone(),
            SourceCtrlOpts {
                chunk_size: limited_chunk_size(*rate_limit_rps),
                split_txn: rate_limit_rps.is_some(),
            },
            source_desc.source.config.clone(),
            None,
        ));

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::error!(error = %e.as_report(), "Fetch Error");
                    files_in_progress = 0;
                }
                Ok(msg) => match msg {
                    // Barrier messages from upstream
                    Either::Left(msg) => match msg {
                        Message::Barrier(barrier) => {
                            let need_rebuild_reader = false;

                            if let Some(mutation) = barrier.mutation.as_deref() {
                                match mutation {
                                    Mutation::Pause => stream.pause_stream(),
                                    Mutation::Resume => stream.resume_stream(),
                                    Mutation::RefreshStart {
                                        associated_source_id,
                                        ..
                                    } if associated_source_id.as_raw_id()
                                        == core.source_id.as_raw_id() =>
                                    {
                                        tracing::info!(
                                            ?barrier.epoch,
                                            actor_id = actor_ctx.id,
                                            source_id = %core.source_id,
                                            queue_len = file_queue.len(),
                                            files_in_progress,
                                            "RefreshStart: clearing state and aborting workload"
                                        );

                                        // Clear all in-memory state
                                        file_queue.clear();
                                        files_in_progress = 0;
                                        list_finished = false;
                                        is_refreshing = true;

                                        // Abort current file reader
                                        stream.replace_data_stream(stream::pending().boxed());
                                    }
                                    Mutation::ListFinish {
                                        associated_source_id,
                                    } => {
                                        // Check if this ListFinish is for our source
                                        if associated_source_id.as_raw_id()
                                            == core.source_id.as_raw_id()
                                        {
                                            tracing::info!(
                                                ?barrier.epoch,
                                                actor_id = actor_ctx.id,
                                                source_id = %core.source_id,
                                                "received ListFinish mutation"
                                            );
                                            list_finished = true;
                                        }
                                    }
                                    _ => (),
                                }
                            }

                            let epoch = barrier.epoch;

                            // Report load finished BEFORE yielding barrier when:
                            // 1. All files have been processed (files_in_progress == 0 and file_queue is empty)
                            // 2. ListFinish mutation has been received
                            //
                            // IMPORTANT: Must report BEFORE yield to ensure epoch is still in inflight_barriers.
                            // If we yield first, the barrier worker may collect the barrier and remove the epoch
                            // from inflight_barriers, causing the report to be ignored with a warning.
                            if files_in_progress == 0
                                && file_queue.is_empty()
                                && list_finished
                                && is_refreshing
                            {
                                tracing::info!(
                                    ?epoch,
                                    actor_id = actor_ctx.id,
                                    source_id = %core.source_id,
                                    "Reporting source load finished"
                                );
                                barrier_manager.report_source_load_finished(
                                    epoch,
                                    actor_ctx.id,
                                    self.associated_table_id,
                                    core.source_id,
                                );
                                // Reset the flag to avoid duplicate reports
                                list_finished = false;
                                is_refreshing = false;
                            }

                            // Propagate the barrier AFTER reporting progress.
                            yield Message::Barrier(barrier);

                            // Rebuild reader when all current files are processed
                            if files_in_progress == 0 || need_rebuild_reader {
                                Self::replace_with_new_batch_reader(
                                    &mut files_in_progress,
                                    &mut file_queue,
                                    &mut stream,
                                    properties.clone(),
                                    parser_config.clone(),
                                    source_ctx.clone(),
                                )?;
                            }
                        }
                        // Receiving file assignments from upstream list executor,
                        // store into in-memory queue (no persistent state).
                        Message::Chunk(chunk) => {
                            for row in chunk.data_chunk().rows() {
                                let file_name = row.datum_at(0).unwrap().into_utf8().to_owned();
                                let split = row.datum_at(1).unwrap().into_jsonb().to_owned_scalar();
                                file_queue.push_back((file_name, split));
                            }

                            tracing::debug!(
                                actor_id = actor_ctx.id,
                                queue_len = file_queue.len(),
                                "Added file assignments to queue"
                            );
                        }
                        Message::Watermark(_) => unreachable!(),
                    },
                    // Data from file reader
                    Either::Right(FileData { chunks, file_path }) => {
                        // Decrement counter after processing a file
                        files_in_progress -= 1;
                        tracing::debug!(
                            file_path = ?file_path,
                            "Processed file"
                        );

                        // Yield all chunks from the file
                        for chunk in chunks {
                            let chunk = prune_additional_cols(
                                &chunk,
                                split_idx,
                                offset_idx,
                                &source_desc.columns,
                            );
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
