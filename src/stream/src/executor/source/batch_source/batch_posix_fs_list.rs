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

use std::path::Path;

use anyhow::{Context, anyhow};
use either::Either;
use futures_async_stream::try_stream;
use glob::Pattern as GlobPattern;
use risingwave_common::array::Op;
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_connector::source::filesystem::OpendalFsSplit;
use risingwave_connector::source::filesystem::opendal_source::OpendalPosixFs;
use risingwave_connector::source::reader::desc::SourceDescBuilder;
use risingwave_connector::source::{ConnectorProperties, SplitMetaData};
use thiserror_ext::AsReport;
use tokio::fs;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::prelude::*;
use crate::executor::source::{StreamSourceCore, barrier_to_message_stream};
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::LocalBarrierManager;

pub struct BatchPosixFsListExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Streaming source for external
    stream_source_core: StreamSourceCore<S>,

    /// Metrics for monitor.
    #[expect(dead_code)]
    metrics: Arc<StreamingMetrics>,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

    /// System parameter reader to read barrier interval
    #[expect(dead_code)]
    system_params: SystemParamsReaderRef,

    /// Rate limit in rows/s.
    #[expect(dead_code)]
    rate_limit_rps: Option<u32>,

    /// Local barrier manager for reporting list finished
    barrier_manager: LocalBarrierManager,
}

impl<S: StateStore> BatchPosixFsListExecutor<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        system_params: SystemParamsReaderRef,
        rate_limit_rps: Option<u32>,
        barrier_manager: LocalBarrierManager,
    ) -> Self {
        Self {
            actor_ctx,
            stream_source_core,
            metrics,
            barrier_receiver: Some(barrier_receiver),
            system_params,
            rate_limit_rps,
            barrier_manager,
        }
    }

    /// List all files from root directory and convert them to a stream of chunks
    async fn list_files_to_stream(
        root: String,
        pattern: Option<String>,
    ) -> StreamExecutorResult<impl Iterator<Item = StreamExecutorResult<StreamChunk>>> {
        // List all files from the root directory
        let root_path = Path::new(&root);
        if !root_path.exists() {
            return Err(anyhow!("Root directory does not exist: {}", root).into());
        }

        // Convert pattern string to glob::Pattern if provided
        let glob_pattern = pattern
            .as_ref()
            .map(|p| GlobPattern::new(p))
            .transpose()
            .with_context(|| format!("Invalid match_pattern: {:?}", pattern))?;

        let mut files = Vec::new();
        Self::collect_files_recursive(root_path, root_path, &glob_pattern, &mut files).await?;

        tracing::debug!(
            "BatchPosixFsListExecutor listed {} files from {}: {:?}",
            files.len(),
            root,
            files
        );

        // Convert files to stream chunks with (utf8, jsonb) schema
        let file_iter = files.into_iter().map(|(file_path, size)| {
            // Create a split for this file
            let split = OpendalFsSplit::<OpendalPosixFs>::new(
                file_path.clone(),
                0, // offset starts at 0
                size as usize,
            );

            let row = (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(ScalarImpl::Utf8(file_path.into())),
                    Some(ScalarImpl::Jsonb(split.encode_to_json())),
                ]),
            );

            Ok(StreamChunk::from_rows(
                &[row],
                &[DataType::Varchar, DataType::Jsonb],
            ))
        });

        Ok(file_iter)
    }

    /// Report that file listing is finished for the current epoch
    fn report_list_finished(&self, epoch: crate::executor::EpochPair) {
        tracing::info!(
            ?epoch,
            actor_id = self.actor_ctx.id,
            source_id = %self.stream_source_core.source_id,
            "reporting source list finished"
        );
        self.barrier_manager.report_source_list_finished(
            epoch,
            self.actor_ctx.id,
            self.stream_source_core.source_id.table_id,
            self.stream_source_core.source_id.table_id,
        );
    }

    /// Recursively collect files from a directory
    fn collect_files_recursive<'a>(
        current_dir: &'a Path,
        root_path: &'a Path,
        pattern: &'a Option<GlobPattern>,
        files: &'a mut Vec<(String, u64)>,
    ) -> futures::future::BoxFuture<'a, StreamExecutorResult<()>> {
        Box::pin(async move {
            let mut entries = fs::read_dir(current_dir)
                .await
                .with_context(|| format!("Failed to read directory: {}", current_dir.display()))?;

            while let Some(entry) = entries.next_entry().await.with_context(|| {
                format!(
                    "Failed to read directory entry in: {}",
                    current_dir.display()
                )
            })? {
                let path = entry.path();
                let metadata = entry
                    .metadata()
                    .await
                    .with_context(|| format!("Failed to get metadata for: {}", path.display()))?;

                if metadata.is_dir() {
                    // Recursively process subdirectories
                    Self::collect_files_recursive(&path, root_path, pattern, files).await?;
                } else if metadata.is_file() {
                    let relative_path = path.strip_prefix(root_path).with_context(|| {
                        format!("Failed to get relative path for: {}", path.display())
                    })?;
                    let relative_path_str = relative_path.to_string_lossy().to_string();

                    // Check if file matches the pattern (if specified)
                    if let Some(pattern) = pattern {
                        if pattern.matches(&relative_path_str) {
                            files.push((relative_path_str, metadata.len()));
                        }
                    } else {
                        files.push((relative_path_str, metadata.len()));
                    }
                }
            }

            Ok(())
        })
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

        // Build source description from the builder.
        let source_desc_builder: SourceDescBuilder =
            self.stream_source_core.source_desc_builder.take().unwrap();

        let properties = source_desc_builder.with_properties();
        let config = ConnectorProperties::extract(properties, false)?;
        let ConnectorProperties::BatchPosixFs(batch_posix_fs_properties) = config else {
            unreachable!("BatchPosixFsListExecutor must be used with BatchPosixFs connector")
        };

        yield Message::Barrier(first_barrier);
        let barrier_stream = barrier_to_message_stream(barrier_receiver).boxed();

        let mut is_refreshing = false;
        let mut stream = StreamReaderWithPause::<true, _>::new(
            barrier_stream,
            futures::stream::pending().boxed(),
        );

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::warn!(error = %e.as_report(), "encountered an error in batch posix fs list");
                }
                Ok(msg) => match msg {
                    // Barrier arrives.
                    Either::Left(msg) => match &msg {
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
                                            actor_id = self.actor_ctx.id,
                                            source_id = %self.stream_source_core.source_id,
                                            "RefreshStart triggered file re-listing"
                                        );

                                        // Re-list all files
                                        match Self::list_files_to_stream(
                                            batch_posix_fs_properties.root.clone(),
                                            batch_posix_fs_properties.match_pattern.clone(),
                                        )
                                        .await
                                        {
                                            Ok(file_iter) => {
                                                let new_file_stream =
                                                    futures::stream::iter(file_iter);
                                                stream.replace_data_stream(new_file_stream);
                                                is_refreshing = true;
                                            }
                                            Err(e) => {
                                                tracing::error!(
                                                    error = %e.as_report(),
                                                    "Failed to refresh file listing during RefreshStart"
                                                );
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }

                            // Report list finished after all files are listed
                            if is_refreshing {
                                self.report_list_finished(barrier.epoch);
                                is_refreshing = false;
                            }

                            // Propagate the barrier.
                            yield msg;
                        }
                        // Only barrier can be received.
                        _ => unreachable!(),
                    },
                    // File chunk arrives.
                    Either::Right(chunk) => {
                        yield Message::Chunk(chunk);
                    }
                },
            }
        }
    }
}

impl<S: StateStore> Execute for BatchPosixFsListExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for BatchPosixFsListExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchPosixFsListExecutor")
            .field("source_id", &self.stream_source_core.source_id)
            .field("column_ids", &self.stream_source_core.column_ids)
            .finish()
    }
}
