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

use anyhow::{Context, anyhow};
use either::Either;
use futures::stream;
use iceberg::scan::FileScanTask;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::array::{DataChunk, Op, SerialArray};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{
    ColumnCatalog, ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_FILE_POS_COLUMN_NAME, ROW_ID_COLUMN_NAME,
};
use risingwave_common::config::StreamingConfig;
use risingwave_common::types::{JsonbVal, Scalar, ScalarRef, Serial, ToOwnedDatum};
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::iceberg::{
    IcebergProperties, IcebergScanOpts, scan_task_to_chunk,
};
use risingwave_connector::source::reader::desc::{SourceDesc, SourceDescBuilder};
use thiserror_ext::AsReport;

use crate::executor::prelude::*;
use crate::executor::source::{
    ChunksWithState, PersistedFileScanTask, StreamSourceCore, prune_additional_cols,
};
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::LocalBarrierManager;

pub struct BatchIcebergFetchExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Core component for managing external streaming source state
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Upstream list executor that provides the list of files to read.
    /// This executor is responsible for discovering new files and changes in the Iceberg table.
    upstream: Option<Executor>,

    // barrier manager for reporting load finished
    barrier_manager: LocalBarrierManager,

    streaming_config: Arc<StreamingConfig>,
}

impl<S: StateStore> BatchIcebergFetchExecutor<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        upstream: Executor,
        barrier_manager: LocalBarrierManager,
        streaming_config: Arc<StreamingConfig>,
    ) -> Self {
        Self {
            actor_ctx,
            stream_source_core: Some(stream_source_core),
            upstream: Some(upstream),
            barrier_manager,
            streaming_config,
        }
    }
}

impl<S: StateStore> BatchIcebergFetchExecutor<S> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut upstream = self.upstream.take().unwrap().execute();
        let barrier = expect_first_barrier(&mut upstream).await?;
        yield Message::Barrier(barrier);

        let mut is_refreshing = false;
        let mut is_list_finished = false;
        let mut splits_on_fetch: usize = 0;
        let is_load_finished = Arc::new(RwLock::new(false));
        let mut file_queue = VecDeque::new();

        let mut core = self.stream_source_core.take().unwrap();
        let source_desc_builder = core.source_desc_builder.take().unwrap();
        let source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;

        let file_path_idx = source_desc
            .columns
            .iter()
            .position(|c| c.name == ICEBERG_FILE_PATH_COLUMN_NAME)
            .unwrap();
        let file_pos_idx = source_desc
            .columns
            .iter()
            .position(|c| c.name == ICEBERG_FILE_POS_COLUMN_NAME)
            .unwrap();

        let mut stream = StreamReaderWithPause::<true, ChunksWithState>::new(
            upstream,
            stream::pending().boxed(),
        );

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::error!(error = %e.as_report(), "Fetch Error");
                    file_queue.clear();
                    *is_load_finished.write() = false;
                    return Err(e);
                }
                Ok(msg) => {
                    match msg {
                        Either::Left(msg) => {
                            match msg {
                                Message::Barrier(barrier) => {
                                    let mut need_rebuild_reader = false;
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
                                                    actor_id = self.actor_ctx.id,
                                                    source_id = %core.source_id,

                                                    "RefreshStart:"
                                                );

                                                // reset states and abort current workload
                                                file_queue.clear();
                                                splits_on_fetch = 0;
                                                is_refreshing = true;
                                                is_list_finished = false;
                                                *is_load_finished.write() = false;

                                                need_rebuild_reader = true;
                                            }
                                            Mutation::ListFinish {
                                                associated_source_id,
                                            } if associated_source_id == &core.source_id => {
                                                tracing::info!(
                                                    ?barrier.epoch,
                                                    actor_id = self.actor_ctx.id,
                                                    source_id = %core.source_id,
                                                    "ListFinish:"
                                                );
                                                is_list_finished = true;
                                            }
                                            _ => {
                                                // ignore other mutations
                                            }
                                        }
                                    }

                                    if splits_on_fetch == 0
                                        && file_queue.is_empty()
                                        && is_list_finished
                                        && is_refreshing
                                    // && *is_load_finished.read()
                                    {
                                        tracing::info!(
                                            ?barrier.epoch,
                                            actor_id = self.actor_ctx.id,
                                            source_id = %core.source_id,
                                            "Reporting load finished"
                                        );
                                        self.barrier_manager.report_source_load_finished(
                                            barrier.epoch,
                                            self.actor_ctx.id,
                                            core.source_id.table_id(),
                                            core.source_id.table_id(),
                                        );

                                        // reset flags
                                        is_list_finished = false;
                                        is_refreshing = false;
                                    }

                                    yield Message::Barrier(barrier);

                                    if need_rebuild_reader
                                        || (splits_on_fetch == 0
                                            && !file_queue.is_empty()
                                            && is_refreshing)
                                    {
                                        Self::replace_with_new_batch_reader(
                                            &mut file_queue,
                                            &mut stream,
                                            self.streaming_config.clone(),
                                            &mut splits_on_fetch,
                                            source_desc.clone(),
                                            is_load_finished.clone(),
                                        )
                                        .await?;
                                    }
                                }
                                Message::Chunk(chunk) => {
                                    let jsonb_values: Vec<(String, JsonbVal)> = chunk
                                        .data_chunk()
                                        .rows()
                                        .map(|row| {
                                            let file_name = row.datum_at(0).unwrap().into_utf8();
                                            let split = row.datum_at(1).unwrap().into_jsonb();
                                            (file_name.to_owned(), split.to_owned_scalar())
                                        })
                                        .collect();
                                    tracing::info!("received file assignments: {:?}", jsonb_values);
                                    file_queue.extend(jsonb_values);
                                }
                                Message::Watermark(_) => unreachable!(),
                            }
                        }
                        Either::Right(ChunksWithState {
                            chunks,
                            data_file_path,
                            ..
                        }) => {
                            tracing::info!("received chunks from file: {}", data_file_path);
                            splits_on_fetch -= 1;

                            for chunk in &chunks {
                                let chunk = prune_additional_cols(
                                    chunk,
                                    file_path_idx,
                                    file_pos_idx,
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

    async fn replace_with_new_batch_reader<const BIASED: bool>(
        file_queue: &mut VecDeque<(String, JsonbVal)>,
        stream: &mut StreamReaderWithPause<BIASED, ChunksWithState>,
        streaming_config: Arc<StreamingConfig>,
        splits_on_fetch: &mut usize,
        source_desc: SourceDesc,
        read_finished: Arc<RwLock<bool>>,
    ) -> StreamExecutorResult<()> {
        let mut batch =
            Vec::with_capacity(streaming_config.developer.iceberg_fetch_batch_size as usize);
        for _ in 0..streaming_config.developer.iceberg_fetch_batch_size {
            if let Some((_, split_json)) = file_queue.pop_front() {
                batch.push(PersistedFileScanTask::decode(split_json.as_scalar_ref())?);
            } else {
                break;
            }
        }

        if batch.is_empty() {
            stream.replace_data_stream(stream::pending().boxed());
        } else {
            tracing::info!("building batch reader with {} files", batch.len());
            *splits_on_fetch += batch.len();
            *read_finished.write() = false;
            let batch_reader = Self::build_batched_stream_reader(
                source_desc,
                batch,
                streaming_config,
                read_finished,
            );
            stream.replace_data_stream(batch_reader.boxed());
        }

        Ok(())
    }

    #[try_stream(ok = ChunksWithState, error = StreamExecutorError)]
    async fn build_batched_stream_reader(
        source_desc: SourceDesc,
        read_batch: Vec<FileScanTask>,
        streaming_config: Arc<StreamingConfig>,
        read_finished: Arc<RwLock<bool>>,
    ) {
        let file_path_idx = source_desc
            .columns
            .iter()
            .position(|c| c.name == ICEBERG_FILE_PATH_COLUMN_NAME)
            .unwrap();
        let file_pos_idx = source_desc
            .columns
            .iter()
            .position(|c| c.name == ICEBERG_FILE_POS_COLUMN_NAME)
            .unwrap();
        let properties = source_desc.source.config.clone();
        let properties = match properties {
            risingwave_connector::source::ConnectorProperties::Iceberg(iceberg_properties) => {
                iceberg_properties
            }
            _ => unreachable!(),
        };
        let table = properties.load_table().await?;

        for task in read_batch {
            let mut chunks = vec![];
            #[for_await]
            for chunk in scan_task_to_chunk(
                table.clone(),
                task,
                IcebergScanOpts {
                    chunk_size: streaming_config.developer.chunk_size,
                    need_seq_num: true, /* Although this column is unnecessary, we still keep it for potential usage in the future */
                    need_file_path_and_pos: true,
                    handle_delete_files: true, // Enable delete file handling for streaming source
                },
                None,
            ) {
                let chunk = chunk?;
                chunks.push(StreamChunk::from_parts(
                    itertools::repeat_n(Op::Insert, chunk.cardinality()).collect_vec(),
                    chunk,
                ));
            }
            // We yield once for each file now, because iceberg-rs doesn't support read part of a file now.
            let last_chunk = chunks.last().unwrap();
            let last_row = last_chunk.row_at(last_chunk.cardinality() - 1).1;
            let data_file_path = last_row
                .datum_at(file_path_idx)
                .unwrap()
                .into_utf8()
                .to_owned();
            let last_read_pos = last_row.datum_at(file_pos_idx).unwrap().to_owned_datum();
            yield ChunksWithState {
                chunks,
                data_file_path,
                last_read_pos,
            };
        }

        *read_finished.write() = true;
    }
}

impl<S: StateStore> Execute for BatchIcebergFetchExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for BatchIcebergFetchExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("BatchIcebergFetchExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .finish()
        } else {
            f.debug_struct("BatchIcebergFetchExecutor").finish()
        }
    }
}
