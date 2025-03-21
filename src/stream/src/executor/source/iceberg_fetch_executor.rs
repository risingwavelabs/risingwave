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

use std::ops::Bound;

use anyhow::Context;
use either::Either;
use futures::{StreamExt, TryStreamExt, stream};
use futures_async_stream::try_stream;
use iceberg::scan::FileScanTask;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, Op, SerialArray};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{
    ColumnId, ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_FILE_POS_COLUMN_NAME, ROW_ID_COLUMN_NAME,
    TableId,
};
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::types::{JsonbVal, ScalarRef, Serial, ToOwnedDatum};
use risingwave_connector::source::iceberg::{IcebergScanOpts, scan_task_to_chunk};
use risingwave_connector::source::reader::desc::SourceDesc;
use risingwave_connector::source::{SourceContext, SourceCtrlOpts};
use risingwave_storage::store::PrefetchOptions;
use thiserror_ext::AsReport;

use super::{SourceStateTableHandler, StreamSourceCore, prune_additional_cols};
use crate::common::rate_limit::limited_chunk_size;
use crate::executor::prelude::*;
use crate::executor::stream_reader::StreamReaderWithPause;

const SPLIT_BATCH_SIZE: usize = 1000;

pub struct IcebergFetchExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Streaming source for external
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Upstream list executor.
    upstream: Option<Executor>,

    /// Rate limit in rows/s.
    rate_limit_rps: Option<u32>,
}

/// Fetched data from 1 [`FileScanTask`], along with states for checkpointing.
///
/// Currently 1 `FileScanTask` -> 1 `ChunksWithState`.
/// Later after we support reading part of a file, we will support 1 `FileScanTask` -> n `ChunksWithState`.
struct ChunksWithState {
    chunks: Vec<StreamChunk>,
    data_file_path: String,
    #[expect(dead_code)]
    last_read_pos: Datum,
}

impl<S: StateStore> IcebergFetchExecutor<S> {
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
        }
    }

    async fn replace_with_new_batch_reader<const BIASED: bool>(
        splits_on_fetch: &mut usize,
        state_store_handler: &SourceStateTableHandler<S>,
        column_ids: Vec<ColumnId>,
        source_ctx: SourceContext,
        source_desc: SourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, ChunksWithState>,
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
            while let Some(item) = table_iter.next().await {
                let row = item?;
                let split: FileScanTask = match row.datum_at(1) {
                    Some(ScalarRefImpl::Jsonb(jsonb_ref)) => {
                        serde_json::from_value(jsonb_ref.to_owned_scalar().take())
                            .with_context(|| format!("invalid state: {:?}", jsonb_ref))?
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
            let batch_reader = Self::build_batched_stream_reader(
                column_ids,
                source_ctx,
                source_desc,
                batch,
                rate_limit_rps,
            )
            .map_err(StreamExecutorError::connector_error);
            stream.replace_data_stream(batch_reader);
        }

        Ok(())
    }

    #[try_stream(ok = ChunksWithState, error = StreamExecutorError)]
    async fn build_batched_stream_reader(
        _column_ids: Vec<ColumnId>,
        _source_ctx: SourceContext,
        source_desc: SourceDesc,
        batch: Vec<FileScanTask>,
        _rate_limit_rps: Option<u32>,
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
        // let (stream, _) = source_desc
        //     .source
        //     .build_stream(batch, column_ids, Arc::new(source_ctx), false)
        //     .await
        //     .map_err(StreamExecutorError::connector_error)?;
        // Ok(apply_rate_limit(stream, rate_limit_rps).boxed())
        let properties = source_desc.source.config.clone();
        let properties = match properties {
            risingwave_connector::source::ConnectorProperties::Iceberg(iceberg_properties) => {
                iceberg_properties
            }
            _ => unreachable!(),
        };
        let table = properties.load_table().await?;

        for task in batch {
            let mut chunks = vec![];
            #[for_await]
            for chunk in scan_task_to_chunk(
                table.clone(),
                task,
                IcebergScanOpts {
                    batch_size: 1024,
                    need_seq_num: true, /* TODO: this column is not needed. But need to support col pruning in frontend to remove it. */
                    need_file_path_and_pos: true,
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
        // TODO: currently we generate row_id here. If for risingwave iceberg table engine, maybe we can use _risingwave_iceberg_row_id instead.
        let row_id_idx = source_desc
            .columns
            .iter()
            .position(|c| c.name == ROW_ID_COLUMN_NAME)
            .unwrap();
        tracing::trace!(
            "source_desc.columns: {:#?}, file_path_idx: {}, file_pos_idx: {}, row_id_idx: {}",
            source_desc.columns,
            file_path_idx,
            file_pos_idx,
            row_id_idx
        );
        // Initialize state table.
        state_store_handler.init_epoch(first_epoch).await?;

        let mut splits_on_fetch: usize = 0;
        let mut stream = StreamReaderWithPause::<true, ChunksWithState>::new(
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
            source_desc.clone(),
            &mut stream,
            self.rate_limit_rps,
        )
        .await?;

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
                                    let mut need_rebuild_reader = false;

                                    if let Some(mutation) = barrier.mutation.as_deref() {
                                        match mutation {
                                            Mutation::Pause => stream.pause_stream(),
                                            Mutation::Resume => stream.resume_stream(),
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

                                    if splits_on_fetch == 0 || need_rebuild_reader {
                                        Self::replace_with_new_batch_reader(
                                            &mut splits_on_fetch,
                                            &state_store_handler,
                                            core.column_ids.clone(),
                                            self.build_source_ctx(
                                                &source_desc,
                                                core.source_id,
                                                &core.source_name,
                                            ),
                                            source_desc.clone(),
                                            &mut stream,
                                            self.rate_limit_rps,
                                        )
                                        .await?;
                                    }
                                }
                                // Receiving file assignments from upstream list executor,
                                // store into state table.
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
                                    state_store_handler.set_states_json(jsonb_values).await?;
                                    state_store_handler.try_flush().await?;
                                }
                                Message::Watermark(_) => unreachable!(),
                            }
                        }
                        // StreamChunk from FsSourceReader, and the reader reads only one file.
                        Either::Right(ChunksWithState {
                            chunks,
                            data_file_path,
                            last_read_pos: _,
                        }) => {
                            // TODO: support persist progress after supporting reading part of a file.
                            if true {
                                splits_on_fetch -= 1;
                                state_store_handler.delete(&data_file_path).await?;
                            }

                            for chunk in &chunks {
                                let chunk = prune_additional_cols(
                                    chunk,
                                    file_path_idx,
                                    file_pos_idx,
                                    &source_desc.columns,
                                );
                                // pad row_id
                                let (chunk, op) = chunk.into_parts();
                                let (mut columns, visibility) = chunk.into_parts();
                                columns.insert(
                                    row_id_idx,
                                    Arc::new(
                                        SerialArray::from_iter_bitmap(
                                            itertools::repeat_n(Serial::from(0), columns[0].len()),
                                            Bitmap::ones(columns[0].len()),
                                        )
                                        .into(),
                                    ),
                                );
                                let chunk = StreamChunk::from_parts(
                                    op,
                                    DataChunk::from_parts(columns.into(), visibility),
                                );

                                yield Message::Chunk(chunk);
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<S: StateStore> Execute for IcebergFetchExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for IcebergFetchExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("IcebergFetchExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .finish()
        } else {
            f.debug_struct("IcebergFetchExecutor").finish()
        }
    }
}
