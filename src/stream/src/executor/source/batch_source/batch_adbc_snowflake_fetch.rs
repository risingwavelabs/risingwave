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

use arrow::RecordBatch;
use either::Either;
use futures::stream;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::array::Op;
use risingwave_common::array::arrow::arrow_array_56 as arrow;
use risingwave_common::id::TableId;
use risingwave_common::types::{JsonbVal, Scalar, ScalarRef};
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::adbc_snowflake::{
    AdbcSnowflakeArrowConvert, AdbcSnowflakeProperties,
};
use risingwave_connector::source::reader::desc::SourceDesc;
use thiserror_ext::AsReport;

use super::batch_adbc_snowflake_list::AdbcSnowflakeSplit;
use crate::executor::prelude::*;
use crate::executor::source::StreamSourceCore;
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::LocalBarrierManager;

pub struct BatchAdbcSnowflakeFetchExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Core component for managing external streaming source state
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Upstream list executor that provides the splits to read.
    upstream: Option<Executor>,

    // barrier manager for reporting load finished
    barrier_manager: LocalBarrierManager,

    associated_table_id: TableId,
}

impl<S: StateStore> BatchAdbcSnowflakeFetchExecutor<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        upstream: Executor,
        barrier_manager: LocalBarrierManager,
        associated_table_id: Option<TableId>,
    ) -> Self {
        assert!(associated_table_id.is_some());
        Self {
            actor_ctx,
            stream_source_core: Some(stream_source_core),
            upstream: Some(upstream),
            barrier_manager,
            associated_table_id: associated_table_id.unwrap(),
        }
    }
}

impl<S: StateStore> BatchAdbcSnowflakeFetchExecutor<S> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut upstream = self.upstream.take().unwrap().execute();
        let barrier = expect_first_barrier(&mut upstream).await?;
        yield Message::Barrier(barrier);

        let mut is_refreshing = false;
        let mut is_list_finished = false;
        let mut splits_on_fetch: usize = 0;
        let is_load_finished = Arc::new(RwLock::new(false));
        let mut split_queue = VecDeque::new();

        let mut core = self.stream_source_core.take().unwrap();
        let source_desc_builder = core.source_desc_builder.take().unwrap();
        let source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;

        // Get column names from the source schema, filtering out additional columns and hidden columns.
        // These columns are derived from the fetch executor's schema and represent the actual
        // Snowflake table columns that should be queried.
        let column_names: Vec<String> = source_desc
            .columns
            .iter()
            .filter(|c| c.is_visible() && c.additional_column.column_type.is_none())
            .map(|c| c.name.clone())
            .collect();
        tracing::debug!("[adbc snowflake fetch] column_names: {:?}", column_names);

        let mut stream =
            StreamReaderWithPause::<true, StreamChunk>::new(upstream, stream::pending().boxed());

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::error!(error = %e.as_report(), "Fetch Error");
                    split_queue.clear();
                    *is_load_finished.write() = false;
                    return Err(e);
                }
                Ok(msg) => match msg {
                    Either::Left(msg) => match msg {
                        Message::Barrier(barrier) => {
                            let mut need_rebuild_reader = false;
                            if let Some(mutation) = barrier.mutation.as_deref() {
                                mutation.on_new_pause_resume(|new_pause| {
                                    if new_pause {
                                        stream.pause_stream();
                                    } else {
                                        stream.resume_stream();
                                    }
                                });
                                match mutation {
                                    Mutation::RefreshStart {
                                        associated_source_id,
                                        ..
                                    } if associated_source_id == &core.source_id => {
                                        tracing::info!(
                                            ?barrier.epoch,
                                            actor_id = %self.actor_ctx.id,
                                            source_id = %core.source_id,
                                            table_id = %self.associated_table_id,
                                            "RefreshStart:"
                                        );

                                        // reset states and abort current workload
                                        split_queue.clear();
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
                                            actor_id = %self.actor_ctx.id,
                                            source_id = %core.source_id,
                                            table_id = %self.associated_table_id,
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
                                && split_queue.is_empty()
                                && is_list_finished
                                && is_refreshing
                                && barrier.is_checkpoint()
                            {
                                tracing::info!(
                                    ?barrier.epoch,
                                    actor_id = %self.actor_ctx.id,
                                    source_id = %core.source_id,
                                    table_id = %self.associated_table_id,
                                    "Reporting load finished"
                                );
                                self.barrier_manager.report_source_load_finished(
                                    barrier.epoch,
                                    self.actor_ctx.id,
                                    self.associated_table_id,
                                    core.source_id,
                                );

                                // reset flags
                                is_list_finished = false;
                                is_refreshing = false;
                            }

                            yield Message::Barrier(barrier);

                            if need_rebuild_reader
                                || (splits_on_fetch == 0
                                    && !split_queue.is_empty()
                                    && is_refreshing)
                            {
                                Self::replace_with_new_reader(
                                    &mut split_queue,
                                    &mut stream,
                                    &mut splits_on_fetch,
                                    source_desc.clone(),
                                    &column_names,
                                    is_load_finished.clone(),
                                )?;
                            }
                        }
                        Message::Chunk(chunk) => {
                            let split_values: Vec<(String, JsonbVal)> = chunk
                                .data_chunk()
                                .rows()
                                .map(|row| {
                                    let split_id = row.datum_at(0).unwrap().into_utf8();
                                    let split = row.datum_at(1).unwrap().into_jsonb();
                                    (split_id.to_owned(), split.to_owned_scalar())
                                })
                                .collect();
                            tracing::debug!("received split assignments: {:?}", split_values);
                            split_queue.extend(split_values);
                        }
                        Message::Watermark(_) => unreachable!(),
                    },
                    Either::Right(chunk) => {
                        // Check if the reader is finished after yielding
                        if *is_load_finished.read() {
                            splits_on_fetch -= 1;
                            tracing::debug!(
                                "split read finished, remaining splits_on_fetch: {}",
                                splits_on_fetch
                            );
                        }
                        yield Message::Chunk(chunk);
                    }
                },
            }
        }
    }

    fn replace_with_new_reader<const BIASED: bool>(
        split_queue: &mut VecDeque<(String, JsonbVal)>,
        stream: &mut StreamReaderWithPause<BIASED, StreamChunk>,
        splits_on_fetch: &mut usize,
        source_desc: SourceDesc,
        column_names: &[String],
        read_finished: Arc<RwLock<bool>>,
    ) -> StreamExecutorResult<()> {
        // For ADBC Snowflake, we process one split at a time to manage connection resources
        // In the future, this could be extended to batch multiple splits

        if let Some((split_id, split_json)) = split_queue.pop_front() {
            tracing::debug!("building reader for split: {}", split_id);
            *splits_on_fetch = 1;
            *read_finished.write() = false;

            let split = AdbcSnowflakeSplit::decode(split_json.as_scalar_ref())?;
            let reader =
                Self::build_split_reader(source_desc, split, column_names.to_vec(), read_finished);
            stream.replace_data_stream(reader.boxed());
        } else {
            stream.replace_data_stream(stream::pending().boxed());
        }

        Ok(())
    }

    /// Build and execute a data reader for a single split.
    /// The reader yields chunks from the split until all data is exhausted,
    /// then sets the `read_finished` flag.
    /// Chunks are split to respect the configured `chunk_size` for rate limiting.
    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn build_split_reader(
        source_desc: SourceDesc,
        split: AdbcSnowflakeSplit,
        column_names: Vec<String>,
        read_finished: Arc<RwLock<bool>>,
    ) {
        let properties = source_desc.source.config.clone();
        let properties = match properties {
            ConnectorProperties::AdbcSnowflake(props) => Box::new(*props),
            _ => unreachable!(),
        };

        let max_chunk_size = crate::config::chunk_size();
        let chunks = Self::read_split(properties, split, &column_names)?;
        for chunk in chunks {
            // Split large chunks to respect the configured chunk_size for rate limiting
            if chunk.capacity() > max_chunk_size {
                for small_chunk in chunk.split(max_chunk_size) {
                    yield small_chunk;
                }
            } else {
                yield chunk;
            }
        }

        *read_finished.write() = true;
    }

    /// Read data from a single split by executing the Snowflake query.
    /// Returns all chunks for the split. The query is built from the source schema,
    /// `table_ref`, optional snapshot timestamp (AT clause), and optional WHERE clause.
    /// Column names are derived from the fetch executor's schema, filtering out additional
    /// columns and hidden columns.
    /// If time travel query fails, falls back to querying without snapshot.
    fn read_split(
        properties: Box<AdbcSnowflakeProperties>,
        split: AdbcSnowflakeSplit,
        column_names: &[String],
    ) -> StreamExecutorResult<Vec<StreamChunk>> {
        let select_list = column_names
            .iter()
            .map(|c| format!(r#""{}""#, c))
            .collect::<Vec<_>>()
            .join(", ");

        // Try with snapshot first if available
        if let Some(ref ts) = split.snapshot_timestamp {
            let table_expr_with_snapshot = format!("{} AT(TIMESTAMP => '{}')", split.table_ref, ts);
            let mut final_query = format!("SELECT {select_list} FROM {}", table_expr_with_snapshot);
            if let Some(ref where_clause) = split.where_clause {
                final_query = format!("{final_query} WHERE {where_clause}");
            }

            tracing::debug!(
                split_id = %split.split_id,
                query = %final_query,
                "executing query for split with time travel"
            );

            match properties.execute_query(&final_query) {
                Ok(batches) => {
                    return Self::convert_batches_to_chunks(&split.split_id, batches);
                }
                Err(e) => {
                    // Time travel may have failed, log and fall back to current data
                    tracing::warn!(
                        split_id = %split.split_id,
                        error = %e.as_report(),
                        "Time travel query failed, falling back to current data"
                    );
                }
            }
        }

        // Fall back to querying without snapshot
        let mut final_query = format!("SELECT {select_list} FROM {}", split.table_ref);
        if let Some(ref where_clause) = split.where_clause {
            final_query = format!("{final_query} WHERE {where_clause}");
        }

        tracing::debug!(
            split_id = %split.split_id,
            query = %final_query,
            "executing query for split without time travel"
        );

        let batches = properties.execute_query(&final_query)?;
        Self::convert_batches_to_chunks(&split.split_id, batches)
    }

    /// Convert Arrow `RecordBatch`es to `StreamChunk`s
    fn convert_batches_to_chunks(
        split_id: &str,
        batches: Vec<RecordBatch>,
    ) -> StreamExecutorResult<Vec<StreamChunk>> {
        let converter = AdbcSnowflakeArrowConvert;
        let mut chunks = Vec::new();

        for batch in batches {
            // Convert Arrow RecordBatch to RisingWave DataChunk
            // The column order in the RecordBatch matches the Snowflake query result,
            // which is consistent with the schema inferred by get_arrow_schema() in the connector.
            let data_chunk = converter.chunk_from_record_batch(&batch)?;

            // Convert DataChunk to StreamChunk (all inserts)
            let stream_chunk = StreamChunk::from_parts(
                itertools::repeat_n(Op::Insert, data_chunk.capacity()).collect_vec(),
                data_chunk,
            );

            chunks.push(stream_chunk);
        }

        tracing::debug!(
            split_id = %split_id,
            num_chunks = chunks.len(),
            "finished reading split"
        );

        Ok(chunks)
    }
}

impl<S: StateStore> Execute for BatchAdbcSnowflakeFetchExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for BatchAdbcSnowflakeFetchExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("BatchAdbcSnowflakeFetchExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .finish()
        } else {
            f.debug_struct("BatchAdbcSnowflakeFetchExecutor").finish()
        }
    }
}
