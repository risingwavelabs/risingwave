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

use std::sync::Arc;

use anyhow::{Context, anyhow};
use either::Either;
use parking_lot::RwLock;
use risingwave_common::array::Op;
use risingwave_common::array::arrow::arrow_array_56 as arrow;
use risingwave_common::id::TableId;
use risingwave_common::types::{DataType, JsonbRef, JsonbVal, ScalarRef};
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::adbc_snowflake::AdbcSnowflakeProperties;
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::prelude::*;
use crate::executor::source::{StreamSourceCore, barrier_to_message_stream};
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::LocalBarrierManager;

/// Split information for ADBC Snowflake batch source.
///
/// Each split represents a portion of the table to fetch. The `snapshot_timestamp` ensures
/// consistent reads across all fetch executors using Snowflake's time travel feature.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AdbcSnowflakeSplit {
    pub split_id: String,
    pub table_ref: String,
    /// Optional WHERE clause to partition the table (e.g., "pk >= 0 AND pk < 1000")
    pub where_clause: Option<String>,
    /// Snowflake timestamp for consistent snapshot reads across splits
    pub snapshot_timestamp: Option<String>,
}

impl AdbcSnowflakeSplit {
    pub fn encode(self) -> JsonbVal {
        JsonbVal::from(serde_json::to_value(self).unwrap())
    }

    pub fn decode(jsonb_ref: JsonbRef<'_>) -> StreamExecutorResult<Self> {
        let split: Self = serde_json::from_value(jsonb_ref.to_owned_scalar().take())
            .context("failed to decode AdbcSnowflakeSplit")?;
        Ok(split)
    }
}

pub struct BatchAdbcSnowflakeListExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,
    stream_source_core: StreamSourceCore<S>,
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,
    barrier_manager: LocalBarrierManager,
    associated_table_id: TableId,
    _metrics: Arc<StreamingMetrics>,
}

impl<S: StateStore> BatchAdbcSnowflakeListExecutor<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        barrier_manager: LocalBarrierManager,
        associated_table_id: Option<TableId>,
    ) -> Self {
        assert!(associated_table_id.is_some());
        Self {
            actor_ctx,
            stream_source_core,
            barrier_receiver: Some(barrier_receiver),
            barrier_manager,
            associated_table_id: associated_table_id.unwrap(),
            _metrics: metrics,
        }
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

        let source_desc_builder = self.stream_source_core.source_desc_builder.take().unwrap();
        let properties = source_desc_builder.with_properties();
        let config = ConnectorProperties::extract(properties, false)?;
        let ConnectorProperties::AdbcSnowflake(snowflake_properties) = config else {
            unreachable!("BatchAdbcSnowflakeListExecutor expects AdbcSnowflake connector")
        };

        yield Message::Barrier(first_barrier);
        let barrier_stream = barrier_to_message_stream(barrier_receiver).boxed();

        let mut stream = StreamReaderWithPause::<true, _>::new(
            barrier_stream,
            futures::stream::pending().boxed(),
        );
        let mut is_refreshing = false;
        let is_list_finished = Arc::new(RwLock::new(false));

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::warn!(error = %e.as_report(), "encountered an error in batch adbc snowflake list");
                    return Err(e);
                }
                Ok(msg) => match msg {
                    Either::Left(msg) => match msg {
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
                                            actor_id = %self.actor_ctx.id,
                                            source_id = %self.stream_source_core.source_id,
                                            table_id = %self.associated_table_id,
                                            "RefreshStart triggered split generation"
                                        );
                                        is_refreshing = true;
                                        *is_list_finished.write() = false;

                                        let split_stream = Self::generate_splits(
                                            *snowflake_properties.clone(),
                                            is_list_finished.clone(),
                                        )
                                        .boxed();
                                        stream.replace_data_stream(split_stream);
                                    }
                                    _ => (),
                                }
                            }

                            if is_refreshing && *is_list_finished.read() && barrier.is_checkpoint()
                            {
                                tracing::info!(
                                    ?barrier.epoch,
                                    source_id = %self.stream_source_core.source_id,
                                    table_id = %self.associated_table_id,
                                    "reporting batch adbc snowflake list finished"
                                );
                                self.barrier_manager.report_source_list_finished(
                                    barrier.epoch,
                                    self.actor_ctx.id,
                                    self.associated_table_id,
                                    self.stream_source_core.source_id,
                                );
                                is_refreshing = false;
                            }

                            yield Message::Barrier(barrier);
                        }
                        _ => unreachable!(),
                    },
                    Either::Right(split) => {
                        tracing::debug!("generating split: {:?}", split);
                        let chunk = StreamChunk::from_rows(
                            &[(
                                Op::Insert,
                                OwnedRow::new(vec![
                                    Some(ScalarImpl::Utf8(split.split_id.clone().into())),
                                    Some(ScalarImpl::Jsonb(split.encode())),
                                ]),
                            )],
                            &[DataType::Varchar, DataType::Jsonb],
                        );

                        yield Message::Chunk(chunk);
                    }
                },
            }
        }
    }

    #[try_stream(ok = AdbcSnowflakeSplit, error = StreamExecutorError)]
    async fn generate_splits(
        snowflake_properties: AdbcSnowflakeProperties,
        is_list_finished: Arc<RwLock<bool>>,
    ) {
        let database = snowflake_properties.create_database()?;
        let mut connection = snowflake_properties.create_connection(&database)?;

        let snapshot_timestamp =
            Self::get_snapshot_timestamp(&snowflake_properties, &mut connection)?;

        tracing::info!(
            snapshot_timestamp = ?snapshot_timestamp,
            table = %snowflake_properties.table_ref(),
            "generating splits for batch adbc snowflake"
        );

        if snapshot_timestamp.is_none() {
            tracing::warn!(
                table = %snowflake_properties.table_ref(),
                "time travel unavailable, falling back to current data"
            );
        }

        // TODO: Implement parallel fetching by generating multiple splits based on primary key ranges.
        // Steps:
        // 1. Query table metadata (SHOW PRIMARY KEYS, INFORMATION_SCHEMA.TABLES) to get PK columns and row count
        // 2. Calculate optimal number of splits (e.g., ~100k rows per split)
        // 3. Generate WHERE clauses for each split:
        //    - For numeric PKs: range-based splits (e.g., "pk >= 0 AND pk < 1000")
        //    - For non-numeric PKs: hash-based distribution (e.g., "MOD(ABS(HASH(pk)), N) = i")
        // 4. Each split should use the same snapshot_timestamp for consistency
        //
        // For now, use a single split for the entire table.
        tracing::info!("using single split for entire table");
        yield AdbcSnowflakeSplit {
            split_id: "0".to_owned(),
            table_ref: snowflake_properties.table_ref(),
            where_clause: None,
            snapshot_timestamp,
        };

        *is_list_finished.write() = true;
    }

    /// Get the current snapshot timestamp from Snowflake for consistent reads across splits.
    fn get_snapshot_timestamp(
        properties: &AdbcSnowflakeProperties,
        connection: &mut risingwave_connector::source::adbc_snowflake::Connection,
    ) -> StreamExecutorResult<Option<String>> {
        let query = "SELECT CURRENT_TIMESTAMP()::STRING";
        let batches = properties.execute_query_with_connection(connection, query)?;

        if let Some(batch) = batches.first()
            && batch.num_rows() > 0
            && let Some(array) = batch.column(0).as_any().downcast_ref::<arrow::StringArray>()
        {
            let timestamp: String = array.value(0).into();
            return Ok(Some(timestamp));
        }

        Ok(None)
    }

    // TODO: Implement helper methods for parallel fetching:
    //
    // get_table_metadata() -> StreamExecutorResult<(Vec<String>, i64)>
    //   Query SHOW PRIMARY KEYS and INFORMATION_SCHEMA.TABLES, return (pk_columns, row_count)
    //
    // get_pk_range(pk_column, snapshot_timestamp) -> StreamExecutorResult<(String, String)>
    //   Query MIN/MAX of PK column with snapshot (AT TIMESTAMP), fallback if time travel fails
    //
    // generate_where_clause(pk_column, min, max, split_idx, total_splits) -> String
    //   For numeric PKs: range-based (e.g., "pk >= 0 AND pk < 1000")
    //   For non-numeric PKs: hash-based (e.g., "MOD(ABS(HASH(pk)), N) = i")
}

impl<S: StateStore> Execute for BatchAdbcSnowflakeListExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for BatchAdbcSnowflakeListExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchAdbcSnowflakeListExecutor")
            .field("source_id", &self.stream_source_core.source_id)
            .field("column_ids", &self.stream_source_core.column_ids)
            .finish()
    }
}
