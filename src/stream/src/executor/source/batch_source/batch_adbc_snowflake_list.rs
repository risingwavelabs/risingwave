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

use anyhow::{Context, anyhow};
use either::Either;
use parking_lot::RwLock;
use risingwave_common::array::Op;
use risingwave_common::array::arrow::arrow_array_55::Array as Array55;
use risingwave_common::id::TableId;
use risingwave_common::types::JsonbVal;
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::adbc_snowflake::AdbcSnowflakeProperties;
use risingwave_connector::source::reader::desc::SourceDescBuilder;
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::prelude::*;
use crate::executor::source::{StreamSourceCore, barrier_to_message_stream};
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::task::LocalBarrierManager;

/// Split information for ADBC Snowflake batch source.
/// Includes snapshot information to ensure all fetch executors read from the same snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AdbcSnowflakeSplit {
    /// Unique identifier for this split
    pub split_id: String,
    /// The base query (without WHERE clause)
    pub base_query: String,
    /// The WHERE clause for this split (e.g., "pk >= 0 AND pk < 1000")
    pub where_clause: Option<String>,
    /// Snapshot timestamp to ensure consistent reads across all fetch executors
    pub snapshot_timestamp: Option<String>,
}

impl AdbcSnowflakeSplit {
    pub fn encode(self) -> JsonbVal {
        JsonbVal::from(serde_json::to_value(self).unwrap())
    }

    pub fn decode(jsonb_val: JsonbVal) -> StreamExecutorResult<Self> {
        let split: Self = serde_json::from_value(jsonb_val.take())
            .context("failed to decode AdbcSnowflakeSplit")?;
        Ok(split)
    }

    /// Get the full query with WHERE clause applied
    pub fn get_query(&self) -> String {
        if let Some(ref where_clause) = self.where_clause {
            format!("{} WHERE {}", self.base_query, where_clause)
        } else {
            self.base_query.clone()
        }
    }
}

pub struct BatchAdbcSnowflakeListExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,
    /// Streaming source for external
    stream_source_core: StreamSourceCore<S>,
    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,
    /// Local barrier manager for reporting list finished
    barrier_manager: LocalBarrierManager,
    associated_table_id: TableId,
    /// Metrics for monitor.
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

        // Build source description from the builder.
        let source_desc_builder: SourceDescBuilder =
            self.stream_source_core.source_desc_builder.take().unwrap();

        let properties = source_desc_builder.with_properties();
        let config = ConnectorProperties::extract(properties, false)?;
        let ConnectorProperties::AdbcSnowflake(snowflake_properties) = config else {
            unreachable!()
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

                                        // Generate splits for snowflake table
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
        // Create a single connection for all metadata queries
        let database = snowflake_properties.create_database()?;
        let mut connection = snowflake_properties.create_connection(&database)?;

        // Get snapshot timestamp for consistent reads
        let snapshot_timestamp =
            Self::get_snapshot_timestamp(&snowflake_properties, &mut connection)?;

        tracing::info!(
            snapshot_timestamp = ?snapshot_timestamp,
            "obtained snapshot timestamp for batch adbc snowflake"
        );

        // Parse the base query to extract table information and generate splits
        let base_query = snowflake_properties.query.clone();

        // Try to get primary key information and row count from the table
        let (pk_columns, estimated_row_count) =
            Self::get_table_metadata(&snowflake_properties, &mut connection)?;

        if pk_columns.is_empty() {
            // If no primary key is found or metadata query fails,
            // fall back to a single split with the entire query
            tracing::info!("no primary key found, using single split");
            yield AdbcSnowflakeSplit {
                split_id: "0".to_owned(),
                base_query: base_query.clone(),
                where_clause: None,
                snapshot_timestamp: snapshot_timestamp.clone(),
            };
        } else {
            // Generate splits based on primary key ranges
            // For simplicity, we'll use the first primary key column
            let pk_column = &pk_columns[0];

            // Determine number of splits based on estimated row count
            // Use roughly 100k rows per split as a heuristic
            let rows_per_split = 100_000;
            let num_splits =
                ((estimated_row_count as f64 / rows_per_split as f64).ceil() as i64).max(1);

            tracing::info!(
                pk_column = %pk_column,
                estimated_rows = estimated_row_count,
                num_splits = num_splits,
                "generating splits for batch adbc snowflake"
            );

            // Get min and max values of the primary key
            let (min_val, max_val) = Self::get_pk_range(
                &snowflake_properties,
                &mut connection,
                pk_column,
                snapshot_timestamp.as_deref(),
            )?;

            // Generate splits based on the range
            for i in 0..num_splits {
                let split_id = format!("{}", i);
                let where_clause =
                    Self::generate_where_clause(pk_column, &min_val, &max_val, i, num_splits);

                yield AdbcSnowflakeSplit {
                    split_id,
                    base_query: base_query.clone(),
                    where_clause: Some(where_clause),
                    snapshot_timestamp: snapshot_timestamp.clone(),
                };
            }
        }

        *is_list_finished.write() = true;
    }

    /// Get the current snapshot timestamp from Snowflake
    fn get_snapshot_timestamp(
        properties: &AdbcSnowflakeProperties,
        connection: &mut risingwave_connector::source::adbc_snowflake::Connection,
    ) -> StreamExecutorResult<Option<String>> {
        use risingwave_common::array::arrow::arrow_array_55;

        // Get current timestamp from Snowflake to use as snapshot reference
        let query = "SELECT CURRENT_TIMESTAMP()::STRING";
        let batches = properties.execute_query_with_connection(connection, query)?;

        // Read the timestamp from the result
        if let Some(batch) = batches.first()
            && batch.num_rows() > 0
            && let Some(array) = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array_55::StringArray>()
        {
            let timestamp: String = array.value(0).into();
            return Ok(Some(timestamp));
        }

        Ok(None)
    }

    /// Get table metadata including primary key columns and estimated row count
    fn get_table_metadata(
        properties: &AdbcSnowflakeProperties,
        connection: &mut risingwave_connector::source::adbc_snowflake::Connection,
    ) -> StreamExecutorResult<(Vec<String>, i64)> {
        use risingwave_common::array::arrow::arrow_array_55;

        // Parse table name from the query (simplified - assumes SELECT * FROM table_name)
        let table_name = Self::extract_table_name(&properties.query)?;

        // Get primary key information
        let pk_query = format!(
            "SELECT COLUMN_NAME FROM {}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
             JOIN {}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu \
             ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME \
             WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' \
             AND tc.TABLE_SCHEMA = '{}' \
             AND tc.TABLE_NAME = '{}' \
             ORDER BY kcu.ORDINAL_POSITION",
            properties.database, properties.database, properties.schema, table_name
        );

        let mut pk_columns = Vec::new();
        let pk_batches = properties.execute_query_with_connection(connection, &pk_query)?;

        for batch in pk_batches {
            if let Some(array) = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array_55::StringArray>()
            {
                let array_len = array.len();
                for i in 0..array_len {
                    let col_name: String = array.value(i).into();
                    pk_columns.push(col_name);
                }
            }
        }

        // Get estimated row count
        let count_query = format!(
            "SELECT ROW_COUNT FROM {}.INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'",
            properties.database, properties.schema, table_name
        );

        let count_batches = properties.execute_query_with_connection(connection, &count_query)?;

        let mut estimated_count: i64 = 0;
        if let Some(batch) = count_batches.first()
            && batch.num_rows() > 0
            && let Some(array) = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array_55::Int64Array>()
        {
            estimated_count = array.value(0);
        }

        Ok((pk_columns, estimated_count))
    }

    /// Extract table name from a simple SELECT query
    fn extract_table_name(query: &str) -> StreamExecutorResult<String> {
        // Simplified parser - expects format like "SELECT ... FROM table_name" or "SELECT ... FROM schema.table_name"
        let query_lower = query.to_lowercase();
        let from_idx = query_lower
            .find(" from ")
            .ok_or_else(|| anyhow!("Could not find FROM clause in query"))?;

        let after_from = &query[from_idx + 6..].trim();

        // Find the table name (stop at whitespace, semicolon, or end of string)
        let table_part = after_from
            .split_whitespace()
            .next()
            .ok_or_else(|| anyhow!("Could not extract table name from query"))?
            .trim_end_matches(';');

        // If schema.table format, take just the table name
        let table_name = table_part.split('.').next_back().unwrap_or(table_part);

        Ok(table_name.to_uppercase())
    }

    /// Get the min and max values of the primary key column
    fn get_pk_range(
        properties: &AdbcSnowflakeProperties,
        connection: &mut risingwave_connector::source::adbc_snowflake::Connection,
        pk_column: &str,
        snapshot_timestamp: Option<&str>,
    ) -> StreamExecutorResult<(String, String)> {
        use risingwave_common::array::arrow::arrow_array_55;

        // Build query with snapshot if available
        let mut range_query = format!(
            "SELECT MIN({})::STRING, MAX({})::STRING FROM ({})",
            pk_column, pk_column, properties.query
        );

        if let Some(ts) = snapshot_timestamp {
            range_query = format!("{} AT(TIMESTAMP => '{}')", range_query, ts);
        }

        let batches = properties.execute_query_with_connection(connection, &range_query)?;

        if let Some(batch) = batches.first()
            && batch.num_rows() > 0
            && let Some(min_array) = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array_55::StringArray>()
            && let Some(max_array) = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array_55::StringArray>()
        {
            let min_val: String = min_array.value(0).into();
            let max_val: String = max_array.value(0).into();
            return Ok((min_val, max_val));
        }

        Err(anyhow!("Failed to get PK range").into())
    }

    /// Generate WHERE clause for a split based on PK range
    fn generate_where_clause(
        pk_column: &str,
        min_val: &str,
        max_val: &str,
        split_index: i64,
        total_splits: i64,
    ) -> String {
        // Try to parse as numeric for better range splitting
        if let (Ok(min), Ok(max)) = (min_val.parse::<i64>(), max_val.parse::<i64>()) {
            let range_size = (max - min) as f64 / total_splits as f64;
            let split_start = min + (range_size * split_index as f64) as i64;
            let split_end = if split_index == total_splits - 1 {
                max + 1 // Include the last value
            } else {
                min + (range_size * (split_index + 1) as f64) as i64
            };

            format!(
                "{} >= {} AND {} < {}",
                pk_column, split_start, pk_column, split_end
            )
        } else {
            // For non-numeric PKs, use string comparison (less optimal but works)
            if split_index == 0 && total_splits == 1 {
                format!(
                    "{} >= '{}' AND {} <= '{}'",
                    pk_column, min_val, pk_column, max_val
                )
            } else {
                // For multiple splits with string keys, fall back to modulo distribution
                format!(
                    "MOD(ABS(HASH({})), {}) = {}",
                    pk_column, total_splits, split_index
                )
            }
        }
    }
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
