// Copyright 2024 RisingWave Labs
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

use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;

use futures::{pin_mut, Stream};
use futures_async_stream::{for_await, try_stream};
use governor::clock::MonotonicClock;
use governor::{Quota, RateLimiter};
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{ColumnDesc, Schema, TableId};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{Scalar, ScalarImpl, Timestamptz};
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::error::ConnectorResult;
use risingwave_connector::source::cdc::external::{
    CdcOffset, CdcTableType, ExternalTableConfig, ExternalTableReader, ExternalTableReaderImpl,
    SchemaTableName,
};

use crate::common::rate_limit::limited_chunk_size;
use crate::executor::backfill::cdc::upstream_table::snapshot::SnapshotReadArgs;
use crate::executor::backfill::utils::{get_new_pos, iter_chunks};
use crate::executor::StreamExecutorError;

/// This struct represents an external table to be read during backfill
pub struct ExternalStorageTable {
    /// Id for this table.
    table_id: TableId,

    /// The normalized name of the table, e.g. `dbname.schema_name.table_name`.
    table_name: String,

    schema_name: String,

    database_name: String,

    config: ExternalTableConfig,

    table_type: CdcTableType,

    // table_reader: ExternalTableReaderImpl,
    /// The schema of the output columns, i.e., this table VIEWED BY some executor like
    /// `RowSeqScanExecutor`.
    /// todo: the schema of the external table defined in the CREATE TABLE DDL
    schema: Schema,

    pk_order_types: Vec<OrderType>,

    /// Indices of primary key.
    /// Note that the index is based on the all columns of the table.
    pk_indices: Vec<usize>,
}

impl ExternalStorageTable {
    pub fn new(
        table_id: TableId,
        SchemaTableName {
            table_name,
            schema_name,
        }: SchemaTableName,
        database_name: String,
        // table_reader: ExternalTableReaderImpl,
        config: ExternalTableConfig,
        table_type: CdcTableType,
        schema: Schema,
        pk_order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
    ) -> Self {
        Self {
            table_id,
            table_name,
            schema_name,
            database_name,
            // table_reader,
            config,
            table_type,
            schema,
            pk_order_types,
            pk_indices,
        }
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn pk_order_types(&self) -> &[OrderType] {
        &self.pk_order_types
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }

    pub fn schema_table_name(&self) -> SchemaTableName {
        SchemaTableName {
            schema_name: self.schema_name.clone(),
            table_name: self.table_name.clone(),
        }
    }

    pub async fn create_table_reader(&self) -> ConnectorResult<ExternalTableReaderImpl> {
        self.table_type
            .create_table_reader(
                self.config.clone(),
                self.schema.clone(),
                self.pk_indices.clone(),
            )
            .await
    }

    pub fn qualified_table_name(&self) -> String {
        format!("{}.{}", self.schema_name, self.table_name)
    }

    pub fn database_name(&self) -> &str {
        self.database_name.as_str()
    }

    pub async fn current_cdc_offset(
        &self,
        table_reader: &ExternalTableReaderImpl,
    ) -> ConnectorResult<Option<CdcOffset>> {
        let binlog = table_reader.current_cdc_offset();
        let binlog = binlog.await?;
        Ok(Some(binlog))
    }

    #[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
    pub async fn snapshot_read_full_table(
        &self,
        table_reader: Arc<ExternalTableReaderImpl>,
        args: SnapshotReadArgs,
        batch_size: u32,
    ) {
        let primary_keys = self
            .pk_indices()
            .iter()
            .map(|idx| {
                let f = &self.schema().fields[*idx];
                f.name.clone()
            })
            .collect_vec();

        // prepare rate limiter
        if args.rate_limit_rps == Some(0) {
            // If limit is 0, we should not read any data from the upstream table.
            // Keep waiting util the stream is rebuilt.
            let future = futures::future::pending::<()>();
            future.await;
            unreachable!();
        }
        let limiter = args.rate_limit_rps.map(|limit| {
            tracing::info!(rate_limit = limit, "rate limit applied");
            RateLimiter::direct_with_clock(
                Quota::per_second(NonZeroU32::new(limit).unwrap()),
                &MonotonicClock,
            )
        });

        let mut read_args = args;
        let schema_table_name = read_args.schema_table_name.clone();
        let database_name = read_args.database_name.clone();
        // loop to read all data from the table
        loop {
            tracing::debug!(
                "snapshot_read primary keys: {:?}, current_pos: {:?}",
                primary_keys,
                read_args.current_pos
            );

            let mut read_count: usize = 0;
            let row_stream = table_reader.snapshot_read(
                self.schema_table_name(),
                read_args.current_pos.clone(),
                primary_keys.clone(),
                batch_size,
            );

            pin_mut!(row_stream);
            let mut builder = DataChunkBuilder::new(
                self.schema().data_types(),
                limited_chunk_size(read_args.rate_limit_rps),
            );
            let chunk_stream = iter_chunks(row_stream, &mut builder);
            let mut current_pk_pos = read_args.current_pos.clone().unwrap_or_default();

            #[for_await]
            for chunk in chunk_stream {
                let chunk = chunk?;
                let chunk_size = chunk.capacity();
                read_count += chunk.cardinality();
                current_pk_pos = get_new_pos(&chunk, &read_args.pk_indices);

                if read_args.rate_limit_rps.is_none() || chunk_size == 0 {
                    // no limit, or empty chunk
                    yield Some(with_additional_columns(
                        chunk,
                        &read_args.additional_columns,
                        schema_table_name.clone(),
                        database_name.clone(),
                    ));
                    continue;
                } else {
                    // Apply rate limit, see `risingwave_stream::executor::source::apply_rate_limit` for more.
                    // May be should be refactored to a common function later.
                    let limiter = limiter.as_ref().unwrap();
                    let limit = read_args.rate_limit_rps.unwrap() as usize;

                    // Because we produce chunks with limited-sized data chunk builder and all rows
                    // are `Insert`s, the chunk size should never exceed the limit.
                    assert!(chunk_size <= limit);

                    // `InsufficientCapacity` should never happen because we have check the cardinality
                    limiter
                        .until_n_ready(NonZeroU32::new(chunk_size as u32).unwrap())
                        .await
                        .unwrap();
                    yield Some(with_additional_columns(
                        chunk,
                        &read_args.additional_columns,
                        schema_table_name.clone(),
                        database_name.clone(),
                    ));
                }
            }

            // check read_count if the snapshot batch is finished
            if read_count < batch_size as _ {
                tracing::debug!("finished loading of full table snapshot");
                yield None;
                unreachable!()
            } else {
                // update PK position and continue to read the table
                read_args.current_pos = Some(current_pk_pos);
            }
        }
    }
}

/// Append additional columns with value as null to the snapshot chunk
fn with_additional_columns(
    snapshot_chunk: StreamChunk,
    additional_columns: &[ColumnDesc],
    schema_table_name: SchemaTableName,
    database_name: String,
) -> StreamChunk {
    let (ops, mut columns, visibility) = snapshot_chunk.into_inner();
    for desc in additional_columns {
        let mut builder = desc.data_type.create_array_builder(visibility.len());
        match *desc.additional_column.column_type.as_ref().unwrap() {
            // set default value for timestamp
            ColumnType::Timestamp(_) => builder.append_n(
                visibility.len(),
                Some(Timestamptz::default().to_scalar_value()),
            ),
            ColumnType::DatabaseName(_) => {
                builder.append_n(
                    visibility.len(),
                    Some(ScalarImpl::from(database_name.clone())),
                );
            }
            ColumnType::SchemaName(_) => {
                builder.append_n(
                    visibility.len(),
                    Some(ScalarImpl::from(schema_table_name.schema_name.clone())),
                );
            }
            ColumnType::TableName(_) => {
                builder.append_n(
                    visibility.len(),
                    Some(ScalarImpl::from(schema_table_name.table_name.clone())),
                );
            }
            // set null for other additional columns
            _ => {
                builder.append_n_null(visibility.len());
            }
        }
        columns.push(builder.finish().into());
    }
    StreamChunk::with_visibility(ops, columns, visibility)
}
