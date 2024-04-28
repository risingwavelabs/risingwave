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

use std::ops::{Bound, Deref};
use std::sync::Arc;

use bytes::Bytes;
use futures::prelude::stream::StreamExt;
use futures_async_stream::try_stream;
use futures_util::pin_mut;
use itertools::Itertools;
use prometheus::Histogram;
use risingwave_common::array::DataChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnId, Field, Schema};
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::ScalarImpl;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::common::BatchQueryEpoch;
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::{collect_data_chunk, KeyedRow, TableDistribution};
use risingwave_storage::{dispatch_state_store, StateStore};

use super::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder, ScanRange,
};
use crate::error::{BatchError, Result};
use crate::monitor::BatchMetricsWithTaskLabels;
use crate::task::BatchTaskContext;

pub struct LogRowSeqScanExecutor<S: StateStore> {
    chunk_size: usize,
    identity: String,
    // It is table schema + op column
    schema: Schema,

    /// Batch metrics.
    /// None: Local mode don't record mertics.
    metrics: Option<BatchMetricsWithTaskLabels>,

    table: StorageTable<S>,
    scan_ranges: Vec<ScanRange>,
    old_epoch: BatchQueryEpoch,
    new_epoch: BatchQueryEpoch,
}

impl<S: StateStore> LogRowSeqScanExecutor<S> {
    pub fn new(
        table: StorageTable<S>,
        scan_ranges: Vec<ScanRange>,
        old_epoch: BatchQueryEpoch,
        new_epoch: BatchQueryEpoch,
        chunk_size: usize,
        identity: String,
        metrics: Option<BatchMetricsWithTaskLabels>,
    ) -> Self {
        let mut schema = table.schema().clone();
        schema.fields.push(Field::with_name(
            risingwave_common::types::DataType::Int16,
            "op",
        ));
        Self {
            chunk_size,
            identity,
            schema,
            metrics,
            table,
            scan_ranges,
            old_epoch,
            new_epoch,
        }
    }
}

pub struct LogStoreRowSeqScanExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for LogStoreRowSeqScanExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "LogStore row sequential scan should not have input executor!"
        );
        let log_store_seq_scan_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::LogRowSeqScan
        )?;

        let table_desc: &StorageTableDesc = log_store_seq_scan_node.get_table_desc()?;
        let column_ids = log_store_seq_scan_node
            .column_ids
            .iter()
            .copied()
            .map(ColumnId::from)
            .collect();

        let vnodes = match &log_store_seq_scan_node.vnode_bitmap {
            Some(vnodes) => Some(Bitmap::from(vnodes).into()),
            // This is possible for dml. vnode_bitmap is not filled by scheduler.
            // Or it's single distribution, e.g., distinct agg. We scan in a single executor.
            None => Some(TableDistribution::all_vnodes()),
        };
        let scan_ranges = vec![ScanRange::full()];

        let chunk_size = source.context.get_config().developer.chunk_size as u32;
        let metrics = source.context().batch_metrics();

        dispatch_state_store!(source.context().state_store(), state_store, {
            let table = StorageTable::new_partial(state_store, column_ids, vnodes, table_desc);
            Ok(Box::new(LogRowSeqScanExecutor::new(
                table,
                scan_ranges,
                log_store_seq_scan_node.old_epoch.clone().unwrap(),
                log_store_seq_scan_node.new_epoch.clone().unwrap(),
                chunk_size as usize,
                source.plan_node().get_identity().clone(),
                metrics,
            )))
        })
    }
}
impl<S: StateStore> Executor for LogRowSeqScanExecutor<S> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

impl<S: StateStore> LogRowSeqScanExecutor<S> {
    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let Self {
            chunk_size,
            identity,
            metrics,
            table,
            scan_ranges,
            old_epoch,
            new_epoch,
            schema,
        } = *self;
        let table = std::sync::Arc::new(table);

        // Create collector.
        let histogram = metrics.as_ref().map(|metrics| {
            metrics
                .executor_metrics()
                .row_seq_scan_next_duration
                .with_guarded_label_values(&metrics.executor_labels(&identity))
        });
        // Range Scan
        // WARN: DO NOT use `select` to execute range scans concurrently
        //       it can consume too much memory if there're too many ranges.
        for range in scan_ranges {
            let stream = Self::execute_range(
                table.clone(),
                range,
                old_epoch.clone(),
                new_epoch.clone(),
                chunk_size,
                histogram.clone(),
                Arc::new(schema.clone()),
            );
            #[for_await]
            for chunk in stream {
                let chunk = chunk?;
                yield chunk;
            }
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn execute_range(
        table: Arc<StorageTable<S>>,
        scan_range: ScanRange,
        old_epoch: BatchQueryEpoch,
        new_epoch: BatchQueryEpoch,
        chunk_size: usize,
        histogram: Option<impl Deref<Target = Histogram>>,
        schema: Arc<Schema>,
    ) {
        let ScanRange {
            pk_prefix,
            next_col_bounds,
        } = scan_range;

        let order_type = table.pk_serializer().get_order_types()[pk_prefix.len()];
        let (start_bound, end_bound) = (next_col_bounds.0, next_col_bounds.1);
        assert!(pk_prefix.len() < table.pk_indices().len());
        // Range Scan.
        let iter = table
            .batch_iter_log_with_pk_bounds(
                old_epoch.into(),
                new_epoch.into(),
                &pk_prefix,
                (
                    match start_bound {
                        Bound::Unbounded => {
                            if order_type.nulls_are_first() {
                                // `NULL`s are at the start bound side, we should exclude them to meet SQL semantics.
                                Bound::Excluded(OwnedRow::new(vec![None]))
                            } else {
                                // Both start and end are unbounded, so we need to select all rows.
                                Bound::Unbounded
                            }
                        }
                        _ => unimplemented!("Log iter range need full"),
                    },
                    match end_bound {
                        Bound::Unbounded => {
                            if order_type.nulls_are_last() {
                                // `NULL`s are at the end bound side, we should exclude them to meet SQL semantics.
                                Bound::Excluded(OwnedRow::new(vec![None]))
                            } else {
                                // Both start and end are unbounded, so we need to select all rows.
                                Bound::Unbounded
                            }
                        }
                        _ => unimplemented!("Log iter range need full"),
                    },
                ),
            )
            .await?;

        pin_mut!(iter);
        loop {
            let timer = histogram.as_ref().map(|histogram| histogram.start_timer());

            let mut iter = iter.as_mut().map(|r| match r {
                Ok((op, value)) => {
                    let (k, row) = value.into_owned_row_key();
                    let full_row = row
                        .into_iter()
                        .chain(vec![Some(ScalarImpl::Int16(op))])
                        .collect_vec();
                    let row = OwnedRow::new(full_row);
                    Ok(KeyedRow::<Bytes>::new(k, row))
                }
                Err(e) => Err(e),
            });
            let chunk = collect_data_chunk(&mut iter, &schema, Some(chunk_size))
                .await
                .map_err(BatchError::from)?;
            if let Some(timer) = timer {
                timer.observe_duration()
            }

            if let Some(chunk) = chunk {
                yield chunk
            } else {
                break;
            }
        }
    }
}
