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

use std::ops::{Bound, Deref, RangeBounds};
use std::sync::Arc;
use futures_util::pin_mut;
use prometheus::Histogram;
use futures::prelude::stream::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnId, Schema};
use risingwave_common::row::{Row, OwnedRow};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::common::BatchQueryEpoch;
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_storage::dispatch_state_store;
use risingwave_storage::table::{TableDistribution, collect_data_chunk};
use risingwave_storage::{table::batch_table::storage_table::StorageTable, StateStore};

use crate::{monitor::BatchMetricsWithTaskLabels, task::BatchTaskContext};
use crate::error::{BatchError, Result};
use super::{BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder, RowSeqScanExecutor, ScanRange};

pub struct LogStoreRowSeqScanExecutor<S: StateStore> {
    chunk_size: usize,
    identity: String,

    /// Batch metrics.
    /// None: Local mode don't record mertics.
    metrics: Option<BatchMetricsWithTaskLabels>,

    table: StorageTable<S>,
    scan_ranges: Vec<ScanRange>,
    epoch: BatchQueryEpoch,
}

impl<S: StateStore> LogStoreRowSeqScanExecutor<S> {
    pub fn new(
        table: StorageTable<S>,
        scan_ranges: Vec<ScanRange>,
        epoch: BatchQueryEpoch,
        chunk_size: usize,
        identity: String,
        metrics: Option<BatchMetricsWithTaskLabels>,
    ) -> Self {
        Self {
            chunk_size,
            identity,
            metrics,
            table,
            scan_ranges,
            epoch,
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
            NodeBody::LogStoreRowSeqScan
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

        let scan_ranges = {
            let scan_ranges = &log_store_seq_scan_node.scan_ranges;
            if scan_ranges.is_empty() {
                vec![ScanRange::full()]
            } else {
                scan_ranges
                    .iter()
                    .map(|scan_range| {
                        let pk_types = table_desc.pk.iter().map(|order| {
                            DataType::from(
                                table_desc.columns[order.column_index as usize]
                                    .column_type
                                    .as_ref()
                                    .unwrap(),
                            )
                        });
                        ScanRange::new(scan_range.clone(), pk_types)
                    })
                    .try_collect()?
            }
        };

        let epoch = source.epoch.clone();
        let chunk_size  = source.context.get_config().developer.chunk_size as u32;
        let metrics = source.context().batch_metrics();

        dispatch_state_store!(source.context().state_store(), state_store, {
            let table = StorageTable::new_partial(state_store, column_ids, vnodes, table_desc);
            Ok(Box::new(LogStoreRowSeqScanExecutor::new(
                table,
                scan_ranges,
                epoch,
                chunk_size as usize,
                source.plan_node().get_identity().clone(),
                metrics,
            )))
        })
    }
}
impl<S: StateStore> Executor for LogStoreRowSeqScanExecutor<S> {
    fn schema(&self) -> &Schema {
        self.table.schema()
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

impl<S: StateStore> LogStoreRowSeqScanExecutor<S> {
    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let Self {
            chunk_size,
            identity,
            metrics,
            table,
            scan_ranges,
            epoch,
        } = *self;
        let table = std::sync::Arc::new(table);

        // Create collector.
        let histogram = metrics.as_ref().map(|metrics| {
            metrics
                .executor_metrics()
                .row_seq_scan_next_duration
                .with_guarded_label_values(&metrics.executor_labels(&identity))
        });

        // if ordered {
        //     // Currently we execute range-scans concurrently so the order is not guaranteed if
        //     // there're multiple ranges.
        //     // TODO: reserve the order for multiple ranges.
        //     assert_eq!(scan_ranges.len(), 1);
        // }

        // the number of rows have been returned as execute result
        let mut returned = 0;

        let mut data_chunk_builder = DataChunkBuilder::new(table.schema().data_types(), chunk_size);

        // Range Scan
        // WARN: DO NOT use `select` to execute range scans concurrently
        //       it can consume too much memory if there're too many ranges.
        for range in scan_ranges {
            let stream = Self::execute_range(
                table.clone(),
                range,
                // ordered,
                epoch.clone(),
                chunk_size,
                // limit,
                histogram.clone(),
            );
            #[for_await]
            for chunk in stream {
                let chunk = chunk?;
                returned += chunk.cardinality() as u64;
                yield chunk;
                // if let Some(limit) = &limit
                //     && returned >= *limit
                // {
                //     return Ok(());
                // }
            }
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn execute_range(
        table: Arc<StorageTable<S>>,
        scan_range: ScanRange,
        // ordered: bool,
        epoch: BatchQueryEpoch,
        chunk_size: usize,
        // limit: Option<u64>,
        histogram: Option<impl Deref<Target = Histogram>>,
    ) {
        let ScanRange {
            pk_prefix,
            next_col_bounds,
        } = scan_range;

        let order_type = table.pk_serializer().get_order_types()[pk_prefix.len()];
        let (start_bound, end_bound) = if order_type.is_ascending() {
            (next_col_bounds.0, next_col_bounds.1)
        } else {
            (next_col_bounds.1, next_col_bounds.0)
        };

        let start_bound_is_bounded = !matches!(start_bound, Bound::Unbounded);
        let end_bound_is_bounded = !matches!(end_bound, Bound::Unbounded);

        // Range Scan.
        assert!(pk_prefix.len() < table.pk_indices().len());
        let iter = table
            .batch_iter_log_with_pk_bounds(
                epoch.clone().into(),
                epoch.into(),
                &pk_prefix,
                (
                    match start_bound {
                        Bound::Unbounded => {
                            if end_bound_is_bounded && order_type.nulls_are_first() {
                                // `NULL`s are at the start bound side, we should exclude them to meet SQL semantics.
                                Bound::Excluded(OwnedRow::new(vec![None]))
                            } else {
                                // Both start and end are unbounded, so we need to select all rows.
                                Bound::Unbounded
                            }
                        }
                        Bound::Included(x) => Bound::Included(OwnedRow::new(vec![x])),
                        Bound::Excluded(x) => Bound::Excluded(OwnedRow::new(vec![x])),
                    },
                    match end_bound {
                        Bound::Unbounded => {
                            if start_bound_is_bounded && order_type.nulls_are_last() {
                                // `NULL`s are at the end bound side, we should exclude them to meet SQL semantics.
                                Bound::Excluded(OwnedRow::new(vec![None]))
                            } else {
                                // Both start and end are unbounded, so we need to select all rows.
                                Bound::Unbounded
                            }
                        }
                        Bound::Included(x) => Bound::Included(OwnedRow::new(vec![x])),
                        Bound::Excluded(x) => Bound::Excluded(OwnedRow::new(vec![x])),
                    },
                ),
            )
            .await?;

        pin_mut!(iter);
        loop {
            let timer = histogram.as_ref().map(|histogram| histogram.start_timer());

            let chunk = collect_data_chunk(&mut iter, table.schema(), Some(chunk_size))
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