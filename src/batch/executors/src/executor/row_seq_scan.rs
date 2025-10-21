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
use std::ops::Deref;
use std::sync::Arc;

use anyhow::anyhow;
use futures::{StreamExt, pin_mut};
use futures_async_stream::try_stream;
use prometheus::Histogram;
use risingwave_common::array::DataChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnId, Schema};
use risingwave_common::hash::VnodeCountCompat;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::common::BatchQueryEpoch;
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::BatchTable;
use risingwave_storage::{StateStore, dispatch_state_store};

use super::ScanRange;
use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
    build_scan_ranges_from_pb,
};
use crate::monitor::BatchMetrics;

/// Executor that scans data from row table
pub struct RowSeqScanExecutor<S: StateStore> {
    chunk_size: usize,
    identity: String,

    /// Batch metrics.
    /// None: Local mode don't record metrics.
    metrics: Option<BatchMetrics>,

    table: BatchTable<S>,
    scan_ranges: Vec<ScanRange>,
    ordered: bool,
    query_epoch: BatchQueryEpoch,
    limit: Option<u64>,
}

impl<S: StateStore> RowSeqScanExecutor<S> {
    pub fn new(
        table: BatchTable<S>,
        scan_ranges: Vec<ScanRange>,
        ordered: bool,
        query_epoch: BatchQueryEpoch,
        chunk_size: usize,
        identity: String,
        limit: Option<u64>,
        metrics: Option<BatchMetrics>,
    ) -> Self {
        Self {
            chunk_size,
            identity,
            metrics,
            table,
            scan_ranges,
            ordered,
            query_epoch,
            limit,
        }
    }
}

pub struct RowSeqScanExecutorBuilder {}

impl BoxedExecutorBuilder for RowSeqScanExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "Row sequential scan should not have input executor!"
        );
        let seq_scan_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::RowSeqScan
        )?;

        let table_desc: &StorageTableDesc = seq_scan_node.get_table_desc()?;
        let column_ids = seq_scan_node
            .column_ids
            .iter()
            .copied()
            .map(ColumnId::from)
            .collect();
        let vnodes = match &seq_scan_node.vnode_bitmap {
            Some(vnodes) => Some(Bitmap::from(vnodes).into()),
            // This is possible for dml. vnode_bitmap is not filled by scheduler.
            // Or it's single distribution, e.g., distinct agg. We scan in a single executor.
            None => Some(Bitmap::ones(table_desc.vnode_count()).into()),
        };

        let scan_ranges = build_scan_ranges_from_pb(&seq_scan_node.scan_ranges, table_desc)?;

        let ordered = seq_scan_node.ordered;
        let limit = seq_scan_node.limit;
        let query_epoch = seq_scan_node
            .query_epoch
            .ok_or_else(|| anyhow!("query_epoch not set in distributed lookup join"))?;

        let chunk_size = if let Some(limit) = seq_scan_node.limit {
            (limit as u32).min(source.context().get_config().developer.chunk_size as u32)
        } else {
            source.context().get_config().developer.chunk_size as u32
        };
        let metrics = source.context().batch_metrics();

        dispatch_state_store!(source.context().state_store(), state_store, {
            let table = BatchTable::new_partial(state_store, column_ids, vnodes, table_desc);
            Ok(Box::new(RowSeqScanExecutor::new(
                table,
                scan_ranges,
                ordered,
                query_epoch,
                chunk_size as usize,
                source.plan_node().get_identity().clone(),
                limit,
                metrics,
            )))
        })
    }
}

impl<S: StateStore> Executor for RowSeqScanExecutor<S> {
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

impl<S: StateStore> RowSeqScanExecutor<S> {
    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let Self {
            chunk_size,
            metrics,
            table,
            scan_ranges,
            ordered,
            query_epoch,
            limit,
            ..
        } = *self;
        let table = Arc::new(table);

        // Create collector.
        let histogram = metrics
            .as_ref()
            .map(|metrics| &metrics.executor_metrics().row_seq_scan_next_duration);

        if ordered {
            // Currently we execute range-scans concurrently so the order is not guaranteed if
            // there're multiple ranges.
            // TODO: reserve the order for multiple ranges.
            assert_eq!(scan_ranges.len(), 1);
        }

        let (point_gets, range_scans): (Vec<ScanRange>, Vec<ScanRange>) = scan_ranges
            .into_iter()
            .partition(|x| x.pk_prefix.len() == table.pk_indices().len());

        // the number of rows have been returned as execute result
        let mut returned = 0;
        if let Some(limit) = &limit
            && returned >= *limit
        {
            return Ok(());
        }
        let mut data_chunk_builder = DataChunkBuilder::new(table.schema().data_types(), chunk_size);
        // Point Get
        for point_get in point_gets {
            let table = table.clone();
            if let Some(row) =
                Self::execute_point_get(table, point_get, query_epoch, histogram).await?
                && let Some(chunk) = data_chunk_builder.append_one_row(row)
            {
                returned += chunk.cardinality() as u64;
                yield chunk;
                if let Some(limit) = &limit
                    && returned >= *limit
                {
                    return Ok(());
                }
            }
        }
        if let Some(chunk) = data_chunk_builder.consume_all() {
            returned += chunk.cardinality() as u64;
            yield chunk;
            if let Some(limit) = &limit
                && returned >= *limit
            {
                return Ok(());
            }
        }

        // Range Scan
        // WARN: DO NOT use `select` to execute range scans concurrently
        //       it can consume too much memory if there're too many ranges.
        for range in range_scans {
            let stream = Self::execute_range(
                table.clone(),
                range,
                ordered,
                query_epoch,
                chunk_size,
                limit,
                histogram,
            );
            #[for_await]
            for chunk in stream {
                let chunk = chunk?;
                returned += chunk.cardinality() as u64;
                yield chunk;
                if let Some(limit) = &limit
                    && returned >= *limit
                {
                    return Ok(());
                }
            }
        }
    }

    async fn execute_point_get(
        table: Arc<BatchTable<S>>,
        scan_range: ScanRange,
        query_epoch: BatchQueryEpoch,
        histogram: Option<impl Deref<Target = Histogram>>,
    ) -> Result<Option<OwnedRow>> {
        let pk_prefix = scan_range.pk_prefix;
        assert!(pk_prefix.len() == table.pk_indices().len());

        let timer = histogram.as_ref().map(|histogram| histogram.start_timer());

        // Point Get.
        let row = table.get_row(&pk_prefix, query_epoch.into()).await?;

        if let Some(timer) = timer {
            timer.observe_duration()
        }

        Ok(row)
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn execute_range(
        table: Arc<BatchTable<S>>,
        scan_range: ScanRange,
        ordered: bool,
        query_epoch: BatchQueryEpoch,
        chunk_size: usize,
        limit: Option<u64>,
        histogram: Option<impl Deref<Target = Histogram>>,
    ) {
        let pk_prefix = scan_range.pk_prefix.clone();
        let range_bounds = scan_range.convert_to_range_bounds(&table);
        // Range Scan.
        assert!(pk_prefix.len() < table.pk_indices().len());
        let iter = table
            .batch_chunk_iter_with_pk_bounds(
                query_epoch.into(),
                &pk_prefix,
                range_bounds,
                ordered,
                chunk_size,
                PrefetchOptions::new(limit.is_none(), true),
            )
            .await?;

        pin_mut!(iter);
        loop {
            let timer = histogram.as_ref().map(|histogram| histogram.start_timer());

            let chunk = iter.next().await.transpose().map_err(BatchError::from)?;

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
