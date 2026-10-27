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
    read_latest_local: bool,
}

fn validate_latest_local_scan(
    scan_ranges: &[ScanRange],
    pk_len: usize,
    query_epoch: BatchQueryEpoch,
) -> Result<()> {
    ensure!(
        scan_ranges.len() == 1
            && pk_len > 0
            && scan_ranges[0].pk_prefix.len() == pk_len
            && matches!(
                scan_ranges[0].range_bounds,
                (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
            ),
        "latest-local reads require exactly one full-primary-key point lookup"
    );
    ensure!(
        matches!(
            query_epoch.epoch,
            Some(risingwave_pb::common::batch_query_epoch::Epoch::Committed(
                _
            ))
        ),
        "latest-local reads require a committed query epoch"
    );
    Ok(())
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
            read_latest_local: false,
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
            let mut executor = RowSeqScanExecutor::new(
                table,
                scan_ranges,
                ordered,
                query_epoch,
                chunk_size as usize,
                source.plan_node().get_identity().clone(),
                limit,
                metrics,
            );
            executor.read_latest_local = seq_scan_node.read_latest_local;
            Ok(Box::new(executor))
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
            read_latest_local,
            ..
        } = *self;
        if read_latest_local {
            validate_latest_local_scan(&scan_ranges, table.pk_indices().len(), query_epoch)?;
        }
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
                Self::execute_point_get(table, point_get, query_epoch, read_latest_local, histogram)
                    .await?
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
        read_latest_local: bool,
        histogram: Option<impl Deref<Target = Histogram>>,
    ) -> Result<Option<OwnedRow>> {
        let pk_prefix = scan_range.pk_prefix;
        assert!(pk_prefix.len() == table.pk_indices().len());

        let timer = histogram.as_ref().map(|histogram| histogram.start_timer());

        // Point Get.
        let row = if read_latest_local {
            table.get_row_latest_committed(&pk_prefix).await?
        } else {
            table.get_row(&pk_prefix, query_epoch.into()).await?
        };

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

#[cfg(test)]
mod tests {
    use std::ops::Bound::{Included, Unbounded};

    use risingwave_pb::common::batch_query_epoch::Epoch;

    use super::*;

    #[test]
    fn test_latest_local_scan_validation() {
        let point = ScanRange {
            pk_prefix: OwnedRow::new(vec![Some(1i32.into())]),
            range_bounds: (Unbounded, Unbounded),
        };
        let committed = BatchQueryEpoch {
            epoch: Some(Epoch::Committed(Default::default())),
        };
        assert!(validate_latest_local_scan(std::slice::from_ref(&point), 1, committed).is_ok());
        assert!(validate_latest_local_scan(&[], 1, committed).is_err());
        assert!(validate_latest_local_scan(&[point.clone(), point.clone()], 1, committed).is_err());
        assert!(validate_latest_local_scan(std::slice::from_ref(&point), 2, committed).is_err());
        assert!(validate_latest_local_scan(std::slice::from_ref(&point), 0, committed).is_err());
        let bounded = ScanRange {
            range_bounds: (Included(OwnedRow::empty()), Unbounded),
            ..point.clone()
        };
        assert!(validate_latest_local_scan(&[bounded], 1, committed).is_err());
        for epoch in [
            None,
            Some(Epoch::Current(1)),
            Some(Epoch::Backup(1)),
            Some(Epoch::TimeTravel(1)),
        ] {
            assert!(
                validate_latest_local_scan(
                    std::slice::from_ref(&point),
                    1,
                    BatchQueryEpoch { epoch }
                )
                .is_err()
            );
        }
    }
}
