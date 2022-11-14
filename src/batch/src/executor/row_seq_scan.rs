// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use prometheus::Histogram;
use risingwave_common::array::{DataChunk, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema, TableId, TableOption};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::select_all;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::deserialize_datum;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{scan_range, ScanRange as ProstScanRange};
use risingwave_pb::plan_common::{OrderType as ProstOrderType, StorageTableDesc};
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::{Distribution, TableIter};
use risingwave_storage::{dispatch_state_store, StateStore};

use super::BatchTaskMetricsWithTaskLabels;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

/// Executor that scans data from row table
pub struct RowSeqScanExecutor<S: StateStore> {
    chunk_size: usize,
    identity: String,

    /// Batch metrics.
    /// None: Local mode don't record mertics.
    metrics: Option<BatchTaskMetricsWithTaskLabels>,

    table: StorageTable<S>,
    scan_ranges: Vec<ScanRange>,
    epoch: u64,
}

/// Range for batch scan.
pub struct ScanRange {
    /// The prefix of the primary key.
    pub pk_prefix: Row,

    /// The range bounds of the next column.
    pub next_col_bounds: (Bound<Datum>, Bound<Datum>),
}

impl ScanRange {
    fn is_full_range<T>(bounds: &impl RangeBounds<T>) -> bool {
        matches!(bounds.start_bound(), Bound::Unbounded)
            && matches!(bounds.end_bound(), Bound::Unbounded)
    }

    /// Create a scan range from the prost representation.
    pub fn new(
        scan_range: ProstScanRange,
        mut pk_types: impl Iterator<Item = DataType>,
    ) -> Result<Self> {
        let pk_prefix = Row(scan_range
            .eq_conds
            .iter()
            .map(|v| {
                let ty = pk_types.next().unwrap();
                deserialize_datum(v.as_slice(), &ty)
            })
            .try_collect()?);
        if scan_range.lower_bound.is_none() && scan_range.upper_bound.is_none() {
            return Ok(Self {
                pk_prefix,
                ..Self::full()
            });
        }

        let bound_ty = pk_types.next().unwrap();
        let build_bound = |bound: &scan_range::Bound| -> Bound<Datum> {
            let datum = deserialize_datum(bound.value.as_slice(), &bound_ty).unwrap();
            if bound.inclusive {
                Bound::Included(datum)
            } else {
                Bound::Excluded(datum)
            }
        };

        let next_col_bounds: (Bound<Datum>, Bound<Datum>) = match (
            scan_range.lower_bound.as_ref(),
            scan_range.upper_bound.as_ref(),
        ) {
            (Some(lb), Some(ub)) => (build_bound(lb), build_bound(ub)),
            (None, Some(ub)) => (Bound::Unbounded, build_bound(ub)),
            (Some(lb), None) => (build_bound(lb), Bound::Unbounded),
            (None, None) => unreachable!(),
        };

        Ok(Self {
            pk_prefix,
            next_col_bounds,
        })
    }

    /// Create a scan range for full table scan.
    pub fn full() -> Self {
        Self {
            pk_prefix: Row::default(),
            next_col_bounds: (Bound::Unbounded, Bound::Unbounded),
        }
    }
}

impl<S: StateStore> RowSeqScanExecutor<S> {
    pub fn new(
        table: StorageTable<S>,
        scan_ranges: Vec<ScanRange>,
        epoch: u64,
        chunk_size: usize,
        identity: String,
        metrics: Option<BatchTaskMetricsWithTaskLabels>,
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

pub struct RowSeqScanExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for RowSeqScanExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
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
        let table_id = TableId {
            table_id: table_desc.table_id,
        };
        let column_descs = table_desc
            .columns
            .iter()
            .map(ColumnDesc::from)
            .collect_vec();
        let column_ids = seq_scan_node
            .column_ids
            .iter()
            .copied()
            .map(ColumnId::from)
            .collect();

        let pk_types = table_desc
            .pk
            .iter()
            .map(|order| column_descs[order.index as usize].clone().data_type)
            .collect_vec();
        let order_types: Vec<OrderType> = table_desc
            .pk
            .iter()
            .map(|order| {
                OrderType::from_prost(&ProstOrderType::from_i32(order.order_type).unwrap())
            })
            .collect();

        let pk_indices = table_desc.pk.iter().map(|k| k.index as usize).collect_vec();

        let dist_key_indices = table_desc
            .dist_key_indices
            .iter()
            .map(|&k| k as usize)
            .collect_vec();
        let distribution = match &seq_scan_node.vnode_bitmap {
            Some(vnodes) => Distribution {
                vnodes: Bitmap::from(vnodes).into(),
                dist_key_indices,
            },
            // This is possible for dml. vnode_bitmap is not filled by scheduler.
            // Or it's single distribution, e.g., distinct agg. We scan in a single executor.
            None => Distribution::all_vnodes(dist_key_indices),
        };

        let table_option = TableOption {
            retention_seconds: if table_desc.retention_seconds > 0 {
                Some(table_desc.retention_seconds)
            } else {
                None
            },
        };
        let value_indices = table_desc
            .get_value_indices()
            .iter()
            .map(|&k| k as usize)
            .collect_vec();

        let scan_ranges = seq_scan_node.scan_ranges.clone();
        let scan_ranges = {
            if scan_ranges.is_empty() {
                vec![ScanRange::full()]
            } else {
                scan_ranges
                    .into_iter()
                    .map(|scan_range| ScanRange::new(scan_range, pk_types.iter().cloned()))
                    .try_collect()?
            }
        };

        let epoch = source.epoch;
        let chunk_size = source.context.get_config().developer.batch_chunk_size;
        let metrics = source.context().task_metrics();

        dispatch_state_store!(source.context().state_store(), state_store, {
            let table = StorageTable::new_partial(
                state_store,
                table_id,
                column_descs,
                column_ids,
                order_types,
                pk_indices,
                distribution,
                table_option,
                value_indices,
            );

            Ok(Box::new(RowSeqScanExecutor::new(
                table,
                scan_ranges,
                epoch,
                chunk_size,
                source.plan_node().get_identity().clone(),
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
    #[try_stream(ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let Self {
            chunk_size,
            identity,
            metrics,
            table,
            scan_ranges,
            epoch,
        } = *self;

        let table = Arc::new(table);

        // Create collector.
        let histogram = if let Some(ref metrics) = metrics {
            let mut labels = metrics.task_labels();
            labels.push(identity.as_str());
            Some(
                metrics
                    .metrics
                    .task_row_seq_scan_next_duration
                    .with_label_values(&labels),
            )
        } else {
            None
        };

        let (point_gets, range_scans): (Vec<ScanRange>, Vec<ScanRange>) = scan_ranges
            .into_iter()
            .partition(|x| x.pk_prefix.size() == table.pk_indices().len());

        let mut data_chunk_builder = DataChunkBuilder::new(table.schema().data_types(), chunk_size);
        // Point Get
        for point_get in point_gets {
            let table = table.clone();
            let histogram = histogram.clone();
            if let Some(row) = Self::execute_point_get(table, point_get, epoch, histogram).await? {
                if let Some(chunk) = data_chunk_builder.append_one_row_from_datums(row.values()) {
                    yield chunk;
                }
            }
        }
        if let Some(chunk) = data_chunk_builder.consume_all() {
            yield chunk;
        }

        // Range Scan
        let range_scans = select_all(range_scans.into_iter().map(|range_scan| {
            let table = table.clone();
            let histogram = histogram.clone();
            Box::pin(Self::execute_range(
                table, range_scan, epoch, chunk_size, histogram,
            ))
        }));
        #[for_await]
        for chunk in range_scans {
            yield chunk?;
        }
    }

    async fn execute_point_get(
        table: Arc<StorageTable<S>>,
        scan_range: ScanRange,
        epoch: u64,
        histogram: Option<Histogram>,
    ) -> Result<Option<Row>> {
        let pk_prefix = scan_range.pk_prefix;
        assert!(pk_prefix.size() == table.pk_indices().len());

        let timer = histogram.as_ref().map(|histogram| histogram.start_timer());

        // Point Get.
        let row = table
            .get_row(&pk_prefix, HummockReadEpoch::Committed(epoch))
            .await?;

        if let Some(timer) = timer {
            timer.observe_duration()
        }

        Ok(row)
    }

    #[try_stream(ok = DataChunk, error = RwError)]
    async fn execute_range(
        table: Arc<StorageTable<S>>,
        scan_range: ScanRange,
        epoch: u64,
        chunk_size: usize,
        histogram: Option<Histogram>,
    ) {
        let ScanRange {
            pk_prefix,
            next_col_bounds,
        } = scan_range;

        // Range Scan.
        assert!(pk_prefix.size() < table.pk_indices().len());
        let iter = table
            .batch_iter_with_pk_bounds(
                HummockReadEpoch::Committed(epoch),
                &pk_prefix,
                next_col_bounds,
            )
            .await?;

        pin_mut!(iter);
        loop {
            let timer = histogram.as_ref().map(|histogram| histogram.start_timer());

            let chunk = iter
                .collect_data_chunk(table.schema(), Some(chunk_size))
                .await
                .map_err(RwError::from)?;

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
    use std::collections::Bound::Unbounded;

    use futures::StreamExt;
    use risingwave_common::array::Row;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId, TableOption};
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::batch_table::storage_table::StorageTable;
    use risingwave_storage::table::streaming_table::state_table::StateTable;
    use risingwave_storage::table::Distribution;

    use crate::executor::{Executor, RowSeqScanExecutor, ScanRange};

    #[tokio::test]
    async fn test_row_seq_scan() {
        let state_store = MemoryStateStore::new();
        let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
            ColumnDesc::unnamed(column_ids[2], DataType::Int32),
        ];
        let pk_indices = vec![0_usize, 1_usize];
        let order_types = vec![OrderType::Ascending, OrderType::Descending];
        let mut state = StateTable::new_without_distribution(
            state_store.clone(),
            TableId::from(0x42),
            column_descs.clone(),
            order_types.clone(),
            pk_indices.clone(),
        )
        .await;
        let column_ids_partial = vec![ColumnId::from(1), ColumnId::from(2)];
        let value_indices: Vec<usize> = vec![0, 1, 2];
        let table = StorageTable::new_partial(
            state_store.clone(),
            TableId::from(0x42),
            column_descs.clone(),
            column_ids_partial,
            order_types.clone(),
            pk_indices,
            Distribution::fallback(),
            TableOption::default(),
            value_indices,
        );
        let epoch = EpochPair::new_test_epoch(1);
        state.init_epoch(epoch);
        epoch.inc();

        state.insert(Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]));
        state.insert(Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]));
        state.insert(Row(vec![
            Some(3_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into()),
        ]));

        state.commit_for_test(epoch).await.unwrap();

        let scan_range1 = ScanRange {
            pk_prefix: Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
            next_col_bounds: (Unbounded, Unbounded),
        };

        let scan_range2 = ScanRange {
            pk_prefix: Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            next_col_bounds: (Unbounded, Unbounded),
        };

        let row_seq_scan_exec = RowSeqScanExecutor::new(
            table.clone(),
            vec![scan_range1, scan_range2],
            epoch.curr,
            1024,
            "row_seq_scan_exec".to_string(),
            None,
        );

        let point_get_row_seq_scan_exec = Box::new(row_seq_scan_exec);

        let mut stream = point_get_row_seq_scan_exec.execute();
        let chunk = stream.next().await.unwrap().unwrap();

        assert_eq!(
            chunk.row_at(0).0.to_owned_row(),
            Row(vec![Some(11_i32.into()), Some(111_i32.into())])
        );
        assert_eq!(
            chunk.row_at(1).0.to_owned_row(),
            Row(vec![Some(22_i32.into()), Some(222_i32.into())])
        );

        let full_row_seq_scan_exec = RowSeqScanExecutor::new(
            table,
            vec![ScanRange::full()],
            epoch.curr,
            1024,
            "row_seq_scan_exec".to_string(),
            None,
        );

        let row_seq_scan_exec = Box::new(full_row_seq_scan_exec);

        let mut stream = row_seq_scan_exec.execute();
        let chunk = stream.next().await.unwrap().unwrap();

        assert_eq!(
            chunk.row_at(0).0.to_owned_row(),
            Row(vec![Some(11_i32.into()), Some(111_i32.into())])
        );
        assert_eq!(
            chunk.row_at(1).0.to_owned_row(),
            Row(vec![Some(22_i32.into()), Some(222_i32.into())])
        );
        assert_eq!(
            chunk.row_at(2).0.to_owned_row(),
            Row(vec![Some(33_i32.into()), Some(333_i32.into())])
        );
    }
}
