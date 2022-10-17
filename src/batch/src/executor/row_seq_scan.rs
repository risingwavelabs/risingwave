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
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_common::util::select_all;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::deserialize_datum;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{scan_range, ScanRange as ProstScanRange};
use risingwave_pb::common::BatchQueryEpoch;
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
    epoch: BatchQueryEpoch,
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
            let scalar =
                ScalarImpl::from_proto_bytes(&bound.value, &bound_ty.to_protobuf()).unwrap();

            let datum = Some(scalar);
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
        epoch: BatchQueryEpoch,
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

        let epoch = source.epoch.clone();
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

        // Scan all ranges concurrently.
        let select_all = select_all(scan_ranges.into_iter().map(|scan_range| {
            let table = table.clone();
            let histogram = histogram.clone();
            Box::pin(Self::execute_range(
                table,
                scan_range,
                epoch.clone(),
                chunk_size,
                histogram,
            ))
        }));

        #[for_await]
        for scan_result in select_all {
            yield scan_result?;
        }
    }

    #[try_stream(ok = DataChunk, error = RwError)]
    async fn execute_range(
        table: Arc<StorageTable<S>>,
        scan_range: ScanRange,
        epoch: BatchQueryEpoch,
        chunk_size: usize,
        histogram: Option<Histogram>,
    ) {
        let ScanRange {
            pk_prefix,
            next_col_bounds,
        } = scan_range;

        // Resolve the scan range to scan type.
        if pk_prefix.size() == table.pk_indices().len() {
            // Point Get.
            let row = table
                .get_row(
                    &pk_prefix,
                    HummockReadEpoch::from_batch_query_epoch(epoch.clone()),
                )
                .await?;

            if let Some(row) = row {
                yield DataChunk::from_rows(&[row], &table.schema().data_types());
            }
        } else {
            // Range Scan.
            assert!(pk_prefix.size() < table.pk_indices().len());
            let iter = table
                .batch_iter_with_pk_bounds(
                    HummockReadEpoch::from_batch_query_epoch(epoch.clone()),
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
        };
    }
}
