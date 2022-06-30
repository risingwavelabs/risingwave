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

use futures::pin_mut;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, OrderedColumnDesc, Schema, TableId};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, Datum, ScalarImpl, VIRTUAL_NODE_COUNT};
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{scan_range, ScanRange};
use risingwave_pb::plan_common::CellBasedTableDesc;
use risingwave_storage::table::cell_based_table::{
    BatchDedupPkIter, CellBasedIter, CellBasedTable,
};
use risingwave_storage::table::{Distribution, TableIter};
use risingwave_storage::{dispatch_state_store, Keyspace, StateStore, StateStoreImpl};

use crate::executor::monitor::BatchMetrics;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

/// Executor that scans data from row table
pub struct RowSeqScanExecutor<S: StateStore> {
    primary: bool,
    chunk_size: usize,
    schema: Schema,
    identity: String,
    stats: Arc<BatchMetrics>,
    scan_type: ScanType<S>,
}

pub enum ScanType<S: StateStore> {
    TableScan(BatchDedupPkIter<S>),
    RangeScan(CellBasedIter<S>),
    PointGet(Option<Row>),
}

impl<S: StateStore> RowSeqScanExecutor<S> {
    pub fn new(
        schema: Schema,
        scan_type: ScanType<S>,
        chunk_size: usize,
        primary: bool,
        identity: String,
        stats: Arc<BatchMetrics>,
    ) -> Self {
        Self {
            primary,
            chunk_size,
            schema,
            identity,
            stats,
            scan_type,
        }
    }

    // TODO: Remove this when we support real partition-scan.
    // For shared storage like Hummock, we are using a fake partition-scan now. If `self.primary` is
    // false, we'll ignore this scanning and yield no chunk.
    fn should_ignore(&self) -> bool {
        !self.primary
    }
}

pub struct RowSeqScanExecutorBuilder {}

impl RowSeqScanExecutorBuilder {
    // TODO: decide the chunk size for row seq scan
    pub const DEFAULT_CHUNK_SIZE: usize = 1024;
}

fn is_full_range<T>(bounds: &impl RangeBounds<T>) -> bool {
    matches!(bounds.start_bound(), Bound::Unbounded)
        && matches!(bounds.end_bound(), Bound::Unbounded)
}

fn get_scan_bound(
    scan_range: ScanRange,
    mut pk_types: impl Iterator<Item = DataType>,
) -> (Row, impl RangeBounds<Datum>) {
    let pk_prefix_value = Row(scan_range
        .eq_conds
        .iter()
        .map(|v| {
            let ty = pk_types.next().unwrap();
            let scalar = ScalarImpl::bytes_to_scalar(v, &ty.to_protobuf()).unwrap();
            Some(scalar)
        })
        .collect_vec());
    if scan_range.lower_bound.is_none() && scan_range.upper_bound.is_none() {
        return (pk_prefix_value, (Bound::Unbounded, Bound::Unbounded));
    }

    let bound_ty = pk_types.next().unwrap();
    let build_bound = |bound: &scan_range::Bound| -> Bound<Datum> {
        let scalar = ScalarImpl::bytes_to_scalar(&bound.value, &bound_ty.to_protobuf()).unwrap();

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
    (pk_prefix_value, next_col_bounds)
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for RowSeqScanExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
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

        dbg!(&seq_scan_node);

        let table_desc: &CellBasedTableDesc = seq_scan_node.get_table_desc()?;
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

        let pk_descs: Vec<OrderedColumnDesc> =
            table_desc.order_key.iter().map(|d| d.into()).collect();
        let order_types: Vec<OrderType> = pk_descs.iter().map(|desc| desc.order).collect();

        let scan_range = seq_scan_node.scan_range.as_ref().unwrap();
        let (pk_prefix_value, next_col_bounds) = get_scan_bound(
            scan_range.clone(),
            pk_descs
                .iter()
                .map(|desc| desc.column_desc.data_type.clone()),
        );

        let pk_indices = table_desc
            .pk_indices
            .iter()
            .map(|&k| k as usize)
            .collect_vec();

        let dist_key_indices = table_desc
            .dist_key_indices
            .iter()
            .map(|&k| k as usize)
            .collect_vec();
        let vnodes = Bitmap::all_high_bits(VIRTUAL_NODE_COUNT); // TODO: use vnodes from scheduler to parallelize scan
        let distribution = Distribution {
            vnodes: vnodes.into(),
            dist_key_indices,
        };

        dispatch_state_store!(source.context().try_get_state_store()?, state_store, {
            let keyspace = Keyspace::table_root(state_store.clone(), &table_id);
            let batch_stats = source.context().stats();
            let table = CellBasedTable::new_partial(
                keyspace.clone(),
                column_descs,
                column_ids,
                order_types,
                pk_indices,
                distribution,
            );

            let scan_type = if pk_prefix_value.size() == 0 && is_full_range(&next_col_bounds) {
                let iter = table.batch_dedup_pk_iter(source.epoch, &pk_descs).await?;
                ScanType::TableScan(iter)
            } else if pk_prefix_value.size() == pk_descs.len() {
                keyspace.state_store().wait_epoch(source.epoch).await?;
                let row = table.get_row(&pk_prefix_value, source.epoch).await?;
                ScanType::PointGet(row)
            } else {
                assert!(pk_prefix_value.size() < pk_descs.len());

                let iter = if is_full_range(&next_col_bounds) {
                    table
                        .batch_iter_with_pk_prefix(source.epoch, pk_prefix_value)
                        .await?
                } else {
                    table
                        .batch_iter_with_pk_bounds(source.epoch, pk_prefix_value, next_col_bounds)
                        .await?
                };
                ScanType::RangeScan(iter)
            };

            Ok(Box::new(RowSeqScanExecutor::new(
                table.schema().clone(),
                scan_type,
                RowSeqScanExecutorBuilder::DEFAULT_CHUNK_SIZE,
                source.task_id.task_id == 0,
                source.plan_node().get_identity().clone(),
                batch_stats,
            )))
        })
    }
}

impl<S: StateStore> Executor for RowSeqScanExecutor<S> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl<S: StateStore> RowSeqScanExecutor<S> {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        if !self.should_ignore() {
            match self.scan_type {
                ScanType::TableScan(iter) => {
                    pin_mut!(iter);
                    loop {
                        let timer = self.stats.row_seq_scan_next_duration.start_timer();

                        let chunk = iter
                            .collect_data_chunk(&self.schema, Some(self.chunk_size))
                            .await
                            .map_err(RwError::from)?;
                        timer.observe_duration();

                        if let Some(chunk) = chunk {
                            yield chunk
                        } else {
                            break;
                        }
                    }
                }
                ScanType::RangeScan(iter) => {
                    pin_mut!(iter);
                    loop {
                        // TODO: same as TableScan except iter type
                        let timer = self.stats.row_seq_scan_next_duration.start_timer();

                        let chunk = iter
                            .collect_data_chunk(&self.schema, Some(self.chunk_size))
                            .await
                            .map_err(RwError::from)?;
                        timer.observe_duration();

                        if let Some(chunk) = chunk {
                            yield chunk
                        } else {
                            break;
                        }
                    }
                }
                ScanType::PointGet(row) => {
                    if let Some(row) = row {
                        yield DataChunk::from_rows(&[row], &self.schema.data_types())?;
                    }
                }
            }
        }
    }
}
