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

use futures::future::try_join_all;
use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use prometheus::{exponential_buckets, Histogram, HistogramOpts};
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
use risingwave_pb::batch_plan::{scan_range, ScanRange};
use risingwave_pb::plan_common::{OrderType as ProstOrderType, StorageTableDesc};
use risingwave_storage::table::batch_table::storage_table::{StorageTable, StorageTableIter};
use risingwave_storage::table::{Distribution, TableIter};
use risingwave_storage::{dispatch_state_store, StateStore, StateStoreImpl};

use super::BatchTaskMetrics;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

/// Executor that scans data from row table
pub struct RowSeqScanExecutor<S: StateStore> {
    chunk_size: usize,
    schema: Schema,
    identity: String,

    /// Batch metrics.
    /// None: Local mode don't record mertics.
    metrics: Option<BatchTaskMetrics>,

    scan_types: Vec<ScanType<S>>,
}

pub enum ScanType<S: StateStore> {
    BatchScan(StorageTableIter<S>),
    PointGet(Option<Row>),
}

impl<S: StateStore> RowSeqScanExecutor<S> {
    pub fn new(
        schema: Schema,
        scan_types: Vec<ScanType<S>>,
        chunk_size: usize,
        identity: String,
        metrics: Option<BatchTaskMetrics>,
    ) -> Self {
        Self {
            chunk_size,
            schema,
            identity,
            metrics,
            scan_types,
        }
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
            deserialize_datum(v.as_slice(), &ty).expect("fail to deserialize datum")
        })
        .collect_vec());
    if scan_range.lower_bound.is_none() && scan_range.upper_bound.is_none() {
        return (pk_prefix_value, (Bound::Unbounded, Bound::Unbounded));
    }

    let bound_ty = pk_types.next().unwrap();
    let build_bound = |bound: &scan_range::Bound| -> Bound<Datum> {
        let scalar = ScalarImpl::from_proto_bytes(&bound.value, &bound_ty.to_protobuf()).unwrap();

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
        let pk_len = table_desc.pk.len();
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
        dispatch_state_store!(source.context().try_get_state_store()?, state_store, {
            let metrics = source.context().get_task_metrics();
            let table = StorageTable::new_partial(
                state_store.clone(),
                table_id,
                column_descs,
                column_ids,
                order_types,
                pk_indices,
                distribution,
                table_option,
                value_indices,
            );

            if seq_scan_node.scan_ranges.is_empty() {
                let iter = table
                    .batch_iter(HummockReadEpoch::Committed(source.epoch))
                    .await?;
                return Ok(Box::new(RowSeqScanExecutor::new(
                    table.schema().clone(),
                    vec![ScanType::BatchScan(iter)],
                    RowSeqScanExecutorBuilder::DEFAULT_CHUNK_SIZE,
                    source.plan_node().get_identity().clone(),
                    metrics,
                )));
            }

            let mut futures = vec![];
            for scan_range in &seq_scan_node.scan_ranges {
                let scan_type = async {
                    let pk_types = pk_types.clone();
                    let mut table = table.clone();

                    let (pk_prefix_value, next_col_bounds) =
                        get_scan_bound(scan_range.clone(), pk_types.into_iter());

                    let scan_type =
                        if pk_prefix_value.size() == 0 && is_full_range(&next_col_bounds) {
                            unreachable!()
                        } else if pk_prefix_value.size() == pk_len {
                            let row = {
                                table
                                    .get_row(
                                        &pk_prefix_value,
                                        HummockReadEpoch::Committed(source.epoch),
                                    )
                                    .await?
                            };
                            ScanType::PointGet(row)
                        } else {
                            assert!(pk_prefix_value.size() < pk_len);
                            let iter = table
                                .batch_iter_with_pk_bounds(
                                    HummockReadEpoch::Committed(source.epoch),
                                    &pk_prefix_value,
                                    next_col_bounds,
                                )
                                .await?;
                            ScanType::BatchScan(iter)
                        };

                    Ok(scan_type)
                };
                futures.push(scan_type);
            }

            let scan_types: Result<Vec<ScanType<_>>> = try_join_all(futures).await;

            Ok(Box::new(RowSeqScanExecutor::new(
                table.schema().clone(),
                scan_types?,
                RowSeqScanExecutorBuilder::DEFAULT_CHUNK_SIZE,
                source.plan_node().get_identity().clone(),
                metrics,
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
        let Self {
            chunk_size,
            schema,
            identity,
            metrics,
            scan_types,
        } = *self;

        // create collector
        let histogram = if let Some(ref metrics) = metrics {
            let mut labels = metrics.task_labels();
            labels.insert("executor_id".to_string(), identity);
            let opts = HistogramOpts::new(
                "batch_row_seq_scan_next_duration",
                "Time spent deserializing into a row in cell based table.",
            )
            .buckets(exponential_buckets(0.0001, 2.0, 20).unwrap())
            .const_labels(labels.clone());
            let histogram = Histogram::with_opts(opts).unwrap();
            // change to error if failed to register.
            match metrics.register(Box::new(histogram.clone())) {
                Ok(_) => Some(histogram),
                Err(err) => return Self::return_err(err.into()).boxed(),
            }
        } else {
            None
        };

        let streams = scan_types
            .into_iter()
            .map(|scan_type| {
                Self::do_execute(scan_type, schema.clone(), chunk_size, histogram.clone())
            })
            .collect();
        if let (Some(histogram), Some(metrics)) = (histogram, metrics) {
            metrics.unregister(Box::new(histogram));
        }
        select_all(streams).boxed()
    }
}

impl<S: StateStore> RowSeqScanExecutor<S> {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn return_err(err: RwError) {
        return Err(err);
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(
        scan_type: ScanType<S>,
        schema: Schema,
        chunk_size: usize,
        histogram: Option<Histogram>,
    ) {
        match scan_type {
            ScanType::BatchScan(iter) => {
                pin_mut!(iter);
                loop {
                    // let timer = stats.row_seq_scan_next_duration.start_timer();
                    let timer = histogram.as_ref().map(|histogram| histogram.start_timer());

                    let chunk = iter
                        .collect_data_chunk(&schema, Some(chunk_size))
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
            ScanType::PointGet(row) => {
                if let Some(row) = row {
                    yield DataChunk::from_rows(&[row], &schema.data_types())?;
                }
            }
        }
    }
}
