// Copyright 2023 RisingWave Labs
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

use std::sync::Arc;

use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId, TableOption};
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::column_aware_row_encoding::ColumnAwareSerde;
use risingwave_common::util::value_encoding::BasicSerde;
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_pb::stream_plan::{StreamScanNode, StreamScanType};
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::Distribution;

use super::*;
use crate::common::table::state_table::{ReplicatedStateTable, StateTable};
use crate::executor::{
    ArrangementBackfillExecutor, BackfillExecutor, ChainExecutor, FlowControlExecutor,
    RearrangedChainExecutor,
};

pub struct StreamScanExecutorBuilder;

impl ExecutorBuilder for StreamScanExecutorBuilder {
    type Node = StreamScanNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        state_store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [upstream, snapshot]: [_; 2] = params.input.try_into().unwrap();
        // For reporting the progress.
        let progress = stream
            .context
            .register_create_mview_progress(params.actor_context.id);

        let output_indices = node
            .output_indices
            .iter()
            .map(|&i| i as usize)
            .collect_vec();

        let executor = match node.stream_scan_type() {
            StreamScanType::Chain | StreamScanType::UpstreamOnly => {
                let upstream_only = matches!(node.stream_scan_type(), StreamScanType::UpstreamOnly);
                ChainExecutor::new(params.info, snapshot, upstream, progress, upstream_only).boxed()
            }
            StreamScanType::Rearrange => {
                RearrangedChainExecutor::new(params.info, snapshot, upstream, progress).boxed()
            }

            StreamScanType::Backfill => {
                let table_desc: &StorageTableDesc = node.get_table_desc()?;
                let table_id = TableId {
                    table_id: table_desc.table_id,
                };

                let column_descs = table_desc
                    .columns
                    .iter()
                    .map(ColumnDesc::from)
                    .collect_vec();
                let column_ids = node
                    .upstream_column_ids
                    .iter()
                    .map(ColumnId::from)
                    .collect_vec();

                let table_pk_order_types = table_desc
                    .pk
                    .iter()
                    .map(|desc| OrderType::from_protobuf(desc.get_order_type().unwrap()))
                    .collect_vec();
                // Use indices based on full table instead of streaming executor output.
                let table_pk_indices = table_desc
                    .pk
                    .iter()
                    .map(|k| k.column_index as usize)
                    .collect_vec();

                let dist_key_in_pk_indices = table_desc
                    .dist_key_in_pk_indices
                    .iter()
                    .map(|&k| k as usize)
                    .collect_vec();

                let vnodes = params.vnode_bitmap.map(Arc::new);
                let distribution = match &vnodes {
                    Some(vnodes) => Distribution {
                        dist_key_in_pk_indices,
                        vnodes: vnodes.clone(),
                    },
                    None => Distribution::fallback(),
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
                let prefix_hint_len = table_desc.get_read_prefix_len_hint() as usize;
                let versioned = table_desc.versioned;

                let state_table = if let Ok(table) = node.get_state_table() {
                    Some(
                        StateTable::from_table_catalog(table, state_store.clone(), vnodes.clone())
                            .await,
                    )
                } else {
                    None
                };

                // TODO: refactor it with from_table_catalog in the future.
                let upstream_table = StorageTable::new_partial(
                    state_store.clone(),
                    table_id,
                    column_descs,
                    column_ids,
                    table_pk_order_types,
                    table_pk_indices,
                    distribution,
                    table_option,
                    value_indices,
                    prefix_hint_len,
                    versioned,
                );

                BackfillExecutor::new(
                    params.info,
                    upstream_table,
                    upstream,
                    state_table,
                    output_indices,
                    progress,
                    stream.streaming_metrics.clone(),
                    params.env.config().developer.chunk_size,
                    node.rate_limit.map(|x| x as _),
                )
                .boxed()
            }
            StreamScanType::ArrangementBackfill => {
                let column_ids = node
                    .upstream_column_ids
                    .iter()
                    .map(ColumnId::from)
                    .collect_vec();

                let vnodes = params.vnode_bitmap.map(Arc::new);

                let state_table = node.get_state_table().unwrap();
                let state_table = StateTable::from_table_catalog(
                    state_table,
                    state_store.clone(),
                    vnodes.clone(),
                )
                .await;

                let upstream_table = node.get_arrangement_table().unwrap();
                let versioned = upstream_table.get_version().is_ok();

                macro_rules! new_executor {
                    ($SD:ident) => {{
                        let upstream_table =
                            ReplicatedStateTable::<_, $SD>::from_table_catalog_with_output_column_ids(
                                upstream_table,
                                state_store.clone(),
                                vnodes,
                                column_ids,
                            )
                            .await;
                        ArrangementBackfillExecutor::<_, $SD>::new(
                            params.info,
                            upstream_table,
                            upstream,
                            state_table,
                            output_indices,
                            progress,
                            stream.streaming_metrics.clone(),
                            params.env.config().developer.chunk_size,
                            node.rate_limit.map(|x| x as _),
                        )
                        .boxed()
                    }};
                }
                if versioned {
                    new_executor!(ColumnAwareSerde)
                } else {
                    new_executor!(BasicSerde)
                }
            }
            StreamScanType::Unspecified => unreachable!(),
        };
        Ok(FlowControlExecutor::new(
            executor,
            params.actor_context,
            node.rate_limit.map(|x| x as _),
        )
        .boxed())
    }
}
