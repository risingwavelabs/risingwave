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
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_pb::stream_plan::{ChainNode, ChainType};
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::Distribution;

use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::{BackfillExecutor, ChainExecutor, RearrangedChainExecutor};

pub struct ChainExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for ChainExecutorBuilder {
    type Node = ChainNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        state_store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [mview, snapshot]: [_; 2] = params.input.try_into().unwrap();

        // For reporting the progress.
        let progress = stream
            .context
            .register_create_mview_progress(params.actor_context.id);

        // The batch query executor scans on a mapped adhoc mview table, thus we should directly use
        // its schema.
        let schema = snapshot.schema().clone();

        let output_indices = node
            .output_indices
            .iter()
            .map(|&i| i as usize)
            .collect_vec();

        // For `Chain`s other than `Backfill`, there should be no extra mapping required. We can
        // directly output the columns received from the upstream or snapshot.
        if !matches!(node.chain_type(), ChainType::Backfill) {
            let all_indices = (0..schema.len()).collect_vec();
            assert_eq!(output_indices, all_indices);
        }

        let executor = match node.chain_type() {
            ChainType::Chain | ChainType::UpstreamOnly => {
                let upstream_only = matches!(node.chain_type(), ChainType::UpstreamOnly);
                ChainExecutor::new(
                    snapshot,
                    mview,
                    progress,
                    schema,
                    params.pk_indices,
                    upstream_only,
                )
                .boxed()
            }
            ChainType::Rearrange => {
                RearrangedChainExecutor::new(snapshot, mview, progress, schema, params.pk_indices)
                    .boxed()
            }
            ChainType::Backfill => {
                let table_desc: &StorageTableDesc = node.get_table_desc()?;
                let table_id = TableId {
                    table_id: table_desc.table_id,
                };

                let order_types = table_desc
                    .pk
                    .iter()
                    .map(|desc| OrderType::from_protobuf(desc.get_order_type().unwrap()))
                    .collect_vec();

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

                // Use indices based on full table instead of streaming executor output.
                let pk_indices = table_desc
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
                // TODO: refactor it with from_table_catalog in the future.
                let upstream_table = StorageTable::new_partial(
                    state_store.clone(),
                    table_id,
                    column_descs,
                    column_ids,
                    order_types,
                    pk_indices,
                    distribution,
                    table_option,
                    value_indices,
                    prefix_hint_len,
                    versioned,
                );
                let state_table = StateTable::from_table_catalog(
                    node.get_state_table().unwrap(),
                    state_store,
                    vnodes,
                )
                .await;

                BackfillExecutor::new(
                    upstream_table,
                    mview,
                    state_table,
                    output_indices,
                    progress,
                    schema,
                    params.pk_indices,
                    stream.streaming_metrics.clone(),
                )
                .boxed()
            }
            ChainType::ChainUnspecified => unreachable!(),
        };
        Ok(executor)
    }
}
