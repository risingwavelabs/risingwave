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

use risingwave_common::catalog::{ColumnDesc, TableId, TableOption};
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::plan_common::{OrderType as ProstOrderType, StorageTableDesc};
use risingwave_pb::stream_plan::{ChainNode, ChainType};
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::Distribution;

use super::*;
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

        let upstream_indices: Vec<usize> = node
            .upstream_column_indices
            .iter()
            .map(|&i| i as usize)
            .collect();

        // For reporting the progress.
        let progress = stream
            .context
            .register_create_mview_progress(params.actor_context.id);

        // The batch query executor scans on a mapped adhoc mview table, thus we should directly use
        // its schema.
        let schema = snapshot.schema().clone();

        let executor = match node.chain_type() {
            ChainType::Chain => ChainExecutor::new(
                snapshot,
                mview,
                upstream_indices,
                progress,
                schema,
                params.pk_indices,
                false,
            )
            .boxed(),
            ChainType::UpstreamOnly => ChainExecutor::new(
                snapshot,
                mview,
                upstream_indices,
                progress,
                schema,
                params.pk_indices,
                true,
            )
            .boxed(),
            ChainType::Rearrange => RearrangedChainExecutor::new(
                snapshot,
                mview,
                upstream_indices,
                progress,
                schema,
                params.pk_indices,
            )
            .boxed(),
            ChainType::Backfill => {
                let table_desc: &StorageTableDesc = node.get_table_desc()?;
                let table_id = TableId {
                    table_id: table_desc.table_id,
                };

                let order_types = table_desc
                    .pk
                    .iter()
                    .map(|desc| {
                        OrderType::from_prost(&ProstOrderType::from_i32(desc.order_type).unwrap())
                    })
                    .collect_vec();

                let column_descs = table_desc
                    .columns
                    .iter()
                    .map(ColumnDesc::from)
                    .collect_vec();
                let column_ids = column_descs.iter().map(|x| x.column_id).collect_vec();

                // Use indices based on full table instead of streaming executor output.
                let pk_indices = table_desc.pk.iter().map(|k| k.index as usize).collect_vec();

                let dist_key_indices = table_desc
                    .dist_key_indices
                    .iter()
                    .map(|&k| k as usize)
                    .collect_vec();
                let distribution = match params.vnode_bitmap {
                    Some(vnodes) => Distribution {
                        dist_key_indices,
                        vnodes: vnodes.into(),
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
                // TODO: refactor it with from_table_catalog in the future.
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
                    prefix_hint_len,
                );

                BackfillExecutor::new(
                    table,
                    mview,
                    upstream_indices,
                    progress,
                    schema,
                    params.pk_indices,
                    params.env.config().barrier_interval_ms,
                )
                .boxed()
            }
            ChainType::ChainUnspecified => unreachable!(),
        };
        Ok(executor)
    }
}
