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

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema, TableId, TableOption};
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::source::external::{CdcTableType, SchemaTableName};
use risingwave_pb::plan_common::{ExternalTableDesc, StorageTableDesc};
use risingwave_pb::stream_plan::{ChainNode, ChainType};
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::Distribution;

use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::external::ExternalStorageTable;
use crate::executor::{
    BackfillExecutor, CdcBackfillExecutor, ChainExecutor, FlowControlExecutor,
    RearrangedChainExecutor, SourceStateTableHandler,
};

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

        let schema = if matches!(node.chain_type(), ChainType::Backfill) {
            Schema::new(
                output_indices
                    .iter()
                    .map(|i| snapshot.schema().fields()[*i].clone())
                    .collect_vec(),
            )
        } else if matches!(node.chain_type(), ChainType::CdcBackfill) {
            let table_desc: &ExternalTableDesc = node.get_cdc_table_desc()?;
            let schema = Schema::new(table_desc.columns.iter().map(Into::into).collect());
            assert_eq!(output_indices, (0..schema.len()).collect_vec());
            schema
        } else {
            // For `Chain`s other than `Backfill`, there should be no extra mapping required. We can
            // directly output the columns received from the upstream or snapshot.
            let all_indices = (0..snapshot.schema().len()).collect_vec();
            assert_eq!(output_indices, all_indices);
            snapshot.schema().clone()
        };

        let executor = match node.chain_type() {
            ChainType::Chain | ChainType::UpstreamOnly => {
                let upstream_only = matches!(node.chain_type(), ChainType::UpstreamOnly);
                ChainExecutor::new(
                    snapshot,
                    upstream,
                    progress,
                    schema,
                    params.pk_indices,
                    upstream_only,
                )
                .boxed()
            }
            ChainType::Rearrange => RearrangedChainExecutor::new(
                snapshot,
                upstream,
                progress,
                schema,
                params.pk_indices,
            )
            .boxed(),
            ChainType::CdcBackfill => {
                let table_desc: &ExternalTableDesc = node.get_cdc_table_desc()?;
                let properties: HashMap<String, String> = table_desc
                    .connect_properties
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let table_type = CdcTableType::from_properties(&properties);
                let table_reader =
                    table_type.create_table_reader(properties.clone(), schema.clone())?;

                let order_types = table_desc
                    .pk
                    .iter()
                    .map(|desc| OrderType::from_protobuf(desc.get_order_type().unwrap()))
                    .collect_vec();

                let pk_indices = table_desc
                    .pk
                    .iter()
                    .map(|k| k.column_index as usize)
                    .collect_vec();

                let schema_table_name = SchemaTableName::from_properties(&properties);
                let external_table = ExternalStorageTable::new(
                    TableId::new(table_desc.table_id),
                    schema_table_name,
                    table_reader,
                    schema.clone(),
                    order_types,
                    pk_indices.clone(),
                    (0..table_desc.columns.len()).collect_vec(),
                );

                let source_state_handler = SourceStateTableHandler::from_table_catalog(
                    node.get_state_table().as_ref().unwrap(),
                    state_store.clone(),
                )
                .await;
                CdcBackfillExecutor::new(
                    params.actor_context.clone(),
                    external_table,
                    upstream,
                    (0..table_desc.columns.len()).collect_vec(), /* eliminate the last column (_rw_offset) */
                    Some(progress),
                    schema.clone(),
                    pk_indices,
                    params.executor_stats,
                    source_state_handler,
                    true,
                    params.env.config().developer.chunk_size,
                ).boxed()
            }
            ChainType::Backfill => {
                let table_desc: &StorageTableDesc = node
                    .get_table_desc()
                    .map_err(|err| anyhow!("chain: table_desc not found! {:?}", err))?;
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
                let state_table = if let Ok(table) = node.get_state_table() {
                    Some(StateTable::from_table_catalog(table, state_store, vnodes).await)
                } else {
                    None
                };

                BackfillExecutor::new(
                    upstream_table,
                    upstream,
                    state_table,
                    output_indices,
                    progress,
                    schema,
                    params.pk_indices,
                    stream.streaming_metrics.clone(),
                    params.env.config().developer.chunk_size,
                    params.executor_id,
                )
                .boxed()
            }
            ChainType::ChainUnspecified => unreachable!(),
        };
        let rate_limit = node.get_rate_limit().cloned().ok();
        Ok(FlowControlExecutor::new(executor, rate_limit).boxed())
    }
}
