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

use risingwave_common::catalog::{ColumnDesc, TableId, TableOption};
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::plan_common::{JoinType as JoinTypeProto, StorageTableDesc};
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::Distribution;

use super::*;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ActorContextRef, JoinType, PkIndices, TemporalJoinExecutor};
use crate::task::AtomicU64Ref;

pub struct TemporalJoinExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for TemporalJoinExecutorBuilder {
    type Node = TemporalJoinNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let table = {
            let table_desc: &StorageTableDesc = node.get_table_desc()?;
            let table_id = TableId {
                table_id: table_desc.table_id,
            };

            let order_types = table_desc
                .pk
                .iter()
                .map(|desc| OrderType::from_protobuf(&desc.get_order_type().unwrap().direction()))
                .collect_vec();

            let column_descs = table_desc
                .columns
                .iter()
                .map(ColumnDesc::from)
                .collect_vec();
            let column_ids = column_descs.iter().map(|x| x.column_id).collect_vec();

            // Use indices based on full table instead of streaming executor output.
            let pk_indices = table_desc
                .pk
                .iter()
                .map(|k| k.column_index as usize)
                .collect_vec();

            let dist_key_indices = table_desc
                .dist_key_indices
                .iter()
                .map(|&k| k as usize)
                .collect_vec();
            let distribution = match params.vnode_bitmap.clone() {
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

            StorageTable::new_partial(
                store,
                table_id,
                column_descs,
                column_ids,
                order_types,
                pk_indices,
                distribution,
                table_option,
                value_indices,
                prefix_hint_len,
                table_desc.versioned,
            )
        };

        let [source_l, source_r]: [_; 2] = params.input.try_into().unwrap();

        let left_join_keys = node
            .get_left_key()
            .iter()
            .map(|key| *key as usize)
            .collect_vec();

        let right_join_keys = node
            .get_right_key()
            .iter()
            .map(|key| *key as usize)
            .collect_vec();

        let null_safe = node.get_null_safe().to_vec();
        let output_indices = node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect_vec();

        let dispatcher_args = TemporalJoinExecutorDispatcherArgs {
            ctx: params.actor_context,
            left: source_l,
            right: source_r,
            right_table: table,
            left_join_keys,
            right_join_keys,
            null_safe,
            pk_indices: params.pk_indices,
            output_indices,
            executor_id: params.executor_id,
            watermark_epoch: stream.get_watermark_epoch(),
            chunk_size: params.env.config().developer.stream_chunk_size,
            metrics: params.executor_stats,
            join_type_proto: node.get_join_type()?,
        };

        dispatcher_args.dispatch()
    }
}

struct TemporalJoinExecutorDispatcherArgs<S: StateStore> {
    ctx: ActorContextRef,
    left: BoxedExecutor,
    right: BoxedExecutor,
    right_table: StorageTable<S>,
    left_join_keys: Vec<usize>,
    right_join_keys: Vec<usize>,
    null_safe: Vec<bool>,
    pk_indices: PkIndices,
    output_indices: Vec<usize>,
    executor_id: u64,
    watermark_epoch: AtomicU64Ref,
    chunk_size: usize,
    metrics: Arc<StreamingMetrics>,
    join_type_proto: JoinTypeProto,
}

impl<S: StateStore> TemporalJoinExecutorDispatcherArgs<S> {
    pub fn dispatch(self) -> StreamResult<BoxedExecutor> {
        macro_rules! build {
            ($join_type:ident) => {
                Ok(Box::new(
                    TemporalJoinExecutor::<S, { JoinType::$join_type }>::new(
                        self.ctx,
                        self.left,
                        self.right,
                        self.right_table,
                        self.left_join_keys,
                        self.right_join_keys,
                        self.null_safe,
                        self.pk_indices,
                        self.output_indices,
                        self.executor_id,
                        self.watermark_epoch,
                        self.metrics,
                        self.chunk_size,
                    ),
                ))
            };
        }
        match self.join_type_proto {
            JoinTypeProto::Inner => build!(Inner),
            JoinTypeProto::LeftOuter => build!(LeftOuter),
            _ => unreachable!(),
        }
    }
}
