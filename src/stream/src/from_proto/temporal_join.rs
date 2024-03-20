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

use std::sync::Arc;

use risingwave_common::catalog::ColumnId;
use risingwave_common::hash::{HashKey, HashKeyDispatcher};
use risingwave_common::types::DataType;
use risingwave_expr::expr::{build_non_strict_from_prost, NonStrictExpression};
use risingwave_pb::plan_common::{JoinType as JoinTypeProto, StorageTableDesc};
use risingwave_storage::table::batch_table::storage_table::StorageTable;

use super::*;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ActorContextRef, JoinType, TemporalJoinExecutor};
use crate::task::AtomicU64Ref;

pub struct TemporalJoinExecutorBuilder;

impl ExecutorBuilder for TemporalJoinExecutorBuilder {
    type Node = TemporalJoinNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let table_desc: &StorageTableDesc = node.get_table_desc()?;
        let table = {
            let column_ids = table_desc
                .columns
                .iter()
                .map(|x| ColumnId::new(x.column_id))
                .collect_vec();

            StorageTable::new_partial(
                store,
                column_ids,
                params.vnode_bitmap.map(Into::into),
                table_desc,
            )
        };

        let table_stream_key_indices = table_desc
            .stream_key
            .iter()
            .map(|&k| k as usize)
            .collect_vec();

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

        let condition = match node.get_condition() {
            Ok(cond_prost) => Some(build_non_strict_from_prost(
                cond_prost,
                params.eval_error_report,
            )?),
            Err(_) => None,
        };

        let table_output_indices = node
            .get_table_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect_vec();

        let output_indices = node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect_vec();

        let join_key_data_types = left_join_keys
            .iter()
            .map(|idx| source_l.schema().fields[*idx].data_type())
            .collect_vec();

        let dispatcher_args = TemporalJoinExecutorDispatcherArgs {
            ctx: params.actor_context,
            info: params.info.clone(),
            left: source_l,
            right: source_r,
            right_table: table,
            left_join_keys,
            right_join_keys,
            null_safe,
            condition,
            output_indices,
            table_output_indices,
            table_stream_key_indices,
            watermark_epoch: params.watermark_epoch,
            chunk_size: params.env.config().developer.chunk_size,
            metrics: params.executor_stats,
            join_type_proto: node.get_join_type()?,
            join_key_data_types,
        };

        Ok((params.info, dispatcher_args.dispatch()?).into())
    }
}

struct TemporalJoinExecutorDispatcherArgs<S: StateStore> {
    ctx: ActorContextRef,
    info: ExecutorInfo,
    left: Executor,
    right: Executor,
    right_table: StorageTable<S>,
    left_join_keys: Vec<usize>,
    right_join_keys: Vec<usize>,
    null_safe: Vec<bool>,
    condition: Option<NonStrictExpression>,
    output_indices: Vec<usize>,
    table_output_indices: Vec<usize>,
    table_stream_key_indices: Vec<usize>,
    watermark_epoch: AtomicU64Ref,
    chunk_size: usize,
    metrics: Arc<StreamingMetrics>,
    join_type_proto: JoinTypeProto,
    join_key_data_types: Vec<DataType>,
}

impl<S: StateStore> HashKeyDispatcher for TemporalJoinExecutorDispatcherArgs<S> {
    type Output = StreamResult<Box<dyn Execute>>;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        /// This macro helps to fill the const generic type parameter.
        macro_rules! build {
            ($join_type:ident) => {
                Ok(Box::new(TemporalJoinExecutor::<
                    K,
                    S,
                    { JoinType::$join_type },
                >::new(
                    self.ctx,
                    self.info,
                    self.left,
                    self.right,
                    self.right_table,
                    self.left_join_keys,
                    self.right_join_keys,
                    self.null_safe,
                    self.condition,
                    self.output_indices,
                    self.table_output_indices,
                    self.table_stream_key_indices,
                    self.watermark_epoch,
                    self.metrics,
                    self.chunk_size,
                    self.join_key_data_types,
                )))
            };
        }
        match self.join_type_proto {
            JoinTypeProto::Inner => build!(Inner),
            JoinTypeProto::LeftOuter => build!(LeftOuter),
            _ => unreachable!(),
        }
    }

    fn data_types(&self) -> &[DataType] {
        &self.join_key_data_types
    }
}
