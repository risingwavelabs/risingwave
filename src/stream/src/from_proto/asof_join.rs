// Copyright 2025 RisingWave Labs
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

use risingwave_common::hash::{HashKey, HashKeyDispatcher};
use risingwave_common::types::DataType;
use risingwave_pb::plan_common::AsOfJoinType as JoinTypeProto;
use risingwave_pb::stream_plan::AsOfJoinNode;

use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::asof_join::*;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ActorContextRef, AsOfDesc, AsOfJoinType, JoinType};
use crate::task::AtomicU64Ref;

pub struct AsOfJoinExecutorBuilder;

impl ExecutorBuilder for AsOfJoinExecutorBuilder {
    type Node = AsOfJoinNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        // This assert is to make sure AsOf join can use `JoinChunkBuilder` as Hash join.
        assert_eq!(AsOfJoinType::Inner, JoinType::Inner);
        assert_eq!(AsOfJoinType::LeftOuter, JoinType::LeftOuter);
        let vnodes = Arc::new(params.vnode_bitmap.expect("vnodes not set for AsOf join"));

        let [source_l, source_r]: [_; 2] = params.input.try_into().unwrap();

        let table_l = node.get_left_table()?;
        let table_r = node.get_right_table()?;

        let params_l = JoinParams::new(
            node.get_left_key()
                .iter()
                .map(|key| *key as usize)
                .collect_vec(),
            node.get_left_deduped_input_pk_indices()
                .iter()
                .map(|key| *key as usize)
                .collect_vec(),
        );
        let params_r = JoinParams::new(
            node.get_right_key()
                .iter()
                .map(|key| *key as usize)
                .collect_vec(),
            node.get_right_deduped_input_pk_indices()
                .iter()
                .map(|key| *key as usize)
                .collect_vec(),
        );
        let null_safe = node.get_null_safe().to_vec();
        let output_indices = node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect_vec();

        let join_key_data_types = params_l
            .join_key_indices
            .iter()
            .map(|idx| source_l.schema().fields[*idx].data_type())
            .collect_vec();

        let state_table_l =
            StateTable::from_table_catalog(table_l, store.clone(), Some(vnodes.clone())).await;

        let state_table_r =
            StateTable::from_table_catalog(table_r, store.clone(), Some(vnodes.clone())).await;

        let join_type_proto = node.get_join_type()?;
        let as_of_desc_proto = node.get_asof_desc()?;
        let asof_desc = AsOfDesc::from_protobuf(as_of_desc_proto)?;

        let args = AsOfJoinExecutorDispatcherArgs {
            ctx: params.actor_context,
            info: params.info.clone(),
            source_l,
            source_r,
            params_l,
            params_r,
            null_safe,
            output_indices,
            state_table_l,
            state_table_r,
            lru_manager: params.watermark_epoch,
            metrics: params.executor_stats,
            join_type_proto,
            join_key_data_types,
            chunk_size: params.env.config().developer.chunk_size,
            high_join_amplification_threshold: params
                .env
                .config()
                .developer
                .high_join_amplification_threshold,
            asof_desc,
        };

        let exec = args.dispatch()?;
        Ok((params.info, exec).into())
    }
}

struct AsOfJoinExecutorDispatcherArgs<S: StateStore> {
    ctx: ActorContextRef,
    info: ExecutorInfo,
    source_l: Executor,
    source_r: Executor,
    params_l: JoinParams,
    params_r: JoinParams,
    null_safe: Vec<bool>,
    output_indices: Vec<usize>,
    state_table_l: StateTable<S>,
    state_table_r: StateTable<S>,
    lru_manager: AtomicU64Ref,
    metrics: Arc<StreamingMetrics>,
    join_type_proto: JoinTypeProto,
    join_key_data_types: Vec<DataType>,
    chunk_size: usize,
    high_join_amplification_threshold: usize,
    asof_desc: AsOfDesc,
}

impl<S: StateStore> HashKeyDispatcher for AsOfJoinExecutorDispatcherArgs<S> {
    type Output = StreamResult<Box<dyn Execute>>;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        /// This macro helps to fill the const generic type parameter.
        macro_rules! build {
            ($join_type:ident) => {
                Ok(AsOfJoinExecutor::<K, S, { AsOfJoinType::$join_type }>::new(
                    self.ctx,
                    self.info,
                    self.source_l,
                    self.source_r,
                    self.params_l,
                    self.params_r,
                    self.null_safe,
                    self.output_indices,
                    self.state_table_l,
                    self.state_table_r,
                    self.lru_manager,
                    self.metrics,
                    self.chunk_size,
                    self.high_join_amplification_threshold,
                    self.asof_desc,
                )
                .boxed())
            };
        }
        match self.join_type_proto {
            JoinTypeProto::Unspecified => unreachable!(),
            JoinTypeProto::Inner => build!(Inner),
            JoinTypeProto::LeftOuter => build!(LeftOuter),
        }
    }

    fn data_types(&self) -> &[DataType] {
        &self.join_key_data_types
    }
}
