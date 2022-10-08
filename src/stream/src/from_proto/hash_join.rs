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

use std::sync::Arc;

use risingwave_common::hash::{HashKey, HashKeyDispatcher};
use risingwave_common::types::DataType;
use risingwave_expr::expr::{build_from_prost, BoxedExpression};
use risingwave_pb::plan_common::JoinType as JoinTypeProto;
use risingwave_storage::table::streaming_table::state_table::StateTable;

use super::*;
use crate::cache::LruManagerRef;
use crate::executor::hash_join::*;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ActorContextRef, PkIndices};

pub struct HashJoinExecutorBuilder;

impl ExecutorBuilder for HashJoinExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::HashJoin)?;
        let is_append_only = node.is_append_only;
        let vnodes = Arc::new(params.vnode_bitmap.expect("vnodes not set for hash join"));

        let [source_l, source_r]: [_; 2] = params.input.try_into().unwrap();

        let table_l = node.get_left_table()?;
        let degree_table_l = node.get_left_degree_table()?;

        let table_r = node.get_right_table()?;
        let degree_table_r = node.get_right_degree_table()?;

        let params_l = JoinParams::new(
            node.get_left_key()
                .iter()
                .map(|key| *key as usize)
                .collect_vec(),
            table_l
                .distribution_key
                .iter()
                .map(|key| *key as usize)
                .collect_vec(),
        );
        let params_r = JoinParams::new(
            node.get_right_key()
                .iter()
                .map(|key| *key as usize)
                .collect_vec(),
            table_r
                .distribution_key
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

        let condition = match node.get_condition() {
            Ok(cond_prost) => Some(build_from_prost(cond_prost)?),
            Err(_) => None,
        };
        trace!("Join non-equi condition: {:?}", condition);

        let join_key_data_types = params_l
            .join_key_indices
            .iter()
            .map(|idx| source_l.schema().fields[*idx].data_type())
            .collect_vec();

        let state_table_l =
            StateTable::from_table_catalog(table_l, store.clone(), Some(vnodes.clone()));
        let degree_state_table_l =
            StateTable::from_table_catalog(degree_table_l, store.clone(), Some(vnodes.clone()));

        let state_table_r =
            StateTable::from_table_catalog(table_r, store.clone(), Some(vnodes.clone()));
        let degree_state_table_r =
            StateTable::from_table_catalog(degree_table_r, store, Some(vnodes));

        let args = HashJoinExecutorDispatcherArgs {
            ctx: params.actor_context,
            source_l,
            source_r,
            params_l,
            params_r,
            null_safe,
            pk_indices: params.pk_indices,
            output_indices,
            executor_id: params.executor_id,
            cond: condition,
            op_info: params.op_info,
            cache_size: stream.config.developer.unsafe_stream_join_cache_size,
            state_table_l,
            degree_state_table_l,
            state_table_r,
            degree_state_table_r,
            lru_manager: stream.context.lru_manager.clone(),
            is_append_only,
            metrics: params.executor_stats,
            join_type_proto: node.get_join_type()?,
            join_key_data_types,
        };

        args.dispatch()
    }
}

struct HashJoinExecutorDispatcherArgs<S: StateStore> {
    ctx: ActorContextRef,
    source_l: Box<dyn Executor>,
    source_r: Box<dyn Executor>,
    params_l: JoinParams,
    params_r: JoinParams,
    null_safe: Vec<bool>,
    pk_indices: PkIndices,
    output_indices: Vec<usize>,
    executor_id: u64,
    cond: Option<BoxedExpression>,
    op_info: String,
    cache_size: usize,
    state_table_l: StateTable<S>,
    degree_state_table_l: StateTable<S>,
    state_table_r: StateTable<S>,
    degree_state_table_r: StateTable<S>,
    lru_manager: Option<LruManagerRef>,
    is_append_only: bool,
    metrics: Arc<StreamingMetrics>,
    join_type_proto: JoinTypeProto,
    join_key_data_types: Vec<DataType>,
}

impl<S: StateStore> HashKeyDispatcher for HashJoinExecutorDispatcherArgs<S> {
    type Output = StreamResult<BoxedExecutor>;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        /// This macro helps to fill the const generic type parameter.
        macro_rules! build {
            ($join_type:ident) => {
                Ok(Box::new(
                    HashJoinExecutor::<K, S, { JoinType::$join_type }>::new(
                        self.ctx,
                        self.source_l,
                        self.source_r,
                        self.params_l,
                        self.params_r,
                        self.null_safe,
                        self.pk_indices,
                        self.output_indices,
                        self.executor_id,
                        self.cond,
                        self.op_info,
                        self.cache_size,
                        self.state_table_l,
                        self.degree_state_table_l,
                        self.state_table_r,
                        self.degree_state_table_r,
                        self.lru_manager,
                        self.is_append_only,
                        self.metrics,
                    ),
                ))
            };
        }
        match self.join_type_proto {
            JoinTypeProto::Unspecified => unreachable!(),
            JoinTypeProto::Inner => build!(Inner),
            JoinTypeProto::LeftOuter => build!(LeftOuter),
            JoinTypeProto::RightOuter => build!(RightOuter),
            JoinTypeProto::FullOuter => build!(FullOuter),
            JoinTypeProto::LeftSemi => build!(LeftSemi),
            JoinTypeProto::LeftAnti => build!(LeftAnti),
            JoinTypeProto::RightSemi => build!(RightSemi),
            JoinTypeProto::RightAnti => build!(RightAnti),
        }
    }

    fn data_types(&self) -> &[DataType] {
        &self.join_key_data_types
    }
}
