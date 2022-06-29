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

use std::marker::PhantomData;
use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_common::hash::{calc_hash_key_kind, HashKey, HashKeyDispatcher, HashKeyKind};
use risingwave_expr::expr::{build_from_prost, RowExpression};
use risingwave_pb::plan_common::JoinType as JoinTypeProto;

use super::*;
use crate::executor::hash_join::*;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::PkIndices;

pub struct DynamicFilterExecutorBuilder;

impl ExecutorBuilder for DynamicFilterExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        // Get table id and used as keyspace prefix.
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::DynamicFilter)?;
        let source_r = params.input.remove(1);
        let source_l = params.input.remove(0);
        let key_l = node.get_left_key() as usize;
        let key_r = node.get_right_key() as usize;

        let condition = RowExpression::new(build_from_prost(node.get_condition()?)?);

        let key = source_l.schema().fields[key_l as usize].data_type();
        let kind = calc_hash_key_kind(&[key]);

        // let left_table_id = TableId::from(node.left_table.as_ref().unwrap().id);
        // let right_table_id = TableId::from(node.right_table.as_ref().unwrap().id);

        let args = DynamicFilterExecutorDispatcherArgs {
            source_l,
            source_r,
            key_l,
            key_r,
            pk_indices: params.pk_indices,
            executor_id: params.executor_id,
            cond: condition,
            op_info: params.op_info,
            // keyspace_l: Keyspace::table_root(store.clone(), &left_table_id),
            // keyspace_r: Keyspace::table_root(store, &right_table_id),
            actor_id: params.actor_id as u64,
            metrics: params.executor_stats,
        };

        DynamicFilterExecutorDispatcher::<_>::dispatch_by_kind(kind, args)
    }
}

struct DynamicFilterExecutorDispatcher<S: StateStore>(PhantomData<S>);

struct DynamicFilterExecutorDispatcherArgs {
    source_l: Box<dyn Executor>,
    source_r: Box<dyn Executor>,
    key_l: usize,
    key_r: usize,
    pk_indices: PkIndices,
    executor_id: u64,
    cond: RowExpression,
    op_info: String,
    actor_id: u64,
    metrics: Arc<StreamingMetrics>,
}

impl<S: StateStore> HashKeyDispatcher for DynamicFilterExecutorDispatcher<S> {
    type Input = DynamicFilterExecutorDispatcherArgs;
    type Output = Result<BoxedExecutor>;

    fn dispatch<K: HashKey>(args: Self::Input) -> Self::Output {
        Ok(Box::new(DynamicFilterExecutor::<K, S>::new(
            args.source_l,
            args.source_r,
            args.key_l,
            args.key_r,
            args.pk_indices,
            args.actor_id,
            args.executor_id,
            args.cond,
            args.op_info,
            args.metrics,
        )))
    }
}
