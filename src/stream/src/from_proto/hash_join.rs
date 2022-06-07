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

use risingwave_common::catalog::TableId;
use risingwave_common::hash::{calc_hash_key_kind, HashKey, HashKeyDispatcher, HashKeyKind};
use risingwave_expr::expr::{build_from_prost, RowExpression};
use risingwave_pb::plan_common::JoinType as JoinTypeProto;

use super::*;
use crate::executor::hash_join::*;
use crate::executor::PkIndices;

pub struct HashJoinExecutorBuilder;

impl ExecutorBuilder for HashJoinExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        // Get table id and used as keyspace prefix.
        let append_only = node.get_append_only();
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::HashJoin)?;
        let source_r = params.input.remove(1);
        let source_l = params.input.remove(0);
        let params_l = JoinParams::new(
            node.get_left_key()
                .iter()
                .map(|key| *key as usize)
                .collect::<Vec<_>>(),
        );
        let params_r = JoinParams::new(
            node.get_right_key()
                .iter()
                .map(|key| *key as usize)
                .collect::<Vec<_>>(),
        );
        let output_indices = node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect_vec();

        let condition = match node.get_condition() {
            Ok(cond_prost) => Some(RowExpression::new(build_from_prost(cond_prost)?)),
            Err(_) => None,
        };
        trace!("Join non-equi condition: {:?}", condition);

        let key_indices = node
            .get_distribution_keys()
            .iter()
            .map(|key| *key as usize)
            .collect::<Vec<_>>();

        macro_rules! impl_create_hash_join_executor {
            ([], $( { $join_type_proto:ident, $join_type:ident } ),*) => {
                fn create_hash_join_executor<S: StateStore>(
                    typ: JoinTypeProto, kind: HashKeyKind,
                    args: HashJoinExecutorDispatcherArgs<S>,
                ) -> Result<BoxedExecutor> {
                    match typ {
                        $( JoinTypeProto::$join_type_proto => HashJoinExecutorDispatcher::<_, {JoinType::$join_type}>::dispatch_by_kind(kind, args), )*
                        // _ => todo!("Join type {:?} not implemented", typ),
                    }
                }
            }
        }

        macro_rules! for_all_join_types {
            ($macro:ident $(, $x:tt)*) => {
                $macro! {
                    [$($x),*],
                    { Inner, Inner },
                    { LeftOuter, LeftOuter },
                    { RightOuter, RightOuter },
                    { FullOuter, FullOuter },
                    { LeftSemi, LeftSemi },
                    { RightSemi, RightSemi },
                    { LeftAnti, LeftAnti },
                    { RightAnti, RightAnti }
                }
            };
        }

        let keys = params_l
            .key_indices
            .iter()
            .map(|idx| source_l.schema().fields[*idx].data_type())
            .collect_vec();
        let kind = calc_hash_key_kind(&keys);

        let left_table_id = TableId::from(node.left_table_id);
        let right_table_id = TableId::from(node.right_table_id);

        let args = HashJoinExecutorDispatcherArgs {
            source_l,
            source_r,
            params_l,
            params_r,
            pk_indices: params.pk_indices,
            output_indices,
            executor_id: params.executor_id,
            cond: condition,
            op_info: params.op_info,
            key_indices,
            keyspace_l: Keyspace::table_root(store.clone(), &left_table_id),
            keyspace_r: Keyspace::table_root(store, &right_table_id),
            append_only,
        };

        for_all_join_types! { impl_create_hash_join_executor };
        let join_type_proto = node.get_join_type()?;
        create_hash_join_executor(join_type_proto, kind, args)
    }
}

struct HashJoinExecutorDispatcher<S: StateStore, const T: JoinTypePrimitive>(PhantomData<S>);

struct HashJoinExecutorDispatcherArgs<S: StateStore> {
    source_l: Box<dyn Executor>,
    source_r: Box<dyn Executor>,
    params_l: JoinParams,
    params_r: JoinParams,
    pk_indices: PkIndices,
    output_indices: Vec<usize>,
    executor_id: u64,
    cond: Option<RowExpression>,
    op_info: String,
    key_indices: Vec<usize>,
    keyspace_l: Keyspace<S>,
    keyspace_r: Keyspace<S>,
    append_only: bool,
}

impl<S: StateStore, const T: JoinTypePrimitive> HashKeyDispatcher
    for HashJoinExecutorDispatcher<S, T>
{
    type Input = HashJoinExecutorDispatcherArgs<S>;
    type Output = Result<BoxedExecutor>;

    fn dispatch<K: HashKey>(args: Self::Input) -> Self::Output {
        Ok(Box::new(HashJoinExecutor::<K, S, T>::new(
            args.source_l,
            args.source_r,
            args.params_l,
            args.params_r,
            args.pk_indices,
            args.output_indices,
            args.executor_id,
            args.cond,
            args.op_info,
            args.key_indices,
            args.keyspace_l,
            args.keyspace_r,
            args.append_only,
        )))
    }
}
