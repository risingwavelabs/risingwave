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

//! Global Streaming Hash Aggregators

use std::marker::PhantomData;

use risingwave_common::catalog::{ColumnDesc, TableId};
use risingwave_common::hash::{calc_hash_key_kind, HashKey, HashKeyDispatcher};
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::table::state_table::StateTable;

use super::*;
use crate::executor::aggregation::AggCall;
use crate::executor::{HashAggExecutor, PkIndices};

struct HashAggExecutorDispatcher<S: StateStore>(PhantomData<S>);

struct HashAggExecutorDispatcherArgs<S: StateStore> {
    input: BoxedExecutor,
    agg_calls: Vec<AggCall>,
    key_indices: Vec<usize>,
    pk_indices: PkIndices,
    executor_id: u64,
    state_tables: Vec<StateTable<S>>,
}

impl<S: StateStore> HashKeyDispatcher for HashAggExecutorDispatcher<S> {
    type Input = HashAggExecutorDispatcherArgs<S>;
    type Output = Result<BoxedExecutor>;

    fn dispatch<K: HashKey>(args: Self::Input) -> Self::Output {
        Ok(HashAggExecutor::<K, S>::new(
            args.input,
            args.agg_calls,
            args.pk_indices,
            args.executor_id,
            args.key_indices,
            args.state_tables,
        )?
        .boxed())
    }
}

pub struct HashAggExecutorBuilder;

impl ExecutorBuilder for HashAggExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::HashAgg)?;
        let key_indices = node
            .get_distribution_keys()
            .iter()
            .map(|key| *key as usize)
            .collect::<Vec<_>>();
        let agg_calls: Vec<AggCall> = node
            .get_agg_calls()
            .iter()
            .map(|agg_call| build_agg_call_from_prost(node.is_append_only, agg_call))
            .try_collect()?;
        // Build vector of keyspace via table ids.
        // One keyspace for one agg call.
        let keyspace: Vec<Keyspace<_>> = node
            .internal_tables
            .iter()
            .map(|table| Keyspace::table_root(store.clone(), &TableId::new(table.id)))
            .collect();
        let input = params.input.remove(0);
        let keys = key_indices
            .iter()
            .map(|idx| input.schema().fields[*idx].data_type())
            .collect_vec();
        let kind = calc_hash_key_kind(&keys);
        let agg_calls_len = agg_calls.len();
        // Create internal tables used by hash agg.
        let mut state_tables = Vec::with_capacity(agg_calls_len);
        for (ks, table_catalog) in keyspace.iter().zip_eq(node.internal_tables.iter()) {
            // Parse info from proto and create state table.
            let state_table = {
                let table_columns = table_catalog
                    .columns
                    .iter()
                    .map(|col| {
                        ColumnDesc::unnamed(
                            col.column_desc.as_ref().unwrap().column_id.into(),
                            col.column_desc
                                .as_ref()
                                .unwrap()
                                .column_type
                                .as_ref()
                                .unwrap()
                                .into(),
                        )
                    })
                    .collect();
                let order_types = table_catalog
                    .orders
                    .iter()
                    .map(|order_type| {
                        OrderType::from_prost(
                            &risingwave_pb::plan_common::OrderType::from_i32(*order_type).unwrap(),
                        )
                    })
                    .collect();
                let dist_key_indices = table_catalog
                    .distribution_keys
                    .iter()
                    .map(|dist_index| *dist_index as usize)
                    .collect();
                let pk_indices = table_catalog
                    .pk
                    .iter()
                    .map(|pk_index| *pk_index as usize)
                    .collect();
                StateTable::new(
                    ks.clone(),
                    table_columns,
                    order_types,
                    Some(dist_key_indices),
                    pk_indices,
                )
            };

            state_tables.push(state_table)
        }

        let args = HashAggExecutorDispatcherArgs {
            input,
            agg_calls,
            key_indices,
            pk_indices: params.pk_indices,
            executor_id: params.executor_id,
            state_tables,
        };
        HashAggExecutorDispatcher::dispatch_by_kind(kind, args)
    }
}
