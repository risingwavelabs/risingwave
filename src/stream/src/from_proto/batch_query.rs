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

use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, OrderedColumnDesc};
use risingwave_storage::table::cell_based_table::CellBasedTable;
use risingwave_storage::{Keyspace, StateStore};

use super::*;
use crate::executor::BatchQueryExecutor;

pub struct BatchQueryExecutorBuilder;

impl ExecutorBuilder for BatchQueryExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        state_store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let pk_indices = node.pk_indices.iter().map(|&i| i as usize).collect();
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::BatchPlan)?;
        let table_id = node.table_desc.as_ref().unwrap().table_id.into();

        let pk_descs_proto = &node.table_desc.as_ref().unwrap().order_key;
        let pk_descs = pk_descs_proto
            .iter()
            .map(OrderedColumnDesc::from)
            .collect_vec();
        let order_types = pk_descs.iter().map(|desc| desc.order).collect_vec();

        let column_descs = node
            .column_descs
            .iter()
            .map(|column_desc| ColumnDesc::from(column_desc.clone()))
            .collect_vec();
        let keyspace = Keyspace::table_root(state_store, &table_id);
        let table = CellBasedTable::new(keyspace, column_descs, order_types, pk_indices, None);
        let key_indices = node
            .get_distribution_keys()
            .iter()
            .map(|key| *key as usize)
            .collect_vec();

        let hash_filter = params.vnode_bitmap.expect("no vnode bitmap");

        let schema = table.schema().clone();
        let executor = BatchQueryExecutor::new(
            table,
            None,
            ExecutorInfo {
                schema,
                pk_indices: params.pk_indices,
                identity: "BatchQuery".to_owned(),
            },
            key_indices,
            hash_filter,
            pk_descs,
        );

        Ok(executor.boxed())
    }
}
