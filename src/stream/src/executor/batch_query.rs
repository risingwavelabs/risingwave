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

use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, TableId};
use risingwave_common::error::Result;
use risingwave_common::hash::VIRTUAL_NODE_COUNT;
use risingwave_common::try_match_expand;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::monitor::StateStoreMetrics;
use risingwave_storage::table::cell_based_table::CellBasedTable;
use risingwave_storage::{Keyspace, StateStore};

use crate::executor::ExecutorBuilder;
use crate::executor_v2::{BatchQueryExecutor, BoxedExecutor, Executor};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

pub struct BatchQueryExecutorBuilder;

impl ExecutorBuilder for BatchQueryExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &stream_plan::StreamNode,
        state_store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::BatchPlanNode)?;
        let table_id = TableId::from(&node.table_ref_id);
        let column_descs = node
            .column_descs
            .iter()
            .map(|column_desc| ColumnDesc::from(column_desc.clone()))
            .collect_vec();
        let keyspace = Keyspace::table_root(state_store, &table_id);
        let table = CellBasedTable::new_adhoc(
            keyspace,
            column_descs,
            Arc::new(StateStoreMetrics::unused()),
        );
        let key_indices = node
            .get_distribution_keys()
            .iter()
            .map(|key| *key as usize)
            .collect_vec();

        let parallel_unit_id = node.get_parallel_unit_id() as u32;
        let mut hash_mapping = node
            .get_hash_mapping()
            .iter()
            .map(|id| *id as u32)
            .collect_vec();
        // TODO: remove this when we deprecate Java frontend;
        if hash_mapping.is_empty() {
            hash_mapping = vec![0; VIRTUAL_NODE_COUNT];
            assert_eq!(parallel_unit_id, 0);
        }
        let executor = BatchQueryExecutor::new_from_v1(
            table,
            params.pk_indices,
            params.op_info,
            key_indices,
            parallel_unit_id,
            hash_mapping,
        );

        Ok(executor.boxed())
    }
}
