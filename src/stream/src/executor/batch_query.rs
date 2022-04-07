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
use risingwave_common::try_match_expand;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::monitor::StateStoreMetrics;
use risingwave_storage::table::cell_based_table::CellBasedTable;
use risingwave_storage::{Keyspace, StateStore};

use crate::executor::{Executor, ExecutorBuilder};
use crate::executor_v2::{BatchQueryExecutor as BatchQueryExecutorV2, Executor as ExecutorV2};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

pub struct BatchQueryExecutorBuilder;

impl ExecutorBuilder for BatchQueryExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &stream_plan::StreamNode,
        state_store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<Box<dyn Executor>> {
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
            .collect::<Vec<_>>();

        let v2 = Box::new(BatchQueryExecutorV2::new_from_v1(
            table,
            params.pk_indices,
            params.op_info,
            key_indices,
        ));

        Ok(Box::new(v2.v1_uninited()))
    }
}
