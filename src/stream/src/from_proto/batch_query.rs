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
use risingwave_common::catalog::{ColumnDesc, ColumnId, OrderedColumnDesc, TableId};
use risingwave_pb::plan_common::CellBasedTableDesc;
use risingwave_storage::table::cell_based_table::CellBasedTable;
use risingwave_storage::table::Distribution;
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
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::BatchPlan)?;

        let table_desc: &CellBasedTableDesc = node.get_table_desc()?;
        let table_id = TableId {
            table_id: table_desc.table_id,
        };

        let pk_descs = table_desc
            .order_key
            .iter()
            .map(OrderedColumnDesc::from)
            .collect_vec();
        let order_types = pk_descs.iter().map(|desc| desc.order).collect_vec();

        let column_descs = table_desc
            .columns
            .iter()
            .map(ColumnDesc::from)
            .collect_vec();
        let column_ids = node
            .column_ids
            .iter()
            .copied()
            .map(ColumnId::from)
            .collect();

        // Use indices based on full table instead of streaming executor output.
        let pk_indices = table_desc
            .pk_indices
            .iter()
            .map(|&k| k as usize)
            .collect_vec();

        let dist_key_indices = table_desc
            .dist_key_indices
            .iter()
            .map(|&k| k as usize)
            .collect_vec();

        let keyspace = Keyspace::table_root(state_store, &table_id);

        let distribution = match params.vnode_bitmap {
            Some(vnodes) => Distribution {
                dist_key_indices,
                vnodes: vnodes.into(),
            },
            None => Distribution::fallback(),
        };
        let table = CellBasedTable::new_partial(
            keyspace,
            column_descs,
            column_ids,
            order_types,
            pk_indices,
            distribution,
        );

        let schema = table.schema().clone();
        let executor = BatchQueryExecutor::new(
            table,
            None,
            ExecutorInfo {
                schema,
                pk_indices: params.pk_indices,
                identity: "BatchQuery".to_owned(),
            },
            pk_descs,
        );

        Ok(executor.boxed())
    }
}
