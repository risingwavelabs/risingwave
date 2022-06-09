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
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::hash::VIRTUAL_NODE_COUNT;
use risingwave_pb::common::ParallelUnitMapping;
use risingwave_storage::monitor::StateStoreMetrics;
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
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::BatchPlan)?;
        let table_id = node.table_desc.as_ref().unwrap().table_id.into();

        let pk_descs_proto = &node.table_desc.as_ref().unwrap().order_keys;
        let pk_descs = pk_descs_proto.iter().map(|d| d.into()).collect();

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
        let hash_filter = if let Some(mapping) = &node.hash_mapping {
            generate_hash_filter(mapping, parallel_unit_id)
        } else {
            // TODO: remove this branch once we deprecate Java frontend.
            // manually build bitmap with full of ones
            let mut hash_filter_builder = BitmapBuilder::with_capacity(VIRTUAL_NODE_COUNT);
            for _ in 0..VIRTUAL_NODE_COUNT {
                hash_filter_builder.append(true);
            }
            hash_filter_builder.finish()
        };

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

/// Generate bitmap from compressed parallel unit mapping.
fn generate_hash_filter(mapping: &ParallelUnitMapping, parallel_unit_id: u32) -> Bitmap {
    let mut builder = BitmapBuilder::with_capacity(VIRTUAL_NODE_COUNT);
    let mut start: usize = 0;
    for (idx, range_right) in mapping.original_indices.iter().enumerate() {
        let bit = parallel_unit_id == mapping.data[idx];
        for _ in start..=*range_right as usize {
            builder.append(bit);
        }
        start = *range_right as usize + 1;
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use risingwave_pb::common::ParallelUnitMapping;

    use super::*;

    #[test]
    fn test_generate_hash_filter() {
        let mapping = ParallelUnitMapping {
            original_indices: vec![681, 1363, 2045, 2046, 2047],
            data: vec![1, 2, 3, 1, 2],
            ..Default::default()
        };
        let hash_filter = generate_hash_filter(&mapping, 1);
        assert!(hash_filter.is_set(0).unwrap());
        assert!(hash_filter.is_set(681).unwrap());
        assert!(!hash_filter.is_set(682).unwrap());
        assert!(hash_filter.is_set(2046).unwrap());
        assert!(!hash_filter.is_set(2047).unwrap());
    }
}
