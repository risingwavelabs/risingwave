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

mod scheduler;
mod source_manager;
mod stream_graph;
mod stream_manager;
#[cfg(test)]
mod test_fragmenter;

use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::StreamNode;
pub use scheduler::*;
pub use source_manager::*;
pub use stream_graph::*;
pub use stream_manager::*;

use crate::MetaResult;

/// Record vnode mapping for stateful operators in meta.
pub fn record_table_vnode_mappings(
    stream_node: &StreamNode,
    fragment: &mut Fragment,
) -> MetaResult<()> {
    // We only consider stateful operators with multiple parallel degrees here. Singleton stateful
    // operators will not have vnode mappings, so that compactors could omit the unnecessary probing
    // on vnode mappings.
    match stream_node.get_node_body()? {
        NodeBody::Materialize(node) => {
            let table_id = node.get_table_id();
            fragment.state_table_ids.push(table_id);
        }
        NodeBody::Arrange(node) => {
            let table_id = node.table.as_ref().unwrap().id;
            fragment.state_table_ids.push(table_id);
        }
        NodeBody::HashAgg(node) => {
            for table in &node.internal_tables {
                fragment.state_table_ids.push(table.id);
            }
        }
        NodeBody::LocalSimpleAgg(node) | NodeBody::GlobalSimpleAgg(node) => {
            for table in &node.internal_tables {
                fragment.state_table_ids.push(table.id);
            }
        }
        NodeBody::HashJoin(node) => {
            fragment
                .state_table_ids
                .push(node.left_table.as_ref().unwrap().id);
            fragment
                .state_table_ids
                .push(node.right_table.as_ref().unwrap().id);
        }
        NodeBody::DynamicFilter(node) => {
            fragment
                .state_table_ids
                .push(node.left_table.as_ref().unwrap().id);
            fragment
                .state_table_ids
                .push(node.right_table.as_ref().unwrap().id);
        }
        NodeBody::AppendOnlyTopN(node) => {
            fragment.state_table_ids.push(node.table_id_l);
            fragment.state_table_ids.push(node.table_id_h);
        }
        _ => {}
    }
    let input_nodes = stream_node.get_input();
    for input_node in input_nodes {
        record_table_vnode_mappings(input_node, fragment)?;
    }
    Ok(())
}
