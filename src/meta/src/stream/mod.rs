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

mod meta;
mod scheduler;
mod source_manager;
mod stream_graph;
mod stream_manager;

#[cfg(test)]
mod test_fragmenter;
pub use meta::*;
pub use scheduler::*;
pub use source_manager::*;
pub use stream_graph::*;
pub use stream_manager::*;

/// Set vnode mapping for stateful operators.
#[macro_export]
macro_rules! set_table_vnode_mappings {
    () => {
        fn set_table_vnode_mappings(
            &self,
            stream_node: &StreamNode,
            fragment_id: FragmentId,
        ) -> Result<()> {
            match stream_node.get_node_body()? {
                NodeBody::Materialize(node) => {
                    let table_id = node.get_table_ref_id()?.get_table_id() as u32;
                    self.hash_mapping_manager
                        .set_fragment_state_table(fragment_id, table_id);
                }
                NodeBody::GlobalSimpleAgg(node) => {
                    let table_ids = node.get_table_ids();
                    for table_id in table_ids {
                        self.hash_mapping_manager
                            .set_fragment_state_table(fragment_id, *table_id);
                    }
                }
                NodeBody::HashAgg(node) => {
                    let table_ids = node.get_table_ids();
                    for table_id in table_ids {
                        self.hash_mapping_manager
                            .set_fragment_state_table(fragment_id, *table_id);
                    }
                }
                NodeBody::HashJoin(node) => {
                    self.hash_mapping_manager
                        .set_fragment_state_table(fragment_id, node.left_table_id);
                    self.hash_mapping_manager
                        .set_fragment_state_table(fragment_id, node.right_table_id);
                }
                _ => {}
            }
            let input_nodes = stream_node.get_input();
            for input_node in input_nodes {
                self.set_table_vnode_mappings(input_node, fragment_id)?;
            }
            Ok(())
        }
    };
}
