use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use risingwave_common::error::Result;
use risingwave_common::{ensure, gen_error};
use risingwave_pb::meta::table_fragments::fragment::FragmentType;
use risingwave_pb::stream_plan::StreamNode;

/// [`StreamFragment`] represent a fragment node in fragment DAG.
#[derive(Clone)]
pub struct StreamFragment {
    /// the allocated fragment id.
    fragment_id: u32,
    /// root stream node in this fragment.
    node: Arc<StreamNode>,

    /// type of this fragment.
    fragment_type: FragmentType,
}

impl StreamFragment {
    pub fn new(fragment_id: u32, node: Arc<StreamNode>) -> Self {
        Self {
            fragment_id,
            node,
            fragment_type: FragmentType::Others,
        }
    }

    pub fn get_fragment_id(&self) -> u32 {
        self.fragment_id
    }

    pub fn get_node(&self) -> Arc<StreamNode> {
        self.node.clone()
    }

    pub fn set_fragment_type(&mut self, fragment_type: FragmentType) {
        self.fragment_type = fragment_type;
    }
}

/// [`StreamFragmentGraph`] stores a fragment graph with a root fragment(id: `fragment_id`).
pub struct StreamFragmentGraph {
    /// represent the root fragment of the graph.
    fragment_id: u32,
    /// stores all the fragments in the graph.
    fragments: HashMap<u32, StreamFragment>,
    /// stores fragment relations: parent_fragment => set(child_fragment).
    child_edges: HashMap<u32, BTreeSet<u32>>,
}

impl StreamFragmentGraph {
    pub fn new(fragment_id: Option<u32>) -> Self {
        Self {
            fragment_id: fragment_id.unwrap_or(0),
            fragments: HashMap::new(),
            child_edges: HashMap::new(),
        }
    }

    pub fn get_root_fragment(&self) -> StreamFragment {
        self.fragments.get(&self.fragment_id).unwrap().clone()
    }

    pub fn add_root_fragment(&mut self, stream_fragment: StreamFragment) {
        self.fragment_id = stream_fragment.fragment_id;
        self.fragments
            .insert(stream_fragment.fragment_id, stream_fragment);
    }

    pub fn add_fragment(&mut self, stream_fragment: StreamFragment) {
        self.fragments
            .insert(stream_fragment.fragment_id, stream_fragment);
    }

    /// Links `child_id` to its belonging parent fragment.
    pub fn link_child(&mut self, parent_id: u32, child_id: u32) {
        self.child_edges
            .entry(parent_id)
            .or_insert_with(BTreeSet::new)
            .insert(child_id);
    }

    pub fn has_downstream(&self, fragment_id: u32) -> bool {
        self.child_edges.contains_key(&fragment_id)
    }

    pub fn get_downstream_fragments(&self, fragment_id: u32) -> Option<BTreeSet<u32>> {
        self.child_edges.get(&fragment_id).cloned()
    }

    pub fn get_fragment_by_id(&self, fragment_id: u32) -> Option<StreamFragment> {
        self.fragments.get(&fragment_id).cloned()
    }

    pub fn set_fragment_type_by_id(
        &mut self,
        fragment_id: u32,
        fragment_type: FragmentType,
    ) -> Result<()> {
        ensure!(
            self.fragments.contains_key(&fragment_id),
            "fragment id not exist!"
        );
        self.fragments
            .get_mut(&fragment_id)
            .unwrap()
            .set_fragment_type(fragment_type);

        Ok(())
    }

    pub fn get_fragment_type_by_id(&self, fragment_id: u32) -> Result<FragmentType> {
        ensure!(
            self.fragments.contains_key(&fragment_id),
            "fragment id not exist!"
        );
        Ok(self.fragments.get(&fragment_id).unwrap().fragment_type)
    }
}
