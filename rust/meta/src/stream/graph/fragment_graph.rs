use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use risingwave_common::error::Result;
use risingwave_common::{ensure, gen_error};
use risingwave_pb::stream_plan::StreamNode;

/// [`StreamFragment`] represent a fragment node in fragment DAG.
#[derive(Clone)]
pub struct StreamFragment {
    /// the allocated fragment id.
    fragment_id: u32,
    /// root stream node in this fragment.
    node: Arc<StreamNode>,

    /// mark whether this fragment is of table source.
    is_table_source_fragment: bool,
}

impl StreamFragment {
    pub fn new(fragment_id: u32, node: Arc<StreamNode>) -> Self {
        Self {
            fragment_id,
            node,
            is_table_source_fragment: false,
        }
    }

    pub fn get_fragment_id(&self) -> u32 {
        self.fragment_id
    }

    pub fn get_node(&self) -> Arc<StreamNode> {
        self.node.clone()
    }

    pub fn set_as_table_source_fragment(&mut self) {
        self.is_table_source_fragment = true;
    }

    pub fn is_table_source_fragment(&self) -> bool {
        self.is_table_source_fragment
    }
}

/// [`StreamFragmentGraph`] stores a fragment graph with a root fragment(id: [`fragment_id`]).
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

    pub fn set_source_fragment_by_id(&mut self, fragment_id: u32) -> Result<()> {
        ensure!(
            self.fragments.contains_key(&fragment_id),
            "fragment id not exist!"
        );
        self.fragments
            .get_mut(&fragment_id)
            .unwrap()
            .set_as_table_source_fragment();

        Ok(())
    }
}
