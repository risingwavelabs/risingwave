use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use risingwave_pb::stream_plan::StreamNode;

/// [`StreamStage`] represent a stage node in stage DAG.
#[derive(Clone)]
pub struct StreamStage {
    /// the allocated stage id.
    stage_id: u32,
    /// root stream node in this stage.
    node: Arc<StreamNode>,
}

impl StreamStage {
    pub fn new(stage_id: u32, node: Arc<StreamNode>) -> Self {
        Self { stage_id, node }
    }

    pub fn get_stage_id(&self) -> u32 {
        self.stage_id
    }

    pub fn get_node(&self) -> Arc<StreamNode> {
        self.node.clone()
    }
}

/// [`StreamStageGraph`] stores a stage graph with a root stage(id: [`stage_id`]).
pub struct StreamStageGraph {
    /// represent the root stage of the graph.
    stage_id: u32,
    /// stores all the stages in the graph.
    stages: HashMap<u32, StreamStage>,
    /// stores stage relations: parent_stage => set(child_stage).
    child_edges: HashMap<u32, HashSet<u32>>,
}

impl StreamStageGraph {
    pub fn new(stage_id: Option<u32>) -> Self {
        Self {
            stage_id: stage_id.unwrap_or(0),
            stages: HashMap::new(),
            child_edges: HashMap::new(),
        }
    }

    pub fn get_root_stage(&self) -> StreamStage {
        self.stages.get(&self.stage_id).unwrap().clone()
    }

    pub fn add_root_stage(&mut self, stream_stage: StreamStage) {
        self.stage_id = stream_stage.stage_id;
        self.stages.insert(stream_stage.stage_id, stream_stage);
    }

    pub fn add_stage(&mut self, stream_stage: StreamStage) {
        self.stages.insert(stream_stage.stage_id, stream_stage);
    }

    /// Links `child_id` to its belonging parent stage.
    pub fn link_child(&mut self, parent_id: u32, child_id: u32) {
        self.child_edges
            .entry(parent_id)
            .or_insert_with(HashSet::new)
            .insert(child_id);
    }

    pub fn has_downstream(&self, stage_id: u32) -> bool {
        self.child_edges.contains_key(&stage_id)
    }

    pub fn get_downstream_stages(&self, stage_id: u32) -> Option<HashSet<u32>> {
        self.child_edges.get(&stage_id).cloned()
    }

    pub fn get_stage_by_id(&self, stage_id: u32) -> Option<StreamStage> {
        self.stages.get(&stage_id).cloned()
    }
}
