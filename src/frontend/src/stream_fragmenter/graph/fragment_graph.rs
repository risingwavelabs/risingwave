// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::rc::Rc;

use risingwave_pb::stream_plan::stream_fragment_graph::{
    StreamFragment as StreamFragmentProto, StreamFragmentEdge as StreamFragmentEdgeProto,
};
use risingwave_pb::stream_plan::{
    DispatchStrategy, FragmentTypeFlag, StreamEnvironment,
    StreamFragmentGraph as StreamFragmentGraphProto, StreamNode,
};

pub type LocalFragmentId = u32;

/// [`StreamFragment`] represent a fragment node in fragment DAG.
#[derive(Clone, Debug)]
pub struct StreamFragment {
    /// the allocated fragment id.
    pub fragment_id: LocalFragmentId,

    /// root stream node in this fragment.
    pub node: Option<Box<StreamNode>>,

    /// Bitwise-OR of type Flags of this fragment.
    pub fragment_type_mask: u32,

    /// Mark whether this fragment requires exactly one actor.
    pub requires_singleton: bool,

    /// Number of table ids (stateful states) for this fragment.
    pub table_ids_cnt: u32,

    /// Mark the upstream table ids of this fragment.
    pub upstream_table_ids: Vec<u32>,
}

/// An edge between the nodes in the fragment graph.
#[derive(Debug, Clone)]
pub struct StreamFragmentEdge {
    /// Dispatch strategy for the fragment.
    pub dispatch_strategy: DispatchStrategy,

    /// A unique identifier of this edge. Generally it should be exchange node's operator id. When
    /// rewriting fragments into delta joins or when inserting 1-to-1 exchange, there will be
    /// virtual links generated.
    pub link_id: u64,
}

impl StreamFragment {
    pub fn new(fragment_id: LocalFragmentId) -> Self {
        Self {
            fragment_id,
            fragment_type_mask: FragmentTypeFlag::FragmentUnspecified as u32,
            requires_singleton: false,
            node: None,
            table_ids_cnt: 0,
            upstream_table_ids: vec![],
        }
    }

    pub fn to_protobuf(&self) -> StreamFragmentProto {
        StreamFragmentProto {
            fragment_id: self.fragment_id,
            node: self.node.clone().map(|n| *n),
            fragment_type_mask: self.fragment_type_mask,
            requires_singleton: self.requires_singleton,
            table_ids_cnt: self.table_ids_cnt,
            upstream_table_ids: self.upstream_table_ids.clone(),
        }
    }
}

/// [`StreamFragmentGraph`] stores a fragment graph (DAG).
#[derive(Default)]
pub struct StreamFragmentGraph {
    /// stores all the fragments in the graph.
    fragments: HashMap<LocalFragmentId, Rc<StreamFragment>>,

    /// stores edges between fragments: (upstream, downstream) => edge.
    edges: HashMap<(LocalFragmentId, LocalFragmentId), StreamFragmentEdgeProto>,

    /// Stores the environment for the streaming plan
    env: StreamEnvironment,
}

impl StreamFragmentGraph {
    pub fn to_protobuf(&self) -> StreamFragmentGraphProto {
        StreamFragmentGraphProto {
            fragments: self
                .fragments
                .iter()
                .map(|(k, v)| (*k, v.to_protobuf()))
                .collect(),
            edges: self.edges.values().cloned().collect(),
            env: Some(self.env.clone()),
            // To be filled later
            dependent_table_ids: vec![],
            table_ids_cnt: 0,
            parallelism: None,
        }
    }

    /// Adds a fragment to the graph.
    pub fn add_fragment(&mut self, stream_fragment: Rc<StreamFragment>) {
        let id = stream_fragment.fragment_id;
        let ret = self.fragments.insert(id, stream_fragment);
        assert!(ret.is_none(), "fragment already exists: {:?}", id);
    }

    pub fn get_fragment(&self, fragment_id: &LocalFragmentId) -> Option<&Rc<StreamFragment>> {
        self.fragments.get(fragment_id)
    }

    /// Links upstream to downstream in the graph.
    pub fn add_edge(
        &mut self,
        upstream_id: LocalFragmentId,
        downstream_id: LocalFragmentId,
        edge: StreamFragmentEdge,
    ) {
        let edge = StreamFragmentEdgeProto {
            upstream_id,
            downstream_id,
            dispatch_strategy: Some(edge.dispatch_strategy),
            link_id: edge.link_id,
        };

        self.edges
            .try_insert((upstream_id, downstream_id), edge)
            .unwrap();
    }
}
