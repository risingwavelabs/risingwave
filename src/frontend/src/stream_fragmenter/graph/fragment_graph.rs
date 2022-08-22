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

use std::collections::HashMap;

use risingwave_pb::stream_plan::stream_fragment_graph::{
    StreamFragment as StreamFragmentProto, StreamFragmentEdge as StreamFragmentEdgeProto,
};
use risingwave_pb::stream_plan::{
    DispatchStrategy, FragmentType, StreamFragmentGraph as StreamFragmentGraphProto, StreamNode,
};

type LocalFragmentId = u32;

/// [`StreamFragment`] represent a fragment node in fragment DAG.
#[derive(Clone, Debug)]
pub struct StreamFragment {
    /// the allocated fragment id.
    pub fragment_id: LocalFragmentId,

    /// root stream node in this fragment.
    pub node: Option<Box<StreamNode>>,

    /// type of this fragment.
    pub fragment_type: FragmentType,

    /// mark whether this fragment should only have one actor.
    pub is_singleton: bool,

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

    /// Whether the two linked nodes should be placed on the same worker node
    pub same_worker_node: bool,

    /// A unique identifier of this edge. Generally it should be exchange node's operator id. When
    /// rewriting fragments into delta joins or when inserting 1-to-1 exchange, there will be
    /// virtual links generated.
    pub link_id: u64,
}

impl StreamFragment {
    pub fn new(fragment_id: LocalFragmentId) -> Self {
        Self {
            fragment_id,
            fragment_type: FragmentType::Others,
            // FIXME: is it okay to use `false` as default value?
            is_singleton: false,
            node: None,
            table_ids_cnt: 0,
            upstream_table_ids: vec![],
        }
    }

    pub fn to_protobuf(&self) -> StreamFragmentProto {
        StreamFragmentProto {
            fragment_id: self.fragment_id,
            node: self.node.clone().map(|n| *n),
            fragment_type: self.fragment_type as i32,
            is_singleton: self.is_singleton,
            table_ids_cnt: self.table_ids_cnt,
            upstream_table_ids: self.upstream_table_ids.clone(),
        }
    }
}

/// [`StreamFragmentGraph`] stores a fragment graph (DAG).
#[derive(Default)]
pub struct StreamFragmentGraph {
    /// stores all the fragments in the graph.
    fragments: HashMap<LocalFragmentId, StreamFragment>,

    /// stores edges between fragments: (upstream, downstream) => edge.
    edges: HashMap<(LocalFragmentId, LocalFragmentId), StreamFragmentEdgeProto>,
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
            // To be filled later
            dependent_table_ids: vec![],
            table_ids_cnt: 0,
        }
    }

    /// Adds a fragment to the graph.
    pub fn add_fragment(&mut self, stream_fragment: StreamFragment) {
        let id = stream_fragment.fragment_id;
        let ret = self.fragments.insert(id, stream_fragment);
        assert!(ret.is_none(), "fragment already exists: {:?}", id);
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
            same_worker_node: edge.same_worker_node,
            link_id: edge.link_id,
        };

        let ret = self
            .edges
            .insert((upstream_id, downstream_id), edge.clone());
        assert!(
            ret.is_none(),
            "edge already exists: {:?}",
            (upstream_id, downstream_id, edge)
        );
    }
}
