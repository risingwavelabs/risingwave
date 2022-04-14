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
use std::sync::Arc;

use risingwave_pb::meta::table_fragments::fragment::FragmentType;
use risingwave_pb::stream_plan::{DispatchStrategy, StreamNode};

use crate::model::LocalFragmentId;

/// An edge between the nodes in the fragment graph.
#[derive(Debug, Clone)]
pub struct StreamFragmentEdge {
    /// Dispatch strategy for the fragment.
    pub dispatch_strategy: DispatchStrategy,

    /// Whether the two linked nodes should be placed on the same worker node
    pub same_worker_node: bool,

    /// A unique identifer of this edge. Generally it should be exchange node's operator id. When
    /// rewriting fragments into delta joins or when inserting 1-to-1 exchange, there will be
    /// virtual links generated.
    pub link_id: u64,
}

/// [`StreamFragment`] represent a fragment node in fragment DAG.
#[derive(Clone, Debug)]
pub struct StreamFragment {
    /// the allocated fragment id.
    pub fragment_id: LocalFragmentId,

    /// root stream node in this fragment.
    pub node: Arc<StreamNode>,

    /// type of this fragment.
    pub fragment_type: FragmentType,

    /// mark whether this fragment should only have one actor.
    pub is_singleton: bool,
}

impl StreamFragment {
    pub fn new(fragment_id: LocalFragmentId, node: Arc<StreamNode>) -> Self {
        Self {
            fragment_id,
            node,
            fragment_type: FragmentType::Others,
            is_singleton: false,
        }
    }
}

/// [`StreamFragmentGraph`] stores a fragment graph (DAG).
pub struct StreamFragmentGraph {
    /// stores all the fragments in the graph.
    fragments: HashMap<LocalFragmentId, StreamFragment>,

    /// stores edges between fragments: upstream => downstream.
    downstreams: HashMap<LocalFragmentId, HashMap<LocalFragmentId, StreamFragmentEdge>>,

    /// stores edges between fragments: downstream -> upstream.
    upstreams: HashMap<LocalFragmentId, HashMap<LocalFragmentId, StreamFragmentEdge>>,

    /// whether the graph is sealed and verified
    sealed: bool,
}

impl StreamFragmentGraph {
    pub fn new() -> Self {
        Self {
            fragments: HashMap::new(),
            downstreams: HashMap::new(),
            upstreams: HashMap::new(),
            sealed: false,
        }
    }

    pub fn fragments(&self) -> &HashMap<LocalFragmentId, StreamFragment> {
        &self.fragments
    }

    /// Adds a fragment to the graph.
    pub fn add_fragment(&mut self, stream_fragment: StreamFragment) {
        assert!(!self.sealed);
        assert!(stream_fragment.fragment_id.is_local());
        let id = stream_fragment.fragment_id;
        let ret = self
            .fragments
            .insert(stream_fragment.fragment_id, stream_fragment);
        assert!(ret.is_none(), "fragment already exists: {:?}", id);
    }

    /// Links upstream to downstream in the graph.
    pub fn add_edge(
        &mut self,
        upstream_id: LocalFragmentId,
        downstream_id: LocalFragmentId,
        edge: StreamFragmentEdge,
    ) {
        assert!(!self.sealed);
        assert!(upstream_id.is_local());
        assert!(downstream_id.is_local());

        let ret = self
            .downstreams
            .entry(upstream_id)
            .or_insert_with(HashMap::new)
            .insert(downstream_id, edge.clone());
        assert!(
            ret.is_none(),
            "edge already exists: {:?}",
            (upstream_id, downstream_id, edge)
        );

        let ret = self
            .upstreams
            .entry(downstream_id)
            .or_insert_with(HashMap::new)
            .insert(upstream_id, edge.clone());
        assert!(
            ret.is_none(),
            "edge already exists: {:?}",
            (upstream_id, downstream_id, edge)
        );
    }

    pub fn get_fragment(&self, fragment_id: LocalFragmentId) -> Option<&StreamFragment> {
        assert_eq!(fragment_id.is_global(), self.sealed);
        self.fragments.get(&fragment_id)
    }

    pub fn get_downstreams(
        &self,
        fragment_id: LocalFragmentId,
    ) -> &HashMap<LocalFragmentId, StreamFragmentEdge> {
        lazy_static::lazy_static! {
            static ref EMPTY_HASHMAP: HashMap<LocalFragmentId, StreamFragmentEdge> = HashMap::new();
        }
        assert_eq!(fragment_id.is_global(), self.sealed);
        self.downstreams.get(&fragment_id).unwrap_or(&EMPTY_HASHMAP)
    }

    pub fn get_upstreams(
        &self,
        fragment_id: LocalFragmentId,
    ) -> &HashMap<LocalFragmentId, StreamFragmentEdge> {
        lazy_static::lazy_static! {
            static ref EMPTY_HASHMAP: HashMap<LocalFragmentId, StreamFragmentEdge> = HashMap::new();
        }
        assert_eq!(fragment_id.is_global(), self.sealed);
        self.upstreams.get(&fragment_id).unwrap_or(&EMPTY_HASHMAP)
    }

    /// Convert all local ids to global ids by `local_id + offset`
    pub fn seal(&mut self, offset: u32, len: u32) {
        self.sealed = true;
        self.fragments = std::mem::take(&mut self.fragments)
            .into_iter()
            .map(|(id, fragment)| {
                let id = id.to_global_id(offset, len);
                (
                    id,
                    StreamFragment {
                        fragment_id: id,
                        ..fragment
                    },
                )
            })
            .collect();

        self.downstreams = std::mem::take(&mut self.downstreams)
            .into_iter()
            .map(|(upstream, links)| {
                let upstream = upstream.to_global_id(offset, len);
                let links = links
                    .into_iter()
                    .map(|(downstream, dispatcher)| {
                        let downstream = downstream.to_global_id(offset, len);
                        (downstream, dispatcher)
                    })
                    .collect();
                (upstream, links)
            })
            .collect();

        self.upstreams = std::mem::take(&mut self.upstreams)
            .into_iter()
            .map(|(downstream, links)| {
                let downstream = downstream.to_global_id(offset, len);
                let links = links
                    .into_iter()
                    .map(|(upstream, dispatcher)| {
                        let upstream = upstream.to_global_id(offset, len);
                        (upstream, dispatcher)
                    })
                    .collect();
                (downstream, links)
            })
            .collect();
    }

    /// Number of fragments
    pub fn fragment_len(&self) -> usize {
        self.fragments.len()
    }
}
