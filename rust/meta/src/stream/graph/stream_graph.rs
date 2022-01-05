use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;

use risingwave_pb::stream_plan::dispatcher::DispatcherType;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{Dispatcher, MergeNode, StreamFragment, StreamNode};

/// [`StreamFragmentBuilder`] build a stream fragment in a stream DAG.
pub struct StreamFragmentBuilder {
    /// fragment id field.
    fragment_id: u32,
    /// associated stream node.
    nodes: Arc<StreamNode>,
    /// dispatcher category.
    dispatcher: Option<Dispatcher>,
    /// downstream fragment set.
    downstream_fragments: HashSet<u32>,
    /// upstream fragment array.
    upstream_fragments: Vec<Vec<u32>>,
}

impl StreamFragmentBuilder {
    pub fn new(fragment_id: u32, node: Arc<StreamNode>) -> Self {
        Self {
            fragment_id,
            nodes: node,
            dispatcher: None,
            downstream_fragments: HashSet::new(),
            upstream_fragments: vec![],
        }
    }

    pub fn get_id(&self) -> u32 {
        self.fragment_id
    }

    pub fn set_simple_dispatcher(&mut self) {
        self.dispatcher = Some(Dispatcher {
            r#type: DispatcherType::Simple as i32,
            column_idx: 0,
        })
    }

    pub fn set_hash_dispatcher(&mut self, column_idx: i32) {
        self.dispatcher = Some(Dispatcher {
            r#type: DispatcherType::Hash as i32,
            column_idx,
        })
    }

    pub fn set_broadcast_dispatcher(&mut self) {
        self.dispatcher = Some(Dispatcher {
            r#type: DispatcherType::Broadcast as i32,
            column_idx: 0,
        })
    }

    pub fn set_dispatcher(&mut self, dispatcher: Dispatcher) {
        self.dispatcher = Some(dispatcher);
    }

    /// Used by stream graph to inject upstream fields.
    pub fn get_upstream_fragments(&self) -> Vec<Vec<u32>> {
        self.upstream_fragments.clone()
    }

    pub fn builder(&self) -> StreamFragment {
        StreamFragment {
            fragment_id: self.fragment_id,
            nodes: Some(self.nodes.deref().clone()),
            dispatcher: self.dispatcher.clone(),
            downstream_fragment_id: self.downstream_fragments.iter().copied().collect(),
        }
    }
}

/// [`StreamGraphBuilder`] build a stream graph with root with id `root_fragment`. It will do some
/// injection here to achieve dependencies. See `build_inner` for more details.
pub struct StreamGraphBuilder {
    root_fragment: u32,
    fragment_builders: HashMap<u32, StreamFragmentBuilder>,
}

impl StreamGraphBuilder {
    pub fn new() -> Self {
        Self {
            root_fragment: 0,
            fragment_builders: HashMap::new(),
        }
    }

    pub fn set_root_fragment(&mut self, id: u32) {
        self.root_fragment = id;
    }

    /// Insert new generated fragment.
    pub fn add_fragment(&mut self, fragment: StreamFragmentBuilder) {
        self.fragment_builders.insert(fragment.get_id(), fragment);
    }

    /// Add dependency between two connected node in the graph.
    pub fn add_dependency(&mut self, upstreams: &[u32], downstreams: &[u32]) {
        downstreams.iter().for_each(|&downstream| {
            upstreams.iter().for_each(|upstream| {
                self.fragment_builders
                    .get_mut(upstream)
                    .unwrap()
                    .downstream_fragments
                    .insert(downstream);
            });
            self.fragment_builders
                .get_mut(&downstream)
                .unwrap()
                .upstream_fragments
                .push(upstreams.to_vec());
        });
    }

    /// Build final stream DAG with dependencies with current fragment builders.
    pub fn build(&self) -> Vec<StreamFragment> {
        self.fragment_builders
            .values()
            .map(|builder| {
                let mut fragment = builder.builder();
                let upstream_fragments = builder.get_upstream_fragments();
                fragment.nodes =
                    Some(self.build_inner(fragment.get_nodes(), &upstream_fragments, 0));
                fragment
            })
            .collect::<Vec<_>>()
    }

    /// Build stream fragment inside, two works will be done:
    /// 1. replace node's input with [`MergeNode`] if it is [`ExchangeNode`], and swallow
    /// mergeNode's input. 2. ignore root node when it's [`ExchangeNode`].
    pub fn build_inner(
        &self,
        stream_node: &StreamNode,
        upstream_fragment_id: &[Vec<u32>],
        next_idx: usize,
    ) -> StreamNode {
        if let Node::ExchangeNode(_) = stream_node.get_node() {
            self.build_inner(
                stream_node.input.get(0).unwrap(),
                upstream_fragment_id,
                next_idx,
            )
        } else {
            let mut new_stream_node = stream_node.clone();
            let mut next_idx_new = next_idx;
            for (idx, input) in stream_node.input.iter().enumerate() {
                if let Node::ExchangeNode(exchange_node) = input.get_node() {
                    assert!(next_idx_new < upstream_fragment_id.len());
                    new_stream_node.input[idx] = StreamNode {
                        input: vec![],
                        pk_indices: input.pk_indices.clone(),
                        node: Some(Node::MergeNode(MergeNode {
                            upstream_fragment_id: upstream_fragment_id
                                .get(next_idx_new)
                                .cloned()
                                .unwrap(),
                            input_column_descs: exchange_node.get_input_column_descs().clone(),
                        })),
                    };
                    next_idx_new += 1;
                } else {
                    new_stream_node.input[idx] =
                        self.build_inner(input, upstream_fragment_id, next_idx_new);
                }
            }
            new_stream_node
        }
    }
}
