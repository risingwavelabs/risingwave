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

use risingwave_pb::meta::table_fragments::GraphRenderType;
use risingwave_pb::meta::PbTableFragments;
use risingwave_pb::stream_plan::stream_actor::PbActorIds;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use crate::util::stream_graph_visitor::visit_stream_node;

pub fn downgrade_table_fragments(table_fragments: &mut PbTableFragments) {
    assert_eq!(
        table_fragments.graph_render_type,
        GraphRenderType::RenderTemplate as i32
    );

    for fragment in table_fragments.fragments.values_mut() {
        assert!(fragment.stream_node_template.is_some());

        for actor in &mut fragment.actors {
            let pb_nodes = {
                let mut nodes = fragment.stream_node_template.clone().unwrap();

                visit_stream_node(&mut nodes, |body| {
                    if let NodeBody::Merge(m) = body
                        && let Some(PbActorIds{  actor_ids: upstream_actor_ids }) = actor.upstream_actors_by_fragment.get(&(m.upstream_fragment_id as _))
                    {
                        m.upstream_actor_id = upstream_actor_ids.iter().map(|id| *id as _).collect();
                    }
                });

                Some(nodes)
            };

            actor.nodes = pb_nodes;
            actor.upstream_actors_by_fragment.clear();
        }

        fragment.stream_node_template = None;
    }

    table_fragments.graph_render_type = GraphRenderType::RenderUnspecified as i32;
}

pub fn upgrade_table_fragments(table_fragments: &mut PbTableFragments) {
    for fragment in table_fragments.fragments.values_mut() {
        let stream_node = {
            let actor_template = fragment
                .actors
                .first()
                .cloned()
                .expect("fragment has no actor");

            let mut stream_node = actor_template.nodes.unwrap();
            visit_stream_node(&mut stream_node, |body| {
                if let NodeBody::Merge(m) = body {
                    m.upstream_actor_id = vec![];
                }
            });

            stream_node
        };

        for actor in &mut fragment.actors {
            let Some(node) = actor.nodes.as_mut() else {
                continue;
            };

            let mut upstream_actors_by_fragment = HashMap::new();
            visit_stream_node(node, |body| {
                if let NodeBody::Merge(m) = body {
                    let upstream_actor_ids = std::mem::take(&mut m.upstream_actor_id);
                    assert!(
                        upstream_actors_by_fragment
                            .insert(
                                m.upstream_fragment_id,
                                PbActorIds {
                                    actor_ids: upstream_actor_ids
                                }
                            )
                            .is_none(),
                        "There should only be one link between two fragments"
                    );
                }
            });

            actor.upstream_actors_by_fragment = upstream_actors_by_fragment;

            // todo: can we directly set as none?
            // actor.nodes = Some(StreamNode {
            //     node_body: Some(NodeBody::NoOp(PbNoOpNode {})),
            //     ..Default::default()
            // });
            actor.nodes = None;
        }

        fragment.stream_node_template = Some(stream_node);
    }

    table_fragments.graph_render_type = GraphRenderType::RenderTemplate as _;
}
