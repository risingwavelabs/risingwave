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

#[easy_ext::ext(TableFragmentsRenderCompression)]
impl PbTableFragments {
    // We decompress `PbTableFragments` using the `stream_node_template` of the `PbFragment`,
    // by re-rendering the template, then repopulating the upstream actor ids and upstream fragment id
    // contained in the `MergeNode` by `upstream_actors_by_fragment` in actor.
    pub fn ensure_uncompressed(&mut self) {
        assert_eq!(
            self.graph_render_type,
            GraphRenderType::RenderTemplate as i32
        );

        for fragment in self.fragments.values_mut() {
            assert!(
                fragment.stream_node_template.is_some(),
                "fragment.stream_node_template should not be None when render type is RenderTemplate"
            );

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

                assert!(
                    actor.nodes.is_none(),
                    "actor.nodes should be None when render type is RenderTemplate"
                );

                actor.nodes = pb_nodes;
                actor.upstream_actors_by_fragment.clear();
            }

            assert!(
                fragment.stream_node_template.is_some(),
                "fragment.stream_node_template should not be None when render type is RenderTemplate"
            );
            fragment.stream_node_template = None;
        }

        self.graph_render_type = GraphRenderType::RenderUnspecified as i32;
    }

    // We compressed the repetitive `node_body` of `PbStreamNode` in actors in Fragment in TableFragments,
    // storing their templates in the `stream_node_template` in the `PbFragment`.
    // We have also made special modifications to `MergeNode`s, extracting their upstream fragment id and upstream actor ids.
    // This way, we can save some memory when conducting RPC and storage.
    pub fn ensure_compressed(&mut self) {
        assert_eq!(
            self.graph_render_type,
            GraphRenderType::RenderUnspecified as i32
        );

        for fragment in self.fragments.values_mut() {
            assert!(
                fragment.stream_node_template.is_none(),
                "fragment.stream_node_template should be None when render type is unspecific"
            );

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

                assert!(
                    actor.nodes.is_some(),
                    "actor.nodes should not be None when render type is unspecified"
                );

                actor.nodes = None;

                assert!(
                    actor.upstream_actors_by_fragment.is_empty(),
                    "actor.upstream_actors_by_fragment should be empty when render type is unspecified"
                );

                actor.upstream_actors_by_fragment = upstream_actors_by_fragment;
            }

            assert!(
                fragment.stream_node_template.is_none(),
                "fragment.stream_node_template should be None when render type is unspecified"
            );
            fragment.stream_node_template = Some(stream_node);
        }

        self.graph_render_type = GraphRenderType::RenderTemplate as _;
    }
}
