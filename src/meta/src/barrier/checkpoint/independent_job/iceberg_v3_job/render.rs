// Copyright 2026 RisingWave Labs
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

use std::collections::{HashMap, HashSet};

use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont;
use risingwave_meta_model::WorkerId;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::id::{ActorId, FragmentId, PartialGraphId};
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::update_mutation::MergeUpdate;
use risingwave_pb::stream_plan::{PbStreamNode, UpdateMutation};

use super::super::RenderedIndependentJobActors;
use crate::MetaResult;
use crate::barrier::edge_builder::{EdgeBuilderFragmentInfo, FragmentEdgeBuilder};
use crate::controller::fragment::InflightFragmentInfo;
use crate::model::{FragmentDownstreamRelation, StreamJobActorsToCreate};

#[derive(Debug)]
pub(super) struct ResolverActivation {
    pub node_actors: HashMap<WorkerId, HashSet<ActorId>>,
    pub actors_to_create: StreamJobActorsToCreate,
    pub attach_mutation: Mutation,
    pub detach_mutation: Mutation,
}

#[derive(Debug)]
pub(super) struct NormalInputActivation {
    pub node_actors: HashMap<WorkerId, HashSet<ActorId>>,
    pub actors_to_create: StreamJobActorsToCreate,
    pub attach_mutation: Mutation,
    pub detach_mutation: Mutation,
}

#[derive(Debug)]
pub(super) struct IcebergV3StaticActors {
    pub normal_input: NormalInputActivation,
    pub resolver: ResolverActivation,
    pub long_running_node_actors: HashMap<WorkerId, HashSet<ActorId>>,
}

impl IcebergV3StaticActors {
    pub fn refresh_build_plans(
        &mut self,
        fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
    ) {
        for fragments in self.normal_input.actors_to_create.values_mut() {
            for (fragment_id, (node, ..)) in fragments {
                *node = fragment_infos[fragment_id].nodes.clone();
            }
        }
    }
}

/// The resolver fragment is rendered and placed with the whole Iceberg graph, but remains
/// disconnected until a compaction command activates it.
#[derive(Debug)]
pub(super) struct RenderedResolver {
    pub fragment_info: InflightFragmentInfo,
}

pub(super) fn build_static_actors(
    rendered: &RenderedIndependentJobActors,
    downstreams: &FragmentDownstreamRelation,
    partial_graph_id: PartialGraphId,
    worker_nodes: &HashMap<WorkerId, WorkerNode>,
) -> MetaResult<IcebergV3StaticActors> {
    let fragment_infos = &rendered.fragment_infos;
    let writer_fragment = fragment_infos
        .values()
        .find(|fragment| find_writer_node(&fragment.nodes).is_some())
        .ok_or_else(|| anyhow::anyhow!("Iceberg V3 graph has no pk-index writer"))?;
    let writer_node = find_writer_node(&writer_fragment.nodes)
        .expect("writer fragment was selected by its writer node");
    let [normal_input, resolver_input] = writer_node.input.as_slice() else {
        return Err(anyhow::anyhow!("Iceberg writer must have exactly two inputs").into());
    };
    let Some(NodeBody::Merge(normal_merge)) = normal_input.node_body.as_ref() else {
        return Err(anyhow::anyhow!("Iceberg writer normal input must be a Merge node").into());
    };
    let Some(NodeBody::Merge(resolver_merge)) = resolver_input.node_body.as_ref() else {
        return Err(anyhow::anyhow!("Iceberg writer resolver input must be a Merge node").into());
    };
    let normal_fragment_id = normal_merge.upstream_fragment_id;
    let resolver_fragment_id = resolver_merge.upstream_fragment_id;
    let resolver_fragment = fragment_infos.get(&resolver_fragment_id).ok_or_else(|| {
        anyhow::anyhow!(
            "Iceberg writer resolver fragment {} was not rendered",
            resolver_fragment_id
        )
    })?;
    if !contains_resolver(&resolver_fragment.nodes) {
        return Err(anyhow::anyhow!(
            "Iceberg writer resolver input fragment {} has no resolver node",
            resolver_fragment_id
        )
        .into());
    }

    let mut long_running = HashSet::from([writer_fragment.fragment_id]);
    let mut pending = vec![writer_fragment.fragment_id];
    while let Some(fragment_id) = pending.pop() {
        for downstream in downstreams.get(&fragment_id).into_iter().flatten() {
            if long_running.insert(downstream.downstream_fragment_id) {
                pending.push(downstream.downstream_fragment_id);
            }
        }
    }
    let normal_fragment_ids: HashSet<_> = fragment_infos
        .keys()
        .filter(|fragment_id| {
            !long_running.contains(*fragment_id) && **fragment_id != resolver_fragment_id
        })
        .copied()
        .collect();
    if !normal_fragment_ids.contains(&normal_fragment_id) {
        return Err(anyhow::anyhow!(
            "normal input fragment {} was classified as long-running",
            normal_fragment_id
        )
        .into());
    }

    let resolver = build_activation(
        resolver_fragment_id,
        HashSet::from([resolver_fragment_id]),
        writer_fragment,
        rendered,
        downstreams,
        partial_graph_id,
        worker_nodes,
    )?;
    let normal_input = build_activation(
        normal_fragment_id,
        normal_fragment_ids,
        writer_fragment,
        rendered,
        downstreams,
        partial_graph_id,
        worker_nodes,
    )?;
    let long_running_node_actors = InflightFragmentInfo::actor_ids_to_collect(
        fragment_infos
            .values()
            .filter(|fragment| long_running.contains(&fragment.fragment_id)),
    );

    Ok(IcebergV3StaticActors {
        normal_input: NormalInputActivation {
            node_actors: normal_input.node_actors,
            actors_to_create: normal_input.actors_to_create,
            attach_mutation: normal_input.attach_mutation,
            detach_mutation: normal_input.detach_mutation,
        },
        resolver: ResolverActivation {
            node_actors: resolver.node_actors,
            actors_to_create: resolver.actors_to_create,
            attach_mutation: resolver.attach_mutation,
            detach_mutation: resolver.detach_mutation,
        },
        long_running_node_actors,
    })
}

struct Activation {
    node_actors: HashMap<WorkerId, HashSet<ActorId>>,
    actors_to_create: StreamJobActorsToCreate,
    attach_mutation: Mutation,
    detach_mutation: Mutation,
}

fn build_activation(
    edge_fragment_id: FragmentId,
    fragment_ids: HashSet<FragmentId>,
    writer_fragment: &InflightFragmentInfo,
    rendered: &RenderedIndependentJobActors,
    downstreams: &FragmentDownstreamRelation,
    partial_graph_id: PartialGraphId,
    worker_nodes: &HashMap<WorkerId, WorkerNode>,
) -> MetaResult<Activation> {
    let fragment_infos = &rendered.fragment_infos;
    let relation = downstreams
        .get(&edge_fragment_id)
        .into_iter()
        .flatten()
        .find(|relation| relation.downstream_fragment_id == writer_fragment.fragment_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "fragment {} has no edge to writer fragment {}",
                edge_fragment_id,
                writer_fragment.fragment_id
            )
        })?;
    let mut builder = FragmentEdgeBuilder::new(
        fragment_ids
            .iter()
            .copied()
            .chain([writer_fragment.fragment_id])
            .map(|fragment_id| {
                let fragment = &fragment_infos[&fragment_id];
                (
                    fragment_id,
                    EdgeBuilderFragmentInfo::from_inflight_with_worker_nodes(
                        fragment,
                        partial_graph_id,
                        worker_nodes,
                    ),
                )
            }),
    );
    for fragment_id in &fragment_ids {
        for relation in downstreams.get(fragment_id).into_iter().flatten() {
            if fragment_ids.contains(&relation.downstream_fragment_id) {
                builder.add_edge(*fragment_id, relation);
            }
        }
    }
    builder.add_edge(edge_fragment_id, relation);
    let mut edges = builder.build();
    let actors_to_create = edges.collect_actors_to_create(fragment_ids.iter().map(|fragment_id| {
        let fragment = &fragment_infos[fragment_id];
        (
            fragment.fragment_id,
            &fragment.nodes,
            fragment.actors.iter().map(|(actor_id, actor)| {
                let stream_actor = rendered.stream_actors[&fragment.fragment_id]
                    .iter()
                    .find(|stream_actor| stream_actor.actor_id == *actor_id)
                    .expect("rendered actor should exist");
                (stream_actor, actor.worker_id)
            }),
            vec![],
        )
    }));
    let mut attach_merge_updates = Vec::with_capacity(writer_fragment.actors.len());
    for actor_id in writer_fragment.actors.keys().copied() {
        let (mut upstreams, dispatchers) =
            edges.take_actor_edges(writer_fragment.fragment_id, actor_id);
        if !dispatchers.is_empty() {
            return Err(anyhow::anyhow!(
                "Iceberg writer actor {} unexpectedly has phase dispatchers: {:?}",
                actor_id,
                dispatchers
            )
            .into());
        }
        let added_upstream_actors = upstreams.remove(&edge_fragment_id).ok_or_else(|| {
            anyhow::anyhow!(
                "Iceberg writer actor {} has no upstream actors from fragment {}",
                actor_id,
                edge_fragment_id
            )
        })?;
        if !upstreams.is_empty() {
            return Err(anyhow::anyhow!(
                "Iceberg writer actor {} has unexpected phase upstreams: {:?}",
                actor_id,
                upstreams.keys().collect::<Vec<_>>()
            )
            .into());
        }
        attach_merge_updates.push(MergeUpdate {
            actor_id,
            upstream_fragment_id: edge_fragment_id,
            new_upstream_fragment_id: None,
            added_upstream_actors: added_upstream_actors.into_values().collect(),
            removed_upstream_actor_id: vec![],
        });
    }
    if !edges.is_empty() {
        return Err(anyhow::anyhow!("unconsumed Iceberg phase edges: {:?}", edges).into());
    }
    let attach_mutation = Mutation::Update(UpdateMutation {
        merge_update: attach_merge_updates.clone(),
        ..Default::default()
    });
    let detach_mutation = Mutation::Update(UpdateMutation {
        merge_update: attach_merge_updates
            .into_iter()
            .map(|update| MergeUpdate {
                actor_id: update.actor_id,
                upstream_fragment_id: edge_fragment_id,
                new_upstream_fragment_id: None,
                added_upstream_actors: vec![],
                removed_upstream_actor_id: update
                    .added_upstream_actors
                    .into_iter()
                    .map(|actor| actor.actor_id)
                    .collect(),
            })
            .collect(),
        dropped_actors: fragment_ids
            .iter()
            .flat_map(|fragment_id| fragment_infos[fragment_id].actors.keys().copied())
            .collect(),
        ..Default::default()
    });
    let node_actors = InflightFragmentInfo::actor_ids_to_collect(
        fragment_ids
            .iter()
            .map(|fragment_id| &fragment_infos[fragment_id]),
    );
    Ok(Activation {
        node_actors,
        actors_to_create,
        attach_mutation,
        detach_mutation,
    })
}

/// Split the dormant resolver from an Iceberg graph after actor placement and before edge
/// construction. The complete relation map must be used during rendering so a no-shuffle
/// resolver edge participates in actor alignment.
pub(super) fn partition_resolver(
    mut rendered: RenderedIndependentJobActors,
    downstreams: &mut FragmentDownstreamRelation,
) -> MetaResult<(RenderedIndependentJobActors, RenderedResolver)> {
    let writer_fragment = rendered
        .fragment_infos
        .values()
        .find(|fragment| find_writer_node(&fragment.nodes).is_some())
        .ok_or_else(|| anyhow::anyhow!("Iceberg V3 graph has no pk-index writer"))?;
    let writer_fragment_id = writer_fragment.fragment_id;
    let writer_node = find_writer_node(&writer_fragment.nodes)
        .expect("writer fragment was selected by its writer node");
    let [_, resolver_input] = writer_node.input.as_slice() else {
        return Err(anyhow::anyhow!("Iceberg writer must have exactly two inputs").into());
    };
    let Some(NodeBody::Merge(resolver_merge)) = resolver_input.node_body.as_ref() else {
        return Err(anyhow::anyhow!("Iceberg writer resolver input must be a Merge node").into());
    };
    let resolver_fragment_id = resolver_merge.upstream_fragment_id;

    let resolver_fragment = rendered
        .fragment_infos
        .remove(&resolver_fragment_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Iceberg writer resolver fragment {} was not rendered",
                resolver_fragment_id
            )
        })?;
    if !contains_resolver(&resolver_fragment.nodes) {
        return Err(anyhow::anyhow!(
            "Iceberg writer resolver input fragment {} has no resolver node",
            resolver_fragment_id
        )
        .into());
    }
    rendered
        .stream_actors
        .remove(&resolver_fragment_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Iceberg writer resolver fragment {} has no rendered actors",
                resolver_fragment_id
            )
        })?;

    {
        let relations = downstreams.get_mut(&resolver_fragment_id).ok_or_else(|| {
            anyhow::anyhow!(
                "resolver fragment {} has no downstream relation",
                resolver_fragment_id
            )
        })?;
        let [relation] = relations.as_slice() else {
            return Err(anyhow::anyhow!(
                "expected resolver fragment {} to have exactly one downstream relation, found {}",
                resolver_fragment_id,
                relations.len(),
            )
            .into());
        };
        if relation.downstream_fragment_id != writer_fragment_id {
            return Err(anyhow::anyhow!(
                "resolver fragment {} points to fragment {} instead of writer fragment {}",
                resolver_fragment_id,
                relation.downstream_fragment_id,
                writer_fragment_id,
            )
            .into());
        }
        relations.remove(0)
    };
    downstreams.remove(&resolver_fragment_id);
    if downstreams
        .values()
        .flatten()
        .any(|relation| relation.downstream_fragment_id == resolver_fragment_id)
    {
        return Err(anyhow::anyhow!(
            "resolver fragment {} must not have an upstream fragment",
            resolver_fragment_id
        )
        .into());
    }

    Ok((
        rendered,
        RenderedResolver {
            fragment_info: resolver_fragment,
        },
    ))
}

fn find_writer_node(node: &PbStreamNode) -> Option<&PbStreamNode> {
    let mut found = None;
    visit_stream_node_cont(node, |node| {
        if found.is_none() && matches!(node.node_body, Some(NodeBody::IcebergWithPkIndexWriter(_)))
        {
            found = Some(node);
        }
        found.is_none()
    });
    found
}

fn contains_resolver(node: &PbStreamNode) -> bool {
    let mut found = false;
    visit_stream_node_cont(node, |node| {
        if matches!(node.node_body, Some(NodeBody::CompactionResolver(_))) {
            found = true;
        }
        !found
    });
    found
}
