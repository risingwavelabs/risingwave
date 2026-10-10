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
use risingwave_pb::id::{ActorId, FragmentId, PartialGraphId};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{PbStreamNode, UpdateMutation};

use super::super::RenderedIndependentJobActors;
use crate::MetaResult;
use crate::barrier::edge_builder::FragmentEdgeBuilder;
use crate::barrier::rpc::ControlStreamManager;
use crate::controller::fragment::InflightFragmentInfo;
use crate::model::{
    DownstreamFragmentRelation, FragmentDownstreamRelation, StreamActor, StreamJobActorsToCreate,
};

#[derive(Debug)]
pub(super) struct CompactionTransition {
    pub actors_to_create: StreamJobActorsToCreate,
    pub mutation: UpdateMutation,
}

/// Actor-level plans generated from the current graph for one compaction round.
#[derive(Debug)]
pub(super) struct CompactionTransitions {
    /// Actors from the normal-input, resolver, and long-running partitions.
    pub node_actors: HashMap<WorkerId, HashSet<ActorId>>,
    pub begin_compaction: CompactionTransition,
    pub end_compaction: CompactionTransition,
}

/// The resolver fragment is rendered and placed with the whole Iceberg graph, but remains
/// disconnected until a compaction command activates it.
#[derive(Debug)]
pub(super) struct RenderedResolver {
    pub fragment_info: InflightFragmentInfo,
    /// Current dormant actor definitions. Scaling must update these together with `fragment_info`.
    pub stream_actors: Vec<StreamActor>,
    pub downstream_relation: DownstreamFragmentRelation,
}

/// Actor-independent fragment partitioning shared by every compaction round. Scaling may replace
/// actors and placements, but does not change which fragments belong to each execution phase.
#[derive(Debug)]
pub(super) struct IcebergV3FragmentPartition {
    normal: HashSet<FragmentId>,
    resolver: HashSet<FragmentId>,
    long_running: HashSet<FragmentId>,
}

pub(super) fn build_compaction_transitions(
    active_fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
    active_stream_actors: &HashMap<FragmentId, Vec<StreamActor>>,
    active_downstreams: &FragmentDownstreamRelation,
    resolver: &RenderedResolver,
    partition: &IcebergV3FragmentPartition,
    partial_graph_id: PartialGraphId,
    control_stream_manager: &ControlStreamManager,
) -> MetaResult<CompactionTransitions> {
    let fragment_infos: HashMap<_, _> = active_fragment_infos
        .iter()
        .map(|(&fragment_id, info)| (fragment_id, info))
        .chain(std::iter::once((
            resolver.fragment_info.fragment_id,
            &resolver.fragment_info,
        )))
        .collect();
    let stream_actors: HashMap<_, _> = active_stream_actors
        .iter()
        .map(|(&fragment_id, actors)| (fragment_id, actors.as_slice()))
        .chain(std::iter::once((
            resolver.fragment_info.fragment_id,
            resolver.stream_actors.as_slice(),
        )))
        .collect();
    let mut downstreams = active_downstreams.clone();
    assert!(
        downstreams
            .insert(
                resolver.fragment_info.fragment_id,
                vec![resolver.downstream_relation.clone()],
            )
            .is_none(),
        "resolver relation must be absent from the active graph"
    );
    let begin_compaction = build_transition(
        &partition.normal,
        &partition.resolver,
        &partition.long_running,
        &fragment_infos,
        &stream_actors,
        &downstreams,
        partial_graph_id,
        control_stream_manager,
    )?;
    let end_compaction = build_transition(
        &partition.resolver,
        &partition.normal,
        &partition.long_running,
        &fragment_infos,
        &stream_actors,
        &downstreams,
        partial_graph_id,
        control_stream_manager,
    )?;
    let node_actors = InflightFragmentInfo::actor_ids_to_collect(fragment_infos.values().copied());

    Ok(CompactionTransitions {
        node_actors,
        begin_compaction,
        end_compaction,
    })
}

fn ensure_writer_relation(
    downstreams: &FragmentDownstreamRelation,
    edge_fragment_id: FragmentId,
    writer_fragment_id: FragmentId,
) -> MetaResult<()> {
    if downstreams
        .get(&edge_fragment_id)
        .into_iter()
        .flatten()
        .any(|relation| relation.downstream_fragment_id == writer_fragment_id)
    {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "fragment {} has no edge to writer fragment {}",
            edge_fragment_id,
            writer_fragment_id
        )
        .into())
    }
}

fn build_transition(
    stopping_fragment_ids: &HashSet<FragmentId>,
    starting_fragment_ids: &HashSet<FragmentId>,
    long_running_fragment_ids: &HashSet<FragmentId>,
    fragment_infos: &HashMap<FragmentId, &InflightFragmentInfo>,
    stream_actors: &HashMap<FragmentId, &[StreamActor]>,
    downstreams: &FragmentDownstreamRelation,
    partial_graph_id: PartialGraphId,
    control_stream_manager: &ControlStreamManager,
) -> MetaResult<CompactionTransition> {
    let (mut edges, _) = FragmentEdgeBuilder::new()
        .add_existing_fragments(
            stopping_fragment_ids
                .iter()
                .map(|fragment_id| fragment_infos[fragment_id])
                .chain(
                    long_running_fragment_ids
                        .iter()
                        .map(|fragment_id| fragment_infos[fragment_id]),
                ),
            partial_graph_id,
            control_stream_manager,
        )
        .stop_existing_fragments(stopping_fragment_ids.iter().copied())
        .add_new_fragments(
            starting_fragment_ids
                .iter()
                .map(|fragment_id| fragment_infos[fragment_id]),
            partial_graph_id,
            control_stream_manager,
        )
        .finish_fragments()
        .add_relations(downstreams)?
        .build();
    let actors_to_create =
        edges.collect_actors_to_create(starting_fragment_ids.iter().map(|fragment_id| {
            let fragment = fragment_infos[fragment_id];
            (
                fragment.fragment_id,
                &fragment.nodes,
                fragment.actors.iter().map(|(actor_id, actor)| {
                    let stream_actor = stream_actors[&fragment.fragment_id]
                        .iter()
                        .find(|stream_actor| stream_actor.actor_id == *actor_id)
                        .expect("rendered actor should exist");
                    (stream_actor, actor.worker_id)
                }),
                vec![],
            )
        }));
    let mut mutation = UpdateMutation {
        dropped_actors: stopping_fragment_ids
            .iter()
            .flat_map(|fragment_id| fragment_infos[fragment_id].actors.keys().copied())
            .collect(),
        ..Default::default()
    };
    edges.apply_to_update_mutation(&mut mutation);
    Ok(CompactionTransition {
        actors_to_create,
        mutation,
    })
}

/// Split the dormant resolver from an Iceberg graph after actor placement and before edge
/// construction. The complete relation map must be used during rendering so a no-shuffle
/// resolver edge participates in actor alignment.
pub(super) fn partition_resolver(
    mut rendered: RenderedIndependentJobActors,
    downstreams: &mut FragmentDownstreamRelation,
) -> MetaResult<(
    RenderedIndependentJobActors,
    RenderedResolver,
    IcebergV3FragmentPartition,
)> {
    // The writer's two Merge inputs identify the phase-specific entry fragments: normal
    // snapshot/log-store input on the left and the dormant compaction resolver on the right.
    let writer_fragment = rendered
        .fragment_infos
        .values()
        .find(|fragment| find_writer_node(&fragment.nodes).is_some())
        .ok_or_else(|| anyhow::anyhow!("Iceberg V3 graph has no pk-index writer"))?;
    let writer_fragment_id = writer_fragment.fragment_id;
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

    // The writer and every fragment transitively downstream of it stay alive while switching
    // phases. They form the long-running partition shared by normal execution and compaction.
    let mut long_running = HashSet::from([writer_fragment_id]);
    let mut pending = vec![writer_fragment_id];
    while let Some(fragment_id) = pending.pop() {
        for downstream in downstreams.get(&fragment_id).into_iter().flatten() {
            if long_running.insert(downstream.downstream_fragment_id) {
                pending.push(downstream.downstream_fragment_id);
            }
        }
    }
    // All remaining rendered fragments belong to normal input, except for the resolver fragment
    // that is activated only during compaction.
    let normal: HashSet<_> = rendered
        .fragment_infos
        .keys()
        .filter(|fragment_id| {
            !long_running.contains(*fragment_id) && **fragment_id != resolver_fragment_id
        })
        .copied()
        .collect();
    if !normal.contains(&normal_fragment_id) {
        return Err(anyhow::anyhow!(
            "normal input fragment {} was classified as long-running",
            normal_fragment_id
        )
        .into());
    }
    // Both phase-specific entry fragments must feed the writer directly. The partition is static
    // across compaction rounds even though scaling may replace its actors and placements.
    ensure_writer_relation(downstreams, normal_fragment_id, writer_fragment_id)?;
    ensure_writer_relation(downstreams, resolver_fragment_id, writer_fragment_id)?;
    let partition = IcebergV3FragmentPartition {
        normal,
        resolver: HashSet::from([resolver_fragment_id]),
        long_running,
    };

    // Actor placement used the complete relation map above. Detach the resolver only afterwards so
    // the initial active graph contains normal input while retaining the dormant actors and edge.
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
    let resolver_actors = rendered
        .stream_actors
        .remove(&resolver_fragment_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Iceberg writer resolver fragment {} has no rendered actors",
                resolver_fragment_id
            )
        })?;

    let downstream_relation = {
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
            stream_actors: resolver_actors,
            downstream_relation,
        },
        partition,
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
