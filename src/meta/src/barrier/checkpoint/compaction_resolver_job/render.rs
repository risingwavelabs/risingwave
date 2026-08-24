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
use std::sync::atomic::Ordering;

use iceberg::spec::SerializedDataFile;
use risingwave_common::catalog::FragmentTypeMask;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_meta_model::{DispatcherType, WorkerId};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::id::{ActorId, FragmentId, IcebergCompactionTaskId, PartialGraphId};
use risingwave_pb::plan_common::{PbExprContext, PbField};
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::update_mutation::MergeUpdate;
use risingwave_pb::stream_plan::{
    CompactionResolverNode, DispatcherType as PbDispatcherType, Dispatchers,
    PbDispatchOutputMapping, PbStreamNode, UpdateMutation,
};

use crate::MetaResult;
use crate::barrier::edge_builder::{EdgeBuilderFragmentInfo, FragmentEdgeBuilder};
use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::model::{DownstreamFragmentRelation, StreamActor, StreamJobActorsToCreate};

/// The transient singleton resolver topology attached to the database main graph.
#[derive(Debug)]
pub(crate) struct CompactionResolverRenderResult {
    pub fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
    pub node_actors: HashMap<WorkerId, HashSet<ActorId>>,
    pub actors_to_create: StreamJobActorsToCreate,
    pub attach_mutation: Mutation,
    pub detach_mutation: Mutation,
    pub resolver_actor_id: ActorId,
}

pub(crate) struct CompactionResolverStreamNodes {
    pub(super) resolver: PbStreamNode,
    pub(super) pk_dist_key_indices: Vec<u32>,
    pub(super) output_column_count: usize,
    pub(super) dispatcher_type: DispatcherType,
}

/// Render one resolver actor on the worker hosting the smallest writer actor and wire its
/// dispatcher directly to every existing writer actor in the database main graph.
pub(crate) fn render_resolver_fragment(
    writer_fragment: &InflightFragmentInfo,
    resolver_fragment_id: FragmentId,
    stream_nodes: CompactionResolverStreamNodes,
    partial_graph_id: PartialGraphId,
    worker_nodes: &HashMap<WorkerId, WorkerNode>,
    actor_id_generator: &std::sync::atomic::AtomicU32,
    definition: &str,
) -> MetaResult<CompactionResolverRenderResult> {
    let (&_writer_actor_id, resolver_placement) = writer_fragment
        .actors
        .iter()
        .min_by_key(|(actor_id, _)| *actor_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "writer fragment {} has no actor for resolver placement",
                writer_fragment.fragment_id
            )
        })?;
    let resolver_actor_id = ActorId::new(actor_id_generator.fetch_add(1, Ordering::Relaxed));
    let resolver_actor = StreamActor {
        actor_id: resolver_actor_id,
        fragment_id: resolver_fragment_id,
        vnode_bitmap: None,
        mview_definition: definition.to_owned(),
        expr_context: Some(PbExprContext {
            time_zone: "UTC".to_owned(),
            strict_mode: false,
        }),
        config_override: "".into(),
    };
    let resolver_fragment = InflightFragmentInfo {
        fragment_id: resolver_fragment_id,
        distribution_type: DistributionType::Single,
        fragment_type_mask: FragmentTypeMask::empty(),
        vnode_count: 1,
        nodes: stream_nodes.resolver,
        actors: HashMap::from([(
            resolver_actor_id,
            InflightActorInfo {
                worker_id: resolver_placement.worker_id,
                vnode_bitmap: None,
                splits: vec![],
            },
        )]),
        state_table_ids: HashSet::new(),
    };

    let writer_edge_fragment = EdgeBuilderFragmentInfo::from_inflight_with_worker_nodes(
        writer_fragment,
        partial_graph_id,
        worker_nodes,
    );
    let mut builder = FragmentEdgeBuilder::new([
        (
            resolver_fragment_id,
            EdgeBuilderFragmentInfo::from_inflight_with_worker_nodes(
                &resolver_fragment,
                partial_graph_id,
                worker_nodes,
            ),
        ),
        (writer_fragment.fragment_id, writer_edge_fragment),
    ]);
    builder.add_edge(
        resolver_fragment_id,
        &DownstreamFragmentRelation {
            downstream_fragment_id: writer_fragment.fragment_id,
            dispatcher_type: stream_nodes.dispatcher_type,
            dist_key_indices: stream_nodes.pk_dist_key_indices,
            output_mapping: PbDispatchOutputMapping::identical(stream_nodes.output_column_count),
        },
    );
    builder.replace_upstream(
        writer_fragment.fragment_id,
        FragmentId::new(0),
        resolver_fragment_id,
    );
    let mut edges = builder.build();
    let actors_to_create = edges.collect_actors_to_create(std::iter::once((
        resolver_fragment_id,
        &resolver_fragment.nodes,
        std::iter::once((&resolver_actor, resolver_placement.worker_id)),
        vec![],
    )));
    let attach_mutation = Mutation::Update(UpdateMutation {
        actor_new_dispatchers: edges
            .dispatchers
            .into_values()
            .flatten()
            .map(|(actor_id, dispatchers)| (actor_id, Dispatchers { dispatchers }))
            .collect(),
        merge_update: edges.merge_updates.into_values().flatten().collect(),
        ..Default::default()
    });
    let detach_mutation = Mutation::Update(UpdateMutation {
        merge_update: writer_fragment
            .actors
            .keys()
            .map(|&writer_actor_id| MergeUpdate {
                actor_id: writer_actor_id,
                upstream_fragment_id: resolver_fragment_id,
                new_upstream_fragment_id: Some(FragmentId::new(0)),
                added_upstream_actors: vec![],
                removed_upstream_actor_id: vec![resolver_actor_id],
            })
            .collect(),
        dropped_actors: vec![resolver_actor_id],
        ..Default::default()
    });

    let fragment_infos = HashMap::from([(resolver_fragment_id, resolver_fragment)]);
    let node_actors = InflightFragmentInfo::actor_ids_to_collect(fragment_infos.values());
    Ok(CompactionResolverRenderResult {
        fragment_infos,
        node_actors,
        actors_to_create,
        attach_mutation,
        detach_mutation,
        resolver_actor_id,
    })
}

/// Build the resolver root using the writer's canonical pk-index schema.
pub(crate) fn build_resolver_stream_node(
    writer_fragment: &InflightFragmentInfo,
    compaction_task_id: IcebergCompactionTaskId,
    output_data_file_paths: Vec<String>,
    input_data_file_paths: Vec<String>,
    read_snapshot_id: i64,
) -> MetaResult<CompactionResolverStreamNodes> {
    let writer_node = find_writer_node(&writer_fragment.nodes).ok_or_else(|| {
        anyhow::anyhow!(
            "writer fragment {} has no IcebergWithPkIndexWriter node",
            writer_fragment.fragment_id
        )
    })?;
    let NodeBody::IcebergWithPkIndexWriter(writer_body) = writer_node
        .node_body
        .as_ref()
        .expect("just matched writer node")
    else {
        unreachable!("just matched writer node body")
    };
    let sink_desc = writer_body.sink_desc.clone();
    let pk_index_table = writer_body.pk_index_table.clone();
    let pk_count = sink_desc
        .as_ref()
        .expect("writer node carries a sink_desc")
        .downstream_pk
        .len();
    let pk_index_columns = &pk_index_table
        .as_ref()
        .expect("writer node carries a pk_index_table")
        .columns;
    if pk_count == 0 || pk_index_columns.len() < pk_count + 2 {
        return Err(anyhow::anyhow!(
            "invalid pk-index schema: {} PK columns require at least {} index columns, got {}",
            pk_count,
            pk_count + 2,
            pk_index_columns.len()
        )
        .into());
    }
    let resolver_fields: Vec<PbField> = pk_index_columns
        .iter()
        .take(pk_count + 2)
        .map(|col| {
            let column_desc = col
                .column_desc
                .as_ref()
                .expect("pk-index table column carries a column_desc");
            PbField {
                data_type: column_desc.column_type.clone(),
                name: column_desc.name.clone(),
            }
        })
        .collect();
    let [_, dormant_right] = writer_node.input.as_slice() else {
        return Err(anyhow::anyhow!(
            "Iceberg pk-index writer must have normal and dormant resolver inputs, got {}",
            writer_node.input.len()
        )
        .into());
    };
    let Some(NodeBody::Merge(dormant_merge)) = dormant_right.node_body.as_ref() else {
        return Err(anyhow::anyhow!(
            "Iceberg pk-index writer dormant resolver input must be a Merge node"
        )
        .into());
    };
    let dispatcher_type = match writer_fragment.distribution_type {
        DistributionType::Single => DispatcherType::Simple,
        DistributionType::Hash => DispatcherType::Hash,
    };
    if !dormant_merge.allow_no_initial_upstream
        || dormant_merge.upstream_fragment_id.as_raw_id() != 0
        || dormant_merge.upstream_dispatcher_type != PbDispatcherType::from(dispatcher_type) as i32
    {
        return Err(anyhow::anyhow!(
            "Iceberg pk-index writer dormant resolver Merge must allow no initial upstream, use fragment 0, and use the writer distribution's dispatcher"
        )
        .into());
    }
    if dormant_right.fields != resolver_fields {
        return Err(anyhow::anyhow!(
            "Iceberg pk-index writer dormant resolver input schema does not match the canonical resolver schema"
        )
        .into());
    }
    let pk_dist_key_indices: Vec<u32> = match dispatcher_type {
        DispatcherType::Hash => (0..pk_count as u32).collect(),
        DispatcherType::Simple => vec![],
        DispatcherType::Broadcast | DispatcherType::NoShuffle => unreachable!(),
    };
    let resolver = PbStreamNode {
        operator_id: writer_node.operator_id + 1,
        input: vec![],
        stream_key: pk_dist_key_indices.clone(),
        stream_kind: writer_node.stream_kind,
        identity: "CompactionResolver".to_owned(),
        fields: resolver_fields.clone(),
        node_body: Some(NodeBody::CompactionResolver(Box::new(
            CompactionResolverNode {
                sink_desc,
                pk_index_table,
                output_data_file_paths,
                input_data_file_paths,
                read_snapshot_id,
                compaction_task_id,
            },
        ))),
    };
    Ok(CompactionResolverStreamNodes {
        resolver,
        pk_dist_key_indices,
        output_column_count: resolver_fields.len(),
        dispatcher_type,
    })
}

/// Extract object-store paths from serialized compaction output data files.
pub(crate) fn output_file_paths(files: &[SerializedDataFile]) -> MetaResult<Vec<String>> {
    files
        .iter()
        .map(|file| {
            let value = serde_json::to_value(file).map_err(|error| {
                anyhow::anyhow!(error).context("serialize compaction output data file")
            })?;
            value
                .get("file_path")
                .and_then(|path| path.as_str())
                .map(str::to_owned)
                .ok_or_else(|| {
                    anyhow::anyhow!("compaction output data file is missing a file_path").into()
                })
        })
        .collect()
}

fn find_writer_node(node: &PbStreamNode) -> Option<&PbStreamNode> {
    if let Some(NodeBody::IcebergWithPkIndexWriter(_)) = &node.node_body {
        return Some(node);
    }
    node.input.iter().find_map(find_writer_node)
}
