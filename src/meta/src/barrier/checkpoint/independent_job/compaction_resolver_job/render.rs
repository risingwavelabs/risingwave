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
use risingwave_common::catalog::{FragmentTypeMask, TableId};
use risingwave_common::id::JobId;
use risingwave_common::util::epoch::EpochPair;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_meta_model::{DispatcherType, WorkerId};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::id::{ActorId, FragmentId, PartialGraphId};
use risingwave_pb::plan_common::PbExprContext;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    CompactionResolverNode, DispatcherType as PbDispatcherType, IcebergWithPkIndexWriterNode,
    MergeNode, PbDispatchOutputMapping, PbStreamNode,
};

use crate::MetaResult;
use crate::barrier::edge_builder::{EdgeBuilderFragmentInfo, FragmentEdgeBuilder};
use crate::barrier::partial_graph::PartialGraphStat;
use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::controller::scale::{ComponentFragmentAligner, EnsembleActorTemplate};
use crate::model::{DownstreamFragmentRelation, StreamActor, StreamJobActorsToCreate};

/// Rendered resolver/apply pipeline, produced by
/// [`render_resolver_fragment`] and consumed by [`CompactionResolveJobControl::new`].
#[derive(Debug)]
pub(crate) struct CompactionResolverRenderResult {
    pub fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
    pub node_actors: HashMap<WorkerId, HashSet<ActorId>>,
    pub state_table_ids: HashSet<TableId>,
    pub actors_to_create: StreamJobActorsToCreate,
    /// All rendered resolver and apply actor ids, used by the initial `Add` and terminal `Stop`.
    pub actor_ids: Vec<ActorId>,
}

pub(crate) struct CompactionResolverStreamNodes {
    pub(super) resolver: PbStreamNode,
    pub(super) apply: PbStreamNode,
    pub(super) pk_dist_key_indices: Vec<u32>,
    pub(super) output_column_count: usize,
}

/// Render a singleton resolver fragment and a vnode-aligned compaction-apply fragment, connected by
/// a hash dispatcher over every PK column in the resolver output.
pub(crate) fn render_resolver_fragment(
    writer_fragment: &InflightFragmentInfo,
    resolver_fragment_id: FragmentId,
    apply_fragment_id: FragmentId,
    pk_index_table_id: TableId,
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

    let template = EnsembleActorTemplate::from_existing_inflight_fragment(writer_fragment);
    let aligner = ComponentFragmentAligner::new_persistent(&template, actor_id_generator);
    let apply_assignments = aligner.align_component_actor(writer_fragment.distribution_type);

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
    let resolver_actors = HashMap::from([(
        resolver_actor_id,
        InflightActorInfo {
            worker_id: resolver_placement.worker_id,
            vnode_bitmap: None,
            splits: vec![],
        },
    )]);

    let mut apply_stream_actors = Vec::with_capacity(apply_assignments.len());
    let mut apply_actors = HashMap::new();
    for (&actor_id, (worker_id, vnode_bitmap)) in &apply_assignments {
        apply_actors.insert(
            actor_id,
            InflightActorInfo {
                worker_id: *worker_id,
                vnode_bitmap: vnode_bitmap.clone(),
                splits: vec![],
            },
        );
        apply_stream_actors.push(StreamActor {
            actor_id,
            fragment_id: apply_fragment_id,
            vnode_bitmap: vnode_bitmap.clone(),
            mview_definition: definition.to_owned(),
            expr_context: Some(PbExprContext {
                time_zone: "UTC".to_owned(),
                strict_mode: false,
            }),
            config_override: "".into(),
        });
    }

    let mut state_table_ids = HashSet::new();
    state_table_ids.insert(pk_index_table_id);

    let resolver_fragment = InflightFragmentInfo {
        fragment_id: resolver_fragment_id,
        distribution_type: DistributionType::Single,
        fragment_type_mask: FragmentTypeMask::empty(),
        vnode_count: 1,
        nodes: stream_nodes.resolver,
        actors: resolver_actors,
        state_table_ids: HashSet::new(),
    };
    let apply_fragment = InflightFragmentInfo {
        fragment_id: apply_fragment_id,
        distribution_type: writer_fragment.distribution_type,
        fragment_type_mask: FragmentTypeMask::empty(),
        vnode_count: writer_fragment.vnode_count,
        nodes: stream_nodes.apply,
        actors: apply_actors,
        state_table_ids: state_table_ids.clone(),
    };
    let mut fragment_infos = HashMap::new();
    fragment_infos.insert(resolver_fragment_id, resolver_fragment);
    fragment_infos.insert(apply_fragment_id, apply_fragment);

    let mut builder = FragmentEdgeBuilder::new(fragment_infos.values().map(|f| {
        (
            f.fragment_id,
            EdgeBuilderFragmentInfo::from_inflight_with_worker_nodes(
                f,
                partial_graph_id,
                worker_nodes,
            ),
        )
    }));
    builder.add_edge(
        resolver_fragment_id,
        &DownstreamFragmentRelation {
            downstream_fragment_id: apply_fragment_id,
            dispatcher_type: DispatcherType::Hash,
            dist_key_indices: stream_nodes.pk_dist_key_indices,
            output_mapping: PbDispatchOutputMapping::identical(stream_nodes.output_column_count),
        },
    );
    let mut edges = builder.build();

    let stream_actors = HashMap::from([
        (resolver_fragment_id, vec![resolver_actor]),
        (apply_fragment_id, apply_stream_actors),
    ]);
    let actors_to_create = edges.collect_actors_to_create(fragment_infos.values().map(|f| {
        (
            f.fragment_id,
            &f.nodes,
            f.actors.iter().map(|(actor_id, actor)| {
                let sa = stream_actors[&f.fragment_id]
                    .iter()
                    .find(|a| a.actor_id == *actor_id)
                    .expect("rendered actor should exist");
                (sa, actor.worker_id)
            }),
            vec![],
        )
    }));

    let node_actors = InflightFragmentInfo::actor_ids_to_collect(fragment_infos.values());
    let actor_ids = stream_actors
        .values()
        .flatten()
        .map(|actor| actor.actor_id)
        .collect();

    Ok(CompactionResolverRenderResult {
        fragment_infos,
        node_actors,
        state_table_ids,
        actors_to_create,
        actor_ids,
    })
}

/// Build the roots of the resolver and compaction-apply fragments. The apply root consumes a Merge
/// whose upstream is the resolver fragment; [`render_resolver_fragment`] materializes the matching
/// hash dispatcher and actor-upstream metadata.
pub(crate) fn build_resolver_stream_node(
    writer_fragment: &InflightFragmentInfo,
    resolver_fragment_id: FragmentId,
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

    // The CompactionResolver's real output schema is
    // `[pk_columns.., file_path: Varchar, position: Int64]` (see the executor's output-chunk
    // contract). Its declared `fields` MUST match, or `schema_check` panics at the first pk column.
    // The pk-index table's columns are exactly `[pk_columns.., file_path, position]`, so its leading
    // `pk_count + 2` columns give the pk/file_path/position fields with their exact types.
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
    let resolver_fields: Vec<risingwave_pb::plan_common::PbField> = pk_index_columns
        .iter()
        .take(pk_count + 2)
        .map(|col| {
            let column_desc = col
                .column_desc
                .as_ref()
                .expect("pk-index table column carries a column_desc");
            risingwave_pb::plan_common::PbField {
                data_type: column_desc.column_type.clone(),
                name: column_desc.name.clone(),
            }
        })
        .collect();

    let pk_dist_key_indices: Vec<u32> = (0..pk_count as u32).collect();

    let resolver_node = PbStreamNode {
        // Keep executor identity distinct from CompactionApply for metrics and diagnostics.
        operator_id: writer_node.operator_id + 1,
        input: vec![],
        stream_key: pk_dist_key_indices.clone(),
        stream_kind: writer_node.stream_kind,
        identity: "CompactionResolver".to_owned(),
        fields: resolver_fields.clone(),
        node_body: Some(NodeBody::CompactionResolver(Box::new(
            CompactionResolverNode {
                sink_desc: sink_desc.clone(),
                pk_index_table: pk_index_table.clone(),
                output_data_file_paths,
                input_data_file_paths,
                read_snapshot_id,
            },
        ))),
    };

    let merge_node = PbStreamNode {
        operator_id: writer_node.operator_id + 2,
        input: vec![],
        stream_key: pk_dist_key_indices.clone(),
        stream_kind: writer_node.stream_kind,
        identity: "MergeExecutor".to_owned(),
        fields: resolver_fields.clone(),
        node_body: Some(NodeBody::Merge(Box::new(MergeNode {
            upstream_fragment_id: resolver_fragment_id,
            upstream_dispatcher_type: PbDispatcherType::Hash as i32,
            ..Default::default()
        }))),
    };

    let apply_node = PbStreamNode {
        operator_id: writer_node.operator_id,
        input: vec![merge_node],
        stream_key: pk_dist_key_indices.clone(),
        stream_kind: writer_node.stream_kind,
        identity: "CompactionApply".to_owned(),
        fields: resolver_fields.clone(),
        node_body: Some(NodeBody::IcebergWithPkIndexWriter(Box::new(
            IcebergWithPkIndexWriterNode {
                sink_desc,
                pk_index_table,
                compaction_apply: true,
            },
        ))),
    };

    Ok(CompactionResolverStreamNodes {
        resolver: resolver_node,
        apply: apply_node,
        pk_dist_key_indices,
        output_column_count: resolver_fields.len(),
    })
}

/// Extract the object-store paths of the compaction output data files (used to seed the
/// `CompactionResolver`'s output scan). `SerializedDataFile`'s `file_path` field is not publicly
/// accessible, so read it through its `Serialize` form.
pub(crate) fn output_file_paths(files: &[SerializedDataFile]) -> MetaResult<Vec<String>> {
    files
        .iter()
        .map(|f| {
            let value = serde_json::to_value(f)
                .map_err(|e| anyhow::anyhow!(e).context("serialize compaction output data file"))?;
            value
                .get("file_path")
                .and_then(|p| p.as_str())
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

// ── Barrier stats ─────────────────────────────────────────────────────────────

pub(super) struct CompactionResolveBarrierStats {
    job_id: JobId,
}

impl CompactionResolveBarrierStats {
    pub(super) fn new(job_id: JobId) -> Self {
        Self { job_id }
    }
}

impl PartialGraphStat for CompactionResolveBarrierStats {
    fn observe_barrier_latency(&self, _epoch: EpochPair, _barrier_latency_secs: f64) {
        let _ = self.job_id;
    }

    fn observe_barrier_num(&self, _inflight_barrier_num: usize, _collected_barrier_num: usize) {}
}
