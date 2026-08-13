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
use std::sync::Arc;

use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::catalog::{DatabaseId, FragmentTypeMask};
use risingwave_meta_model::WorkerId;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_pb::catalog::PbTable;
use risingwave_pb::common::{PbHostAddress, PbWorkerNode};
use risingwave_pb::data::PbDataType;
use risingwave_pb::data::data_type::PbTypeName;
use risingwave_pb::id::{ActorId, FragmentId, IcebergCompactionTaskId, SinkId};
use risingwave_pb::plan_common::{PbColumnCatalog, PbColumnDesc, PbField};
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::stream_node::{NodeBody, PbStreamKind};
use risingwave_pb::stream_plan::{
    DispatcherType as PbDispatcherType, IcebergWithPkIndexWriterNode, MergeNode, PbSinkDesc,
    PbStreamNode,
};
use risingwave_pb::stream_service::PbIcebergPkIndexSinkRole;
use risingwave_pb::stream_service::barrier_complete_response::PbIcebergPkIndexSinkMetadata;

use super::super::render::*;
use super::*;
use crate::barrier::rpc::to_partial_graph_id;
use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::manager::iceberg_compaction::CompactionResolveCompletion;

fn writer_node(sink_id: u32, table_id: u32, dispatcher_type: PbDispatcherType) -> PbStreamNode {
    let column = |name: &str, type_name: PbTypeName| PbColumnCatalog {
        column_desc: Some(PbColumnDesc {
            column_type: Some(PbDataType {
                type_name: type_name as i32,
                ..Default::default()
            }),
            name: name.to_owned(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let field = |name: &str, type_name: PbTypeName| PbField {
        data_type: Some(PbDataType {
            type_name: type_name as i32,
            ..Default::default()
        }),
        name: name.to_owned(),
    };
    let dormant_right = PbStreamNode {
        identity: "IcebergCompactionResolverEdge".to_owned(),
        fields: vec![
            field("pk1", PbTypeName::Int64),
            field("pk2", PbTypeName::Int64),
            field("file_path", PbTypeName::Varchar),
            field("position", PbTypeName::Int64),
        ],
        stream_kind: PbStreamKind::AppendOnly as i32,
        node_body: Some(NodeBody::Merge(Box::new(MergeNode {
            upstream_fragment_id: FragmentId::new(0),
            upstream_dispatcher_type: dispatcher_type as i32,
            allow_no_initial_upstream: true,
            ..Default::default()
        }))),
        ..Default::default()
    };
    PbStreamNode {
        operator_id: 42u64.into(),
        input: vec![PbStreamNode::default(), dormant_right],
        stream_key: vec![0],
        identity: "IcebergWithPkIndexWriter".to_owned(),
        node_body: Some(NodeBody::IcebergWithPkIndexWriter(Box::new(
            IcebergWithPkIndexWriterNode {
                sink_desc: Some(PbSinkDesc {
                    id: sink_id.into(),
                    downstream_pk: vec![0, 1],
                    ..Default::default()
                }),
                pk_index_table: Some(PbTable {
                    id: table_id.into(),
                    columns: vec![
                        column("pk1", PbTypeName::Int64),
                        column("pk2", PbTypeName::Int64),
                        column("file_path", PbTypeName::Varchar),
                        column("position", PbTypeName::Int64),
                    ],
                    ..Default::default()
                }),
            },
        ))),
        ..Default::default()
    }
}

fn vnode_bitmap(vnode_count: usize, owned: &[usize]) -> risingwave_common::bitmap::Bitmap {
    let mut builder = BitmapBuilder::zeroed(vnode_count);
    for &v in owned {
        builder.set(v, true);
    }
    builder.finish()
}

fn writer_fragment() -> InflightFragmentInfo {
    let vnode_count = 4;
    InflightFragmentInfo {
        fragment_id: FragmentId::new(100),
        distribution_type: DistributionType::Hash,
        fragment_type_mask: FragmentTypeMask::empty(),
        vnode_count,
        nodes: writer_node(7, 99, PbDispatcherType::Hash),
        actors: HashMap::from([
            (
                ActorId::new(10),
                InflightActorInfo {
                    worker_id: WorkerId::new(2),
                    vnode_bitmap: Some(vnode_bitmap(vnode_count, &[0, 1])),
                    splits: vec![],
                },
            ),
            (
                ActorId::new(11),
                InflightActorInfo {
                    worker_id: WorkerId::new(1),
                    vnode_bitmap: Some(vnode_bitmap(vnode_count, &[2, 3])),
                    splits: vec![],
                },
            ),
        ]),
        state_table_ids: HashSet::new(),
    }
}

fn single_writer_fragment() -> InflightFragmentInfo {
    InflightFragmentInfo {
        fragment_id: FragmentId::new(100),
        distribution_type: DistributionType::Single,
        fragment_type_mask: FragmentTypeMask::empty(),
        vnode_count: 4,
        nodes: writer_node(7, 99, PbDispatcherType::Simple),
        actors: HashMap::from([(
            ActorId::new(10),
            InflightActorInfo {
                worker_id: WorkerId::new(2),
                vnode_bitmap: None,
                splits: vec![],
            },
        )]),
        state_table_ids: HashSet::new(),
    }
}

fn worker_nodes() -> HashMap<WorkerId, PbWorkerNode> {
    [WorkerId::new(1), WorkerId::new(2)]
        .into_iter()
        .map(|worker_id| {
            (
                worker_id,
                PbWorkerNode {
                    id: worker_id,
                    host: Some(PbHostAddress {
                        host: "127.0.0.1".to_owned(),
                        port: 5688 + worker_id.as_raw_id() as i32,
                    }),
                    ..Default::default()
                },
            )
        })
        .collect()
}

#[test]
fn test_build_resolver_node_has_writer_schema_without_apply_root() {
    let writer_fragment = writer_fragment();
    let nodes = build_resolver_stream_node(
        &writer_fragment,
        IcebergCompactionTaskId::new(42),
        vec!["s3://out/o.parquet".to_owned()],
        vec!["s3://in/i.parquet".to_owned()],
        777,
    )
    .unwrap();

    let NodeBody::CompactionResolver(resolver) = nodes.resolver.node_body.as_ref().unwrap() else {
        panic!("expected resolver root");
    };
    assert_eq!(resolver.sink_desc.as_ref().unwrap().id.as_raw_id(), 7);
    assert_eq!(resolver.pk_index_table.as_ref().unwrap().id.as_raw_id(), 99);
    assert_eq!(resolver.output_data_file_paths, vec!["s3://out/o.parquet"]);
    assert_eq!(resolver.input_data_file_paths, vec!["s3://in/i.parquet"]);
    assert_eq!(resolver.read_snapshot_id, 777);
    assert_eq!(
        resolver.compaction_task_id,
        IcebergCompactionTaskId::new(42)
    );
    assert!(nodes.resolver.input.is_empty());
    assert_eq!(nodes.pk_dist_key_indices, vec![0, 1]);
    assert_eq!(nodes.output_column_count, 4);
}

#[test]
fn test_build_resolver_node_rejects_invalid_dormant_right_input() {
    let mut missing_right = writer_fragment();
    missing_right.nodes.input.pop();
    let error = build_resolver_stream_node(
        &missing_right,
        IcebergCompactionTaskId::new(42),
        vec![],
        vec![],
        1,
    )
    .err()
    .expect("writer without a dormant right input should fail");
    assert!(
        error
            .to_string()
            .contains("normal and dormant resolver inputs")
    );

    let mut active_right = writer_fragment();
    let Some(NodeBody::Merge(merge)) = active_right.nodes.input[1].node_body.as_mut() else {
        unreachable!("test writer has a Merge right input")
    };
    merge.allow_no_initial_upstream = false;
    let error = build_resolver_stream_node(
        &active_right,
        IcebergCompactionTaskId::new(42),
        vec![],
        vec![],
        1,
    )
    .err()
    .expect("non-dormant right input should fail");
    assert!(error.to_string().contains("allow no initial upstream"));
}

#[test]
fn test_render_singleton_resolver_attaches_directly_to_all_writer_actors() {
    let writer_fragment = writer_fragment();
    let resolver_fragment_id = FragmentId::new(500);
    let nodes = build_resolver_stream_node(
        &writer_fragment,
        IcebergCompactionTaskId::new(42),
        vec![],
        vec![],
        1,
    )
    .unwrap();
    let actor_id_gen = std::sync::atomic::AtomicU32::new(1000);
    let render = render_resolver_fragment(
        &writer_fragment,
        resolver_fragment_id,
        nodes,
        to_partial_graph_id(DatabaseId::new(1), None),
        &worker_nodes(),
        &actor_id_gen,
        "test",
    )
    .unwrap();

    assert_eq!(render.fragment_infos.len(), 1);
    assert!(render.state_table_ids.is_empty());
    assert_eq!(render.resolver_actor_ids.len(), 1);
    let resolver_fragment = &render.fragment_infos[&resolver_fragment_id];
    assert_eq!(
        resolver_fragment.distribution_type,
        DistributionType::Single
    );
    assert!(resolver_fragment.state_table_ids.is_empty());

    let resolver_actor_id = render.resolver_actor_ids[0];
    assert!(
        render.actors_to_create[&WorkerId::new(2)].contains_key(&resolver_fragment_id),
        "resolver placement follows the smallest writer actor"
    );
    let resolver_actor = render
        .actors_to_create
        .values()
        .flat_map(|fragments| fragments.values())
        .flat_map(|(_, actors, _)| actors)
        .find(|(actor, _, _)| actor.actor_id == resolver_actor_id)
        .expect("resolver actor should be created");
    assert_eq!(
        resolver_actor.0.actor_id, resolver_actor_id,
        "exactly the singleton resolver is created"
    );
    let resolver_dispatcher = &resolver_actor.2[0];
    assert_eq!(
        resolver_dispatcher.get_type().unwrap(),
        PbDispatcherType::Hash
    );
    assert_eq!(resolver_dispatcher.dist_key_indices, vec![0, 1]);
    assert_eq!(
        resolver_dispatcher.dispatcher_id,
        writer_fragment.fragment_id
    );
    assert_eq!(
        resolver_dispatcher.downstream_actor_id.len(),
        writer_fragment.actors.len()
    );

    let Mutation::Update(attach) = &render.attach_mutation else {
        panic!("B1 must carry Update");
    };
    assert!(attach.dropped_actors.is_empty());
    assert_eq!(attach.merge_update.len(), writer_fragment.actors.len());
    for update in &attach.merge_update {
        assert_eq!(update.upstream_fragment_id, FragmentId::new(0));
        assert_eq!(update.new_upstream_fragment_id, Some(resolver_fragment_id));
        assert_eq!(update.added_upstream_actors.len(), 1);
        assert_eq!(update.added_upstream_actors[0].actor_id, resolver_actor_id);
    }

    let Mutation::Update(detach) = &render.detach_mutation else {
        panic!("B2 must carry Update");
    };
    assert_eq!(detach.dropped_actors, vec![resolver_actor_id]);
    assert_eq!(detach.merge_update.len(), writer_fragment.actors.len());
    for update in &detach.merge_update {
        assert_eq!(update.upstream_fragment_id, resolver_fragment_id);
        assert_eq!(update.new_upstream_fragment_id, Some(FragmentId::new(0)));
        assert!(update.added_upstream_actors.is_empty());
        assert_eq!(update.removed_upstream_actor_id, vec![resolver_actor_id]);
    }
}

#[test]
fn test_render_hash_dispatcher_to_single_writer_without_persisted_bitmap() {
    let writer_fragment = single_writer_fragment();
    let resolver_fragment_id = FragmentId::new(500);
    let nodes = build_resolver_stream_node(
        &writer_fragment,
        IcebergCompactionTaskId::new(42),
        vec![],
        vec![],
        1,
    )
    .unwrap();
    let render = render_resolver_fragment(
        &writer_fragment,
        resolver_fragment_id,
        nodes,
        to_partial_graph_id(DatabaseId::new(1), None),
        &worker_nodes(),
        &std::sync::atomic::AtomicU32::new(1000),
        "test",
    )
    .unwrap();

    let resolver_actor_id = render.resolver_actor_ids[0];
    let dispatcher = render
        .actors_to_create
        .values()
        .flat_map(|fragments| fragments.values())
        .flat_map(|(_, actors, _)| actors)
        .find(|(actor, _, _)| actor.actor_id == resolver_actor_id)
        .and_then(|(_, _, dispatchers)| dispatchers.first())
        .expect("resolver dispatcher should be rendered");
    assert_eq!(dispatcher.get_type().unwrap(), PbDispatcherType::Simple);
    assert!(dispatcher.dist_key_indices.is_empty());
    assert_eq!(dispatcher.downstream_actor_id, vec![ActorId::new(10)]);
    assert_eq!(
        writer_fragment.actors[&ActorId::new(10)].vnode_bitmap,
        None,
        "rendering must not mutate persisted writer actor metadata"
    );
}

fn dummy_overwrite() -> OverwriteInput {
    OverwriteInput {
        sink_id: SinkId::new(7),
        schema_id: 3,
        partition_spec_id: 4,
        output_files: vec![],
        input_file_paths: vec!["in.parquet".to_owned()],
        read_snapshot_id: 777,
    }
}

fn dummy_render() -> CompactionResolverRenderResult {
    let fragment = writer_fragment();
    let resolver_fragment_id = FragmentId::new(500);
    render_resolver_fragment(
        &fragment,
        resolver_fragment_id,
        build_resolver_stream_node(
            &fragment,
            IcebergCompactionTaskId::new(42),
            vec![],
            vec![],
            1,
        )
        .unwrap(),
        to_partial_graph_id(DatabaseId::new(1), None),
        &worker_nodes(),
        &std::sync::atomic::AtomicU32::new(1000),
        "test",
    )
    .unwrap()
}

fn test_completion() -> Arc<CompactionResolveCompletion> {
    Arc::new(CompactionResolveCompletion::for_test(
        SinkId::new(7),
        IcebergCompactionTaskId::new(42),
    ))
}

#[test]
fn test_new_job_is_resolving_with_adjacent_b2_epoch() {
    let job = CompactionResolveJobControl::new(
        SinkId::new(7),
        IcebergCompactionTaskId::new(42),
        test_completion(),
        30,
        dummy_overwrite(),
        dummy_render(),
    );
    assert!(job.fragment_infos().is_some());
    let Phase::Resolving { b2_prev_epoch, .. } = &job.phase else {
        panic!("expected Resolving phase");
    };
    assert_eq!(*b2_prev_epoch, 30);
    assert_eq!(job.task_id(), IcebergCompactionTaskId::new(42));
}

#[test]
fn test_finish_b2_collects_main_response_and_takes_overwrite_once() {
    let mut job = CompactionResolveJobControl::new(
        SinkId::new(7),
        IcebergCompactionTaskId::new(42),
        test_completion(),
        60,
        dummy_overwrite(),
        dummy_render(),
    );
    let resolver_actor = job.resolver_actor_ids()[0];
    assert!(
        job.node_actors()
            .unwrap()
            .values()
            .any(|actor_ids| actor_ids.contains(&resolver_actor)),
        "the terminal B2 collection set must still include the resolver"
    );
    let response = risingwave_pb::stream_service::BarrierCompleteResponse {
        epoch: 60,
        iceberg_pk_index_sink_metadata: vec![PbIcebergPkIndexSinkMetadata {
            reporter_actor_id: resolver_actor,
            sink_id: SinkId::new(7),
            prev_epoch: 60,
            role: PbIcebergPkIndexSinkRole::CompactionResolver as i32,
            metadata: None,
        }],
        ..Default::default()
    };

    let overwrite = job.finish_b2(60, [&response].into_iter()).unwrap();
    assert_eq!(overwrite.prev_epoch, 60);
    assert_eq!(overwrite.compaction.as_ref().unwrap().schema_id, 3);
    assert_eq!(overwrite.compaction.as_ref().unwrap().partition_spec_id, 4);
    assert!(overwrite.reports.is_empty());
    assert!(matches!(job.phase, Phase::Committing { b2_prev_epoch: 60 }));
    assert!(job.finish_b2(60, [&response].into_iter()).is_none());
}

#[test]
fn test_b2_commit_finishes_without_control_rpc() {
    let completion = test_completion();
    let mut job = CompactionResolveJobControl::new(
        SinkId::new(7),
        IcebergCompactionTaskId::new(42),
        completion,
        60,
        dummy_overwrite(),
        dummy_render(),
    );
    let response = risingwave_pb::stream_service::BarrierCompleteResponse {
        epoch: 60,
        ..Default::default()
    };
    assert!(job.finish_b2(60, [&response].into_iter()).is_some());
    assert!(job.on_main_graph_committed(60));
}
