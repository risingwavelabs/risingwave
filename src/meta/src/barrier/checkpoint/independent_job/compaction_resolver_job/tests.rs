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
use risingwave_common::catalog::{DatabaseId, FragmentTypeMask, TableId};
use risingwave_common::id::JobId;
use risingwave_common::util::epoch::{Epoch, EpochPair};
use risingwave_meta_model::WorkerId;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_pb::catalog::PbTable;
use risingwave_pb::common::{PbHostAddress, PbWorkerNode};
use risingwave_pb::data::PbDataType;
use risingwave_pb::data::data_type::PbTypeName;
use risingwave_pb::id::{ActorId, FragmentId, IcebergCompactionTaskId, SinkId};
use risingwave_pb::plan_common::{PbColumnCatalog, PbColumnDesc};
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    DispatcherType as PbDispatcherType, IcebergWithPkIndexWriterNode, PbSinkDesc, PbStreamNode,
};
use risingwave_pb::stream_service::PbIcebergPkIndexSinkRole;
use risingwave_pb::stream_service::barrier_complete_response::PbIcebergPkIndexSinkMetadata;

use super::super::render::*;
use super::*;
use crate::barrier::info::BarrierInfo;
use crate::barrier::partial_graph::CollectedBarrier;
use crate::barrier::rpc::to_partial_graph_id;
use crate::barrier::{BarrierKind, TracedEpoch};
use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::manager::iceberg_compaction::CompactionResolveCompletion;

fn writer_node(sink_id: u32, table_id: u32) -> PbStreamNode {
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
    PbStreamNode {
        operator_id: 42u64.into(),
        input: vec![],
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
                compaction_apply: false,
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

#[test]
fn test_build_resolver_and_apply_nodes() {
    let writer_fragment = InflightFragmentInfo {
        fragment_id: FragmentId::new(100),
        distribution_type: DistributionType::Hash,
        fragment_type_mask: FragmentTypeMask::empty(),
        vnode_count: 4,
        nodes: writer_node(7, 99),
        actors: HashMap::new(),
        state_table_ids: HashSet::new(),
    };

    let node = build_resolver_stream_node(
        &writer_fragment,
        FragmentId::new(500),
        vec!["s3://out/o.parquet".to_owned()],
        vec!["s3://in/i.parquet".to_owned()],
        777,
    )
    .unwrap();

    let NodeBody::IcebergWithPkIndexWriter(wb) = node.apply.node_body.as_ref().unwrap() else {
        panic!("expected writer_b root node body");
    };
    assert!(
        wb.compaction_apply,
        "CompactionApply renders with compaction_apply"
    );
    let resolver = &node.resolver;
    let NodeBody::CompactionResolver(res) = resolver.node_body.as_ref().unwrap() else {
        panic!("expected resolver leaf node body");
    };
    assert_eq!(res.sink_desc.as_ref().unwrap().id.as_raw_id(), 7);
    assert_eq!(res.pk_index_table.as_ref().unwrap().id.as_raw_id(), 99);
    assert_eq!(res.output_data_file_paths, vec!["s3://out/o.parquet"]);
    assert_eq!(res.input_data_file_paths, vec!["s3://in/i.parquet"]);
    assert_eq!(res.read_snapshot_id, 777);
    assert!(resolver.input.is_empty(), "resolver is the chain leaf");
    assert_ne!(
        node.apply.operator_id, resolver.operator_id,
        "CompactionApply and resolver need distinct executor identities"
    );
    let merge = &node.apply.input[0];
    let NodeBody::Merge(merge_body) = merge.node_body.as_ref().unwrap() else {
        panic!("expected hash merge under CompactionApply");
    };
    assert_eq!(merge_body.upstream_fragment_id, FragmentId::new(500));
    assert_eq!(
        merge_body.upstream_dispatcher_type,
        PbDispatcherType::Hash as i32
    );
    assert_eq!(merge.fields, resolver.fields);
}

#[test]
fn test_render_resolver_fragment_aligns_to_writer_vnodes() {
    let worker_id = WorkerId::new(1);
    let vnode_count = 4;
    let bmp_a = vnode_bitmap(vnode_count, &[0, 1]);
    let bmp_b = vnode_bitmap(vnode_count, &[2, 3]);
    let mut actors = HashMap::new();
    actors.insert(
        ActorId::new(10),
        InflightActorInfo {
            worker_id,
            vnode_bitmap: Some(bmp_a.clone()),
            splits: vec![],
        },
    );
    actors.insert(
        ActorId::new(11),
        InflightActorInfo {
            worker_id,
            vnode_bitmap: Some(bmp_b.clone()),
            splits: vec![],
        },
    );
    let writer_fragment = InflightFragmentInfo {
        fragment_id: FragmentId::new(100),
        distribution_type: DistributionType::Hash,
        fragment_type_mask: FragmentTypeMask::empty(),
        vnode_count,
        nodes: writer_node(7, 99),
        actors,
        state_table_ids: HashSet::from([TableId::new(99)]),
    };

    let mut worker_nodes = HashMap::new();
    worker_nodes.insert(
        worker_id,
        PbWorkerNode {
            id: (worker_id.as_raw_id() as u32).into(),
            host: Some(PbHostAddress {
                host: "127.0.0.1".to_owned(),
                port: 5688,
            }),
            ..Default::default()
        },
    );

    let pk_index_table_id = TableId::new(99);
    let resolver_fragment_id = FragmentId::new(500);
    let apply_fragment_id = FragmentId::new(501);
    let resolver_node =
        build_resolver_stream_node(&writer_fragment, resolver_fragment_id, vec![], vec![], 1)
            .unwrap();
    let actor_id_gen = std::sync::atomic::AtomicU32::new(1000);
    let render = render_resolver_fragment(
        &writer_fragment,
        resolver_fragment_id,
        apply_fragment_id,
        pk_index_table_id,
        resolver_node,
        to_partial_graph_id(DatabaseId::new(1), Some(JobId::new(7))),
        &worker_nodes,
        &actor_id_gen,
        "test",
    )
    .unwrap();

    assert_eq!(
        render.fragment_infos.len(),
        2,
        "resolver and compaction apply must be separate fragments"
    );

    let resolver_fragment = render
        .fragment_infos
        .values()
        .find(|fragment| {
            matches!(
                fragment.nodes.node_body,
                Some(NodeBody::CompactionResolver(_))
            )
        })
        .expect("resolver fragment should exist");
    assert_eq!(
        resolver_fragment.distribution_type,
        DistributionType::Single
    );
    assert_eq!(resolver_fragment.actors.len(), 1);
    assert!(resolver_fragment.state_table_ids.is_empty());

    let apply_fragment = render
        .fragment_infos
        .values()
        .find(|fragment| {
            matches!(
                fragment.nodes.node_body,
                Some(NodeBody::IcebergWithPkIndexWriter(_))
            )
        })
        .expect("compaction apply fragment should exist");

    assert_eq!(
        render.state_table_ids,
        HashSet::from([pk_index_table_id]),
        "the transient pipeline owns exactly the writer's pk-index table"
    );

    assert_eq!(
        apply_fragment.actors.len(),
        2,
        "one apply actor per writer actor"
    );
    assert_eq!(apply_fragment.distribution_type, DistributionType::Hash);
    assert_eq!(apply_fragment.vnode_count, vnode_count);

    let bmp_bits = |b: &risingwave_common::bitmap::Bitmap| b.iter().collect::<Vec<bool>>();
    let rendered_bitmaps: HashSet<Vec<bool>> = apply_fragment
        .actors
        .values()
        .map(|a| bmp_bits(a.vnode_bitmap.as_ref().unwrap()))
        .collect();
    let writer_bitmaps: HashSet<Vec<bool>> = HashSet::from([bmp_bits(&bmp_a), bmp_bits(&bmp_b)]);
    assert_eq!(
        rendered_bitmaps, writer_bitmaps,
        "apply actors are vnode-aligned to the writer"
    );
    for a in apply_fragment.actors.values() {
        assert_eq!(a.worker_id, worker_id, "same worker placement as writer");
    }
    let rendered_actor_ids: HashSet<ActorId> = apply_fragment.actors.keys().copied().collect();
    assert!(
        rendered_actor_ids.is_disjoint(&HashSet::from([ActorId::new(10), ActorId::new(11)])),
        "apply actors get fresh ids, not the writer's"
    );

    let resolver_actor_id = *resolver_fragment.actors.keys().next().unwrap();
    let resolver_dispatchers = render
        .actors_to_create
        .values()
        .flat_map(|fragments| fragments.values())
        .flat_map(|(_, actors, _)| actors)
        .find_map(|(actor, _, dispatchers)| {
            (actor.actor_id == resolver_actor_id).then_some(dispatchers)
        })
        .expect("resolver actor should be rendered");
    assert_eq!(resolver_dispatchers.len(), 1);
    let dispatcher = &resolver_dispatchers[0];
    assert_eq!(
        dispatcher.get_type().unwrap(),
        risingwave_pb::stream_plan::DispatcherType::Hash
    );
    assert_eq!(dispatcher.dist_key_indices, vec![0, 1]);
    assert_eq!(dispatcher.dispatcher_id, apply_fragment.fragment_id);
    assert_eq!(
        dispatcher.downstream_actor_id.len(),
        apply_fragment.actors.len()
    );
}

fn dummy_overwrite() -> OverwriteInput {
    OverwriteInput {
        sink_id: SinkId::new(7),
        output_files: vec![],
        input_file_paths: vec!["in.parquet".to_owned()],
        read_snapshot_id: 777,
    }
}

fn dummy_render() -> CompactionResolverRenderResult {
    let mut fragment_infos = HashMap::new();
    fragment_infos.insert(
        FragmentId::new(500),
        InflightFragmentInfo {
            fragment_id: FragmentId::new(500),
            distribution_type: DistributionType::Single,
            fragment_type_mask: FragmentTypeMask::empty(),
            vnode_count: 1,
            nodes: PbStreamNode::default(),
            actors: HashMap::new(),
            state_table_ids: HashSet::from([TableId::new(99)]),
        },
    );
    CompactionResolverRenderResult {
        fragment_infos,
        node_actors: HashMap::new(),
        state_table_ids: HashSet::from([TableId::new(99)]),
        actors_to_create: Default::default(),
        actor_ids: vec![ActorId::new(1000)],
    }
}

fn test_completion() -> Arc<CompactionResolveCompletion> {
    Arc::new(CompactionResolveCompletion::for_test(
        SinkId::new(7),
        IcebergCompactionTaskId::new(42),
    ))
}

#[test]
fn test_new_job_starts_pausing() {
    let pause_barrier = main_graph_barrier(20, 30);
    let job = CompactionResolveJobControl::new(
        DatabaseId::new(1),
        SinkId::new(7),
        IcebergCompactionTaskId::new(42),
        test_completion(),
        HashMap::new(),
        pause_barrier.clone(),
        dummy_overwrite(),
        HashSet::from([TableId::new(99), TableId::new(100)]),
        dummy_render(),
    );
    assert!(job.fragment_infos().is_some());
    let Phase::Pausing {
        pause_barrier: stored,
        ..
    } = &job.phase
    else {
        panic!("expected Pausing phase");
    };
    assert_eq!(stored.prev_epoch(), pause_barrier.prev_epoch());
    assert_eq!(stored.curr_epoch(), pause_barrier.curr_epoch());
    assert_eq!(
        CompactionResolveJobControl::job_id_for_sink(SinkId::new(7)),
        JobId::new(7)
    );
}

#[test]
fn test_job_id_dedupes_per_sink() {
    assert_eq!(
        CompactionResolveJobControl::job_id_for_sink(SinkId::new(7)),
        CompactionResolveJobControl::job_id_for_sink(SinkId::new(7)),
    );
    assert_ne!(
        CompactionResolveJobControl::job_id_for_sink(SinkId::new(7)),
        CompactionResolveJobControl::job_id_for_sink(SinkId::new(8)),
    );
}

fn main_graph_barrier(prev: u64, curr: u64) -> BarrierInfo {
    BarrierInfo {
        prev_epoch: TracedEpoch::new(Epoch(prev)),
        curr_epoch: TracedEpoch::new(Epoch(curr)),
        kind: BarrierKind::Checkpoint(vec![prev]),
    }
}

#[test]
fn test_bootstrap_partial_graph_barrier_is_non_checkpoint() {
    let b = main_graph_barrier(20, 30);
    let bootstrap = CompactionResolveJobControl::bootstrap_partial_graph_barrier(&b);
    assert_eq!(bootstrap.kind, BarrierKind::Barrier);
    assert_eq!(bootstrap.prev_epoch(), 20);
    assert_eq!(bootstrap.curr_epoch(), 30);
}

#[test]
fn test_compaction_resolver_main_checkpoint_stop_is_non_checkpoint() {
    let pause = main_graph_barrier(50, 60);
    let stop = CompactionResolveJobControl::resolve_stop_barrier(&pause);
    assert_eq!(stop.prev_epoch(), 60);
    assert_eq!(stop.curr_epoch(), u64::MAX);
    assert_eq!(stop.kind, BarrierKind::Barrier);
}

#[test]
fn test_pause_mutation_targets_sink() {
    let actor_id = ActorId::new(9);
    let mutation = crate::barrier::command::Command::iceberg_pk_index_barrier_to_mutation(
        SinkId::new(7),
        IcebergCompactionTaskId::new(42),
        [actor_id],
        risingwave_pb::stream_plan::iceberg_pk_index_barrier_mutation::Phase::Pause,
    );
    let Mutation::IcebergPkIndexBarrier(pause) = mutation else {
        panic!("expected IcebergPkIndexBarrier mutation");
    };
    assert_eq!(pause.sink_id, 7);
    assert_eq!(pause.task_id, 42);
    assert_eq!(pause.gated_actor_ids, vec![actor_id]);
    assert_eq!(
        pause.phase(),
        risingwave_pb::stream_plan::iceberg_pk_index_barrier_mutation::Phase::Pause
    );
}

#[test]
fn test_compaction_resolver_main_checkpoint_takes_overwrite_once() {
    let mut job = CompactionResolveJobControl::new(
        DatabaseId::new(1),
        SinkId::new(7),
        IcebergCompactionTaskId::new(42),
        test_completion(),
        HashMap::new(),
        main_graph_barrier(50, 60),
        dummy_overwrite(),
        HashSet::from([TableId::new(99), TableId::new(100)]),
        dummy_render(),
    );
    job.phase = Phase::AwaitingMainCommit {
        render_result: dummy_render(),
        seal_epoch: 60,
        overwrite: dummy_overwrite(),
        delete_reports: Vec::new(),
    };

    let overwrite = job
        .take_overwrite_for_main_epoch(60)
        .expect("main B2 should take the resolver overwrite");
    assert_eq!(overwrite.epoch, 60);
    assert!(matches!(
        job.phase,
        Phase::Committing { seal_epoch: 60, .. }
    ));
    assert!(job.take_overwrite_for_main_epoch(60).is_none());
}

#[test]
fn test_collect_routes_compaction_resolver_delete_metadata() {
    let mut job = CompactionResolveJobControl::new(
        DatabaseId::new(1),
        SinkId::new(7),
        IcebergCompactionTaskId::new(42),
        test_completion(),
        HashMap::new(),
        main_graph_barrier(50, 60),
        dummy_overwrite(),
        HashSet::from([TableId::new(99), TableId::new(100)]),
        dummy_render(),
    );
    job.phase = Phase::Resolving {
        render_result: dummy_render(),
        resolve_epoch: 60,
        overwrite: dummy_overwrite(),
        delete_reports: Vec::new(),
    };

    let report = PbIcebergPkIndexSinkMetadata {
        reporter_actor_id: ActorId::new(1000),
        sink_id: SinkId::new(7),
        prev_epoch: 60,
        role: PbIcebergPkIndexSinkRole::CompactionResolver as i32,
        metadata: None,
    };
    let response = risingwave_pb::stream_service::BarrierCompleteResponse {
        iceberg_pk_index_sink_metadata: vec![report],
        ..Default::default()
    };
    let responses = HashMap::from([(WorkerId::new(1), response)]);
    job.collect_delete_reports(&CollectedBarrier {
        epoch: EpochPair::new(61, 60),
        resps: &responses,
        pending_barrier_num: 0,
    });

    let Phase::Resolving { delete_reports, .. } = &job.phase else {
        panic!("expected Resolving phase");
    };
    assert_eq!(delete_reports.len(), 1);
}

#[test]
fn test_compaction_resolver_main_checkpoint_owns_no_sync_tables() {
    let job_state_table_ids = HashSet::from([TableId::new(99), TableId::new(100)]);
    let job = CompactionResolveJobControl::new(
        DatabaseId::new(1),
        SinkId::new(7),
        IcebergCompactionTaskId::new(42),
        test_completion(),
        HashMap::new(),
        main_graph_barrier(50, 60),
        dummy_overwrite(),
        job_state_table_ids.clone(),
        dummy_render(),
    );

    let barrier_info = job.new_partial_graph_barrier_info(main_graph_barrier(60, u64::MAX));

    assert!(barrier_info.table_ids_to_commit.is_empty());
    assert_eq!(
        InflightFragmentInfo::existing_table_ids(job.fragment_infos().unwrap().values())
            .collect::<HashSet<_>>(),
        HashSet::from([TableId::new(99)])
    );
}
