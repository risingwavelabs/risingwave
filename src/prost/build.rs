// Copyright 2025 RisingWave Labs
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

use std::path::{Path, PathBuf};

const DEBUG: bool = false;

macro_rules! debug {
    ($($tokens: tt)*) => {
        if DEBUG {
            println!("cargo:warning={}", format!($($tokens)*))
        }
    }
}

macro_rules! for_all_wrapped_id_fields {
    ($(
        $outer_part:ident {
            $(
                $($type_part:ident).* {
                    $(
                        $field_name:ident: $type_name:ty,
                    )+
                }
            )+
        }
    )+) => {
        fn wrapped_fields() -> Vec<(&'static str, Vec<(&'static str, &'static str)>)> {
            vec![
                $($(
                    (
                        &concat!(stringify!($outer_part), $(stringify!(.$type_part)),*),
                        vec![
                            $(
                                (stringify!($field_name), stringify!($type_name))
                            ),*
                        ]
                    ),
                )+)+
            ]
        }
    };
}

for_all_wrapped_id_fields! (
    backup_service {
        MetaSnapshotMetadata {
            state_table_info: TableId,
        }
    }
    batch_plan {
        DeleteNode {
            table_id: TableId,
        }
        InsertNode {
            table_id: TableId,
        }
        SourceNode {
            source_id: SourceId,
        }
        SysRowSeqScanNode {
            table_id: TableId,
        }
        UpdateNode {
            table_id: TableId,
        }
    }
    catalog {
        Comment {
            table_id: TableId,
            schema_id: SchemaId,
            database_id: DatabaseId,
        }
        Connection {
            id: ConnectionId,
            schema_id: SchemaId,
            database_id: DatabaseId,
        }
        Database {
            id: DatabaseId,
        }
        Function {
            id: FunctionId,
            schema_id: SchemaId,
            database_id: DatabaseId,
        }
        Index {
            id: IndexId,
            index_table_id: TableId,
            primary_table_id: TableId,
            schema_id: SchemaId,
            database_id: DatabaseId,
        }
        Schema {
            id: SchemaId,
            database_id: DatabaseId,
        }
        Secret {
            id: SecretId,
            schema_id: SchemaId,
            database_id: DatabaseId,
        }
        Sink {
            id: SinkId,
            schema_id: SchemaId,
            database_id: DatabaseId,
            target_table: TableId,
            auto_refresh_schema_from_table: TableId,
            connection_id: ConnectionId,
        }
        SinkFormatDesc {
            connection_id: ConnectionId,
        }
        Source {
            id: SourceId,
            schema_id: SchemaId,
            database_id: DatabaseId,
            connection_id: ConnectionId,
        }
        StreamSourceInfo {
            connection_id: ConnectionId,
        }
        Subscription {
            id: SubscriptionId,
            dependent_table_id: TableId,
            schema_id: SchemaId,
            database_id: DatabaseId,
        }
        Table {
            id: TableId,
            primary_table_id: TableId,
            job_id: JobId,
            schema_id: SchemaId,
            database_id: DatabaseId,
            fragment_id: FragmentId,
            dml_fragment_id: FragmentId,
        }
        View {
            id: ViewId,
            schema_id: SchemaId,
            database_id: DatabaseId,
        }
    }
    common {
        ActorInfo {
            actor_id: ActorId,
        }
        ActorLocation {
            worker_node_id: WorkerId,
        }
        WorkerNode {
            id: WorkerId,
        }
    }
    connector_service {
        SinkParam {
            sink_id: SinkId,
        }
    }
    ddl_service {
        AlterCdcTableBackfillParallelismRequest {
            table_id: JobId,
        }
        AlterDatabaseParamRequest {
            database_id: DatabaseId,
        }
        AlterFragmentParallelismRequest {
            fragment_ids: FragmentId,
        }
        AlterParallelismRequest {
            table_id: JobId,
        }
        AlterResourceGroupRequest {
            table_id: TableId,
        }
        AlterSecretRequest {
            database_id: DatabaseId,
            schema_id: SchemaId,
            secret_id: SecretId,
        }
        AlterSetSchemaRequest {
            new_schema_id: SchemaId,
        }
        AlterSwapRenameRequest.ObjectNameSwapPair {
            src_object_id: ObjectId,
            dst_object_id: ObjectId,
        }
        CompactIcebergTableRequest {
            sink_id: SinkId,
        }
        CreateConnectionRequest {
            database_id: DatabaseId,
            schema_id: SchemaId,
        }
        CreateMaterializedViewRequest {
            dependencies: ObjectId,
        }
        CreateSecretRequest {
            database_id: DatabaseId,
            schema_id: SchemaId,
        }
        CreateSinkRequest {
            dependencies: ObjectId,
        }
        CreateTableRequest {
            dependencies: ObjectId,
        }
        CreateViewRequest {
            dependencies: ObjectId,
        }
        DropConnectionRequest {
            connection_id: ConnectionId,
        }
        DropDatabaseRequest {
            database_id: DatabaseId,
        }
        DropFunctionRequest {
            function_id: FunctionId,
        }
        DropIndexRequest {
            index_id: IndexId,
        }
        DropMaterializedViewRequest {
            table_id: TableId,
        }
        DropSchemaRequest {
            schema_id: SchemaId,
        }
        DropSecretRequest {
            secret_id: SecretId,
        }
        DropSinkRequest {
            sink_id: SinkId,
        }
        DropSourceRequest {
            source_id: SourceId,
        }
        DropSubscriptionRequest {
            subscription_id: SubscriptionId,
        }
        DropTableRequest {
            table_id: TableId,
        }
        DropViewRequest {
            view_id: ViewId,
        }
        ExpireIcebergTableSnapshotsRequest {
            sink_id: SinkId,
        }
        GetTablesRequest {
            table_ids: TableId,
        }
        GetTablesResponse {
            tables: TableId,
        }
    }
    frontend_service {
        GetTableReplacePlanRequest {
            table_id: TableId,
            database_id: DatabaseId,
        }
    }
    hummock {
        CancelCompactTask {
            context_id: WorkerId,
        }
        CompactTask {
            existing_table_ids: TableId,
            table_options: TableId,
            table_vnode_partition: TableId,
            table_watermarks: TableId,
            table_schemas: TableId,
        }
        CompactTaskAssignment {
            context_id: WorkerId,
        }
        CompactionGroupInfo {
            member_table_ids: TableId,
        }
        GetVersionByEpochRequest {
            table_id: TableId,
        }
        HummockPinnedSnapshot {
            context_id: WorkerId,
        }
        HummockPinnedVersion {
            context_id: WorkerId,
        }
        HummockVersion {
            table_watermarks: TableId,
            table_change_logs: TableId,
            state_table_info: TableId,
            vector_indexes: TableId,
        }
        HummockVersionDelta {
            new_table_watermarks: TableId,
            removed_table_ids: TableId,
            change_log_delta: TableId,
            state_table_info_delta: TableId,
            vector_index_delta: TableId,
        }
        HummockVersionStats {
            table_stats: TableId,
        }
        PinVersionRequest {
            context_id: WorkerId,
        }
        PinnedVersionsSummary {
            workers: WorkerId,
        }
        ReportCompactionTaskRequest.ReportTask {
            table_stats_change: TableId,
        }
        SplitCompactionGroupRequest {
            table_ids: TableId,
        }
        SstableInfo {
            table_ids: TableId,
        }
        SubscribeCompactionEventRequest.Register {
            context_id: WorkerId,
        }
        SubscribeCompactionEventRequest.ReportTask {
            table_stats_change: TableId,
        }
        TriggerManualCompactionRequest {
            table_id: JobId,
        }
        TruncateTables {
            table_ids: TableId,
        }
        UnpinVersionBeforeRequest {
            context_id: WorkerId,
        }
        UnpinVersionRequest {
            context_id: WorkerId,
        }
        WriteLimits.WriteLimit {
            table_ids: TableId,
        }
    }
    iceberg_compaction {
        SubscribeIcebergCompactionEventRequest.Register {
            context_id: WorkerId,
        }
    }
    meta {
        ActivateWorkerNodeRequest {
            node_id: WorkerId,
        }
        ActorCountPerParallelism {
            worker_id_to_actor_count: WorkerId,
        }
        ActorIds {
            ids: ActorId,
        }
        AddWorkerNodeResponse {
            node_id: WorkerId,
        }
        AlterConnectorPropsRequest {
            connector_conn_ref: ConnectionId,
        }
        AlterConnectorPropsRequest.AlterIcebergTableIds {
            sink_id: SinkId,
            source_id: SourceId,
        }
        CancelCreatingJobsRequest.CreatingJobIds {
            job_ids: JobId,
        }
        CancelCreatingJobsRequest.CreatingJobInfo {
            database_id: DatabaseId,
            schema_id: SchemaId,
        }
        DatabaseRecoveryFailure {
            database_id: DatabaseId,
        }
        DatabaseRecoveryStart {
            database_id: DatabaseId,
        }
        DatabaseRecoverySuccess {
            database_id: DatabaseId,
        }
        EventLog.EventAutoSchemaChangeFail {
            table_id: TableId,
        }
        EventLog.EventCreateStreamJobFail {
            id: JobId,
        }
        EventLog.EventDirtyStreamJobClear {
            id: JobId,
        }
        EventLog.EventSinkFail {
            sink_id: SinkId,
        }
        EventLog.EventWorkerNodePanic {
            worker_id: WorkerId,
        }
        EventLog.GlobalRecoverySuccess {
            running_database_ids: DatabaseId,
            recovering_database_ids: DatabaseId,
        }
        FlushRequest {
            database_id: DatabaseId,
        }
        FragmentDistribution {
            fragment_id: FragmentId,
            upstream_fragment_ids: FragmentId,
            table_id: JobId,
            state_table_ids: TableId,
        }
        FragmentIdToActorIdMap {
            map: FragmentId,
        }
        FragmentToRelationMap {
            fragment_to_relation_map: FragmentId,
        }
        FragmentWorkerSlotMapping {
            fragment_id: FragmentId,
        }
        GetActorVnodesRequest {
            actor_id: ActorId,
        }
        GetClusterInfoResponse {
            actor_splits: ActorId,
            source_infos: SourceId,
        }
        GetFragmentByIdRequest {
            fragment_id: FragmentId,
        }
        GetFragmentVnodesRequest {
            fragment_id: FragmentId,
        }
        GetFragmentVnodesResponse.ActorVnodes {
            actor_id: ActorId,
        }
        GetServerlessStreamingJobsStatusResponse.Status {
            table_id: TableId,
        }
        GetServingVnodeMappingsResponse {
            fragment_to_table: FragmentId,
        }
        HeartbeatRequest {
            node_id: WorkerId,
        }
        ListActorSplitsResponse.ActorSplit {
            actor_id: ActorId,
            fragment_id: FragmentId,
            source_id: SourceId,
        }
        ListActorStatesResponse.ActorState {
            actor_id: ActorId,
            fragment_id: FragmentId,
            worker_id: WorkerId,
        }
        ListCdcProgressResponse {
            cdc_progress: JobId,
        }
        ListObjectDependenciesResponse.ObjectDependencies {
            object_id: ObjectId,
            referenced_object_id: ObjectId,
        }
        ListRateLimitsResponse.RateLimitInfo {
            job_id: JobId,
            fragment_id: FragmentId,
        }
        ListRefreshTableStatesResponse.RefreshTableState {
            table_id: TableId,
        }
        ListStreamingJobStatesResponse.StreamingJobState {
            table_id: JobId,
            database_id: DatabaseId,
            schema_id: SchemaId,
        }
        ListTableFragmentsRequest {
            table_ids: JobId,
        }
        ListTableFragmentsResponse {
            table_fragments: JobId,
        }
        ListTableFragmentsResponse.ActorInfo {
            id: ActorId,
        }
        ListTableFragmentsResponse.FragmentInfo {
            id: FragmentId,
        }
        ListUnmigratedTablesResponse.UnmigratedTable {
            table_id: TableId,
        }
        RefreshRequest {
            table_id: TableId,
            associated_source_id: SourceId,
        }
        SetSyncLogStoreAlignedRequest {
            job_id: JobId,
        }
        SubscribeRequest {
            worker_id: WorkerId,
        }
        TableFragments {
            table_id: JobId,
            fragments: FragmentId,
            actor_status: ActorId,
        }
        TableFragments.Fragment {
            fragment_id: FragmentId,
            upstream_fragment_ids: FragmentId,
            state_table_ids: TableId,
            table_id: JobId,
        }
        UpdateWorkerNodeSchedulabilityRequest {
            worker_ids: WorkerId,
        }
        WorkerReschedule {
            worker_actor_diff: WorkerId,
        }
    }
    monitor_service {
        GetProfileStatsRequest {
            dispatcher_fragment_ids: FragmentId,
        }
        GetProfileStatsResponse {
            dispatch_fragment_output_row_count: FragmentId,
            dispatch_fragment_output_blocking_duration_ns: FragmentId,
        }
        StackTraceResponse {
            barrier_worker_state: WorkerId,
            jvm_stack_traces: WorkerId,
        }
    }
    plan_common {
        ExternalTableDesc {
            table_id: TableId,
            source_id: SourceId,
        }
        StorageTableDesc {
            table_id: TableId,
        }
        VectorIndexReaderDesc {
            table_id: TableId,
        }
    }
    recursive {
        ComplexRecursiveMessage {
            node_id: WorkerId,
        }
    }
    secret {
        SecretRef {
            secret_id: SecretId,
        }
    }
    source {
        CdcTableSnapshotSplitsWithGeneration {
            splits: ActorId,
        }
        SourceActorInfo {
            actor_id: ActorId,
        }
    }
    stream_plan {
        ActorMapping {
            data: ActorId,
        }
        AddMutation {
            new_upstream_sinks: FragmentId,
            backfill_nodes_to_pause: FragmentId,
            added_actors: ActorId,
            actor_splits: ActorId,
            actor_dispatchers: ActorId,
        }
        Barrier {
            passed_actors: ActorId,
        }
        CdcFilterNode {
            upstream_source_id: SourceId,
        }
        DeltaIndexJoinNode {
            left_table_id: TableId,
            right_table_id: TableId,
        }
        Dispatcher {
            downstream_actor_id: ActorId,
        }
        DmlNode {
            table_id: TableId,
        }
        ListFinishMutation {
            associated_source_id: SourceId,
        }
        LoadFinishMutation {
            associated_source_id: SourceId,
        }
        MaterializeNode {
            table_id: TableId,
        }
        MergeNode {
            upstream_fragment_id: FragmentId,
        }
        RefreshStartMutation {
            table_id: TableId,
            associated_source_id: SourceId,
        }
        SinkDesc {
            id: SinkId,
        }
        SourceBackfillNode {
            upstream_source_id: SourceId,
        }
        SourceChangeSplitMutation {
            actor_splits: ActorId,
        }
        StartFragmentBackfillMutation {
            fragment_ids: FragmentId,
        }
        StopMutation {
            dropped_sink_fragments: FragmentId,
            actors: ActorId,
        }
        StreamActor {
            actor_id: ActorId,
            fragment_id: FragmentId,
        }
        StreamCdcScanNode {
            table_id: TableId,
        }
        StreamFragmentGraph {
            dependent_table_ids: TableId,
            fragments: FragmentId,
        }
        StreamFragmentGraph.StreamFragment {
            fragment_id: FragmentId,
        }
        StreamFragmentGraph.StreamFragmentEdge {
            upstream_id: FragmentId,
            downstream_id: FragmentId,
        }
        StreamFsFetch {
            source_id: SourceId,
            associated_table_id: TableId,
        }
        StreamScanNode {
            table_id: TableId,
        }
        StreamSource {
            source_id: SourceId,
            associated_table_id: TableId,
        }
        SubscriptionUpstreamInfo {
            upstream_mv_table_id: TableId,
        }
        ThrottleMutation {
            actor_throttle: ActorId,
        }
        UpdateMutation {
            dropped_actors: ActorId,
            actor_splits: ActorId,
            actor_vnode_bitmap_update: ActorId,
            actor_new_dispatchers: ActorId,
            sink_add_columns: SinkId,
        }
        UpdateMutation.DispatcherUpdate {
            actor_id: ActorId,
            added_downstream_actor_id: ActorId,
            removed_downstream_actor_id: ActorId,
        }
        UpdateMutation.MergeUpdate {
            upstream_fragment_id: FragmentId,
            new_upstream_fragment_id: FragmentId,
            actor_id: ActorId,
            removed_upstream_actor_id: ActorId,
            actor_vnode_bitmap_update: ActorId,
            dropped_actors: ActorId,
            actor_splits: ActorId,
            actor_new_dispatchers: ActorId,
        }
        UpstreamSinkInfo {
            upstream_fragment_id: FragmentId,
        }
        VectorIndexLookupJoinNode {
            table_id: TableId,
        }
    }
    stream_service {
        BarrierCompleteResponse {
            database_id: DatabaseId,
            truncate_tables: TableId,
            refresh_finished_tables: TableId,
            table_watermarks: TableId,
            vector_index_adds: TableId,
            worker_id: WorkerId,
            list_finished_source_ids: SourceId,
            load_finished_source_ids: SourceId,
        }
        BarrierCompleteResponse.CdcTableBackfillProgress {
            fragment_id: FragmentId,
            actor_id: ActorId,
        }
        BarrierCompleteResponse.CreateMviewProgress {
            backfill_actor_id: ActorId,
            fragment_id: FragmentId,
        }
        BarrierCompleteResponse.ListFinishedSource {
            reporter_actor_id: ActorId,
            table_id: TableId,
            associated_source_id: SourceId,
        }
        BarrierCompleteResponse.LoadFinishedSource {
            reporter_actor_id: ActorId,
            table_id: TableId,
            associated_source_id: SourceId,
        }
        BarrierCompleteResponse.LocalSstableInfo {
            table_stats_map: TableId,
        }
        InjectBarrierRequest {
            database_id: DatabaseId,
            table_ids_to_sync: TableId,
            actor_ids_to_collect: ActorId,
        }
        InjectBarrierRequest.BuildActorInfo {
            fragment_upstreams: FragmentId,
            actor_id: ActorId,
        }
        InjectBarrierRequest.FragmentBuildActorInfo {
            fragment_id: FragmentId,
        }
        StreamingControlStreamRequest.CreatePartialGraphRequest {
            database_id: DatabaseId,
        }
        StreamingControlStreamRequest.RemovePartialGraphRequest {
            database_id: DatabaseId,
        }
        StreamingControlStreamRequest.ResetDatabaseRequest {
            database_id: DatabaseId,
        }
        StreamingControlStreamResponse.ReportDatabaseFailureResponse {
            database_id: DatabaseId,
        }
        StreamingControlStreamResponse.ResetDatabaseResponse {
            database_id: DatabaseId,
        }
    }
    task_service {
        FastInsertRequest {
            table_id: TableId,
        }
        GetStreamRequest.Get {
            database_id: DatabaseId,
            up_fragment_id: FragmentId,
            down_fragment_id: FragmentId,
            up_actor_id: ActorId,
            down_actor_id: ActorId,
        }
    }
    user {
        AlterDefaultPrivilegeRequest {
            database_id: DatabaseId,
            schema_ids: SchemaId,
        }
    }
);

fn check_declared_wrapped_fields_sorted() {
    let wrapped_fields = wrapped_fields();
    if let Some(i) =
        (0..wrapped_fields.len() - 1).find(|i| wrapped_fields[*i].0 >= wrapped_fields[*i + 1].0)
    {
        panic!("types not sorted: first {}", wrapped_fields[i + 1].0)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";

    println!("cargo:rerun-if-changed={}", proto_dir);

    let proto_files = vec![
        "backup_service",
        "batch_plan",
        "catalog",
        "cloud_service",
        "common",
        "compactor",
        "compute",
        "connector_service",
        "data",
        "ddl_service",
        "expr",
        "health",
        "hummock",
        "iceberg_compaction",
        "java_binding",
        "meta",
        "monitor_service",
        "plan_common",
        "source",
        "stream_plan",
        "stream_service",
        "task_service",
        "telemetry",
        "user",
        "serverless_backfill_controller",
        "secret",
        "frontend_service",
    ];
    let protos: Vec<String> = proto_files
        .iter()
        .map(|f| format!("{}/{}.proto", proto_dir, f))
        .collect();

    // Paths to generate `BTreeMap` for protobuf maps.
    let btree_map_paths = [
        ".monitor_service.StackTraceResponse",
        ".plan_common.ExternalTableDesc",
        ".hummock.CompactTask",
        ".catalog.StreamSourceInfo",
        ".secret.SecretRef",
        ".catalog.Source",
        ".catalog.Sink",
        ".catalog.View",
        ".catalog.SinkFormatDesc",
        ".connector_service.ValidateSourceRequest",
        ".connector_service.GetEventStreamRequest",
        ".connector_service.SinkParam",
        ".stream_plan.SinkDesc",
        ".stream_plan.StreamFsFetch",
        ".stream_plan.SourceBackfillNode",
        ".stream_plan.StreamSource",
        ".batch_plan.SourceNode",
        ".batch_plan.IcebergScanNode",
        ".iceberg_compaction.IcebergCompactionTask",
    ];

    // Build protobuf structs.

    // We first put generated files to `OUT_DIR`, then copy them to `/src` only if they are changed.
    // This is to avoid unexpected recompilation due to the timestamps of generated files to be
    // changed by different kinds of builds. See https://github.com/risingwavelabs/risingwave/issues/11449 for details.
    //
    // Put generated files in /src may also benefit IDEs https://github.com/risingwavelabs/risingwave/pull/2581
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR envvar is missing"));
    let file_descriptor_set_path: PathBuf = out_dir.join("file_descriptor_set.bin");

    let tonic_config = tonic_build::configure()
        .file_descriptor_set_path(file_descriptor_set_path.as_path())
        .compile_well_known_types(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute(".", "#[derive(prost_helpers::AnyPB)]")
        .type_attribute(
            "node_body",
            "#[derive(::enum_as_inner::EnumAsInner, ::strum::Display, ::strum::EnumDiscriminants)]",
        )
        .type_attribute(
            "node_body",
            "#[strum_discriminants(derive(::strum::Display, Hash))]",
        )
        .type_attribute("rex_node", "#[derive(::enum_as_inner::EnumAsInner)]")
        .type_attribute(
            "meta.PausedReason",
            "#[derive(::enum_as_inner::EnumAsInner)]",
        )
        .type_attribute(
            "stream_plan.Barrier.BarrierKind",
            "#[derive(::enum_as_inner::EnumAsInner)]",
        )
        .btree_map(btree_map_paths)
        // node body is a very large enum, so we box it to avoid stack overflow.
        // TODO: ideally we should box all enum variants automatically https://github.com/tokio-rs/prost/issues/1209
        .boxed(".stream_plan.StreamNode.node_body.source")
        .boxed(".stream_plan.StreamNode.node_body.project")
        .boxed(".stream_plan.StreamNode.node_body.filter")
        .boxed(".stream_plan.StreamNode.node_body.materialize")
        .boxed(".stream_plan.StreamNode.node_body.stateless_simple_agg")
        .boxed(".stream_plan.StreamNode.node_body.simple_agg")
        .boxed(".stream_plan.StreamNode.node_body.hash_agg")
        .boxed(".stream_plan.StreamNode.node_body.append_only_top_n")
        .boxed(".stream_plan.StreamNode.node_body.hash_join")
        .boxed(".stream_plan.StreamNode.node_body.top_n")
        .boxed(".stream_plan.StreamNode.node_body.hop_window")
        .boxed(".stream_plan.StreamNode.node_body.merge")
        .boxed(".stream_plan.StreamNode.node_body.upstream_sink_union")
        .boxed(".stream_plan.StreamNode.node_body.exchange")
        .boxed(".stream_plan.StreamNode.node_body.stream_scan")
        .boxed(".stream_plan.StreamNode.node_body.batch_plan")
        .boxed(".stream_plan.StreamNode.node_body.lookup")
        .boxed(".stream_plan.StreamNode.node_body.arrange")
        .boxed(".stream_plan.StreamNode.node_body.lookup_union")
        .boxed(".stream_plan.StreamNode.node_body.delta_index_join")
        .boxed(".stream_plan.StreamNode.node_body.sink")
        .boxed(".stream_plan.StreamNode.node_body.expand")
        .boxed(".stream_plan.StreamNode.node_body.dynamic_filter")
        .boxed(".stream_plan.StreamNode.node_body.project_set")
        .boxed(".stream_plan.StreamNode.node_body.group_top_n")
        .boxed(".stream_plan.StreamNode.node_body.sort")
        .boxed(".stream_plan.StreamNode.node_body.watermark_filter")
        .boxed(".stream_plan.StreamNode.node_body.dml")
        .boxed(".stream_plan.StreamNode.node_body.row_id_gen")
        .boxed(".stream_plan.StreamNode.node_body.now")
        .boxed(".stream_plan.StreamNode.node_body.append_only_group_top_n")
        .boxed(".stream_plan.StreamNode.node_body.temporal_join")
        .boxed(".stream_plan.StreamNode.node_body.barrier_recv")
        .boxed(".stream_plan.StreamNode.node_body.values")
        .boxed(".stream_plan.StreamNode.node_body.append_only_dedup")
        .boxed(".stream_plan.StreamNode.node_body.eowc_over_window")
        .boxed(".stream_plan.StreamNode.node_body.over_window")
        .boxed(".stream_plan.StreamNode.node_body.stream_fs_fetch")
        .boxed(".stream_plan.StreamNode.node_body.stream_cdc_scan")
        .boxed(".stream_plan.StreamNode.node_body.cdc_filter")
        .boxed(".stream_plan.StreamNode.node_body.source_backfill")
        .boxed(".stream_plan.StreamNode.node_body.changelog")
        .boxed(".stream_plan.StreamNode.node_body.local_approx_percentile")
        .boxed(".stream_plan.StreamNode.node_body.global_approx_percentile")
        .boxed(".stream_plan.StreamNode.node_body.row_merge")
        .boxed(".stream_plan.StreamNode.node_body.as_of_join")
        .boxed(".stream_plan.StreamNode.node_body.sync_log_store")
        .boxed(".stream_plan.StreamNode.node_body.materialized_exprs")
        .boxed(".stream_plan.StreamNode.node_body.vector_index_write")
        .boxed(".stream_plan.StreamNode.node_body.locality_provider")
        .boxed(".stream_plan.StreamNode.node_body.eowc_gap_fill")
        .boxed(".stream_plan.StreamNode.node_body.gap_fill")
        .boxed(".stream_plan.StreamNode.node_body.vector_index_lookup_join")
        // `Udf` is 248 bytes, while 2nd largest field is 32 bytes.
        .boxed(".expr.ExprNode.rex_node.udf")
        // Eq + Hash are for plan nodes to do common sub-plan detection.
        // The requirement is from Source node -> SourceCatalog -> WatermarkDesc -> expr
        .type_attribute("catalog.WatermarkDesc", "#[derive(Eq, Hash)]")
        .type_attribute("catalog.StreamSourceInfo", "#[derive(Eq, Hash)]")
        .type_attribute("catalog.WebhookSourceInfo", "#[derive(Eq, Hash)]")
        .type_attribute("secret.SecretRef", "#[derive(Eq, Hash)]")
        .type_attribute("catalog.IndexColumnProperties", "#[derive(Eq, Hash)]")
        .type_attribute("expr.ExprNode", "#[derive(Eq, Hash)]")
        .type_attribute("data.DataType", "#[derive(Eq, Hash)]")
        .type_attribute("expr.ExprNode.rex_node", "#[derive(Eq, Hash)]")
        .type_attribute("expr.ExprNode.NowRexNode", "#[derive(Eq, Hash)]")
        .type_attribute("expr.InputRef", "#[derive(Eq, Hash)]")
        .type_attribute("expr.UserDefinedFunctionMetadata", "#[derive(Eq, Hash)]")
        .type_attribute("data.Datum", "#[derive(Eq, Hash)]")
        .type_attribute("expr.FunctionCall", "#[derive(Eq, Hash)]")
        .type_attribute("expr.UserDefinedFunction", "#[derive(Eq, Hash)]")
        .type_attribute(
            "plan_common.ColumnDesc.generated_or_default_column",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute("plan_common.GeneratedColumnDesc", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.DefaultColumnDesc", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.Cardinality", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.ExternalTableDesc", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.ColumnDesc", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalColumn", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalColumnPulsarMessageIdData", "#[derive(Eq, Hash)]")
        .type_attribute(
            "plan_common.AdditionalColumn.column_type",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute("plan_common.AdditionalColumnNormal", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalColumnKey", "#[derive(Eq, Hash)]")
        .type_attribute(
            "plan_common.AdditionalColumnPartition",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute("plan_common.AdditionalColumnPayload", "#[derive(Eq, Hash)]")
        .type_attribute(
            "plan_common.AdditionalColumnTimestamp",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute(
            "plan_common.AdditionalColumnFilename",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute("plan_common.AdditionalColumnHeader", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalColumnHeaders", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalColumnOffset", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalDatabaseName", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalSchemaName", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalTableName", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.AdditionalSubject", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.SourceRefreshMode", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.SourceRefreshMode.refresh_mode", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.SourceRefreshMode.SourceRefreshModeStreaming", "#[derive(Eq, Hash)]")
        .type_attribute("plan_common.SourceRefreshMode.SourceRefreshModeFullReload", "#[derive(Eq, Hash)]")
        .type_attribute(
            "plan_common.AdditionalCollectionName",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute("plan_common.AsOfJoinDesc", "#[derive(Eq, Hash)]")
        .type_attribute("common.ColumnOrder", "#[derive(Eq, Hash)]")
        .type_attribute("common.OrderType", "#[derive(Eq, Hash)]")
        .type_attribute("common.Buffer", "#[derive(Eq)]")
        // Eq is required to derive `FromJsonQueryResult` for models in risingwave_meta_model.
        .type_attribute("hummock.TableStats", "#[derive(Eq)]")
        .type_attribute("hummock.SstableInfo", "#[derive(Eq)]")
        .type_attribute("hummock.KeyRange", "#[derive(Eq)]")
        .type_attribute("hummock.CompactionConfig", "#[derive(Eq)]")
        .type_attribute("hummock.GroupDelta.delta_type", "#[derive(Eq)]")
        .type_attribute("hummock.IntraLevelDelta", "#[derive(Eq)]")
        .type_attribute("hummock.GroupConstruct", "#[derive(Eq)]")
        .type_attribute("hummock.GroupDestroy", "#[derive(Eq)]")
        .type_attribute("hummock.GroupMetaChange", "#[derive(Eq)]")
        .type_attribute("hummock.GroupTableChange", "#[derive(Eq)]")
        .type_attribute("hummock.GroupMerge", "#[derive(Eq)]")
        .type_attribute("hummock.GroupDelta", "#[derive(Eq)]")
        .type_attribute("hummock.NewL0SubLevel", "#[derive(Eq)]")
        .type_attribute("hummock.TruncateTables", "#[derive(Eq)]")
        .type_attribute("hummock.LevelHandler.RunningCompactTask", "#[derive(Eq)]")
        .type_attribute("hummock.LevelHandler", "#[derive(Eq)]")
        .type_attribute("hummock.TableOption", "#[derive(Eq)]")
        .type_attribute("hummock.InputLevel", "#[derive(Eq)]")
        .type_attribute("hummock.TableSchema", "#[derive(Eq)]")
        .type_attribute("hummock.CompactTask", "#[derive(Eq)]")
        .type_attribute("hummock.TableWatermarks", "#[derive(Eq)]")
        .type_attribute("hummock.VnodeWatermark", "#[derive(Eq)]")
        .type_attribute(
            "hummock.TableWatermarks.EpochNewWatermarks",
            "#[derive(Eq)]",
        )
        .type_attribute(
            "catalog.VectorIndexInfo",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute(
            "catalog.VectorIndexInfo.config",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute(
            "catalog.FlatIndexConfig",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute(
            "catalog.HnswFlatIndexConfig",
            "#[derive(Eq, Hash)]",
        )
        // proto version enums
        .type_attribute("stream_plan.AggNodeVersion", "#[derive(prost_helpers::Version)]")
        .type_attribute(
            "plan_common.ColumnDescVersion",
            "#[derive(prost_helpers::Version)]",
        )
        .type_attribute(
            "hummock.CompatibilityVersion",
            "#[derive(prost_helpers::Version)]",
        )
        .type_attribute("expr.UdfExprVersion", "#[derive(prost_helpers::Version)]")
        .type_attribute("meta.Object.object_info", "#[derive(strum::Display)]")
        .type_attribute("meta.SubscribeResponse.info", "#[derive(strum::Display)]")
        // end
        ;

    // If any configuration for `prost_build` is not exposed by `tonic_build`, specify it here.
    let mut prost_config = prost_build::Config::new();
    prost_config.skip_debug([
        "meta.SystemParams",
        "plan_common.ColumnDesc",
        "data.DataType",
        // TODO:
        //"stream_plan.StreamNode"
    ]);

    check_declared_wrapped_fields_sorted();

    for (wrapped_type, wrapped_fields) in &wrapped_fields() {
        for (field_name, field_type) in wrapped_fields {
            prost_config.field_wrapper(
                format!("{wrapped_type}.{field_name}"),
                format!("crate::id::{field_type}"),
            );
        }
    }
    // Compile the proto files.
    tonic_config
        .out_dir(out_dir.as_path())
        .compile_protos_with_config(prost_config, &protos, &[proto_dir.to_owned()])
        .expect("Failed to compile grpc!");

    // Implement `serde::Serialize` on those structs.
    let descriptor_set = fs_err::read(file_descriptor_set_path)?;
    pbjson_build::Builder::new()
        .btree_map(btree_map_paths)
        .register_descriptors(&descriptor_set)?
        .out_dir(out_dir.as_path())
        .build(&["."])
        .expect("Failed to compile serde");

    // Tweak the serde files so that they can be compiled in our project.
    // By adding a `use crate::module::*`
    let rewrite_files = proto_files;
    for serde_proto_file in &rewrite_files {
        let out_file = out_dir.join(format!("{}.serde.rs", serde_proto_file));
        let file_content = String::from_utf8(fs_err::read(&out_file)?)?;
        let file_content = file_content.replace(
            ".map(|(k,v)| (k.0, v)).collect()",
            ".map(|(k,v)| (k.0.into(), v)).collect()",
        );
        let file_content = file_content.replace(
            ".map(|(k,v)| (k.0, v.0)).collect()",
            ".map(|(k,v)| (k.0.into(), v.0)).collect()",
        );
        let module_path_id = serde_proto_file.replace('.', "::");
        fs_err::write(
            &out_file,
            format!(
                "#![allow(clippy::useless_conversion)]\nuse crate::{}::*;\n{}",
                module_path_id, file_content
            ),
        )?;
    }

    compare_and_copy(&out_dir, &PathBuf::from("./src")).unwrap_or_else(|_| {
        panic!(
            "Failed to copy generated files from {} to ./src",
            out_dir.display()
        )
    });

    Ok(())
}

/// Copy all files from `src_dir` to `dst_dir` only if they are changed.
fn compare_and_copy(src_dir: &Path, dst_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    debug!(
        "copying files from {} to {}",
        src_dir.display(),
        dst_dir.display()
    );
    let mut updated = false;
    let t1 = std::time::Instant::now();
    for entry in walkdir::WalkDir::new(src_dir) {
        let entry = entry?;
        if entry.file_type().is_file() {
            let src_file = entry.path();
            let dst_file = dst_dir.join(src_file.strip_prefix(src_dir)?);
            if dst_file.exists() {
                let src_content = fs_err::read(src_file)?;
                let dst_content = fs_err::read(dst_file.as_path())?;
                if src_content == dst_content {
                    continue;
                }
            }
            updated = true;
            debug!("copying {} to {}", src_file.display(), dst_file.display());
            fs_err::create_dir_all(dst_file.parent().unwrap())?;
            fs_err::copy(src_file, dst_file)?;
        }
    }
    debug!(
        "Finished generating risingwave_prost in {:?}{}",
        t1.elapsed(),
        if updated { "" } else { ", no file is updated" }
    );

    Ok(())
}
