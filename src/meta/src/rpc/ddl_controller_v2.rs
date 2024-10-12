// Copyright 2024 RisingWave Labs
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

use itertools::Itertools;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::stream_graph_visitor::{visit_fragment, visit_stream_node};
use risingwave_meta_model_v2::object::ObjectType;
use risingwave_meta_model_v2::ObjectId;
use risingwave_pb::catalog::CreateType;
use risingwave_pb::ddl_service::TableJobType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::update_mutation::PbMergeUpdate;
use risingwave_pb::stream_plan::StreamFragmentGraph as StreamFragmentGraphProto;
use thiserror_ext::AsReport;

use crate::controller::catalog::ReleaseContext;
use crate::manager::{
    MetadataManagerV2, NotificationVersion, StreamingJob, IGNORED_NOTIFICATION_VERSION,
};
use crate::model::{MetadataModel, StreamContext};
use crate::rpc::ddl_controller::{
    fill_table_stream_graph_info, DdlController, DropMode, ReplaceTableInfo,
};
use crate::stream::{validate_sink, StreamFragmentGraph};
use crate::MetaResult;

impl DdlController {
    pub async fn create_streaming_job_v2(
        &self,
        mut streaming_job: StreamingJob,
        mut fragment_graph: StreamFragmentGraphProto,
        affected_table_replace_info: Option<ReplaceTableInfo>,
    ) -> MetaResult<NotificationVersion> {
        let mgr = self.metadata_manager.as_v2_ref();

        let ctx = StreamContext::from_protobuf(fragment_graph.get_ctx().unwrap());
        mgr.catalog_controller
            .create_job_catalog(
                &mut streaming_job,
                &ctx,
                &fragment_graph.parallelism,
                fragment_graph.max_parallelism as _,
            )
            .await?;
        let job_id = streaming_job.id();

        match &mut streaming_job {
            StreamingJob::Table(src, table, job_type) => {
                // If we're creating a table with connector, we should additionally fill its ID first.
                fill_table_stream_graph_info(src, table, *job_type, &mut fragment_graph);
            }
            StreamingJob::Source(src) => {
                // set the inner source id of source node.
                for fragment in fragment_graph.fragments.values_mut() {
                    visit_fragment(fragment, |node_body| {
                        if let NodeBody::Source(source_node) = node_body {
                            source_node.source_inner.as_mut().unwrap().source_id = src.id;
                        }
                    });
                }
            }
            _ => {}
        }

        tracing::debug!(
            id = job_id,
            definition = streaming_job.definition(),
            "starting streaming job",
        );
        let _permit = self
            .creating_streaming_job_permits
            .semaphore
            .acquire()
            .await
            .unwrap();
        let _reschedule_job_lock = self.stream_manager.reschedule_lock_read_guard().await;

        let id = streaming_job.id();
        let name = streaming_job.name();
        let definition = streaming_job.definition();
        let source_id = match &streaming_job {
            StreamingJob::Table(Some(src), _, _) | StreamingJob::Source(src) => Some(src.id),
            _ => None,
        };

        // create streaming job.
        match self
            .create_streaming_job_inner_v2(
                mgr,
                ctx,
                streaming_job,
                fragment_graph,
                affected_table_replace_info,
            )
            .await
        {
            Ok(version) => Ok(version),
            Err(err) => {
                tracing::error!(id = job_id, error = %err.as_report(), "failed to create streaming job");
                let event = risingwave_pb::meta::event_log::EventCreateStreamJobFail {
                    id,
                    name,
                    definition,
                    error: err.as_report().to_string(),
                };
                self.env.event_log_manager_ref().add_event_logs(vec![
                    risingwave_pb::meta::event_log::Event::CreateStreamJobFail(event),
                ]);
                let aborted = mgr
                    .catalog_controller
                    .try_abort_creating_streaming_job(job_id as _, false)
                    .await?;
                if aborted {
                    tracing::warn!(id = job_id, "aborted streaming job");
                    if let Some(source_id) = source_id {
                        self.source_manager
                            .unregister_sources(vec![source_id])
                            .await;
                    }
                }
                Err(err)
            }
        }
    }

    async fn create_streaming_job_inner_v2(
        &self,
        mgr: &MetadataManagerV2,
        ctx: StreamContext,
        mut streaming_job: StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
        affected_table_replace_info: Option<ReplaceTableInfo>,
    ) -> MetaResult<NotificationVersion> {
        let mut fragment_graph =
            StreamFragmentGraph::new(&self.env, fragment_graph, &streaming_job).await?;
        streaming_job.set_table_fragment_id(fragment_graph.table_fragment_id());
        streaming_job.set_dml_fragment_id(fragment_graph.dml_fragment_id());

        // create internal table catalogs and refill table id.
        let internal_tables = fragment_graph.internal_tables().into_values().collect_vec();
        let table_id_map = mgr
            .catalog_controller
            .create_internal_table_catalog(&streaming_job, internal_tables)
            .await?;
        fragment_graph.refill_internal_table_ids(table_id_map);

        let affected_table_replace_info = match affected_table_replace_info {
            Some(replace_table_info) => {
                let ReplaceTableInfo {
                    mut streaming_job,
                    fragment_graph,
                    ..
                } = replace_table_info;

                let fragment_graph =
                    StreamFragmentGraph::new(&self.env, fragment_graph, &streaming_job).await?;
                streaming_job.set_table_fragment_id(fragment_graph.table_fragment_id());
                streaming_job.set_dml_fragment_id(fragment_graph.dml_fragment_id());
                let streaming_job = streaming_job;

                Some((streaming_job, fragment_graph))
            }
            None => None,
        };

        // create fragment and actor catalogs.
        tracing::debug!(id = streaming_job.id(), "building streaming job");
        let (ctx, table_fragments) = self
            .build_stream_job(
                ctx,
                streaming_job,
                fragment_graph,
                affected_table_replace_info,
            )
            .await?;

        let streaming_job = &ctx.streaming_job;

        match streaming_job {
            StreamingJob::Table(None, table, TableJobType::SharedCdcSource) => {
                Self::validate_cdc_table(table, &table_fragments).await?;
            }
            StreamingJob::Table(Some(source), ..) => {
                // Register the source on the connector node.
                self.source_manager.register_source(source).await?;
            }
            StreamingJob::Sink(sink, _) => {
                // Validate the sink on the connector node.
                validate_sink(sink).await?;
            }
            StreamingJob::Source(source) => {
                // Register the source on the connector node.
                self.source_manager.register_source(source).await?;
            }
            _ => {}
        }

        mgr.catalog_controller
            .prepare_streaming_job(table_fragments.to_protobuf(), streaming_job, false)
            .await?;

        // create streaming jobs.
        let stream_job_id = streaming_job.id();
        match (streaming_job.create_type(), &streaming_job) {
            (CreateType::Unspecified, _)
            | (CreateType::Foreground, _)
            // FIXME(kwannoel): Unify background stream's creation path with MV below.
            | (CreateType::Background, StreamingJob::Sink(_, _)) => {
                let version = self.stream_manager
                    .create_streaming_job(table_fragments, ctx)
                    .await?;
                Ok(version)
            }
            (CreateType::Background, _) => {
                let ctrl = self.clone();
                let fut = async move {
                    let _ = ctrl
                        .stream_manager
                        .create_streaming_job(table_fragments, ctx)
                        .await.inspect_err(|err| {
                            tracing::error!(id = stream_job_id, error = ?err.as_report(), "failed to create background streaming job");
                        });
                };
                tokio::spawn(fut);
                Ok(IGNORED_NOTIFICATION_VERSION)
            }
        }
    }

    pub async fn drop_object(
        &self,
        object_type: ObjectType,
        object_id: ObjectId,
        drop_mode: DropMode,
        target_replace_info: Option<ReplaceTableInfo>,
    ) -> MetaResult<NotificationVersion> {
        let mgr = self.metadata_manager.as_v2_ref();
        let (release_ctx, mut version) = match object_type {
            ObjectType::Database => mgr.catalog_controller.drop_database(object_id).await?,
            ObjectType::Schema => {
                return mgr
                    .catalog_controller
                    .drop_schema(object_id, drop_mode)
                    .await;
            }
            ObjectType::Function => {
                return mgr.catalog_controller.drop_function(object_id).await;
            }
            ObjectType::Connection => {
                let (version, conn) = mgr.catalog_controller.drop_connection(object_id).await?;
                self.delete_vpc_endpoint(&conn).await?;
                return Ok(version);
            }
            _ => {
                mgr.catalog_controller
                    .drop_relation(object_type, object_id, drop_mode)
                    .await?
            }
        };

        if let Some(replace_table_info) = target_replace_info {
            let stream_ctx =
                StreamContext::from_protobuf(replace_table_info.fragment_graph.get_ctx().unwrap());

            let ReplaceTableInfo {
                mut streaming_job,
                fragment_graph,
                ..
            } = replace_table_info;

            let sink_id = if let ObjectType::Sink = object_type {
                object_id as _
            } else {
                panic!("additional replace table event only occurs when dropping sink into table")
            };

            let fragment_graph =
                StreamFragmentGraph::new(&self.env, fragment_graph, &streaming_job).await?;
            streaming_job.set_table_fragment_id(fragment_graph.table_fragment_id());
            streaming_job.set_dml_fragment_id(fragment_graph.dml_fragment_id());
            let streaming_job = streaming_job;

            let table = streaming_job.table().unwrap();

            tracing::debug!(id = streaming_job.id(), "replacing table for dropped sink");
            let dummy_id = mgr
                .catalog_controller
                .create_job_catalog_for_replace(
                    &streaming_job,
                    &stream_ctx,
                    table.get_version()?,
                    &fragment_graph.specified_parallelism(),
                    fragment_graph.max_parallelism(),
                )
                .await? as u32;

            let (ctx, table_fragments) = self
                .inject_replace_table_job_for_table_sink(
                    dummy_id,
                    &self.metadata_manager,
                    stream_ctx,
                    None,
                    None,
                    Some(sink_id),
                    &streaming_job,
                    fragment_graph,
                )
                .await?;

            let result: MetaResult<Vec<PbMergeUpdate>> = try {
                let merge_updates = ctx.merge_updates.clone();

                mgr.catalog_controller
                    .prepare_streaming_job(table_fragments.to_protobuf(), &streaming_job, true)
                    .await?;

                self.stream_manager
                    .replace_table(table_fragments, ctx)
                    .await?;

                merge_updates
            };

            version = match result {
                Ok(merge_updates) => {
                    let version = mgr
                        .catalog_controller
                        .finish_replace_streaming_job(
                            dummy_id as _,
                            streaming_job,
                            merge_updates,
                            None,
                            None,
                            Some(sink_id),
                            vec![],
                        )
                        .await?;
                    Ok(version)
                }
                Err(err) => {
                    tracing::error!(id = object_id, error = ?err.as_report(), "failed to replace table");
                    let _ = mgr
                        .catalog_controller
                        .try_abort_replacing_streaming_job(dummy_id as _)
                        .await
                        .inspect_err(|err| {
                            tracing::error!(id = object_id, error = ?err.as_report(), "failed to abort replacing table");
                        });
                    Err(err)
                }
            }?;
        }

        let ReleaseContext {
            streaming_job_ids,
            state_table_ids,
            source_ids,
            connections,
            source_fragments,
            removed_actors,
            removed_fragments,
        } = release_ctx;

        // delete vpc endpoints.
        for conn in connections {
            let _ = self
                .delete_vpc_endpoint_v2(conn.to_protobuf())
                .await
                .inspect_err(|err| {
                    tracing::warn!(err = ?err.as_report(), "failed to delete vpc endpoint");
                });
        }

        // unregister sources.
        self.source_manager
            .unregister_sources(source_ids.into_iter().map(|id| id as _).collect())
            .await;

        // unregister fragments and actors from source manager.
        self.source_manager
            .drop_source_fragments_v2(
                source_fragments
                    .into_iter()
                    .map(|(source_id, fragments)| {
                        (
                            source_id as u32,
                            fragments.into_iter().map(|id| id as u32).collect(),
                        )
                    })
                    .collect(),
                removed_actors.iter().map(|id| *id as _).collect(),
            )
            .await;

        // drop streaming jobs.
        self.stream_manager
            .drop_streaming_jobs_v2(
                removed_actors.into_iter().map(|id| id as _).collect(),
                streaming_job_ids,
                state_table_ids,
                removed_fragments.iter().map(|id| *id as _).collect(),
            )
            .await;

        Ok(version)
    }

    /// This is used for `ALTER TABLE ADD/DROP COLUMN`.
    pub async fn replace_table_v2(
        &self,
        mut streaming_job: StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
        table_col_index_mapping: Option<ColIndexMapping>,
    ) -> MetaResult<NotificationVersion> {
        let mgr = self.metadata_manager.as_v2_ref();
        let job_id = streaming_job.id();

        let _reschedule_job_lock = self.stream_manager.reschedule_lock_read_guard().await;
        let ctx = StreamContext::from_protobuf(fragment_graph.get_ctx().unwrap());

        // 1. build fragment graph.
        let fragment_graph =
            StreamFragmentGraph::new(&self.env, fragment_graph, &streaming_job).await?;
        streaming_job.set_table_fragment_id(fragment_graph.table_fragment_id());
        streaming_job.set_dml_fragment_id(fragment_graph.dml_fragment_id());
        let streaming_job = streaming_job;

        let StreamingJob::Table(_, table, ..) = &streaming_job else {
            unreachable!("unexpected job: {streaming_job:?}")
        };
        let dummy_id = mgr
            .catalog_controller
            .create_job_catalog_for_replace(
                &streaming_job,
                &ctx,
                table.get_version()?,
                &fragment_graph.specified_parallelism(),
                fragment_graph.max_parallelism(),
            )
            .await?;

        tracing::debug!(id = streaming_job.id(), "building replace streaming job");
        let mut updated_sink_catalogs = vec![];

        let result: MetaResult<Vec<PbMergeUpdate>> = try {
            let (mut ctx, mut table_fragments) = self
                .build_replace_table(
                    ctx,
                    &streaming_job,
                    fragment_graph,
                    table_col_index_mapping.clone(),
                    dummy_id as _,
                )
                .await?;

            let mut union_fragment_id = None;

            for (fragment_id, fragment) in &mut table_fragments.fragments {
                for actor in &mut fragment.actors {
                    if let Some(node) = &mut actor.nodes {
                        visit_stream_node(node, |body| {
                            if let NodeBody::Union(_) = body {
                                if let Some(union_fragment_id) = union_fragment_id.as_mut() {
                                    // The union fragment should be unique.
                                    assert_eq!(*union_fragment_id, *fragment_id);
                                } else {
                                    union_fragment_id = Some(*fragment_id);
                                }
                            }
                        })
                    };
                }
            }

            let target_fragment_id =
                union_fragment_id.expect("fragment of placeholder merger not found");

            let catalogs = self
                .metadata_manager
                .get_sink_catalog_by_ids(&table.incoming_sinks)
                .await?;

            for sink in catalogs {
                let sink_id = &sink.id;

                let sink_table_fragments = self
                    .metadata_manager
                    .get_job_fragments_by_id(&risingwave_common::catalog::TableId::new(*sink_id))
                    .await?;

                let sink_fragment = sink_table_fragments.sink_fragment().unwrap();

                Self::inject_replace_table_plan_for_sink(
                    Some(*sink_id),
                    &sink_fragment,
                    table,
                    &mut ctx,
                    &mut table_fragments,
                    target_fragment_id,
                    Some(&sink.unique_identity()),
                );

                if sink.original_target_columns.is_empty() {
                    updated_sink_catalogs.push(sink.id);
                }
            }

            let merge_updates = ctx.merge_updates.clone();

            mgr.catalog_controller
                .prepare_streaming_job(table_fragments.to_protobuf(), &streaming_job, true)
                .await?;

            self.stream_manager
                .replace_table(table_fragments, ctx)
                .await?;
            merge_updates
        };

        match result {
            Ok(merge_updates) => {
                let version = mgr
                    .catalog_controller
                    .finish_replace_streaming_job(
                        dummy_id,
                        streaming_job,
                        merge_updates,
                        table_col_index_mapping,
                        None,
                        None,
                        updated_sink_catalogs,
                    )
                    .await?;
                Ok(version)
            }
            Err(err) => {
                tracing::error!(id = job_id, error = ?err.as_report(), "failed to replace table");
                let _ = mgr
                    .catalog_controller
                    .try_abort_replacing_streaming_job(dummy_id)
                    .await.inspect_err(|err| {
                        tracing::error!(id = job_id, error = ?err.as_report(), "failed to abort replacing table");
                    });
                Err(err)
            }
        }
    }
}
