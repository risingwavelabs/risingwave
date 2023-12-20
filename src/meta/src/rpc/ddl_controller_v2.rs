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

use itertools::Itertools;
use risingwave_common::util::stream_graph_visitor::visit_fragment;
use risingwave_pb::catalog::CreateType;
use risingwave_pb::ddl_service::TableJobType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::StreamFragmentGraph as StreamFragmentGraphProto;

use crate::controller::catalog::CatalogController;
use crate::manager::{MetadataManager, NotificationVersion, StreamingJob};
use crate::model::{MetadataModel, StreamEnvironment};
use crate::rpc::ddl_controller::{fill_table_stream_graph_info, DdlController};
use crate::stream::{validate_sink, StreamFragmentGraph};
use crate::MetaResult;

impl DdlController {
    pub async fn create_streaming_job_v2(
        &self,
        mut stream_job: StreamingJob,
        mut fragment_graph: StreamFragmentGraphProto,
        create_type: CreateType,
    ) -> MetaResult<NotificationVersion> {
        let MetadataManager::V2(mgr) = &self.metadata_manager else {
            unreachable!("MetadataManager should be V2")
        };

        // TODO: add revert function to clean metadata from db if failed.
        // create catalogs for streaming jobs.
        let env = StreamEnvironment::from_protobuf(fragment_graph.get_env().unwrap());
        mgr.catalog_controller
            .create_job_catalog(&mut stream_job, create_type, &env)
            .await?;

        match &mut stream_job {
            StreamingJob::Table(Some(src), table, job_type) => {
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
            id = stream_job.id(),
            definition = stream_job.definition(),
            "starting stream job",
        );
        let _permit = self
            .creating_streaming_job_permits
            .semaphore
            .acquire()
            .await
            .unwrap();
        let _reschedule_job_lock = self.stream_manager.reschedule_lock.read().await;

        // build streaming graph.
        let mut fragment_graph =
            StreamFragmentGraph::new(&self.env, fragment_graph, &stream_job).await?;
        stream_job.set_table_fragment_id(fragment_graph.table_fragment_id());
        stream_job.set_dml_fragment_id(fragment_graph.dml_fragment_id());

        // create internal table catalogs and refill table id.
        let internal_tables = fragment_graph.internal_tables().into_values().collect_vec();
        let table_id_map = mgr
            .catalog_controller
            .create_internal_table_catalog(stream_job.id() as _, internal_tables)
            .await?;
        fragment_graph.refill_internal_table_ids(table_id_map);

        // create fragment and actor catalogs.
        tracing::debug!(id = stream_job.id(), "building stream job");
        let (ctx, table_fragments) = self
            .build_stream_job(env, &stream_job, fragment_graph)
            .await?;

        match &stream_job {
            StreamingJob::Table(None, table, TableJobType::SharedCdcSource) => {
                Self::validate_cdc_table(table, &table_fragments).await?;
            }
            StreamingJob::Table(Some(source), ..) => {
                // Register the source on the connector node.
                self.source_manager.register_source(source).await?;
            }
            StreamingJob::Sink(sink) => {
                // Validate the sink on the connector node.
                validate_sink(sink).await?;
            }
            StreamingJob::Source(source) => {
                // Register the source on the connector node.
                self.source_manager.register_source(source).await?;
            }
            _ => {}
        }

        let fragment_actors = CatalogController::extract_fragment_and_actors_from_table_fragments(
            table_fragments.to_protobuf(),
        )?;
        mgr.catalog_controller
            .create_fragment_actors(fragment_actors)
            .await?;

        // update fragment id and dml fragment id.
        mgr.catalog_controller
            .update_fragment_id_in_table(&stream_job)
            .await?;

        // TODO: check if the job is in background mode.
        // create streaming jobs.
        self.stream_manager
            .create_streaming_job(table_fragments, ctx)
            .await?;

        // finish the job.
        let version = mgr
            .catalog_controller
            .finish_streaming_job(stream_job.id() as _)
            .await?;
        tracing::debug!(id = stream_job.id(), "finished stream job");

        Ok(version)
    }
}
