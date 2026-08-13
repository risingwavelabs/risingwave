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

use iceberg::spec::SerializedDataFile;
use risingwave_pb::id::{ActorId, FragmentId, IcebergCompactionTaskId, SinkId};
use risingwave_pb::stream_service::{BarrierCompleteResponse, PbIcebergPkIndexSinkRole};
use tracing::info;

use super::render::CompactionResolverRenderResult;
use crate::controller::fragment::InflightFragmentInfo;
use crate::manager::iceberg_compaction::CompactionResolveCompletion;
use crate::manager::iceberg_pk_index_sink::{CompactionOverwrite, IcebergPkIndexPreCommitMetadata};

#[derive(educe::Educe)]
#[educe(Debug)]
pub(crate) struct OverwriteInput {
    pub sink_id: SinkId,
    pub schema_id: i32,
    pub partition_spec_id: i32,
    #[educe(Debug(ignore))]
    pub output_files: Vec<SerializedDataFile>,
    pub input_file_paths: Vec<String>,
    pub read_snapshot_id: i64,
}

#[derive(Debug)]
enum Phase {
    Resolving {
        render_result: CompactionResolverRenderResult,
        b2_prev_epoch: u64,
        overwrite: OverwriteInput,
    },
    Committing {
        b2_prev_epoch: u64,
    },
    Done,
}

/// Bookkeeping for the transient resolver that participates in the database main graph at B1/B2.
#[derive(Debug)]
pub(crate) struct CompactionResolveJobControl {
    sink_id: SinkId,
    task_id: IcebergCompactionTaskId,
    completion: Arc<CompactionResolveCompletion>,
    phase: Phase,
}

impl CompactionResolveJobControl {
    pub(crate) fn new(
        sink_id: SinkId,
        task_id: IcebergCompactionTaskId,
        completion: Arc<CompactionResolveCompletion>,
        b2_prev_epoch: u64,
        overwrite: OverwriteInput,
        render_result: CompactionResolverRenderResult,
    ) -> Self {
        assert!(render_result.state_table_ids.is_empty());
        info!(%sink_id, %task_id, b2_prev_epoch, "created main-graph pk-index resolver job");
        Self {
            sink_id,
            task_id,
            completion,
            phase: Phase::Resolving {
                render_result,
                b2_prev_epoch,
                overwrite,
            },
        }
    }

    pub(crate) fn fragment_infos(&self) -> Option<&HashMap<FragmentId, InflightFragmentInfo>> {
        match &self.phase {
            Phase::Resolving { render_result, .. } => Some(&render_result.fragment_infos),
            Phase::Committing { .. } | Phase::Done => None,
        }
    }

    pub(crate) fn node_actors(
        &self,
    ) -> Option<&HashMap<risingwave_meta_model::WorkerId, HashSet<ActorId>>> {
        match &self.phase {
            Phase::Resolving { render_result, .. } => Some(&render_result.node_actors),
            Phase::Committing { .. } | Phase::Done => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn task_id(&self) -> IcebergCompactionTaskId {
        self.task_id
    }

    #[cfg(test)]
    pub(crate) fn resolver_actor_ids(&self) -> &[ActorId] {
        match &self.phase {
            Phase::Resolving { render_result, .. } => &render_result.resolver_actor_ids,
            Phase::Committing { .. } | Phase::Done => &[],
        }
    }

    /// Collect resolver reports from the main B2 responses and contribute the overwrite exactly once.
    pub(crate) fn finish_b2<'a>(
        &mut self,
        epoch: u64,
        responses: impl Iterator<Item = &'a BarrierCompleteResponse>,
    ) -> Option<IcebergPkIndexPreCommitMetadata> {
        let Phase::Resolving {
            b2_prev_epoch,
            render_result,
            ..
        } = &self.phase
        else {
            return None;
        };
        if epoch != *b2_prev_epoch {
            return None;
        }
        let expected_actor_ids: HashSet<_> =
            render_result.resolver_actor_ids.iter().copied().collect();
        for response in responses {
            for metadata in &response.iceberg_pk_index_sink_metadata {
                if PbIcebergPkIndexSinkRole::try_from(metadata.role)
                    != Ok(PbIcebergPkIndexSinkRole::CompactionResolver)
                {
                    continue;
                }
                assert_eq!(metadata.sink_id, self.sink_id);
                assert_eq!(metadata.prev_epoch, epoch);
                assert!(expected_actor_ids.contains(&metadata.reporter_actor_id));
            }
        }

        let Phase::Resolving {
            b2_prev_epoch,
            overwrite,
            ..
        } = std::mem::replace(&mut self.phase, Phase::Done)
        else {
            unreachable!()
        };
        let OverwriteInput {
            sink_id,
            schema_id,
            partition_spec_id,
            output_files,
            input_file_paths,
            read_snapshot_id,
        } = overwrite;
        self.phase = Phase::Committing { b2_prev_epoch };
        Some(IcebergPkIndexPreCommitMetadata {
            sink_id,
            prev_epoch: epoch,
            reports: vec![],
            compaction: Some(CompactionOverwrite {
                sink_id,
                epoch,
                schema_id,
                partition_spec_id,
                output_files,
                input_file_paths,
                read_snapshot_id,
            }),
        })
    }

    /// B2 commit/ack is the terminal event; no side-channel writer control is required.
    pub(crate) fn on_main_graph_committed(&mut self, epoch: u64) -> bool {
        let Phase::Committing { b2_prev_epoch } = self.phase else {
            return false;
        };
        if epoch != b2_prev_epoch {
            return false;
        }
        info!(sink_id = %self.sink_id, task_id = %self.task_id, "finish main-graph pk-index resolver job");
        self.phase = Phase::Done;
        self.completion.finish(true);
        true
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
