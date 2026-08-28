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

use risingwave_pb::id::{ActorId, FragmentId, IcebergCompactionTaskId, SinkId};
use risingwave_pb::stream_service::{BarrierCompleteResponse, PbIcebergPkIndexSinkRole};

use super::render::CompactionResolverRenderResult;
use crate::controller::fragment::InflightFragmentInfo;
use crate::manager::iceberg_compaction::CompactionResolveCompletion;
use crate::manager::iceberg_pk_index_sink::{CompactionOverwrite, IcebergPkIndexPreCommitMetadata};

/// A transient resolver attached to the database main graph until its detach checkpoint.
#[derive(Debug)]
pub(crate) struct CompactionResolveJob {
    sink_id: SinkId,
    task_id: IcebergCompactionTaskId,
    completion: Arc<CompactionResolveCompletion>,
    render_result: CompactionResolverRenderResult,
    overwrite: CompactionOverwrite,
}

impl CompactionResolveJob {
    pub(crate) fn new(
        sink_id: SinkId,
        task_id: IcebergCompactionTaskId,
        completion: Arc<CompactionResolveCompletion>,
        overwrite: CompactionOverwrite,
        render_result: CompactionResolverRenderResult,
    ) -> Self {
        debug_assert_eq!(overwrite.sink_id, sink_id);
        tracing::info!(%sink_id, %task_id, commit_epoch = overwrite.epoch, "created main-graph pk-index resolver job");
        Self {
            sink_id,
            task_id,
            completion,
            render_result,
            overwrite,
        }
    }

    fn commit_epoch(&self) -> u64 {
        self.overwrite.epoch
    }

    fn fragment_infos(&self) -> &HashMap<FragmentId, InflightFragmentInfo> {
        &self.render_result.fragment_infos
    }

    fn node_actors(&self) -> &HashMap<risingwave_meta_model::WorkerId, HashSet<ActorId>> {
        &self.render_result.node_actors
    }

    /// Complete resolver execution at the detach checkpoint and contribute the overwrite.
    fn complete<'a>(
        self,
        responses: impl Iterator<Item = &'a BarrierCompleteResponse>,
    ) -> (
        IcebergPkIndexPreCommitMetadata,
        PendingCompactionResolveCommit,
    ) {
        let Self {
            sink_id,
            task_id,
            completion,
            render_result,
            overwrite,
        } = self;
        let commit_epoch = overwrite.epoch;
        for response in responses {
            for metadata in &response.iceberg_pk_index_sink_metadata {
                if PbIcebergPkIndexSinkRole::try_from(metadata.role)
                    != Ok(PbIcebergPkIndexSinkRole::CompactionResolver)
                {
                    continue;
                }
                assert_eq!(metadata.sink_id, sink_id);
                assert_eq!(metadata.prev_epoch, commit_epoch);
                assert_eq!(metadata.reporter_actor_id, render_result.resolver_actor_id);
            }
        }

        (
            overwrite.into(),
            PendingCompactionResolveCommit {
                sink_id,
                task_id,
                commit_epoch,
                completion,
            },
        )
    }
}

#[derive(Debug)]
struct PendingCompactionResolveCommit {
    sink_id: SinkId,
    task_id: IcebergCompactionTaskId,
    commit_epoch: u64,
    completion: Arc<CompactionResolveCompletion>,
}

impl PendingCompactionResolveCommit {
    fn finish(self) {
        tracing::info!(sink_id = %self.sink_id, task_id = %self.task_id, "finish main-graph pk-index resolver job");
        self.completion.finish(true);
    }
}

/// Owns the resolver job across its transient actor and durable-commit lifetimes.
#[derive(Debug, Default)]
pub(crate) struct CompactionResolveJobRegistry {
    resolving: HashMap<SinkId, CompactionResolveJob>,
    pending_commits: HashMap<SinkId, PendingCompactionResolveCommit>,
}

impl CompactionResolveJobRegistry {
    /// Insert a new resolver job, returning `false` if a job for the same sink already exists.
    pub(crate) fn insert(&mut self, job: CompactionResolveJob) -> bool {
        if self.contains_sink(job.sink_id) {
            return false;
        }
        self.resolving.insert(job.sink_id, job);
        true
    }

    pub(crate) fn contains_sink(&self, sink_id: SinkId) -> bool {
        self.resolving.contains_key(&sink_id) || self.pending_commits.contains_key(&sink_id)
    }

    pub(crate) fn contains_worker(&self, worker_id: risingwave_meta_model::WorkerId) -> bool {
        self.resolving.values().any(|job| {
            InflightFragmentInfo::contains_worker(job.fragment_infos().values(), worker_id)
        })
    }

    pub(crate) fn extend_node_actors(
        &self,
        node_actors: &mut HashMap<risingwave_meta_model::WorkerId, HashSet<ActorId>>,
    ) {
        for job in self.resolving.values() {
            for (worker_id, actor_ids) in job.node_actors() {
                node_actors
                    .entry(*worker_id)
                    .or_default()
                    .extend(actor_ids.iter().copied());
            }
        }
    }

    /// Complete resolving jobs for `commit_epoch` and move them to pending durable commits.
    pub(crate) fn complete_resolve<'a>(
        &mut self,
        commit_epoch: u64,
        responses: impl Iterator<Item = &'a BarrierCompleteResponse> + Clone,
    ) -> Vec<IcebergPkIndexPreCommitMetadata> {
        let Self {
            resolving,
            pending_commits,
        } = self;
        resolving
            .extract_if(|_, job| job.commit_epoch() == commit_epoch)
            .map(|(sink_id, job)| {
                let (metadata, pending_commit) = job.complete(responses.clone());
                let unique = pending_commits.insert(sink_id, pending_commit).is_none();
                assert!(unique, "duplicate pending commit for sink_id={sink_id}");
                metadata
            })
            .collect()
    }

    pub(crate) fn ack_committed(&mut self, commit_epoch: u64) {
        for (_, pending_commit) in self
            .pending_commits
            .extract_if(|_, pending_commit| pending_commit.commit_epoch == commit_epoch)
        {
            pending_commit.finish();
        }
    }

    pub(crate) fn clear(&mut self) {
        self.resolving.clear();
        self.pending_commits.clear();
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
