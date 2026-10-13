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
use std::sync::atomic::{AtomicU64, Ordering};

use risingwave_common::bail;
use risingwave_common::id::{ActorId, DatabaseId, FragmentId, JobId, SourceId};
use risingwave_common::util::stream_graph_visitor::visit_stream_node_body;
use risingwave_connector::source::refresh_mode::RefreshMode;
use risingwave_meta_model::{bounded_job, object};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{BoundedLoadAttempt, StreamNode};
use risingwave_pb::stream_service::barrier_complete_response::BoundedLoadProgress;
use risingwave_pb::stream_service::barrier_complete_response::bounded_load_progress::Progress;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, JoinType, QueryFilter, QuerySelect, RelationTrait,
};

use crate::MetaResult;
use crate::controller::fragment::InflightFragmentInfo;

// Generations must be unique across all bounded jobs in this meta process: actors use
// the generation alone to decide whether a bounded load mutation targets their attempt.
// Recovery isolates responses from the previous meta process before starting new attempts.
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

/// Lifecycle actions applied only after the checkpoint carrying their progress commits.
#[derive(Debug)]
pub(super) enum BoundedLoadAction {
    SealListing {
        database_id: DatabaseId,
        attempt: BoundedLoadAttempt,
    },
    Complete {
        job_id: JobId,
        source_id: SourceId,
    },
}

/// Tracks one whole-job attempt. Actor progress is never restored individually:
/// recovery restarts every list/fetch actor unless the durable job row is complete.
#[derive(Debug)]
pub(super) struct BoundedLoadTracker {
    pub source_id: SourceId,
    attempt: BoundedLoadAttempt,
    list_actors: HashMap<ActorId, FragmentId>,
    fetch_actors: HashMap<ActorId, FragmentId>,
    listed: HashSet<ActorId>,
    fetched: HashSet<ActorId>,
    seal_taken: bool,
    completion_taken: bool,
}

impl BoundedLoadTracker {
    /// Creates a tracker for a new attempt with a fresh generation.
    ///
    /// List and fetch actors come from the fragments whose source or fetch node refers to
    /// `source_id`. Fails if the job has no list or no fetch actors.
    pub fn new(
        job_id: JobId,
        source_id: SourceId,
        fragments: &HashMap<FragmentId, InflightFragmentInfo>,
    ) -> MetaResult<Self> {
        let mut list_actors = HashMap::new();
        let mut fetch_actors = HashMap::new();

        for fragment in fragments.values() {
            let actors = match bounded_role(&fragment.nodes, source_id) {
                Some(BoundedRole::List) => &mut list_actors,
                Some(BoundedRole::Fetch) => &mut fetch_actors,
                None => continue,
            };

            actors.extend(
                fragment
                    .actors
                    .keys()
                    .map(|actor| (*actor, fragment.fragment_id)),
            );
        }

        if list_actors.is_empty() || fetch_actors.is_empty() {
            bail!("bounded job {job_id} requires list and fetch actors");
        }

        Ok(Self {
            source_id,
            attempt: BoundedLoadAttempt {
                generation: NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
            },
            list_actors,
            fetch_actors,
            listed: HashSet::new(),
            fetched: HashSet::new(),
            seal_taken: false,
            completion_taken: false,
        })
    }

    /// Returns the id of the bounded source in the job's fragments, or `None` if the job is not a
    /// bounded load. Source nodes without `source_inner` (such as the DML input) are skipped.
    pub fn bounded_source(
        fragments: &HashMap<FragmentId, InflightFragmentInfo>,
    ) -> Option<SourceId> {
        let mut source_id = None;

        for fragment in fragments.values() {
            visit_stream_node_body(&fragment.nodes, |body| {
                if let NodeBody::Source(source) = body
                    && let Some(source) = &source.source_inner
                    && RefreshMode::from_proto(source.refresh_mode.as_ref())
                        .is_ok_and(|mode| mode.is_bounded())
                {
                    source_id = Some(source.source_id);
                }
            });
        }

        source_id
    }

    /// Pairs every list and fetch actor with the current attempt, to be sent in an `AddMutation`
    /// so each actor starts loading under this generation.
    pub fn attempts(&self) -> impl Iterator<Item = (ActorId, BoundedLoadAttempt)> + '_ {
        self.list_actors
            .keys()
            .chain(self.fetch_actors.keys())
            .map(|actor| (*actor, self.attempt))
    }

    /// Records a progress report from a list or fetch actor.
    ///
    /// Reports are ignored after completion or if they belong to another generation. Reports from
    /// unknown actors, and `FetchDone` received before the seal, are dropped with a warning.
    pub fn apply_progress(&mut self, report: &BoundedLoadProgress) {
        if self.completion_taken || report.generation != self.attempt.generation {
            return;
        }
        match &report.progress {
            Some(Progress::ListSealed(_))
                if self.list_actors.get(&report.actor_id) == Some(&report.fragment_id) =>
            {
                self.listed.insert(report.actor_id);
            }
            Some(Progress::FetchDone(_))
                if self.seal_taken
                    && self.fetch_actors.get(&report.actor_id) == Some(&report.fragment_id) =>
            {
                self.fetched.insert(report.actor_id);
            }
            _ => tracing::warn!(?report, "ignore unexpected bounded load progress"),
        }
    }

    /// Returns the attempt to seal once every list actor has reported `ListSealed`.
    /// Returns `Some` at most once per attempt.
    pub fn take_seal(&mut self) -> Option<BoundedLoadAttempt> {
        if !self.seal_taken && self.listed.len() == self.list_actors.len() {
            self.seal_taken = true;
            Some(self.attempt)
        } else {
            None
        }
    }

    /// Returns `true` once the seal has been taken and every fetch actor has reported
    /// `FetchDone`. Returns `true` at most once per attempt.
    pub fn take_completed(&mut self) -> bool {
        if !self.completion_taken
            && self.seal_taken
            && self.fetched.len() == self.fetch_actors.len()
        {
            self.completion_taken = true;
            true
        } else {
            false
        }
    }
}

enum BoundedRole {
    List,
    Fetch,
}

/// Returns the role a fragment plays in the bounded load of `source_id`, if any.
fn bounded_role(nodes: &StreamNode, source_id: SourceId) -> Option<BoundedRole> {
    let mut role = None;
    visit_stream_node_body(nodes, |body| match body {
        NodeBody::Source(node)
            if node
                .source_inner
                .as_ref()
                .is_some_and(|source| source.source_id == source_id) =>
        {
            role = Some(BoundedRole::List);
        }
        NodeBody::StreamFsFetch(node)
            if node
                .node_inner
                .as_ref()
                .is_some_and(|fetch| fetch.source_id == source_id) =>
        {
            role = Some(BoundedRole::Fetch);
        }
        _ => {}
    });
    role
}

pub(crate) async fn load_unfinished_bounded_jobs(
    conn: &impl ConnectionTrait,
    database_id: Option<DatabaseId>,
) -> MetaResult<HashMap<JobId, SourceId>> {
    let mut query = bounded_job::Entity::find()
        .select_only()
        .column(bounded_job::Column::JobId)
        .column(bounded_job::Column::SourceId)
        .filter(bounded_job::Column::CompletedAt.is_null());
    if let Some(database_id) = database_id {
        query = query
            .join(JoinType::InnerJoin, bounded_job::Relation::Job.def())
            .filter(object::Column::DatabaseId.eq(database_id));
    }
    Ok(query
        .into_tuple::<(JobId, SourceId)>()
        .all(conn)
        .await?
        .into_iter()
        .collect())
}

/// Fails if any of `job_ids` has an unfinished bounded load.
///
/// A [`BoundedLoadTracker`] counts progress against the actors captured when the attempt starts,
/// so operations that change a job's actors (reschedule, replace) would leave the load unable to
/// complete. Call this inside the same transaction as such operations.
pub(crate) async fn ensure_not_loading_bounded(
    conn: &impl ConnectionTrait,
    job_ids: impl IntoIterator<Item = JobId>,
) -> MetaResult<()> {
    let loading = bounded_job::Entity::find()
        .filter(bounded_job::Column::JobId.is_in(job_ids))
        .filter(bounded_job::Column::CompletedAt.is_null())
        .one(conn)
        .await?;

    if let Some(job) = loading {
        bail!(
            "Cannot change job {} while bounded ingestion is loading",
            job.job_id
        );
    }

    Ok(())
}
