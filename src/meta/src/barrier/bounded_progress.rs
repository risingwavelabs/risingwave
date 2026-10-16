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

use risingwave_common::bail;
use risingwave_common::id::{ActorId, DatabaseId, FragmentId, JobId, SourceId};
use risingwave_common::util::stream_graph_visitor::visit_stream_node_body;
use risingwave_connector::source::refresh_mode::RefreshMode;
use risingwave_meta_model::{bounded_job, object};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_service::barrier_complete_response::BoundedLoadProgress;
use risingwave_pb::stream_service::barrier_complete_response::bounded_load_progress::Progress;
use sea_orm::prelude::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, JoinType, QueryFilter, QuerySelect, RelationTrait,
};

use crate::MetaResult;
use crate::controller::fragment::InflightFragmentInfo;

/// Lifecycle actions applied only after the checkpoint carrying their progress commits.
#[derive(Debug)]
pub(super) enum BoundedLoadAction {
    SealListing {
        database_id: DatabaseId,
        source_id: SourceId,
    },
    Complete {
        job_id: JobId,
        source_id: SourceId,
    },
}

/// two-phase source (iceberg, object store, files)
///
///   Listing -> Reading -> Completed
///
/// one-phase source (postgres, kafka, etc)
///
///   Reading -> Completed
#[derive(Debug, Clone, Copy)]
enum Phase {
    Listing,
    Reading,
    Completed,
}

#[derive(Debug)]
pub(super) struct BoundedLoadTracker {
    pub source_id: SourceId,
    /// Actors that hand out work to readers and must be sealed, e.g. Iceberg list actors.
    /// Empty for sources that read their pinned input directly.
    listers: HashMap<ActorId, FragmentId>,
    /// Actors that consume the pinned input, e.g. Iceberg fetch actors.
    readers: HashMap<ActorId, FragmentId>,
    sealed: HashSet<ActorId>,
    done: HashSet<ActorId>,
    phase: Phase,
}

impl BoundedLoadTracker {
    pub fn new(
        job_id: JobId,
        source_id: SourceId,
        fragments: &HashMap<FragmentId, InflightFragmentInfo>,
    ) -> MetaResult<Self> {
        let mut listers = HashMap::new();
        let mut readers = HashMap::new();

        for (fragment, role) in bounded_roles(fragments, source_id) {
            let actors = match role {
                BoundedRole::Lister => &mut listers,
                BoundedRole::Reader => &mut readers,
            };

            actors.extend(
                fragment
                    .actors
                    .keys()
                    .map(|actor| (*actor, fragment.fragment_id)),
            );
        }

        if readers.is_empty() {
            bail!("bounded job {job_id} requires reader actors");
        }

        let phase = if listers.is_empty() {
            Phase::Reading
        } else {
            Phase::Listing
        };

        Ok(Self {
            source_id,
            listers,
            readers,
            sealed: HashSet::new(),
            done: HashSet::new(),
            phase,
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

    /// Returns every lister and reader, to be sent in an `AddMutation` so each actor starts
    /// loading on its first barrier.
    pub fn actors(&self) -> impl Iterator<Item = ActorId> + '_ {
        self.listers.keys().chain(self.readers.keys()).copied()
    }

    /// Records a progress report from a lister or reader.
    ///
    /// Reports are ignored after completion.
    pub fn apply_progress(&mut self, report: &BoundedLoadProgress) {
        if self.is_completed() {
            return;
        }

        match &report.progress {
            Some(Progress::ListSealed(_))
                if self.is_listing()
                    && self.listers.get(&report.actor_id) == Some(&report.fragment_id) =>
            {
                self.sealed.insert(report.actor_id);
            }
            Some(Progress::ReaderDone(_))
                if self.is_reading()
                    && self.readers.get(&report.actor_id) == Some(&report.fragment_id) =>
            {
                self.done.insert(report.actor_id);
            }
            _ => tracing::warn!(?report, "ignore unexpected bounded load progress"),
        }
    }

    /// Returns `true` once every lister has reported `ListSealed`.
    /// Returns `true` at most once per load, and never when there are no listers.
    pub fn take_seal(&mut self) -> bool {
        if self.is_listing() && self.sealed.len() == self.listers.len() {
            self.phase = Phase::Reading;
            true
        } else {
            false
        }
    }

    /// Returns `true` once every reader has reported `ReaderDone` after the seal, if any.
    /// Returns `true` at most once per load.
    pub fn take_completed(&mut self) -> bool {
        if self.is_reading() && self.done.len() == self.readers.len() {
            self.phase = Phase::Completed;
            true
        } else {
            false
        }
    }

    fn is_listing(&self) -> bool {
        matches!(self.phase, Phase::Listing)
    }

    fn is_reading(&self) -> bool {
        matches!(self.phase, Phase::Reading)
    }

    fn is_completed(&self) -> bool {
        matches!(self.phase, Phase::Completed)
    }
}

#[derive(Clone, Copy)]
enum BoundedRole {
    Lister,
    Reader,
}

/// Returns the fragments taking part in the bounded load of `source_id`, with their roles.
fn bounded_roles(
    fragments: &HashMap<FragmentId, InflightFragmentInfo>,
    source_id: SourceId,
) -> Vec<(&InflightFragmentInfo, BoundedRole)> {
    let mut sources = vec![];
    let mut fetches = vec![];

    for fragment in fragments.values() {
        visit_stream_node_body(&fragment.nodes, |body| match body {
            NodeBody::Source(node)
                if node
                    .source_inner
                    .as_ref()
                    .is_some_and(|source| source.source_id == source_id) =>
            {
                sources.push(fragment);
            }
            NodeBody::StreamFsFetch(node)
                if node
                    .node_inner
                    .as_ref()
                    .is_some_and(|fetch| fetch.source_id == source_id) =>
            {
                fetches.push(fragment);
            }
            _ => {}
        });
    }

    // if there are no fetchers, then this is a one-phase source, so it's role becomes a reader instead of a lister
    let source_role = if fetches.is_empty() {
        BoundedRole::Reader
    } else {
        BoundedRole::Lister
    };

    sources
        .into_iter()
        .map(|fragment| (fragment, source_role))
        .chain(
            fetches
                .into_iter()
                .map(|fragment| (fragment, BoundedRole::Reader)),
        )
        .collect()
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

/// Marks the bounded load of `job_id` as complete.
///
/// Call only after the checkpoint carrying the final `ReaderDone` reports has committed, so a
/// recovery never skips a load whose data is not durable yet.
pub(crate) async fn mark_bounded_job_complete(
    conn: &impl ConnectionTrait,
    job_id: JobId,
    source_id: SourceId,
) -> MetaResult<()> {
    bounded_job::Entity::update_many()
        .col_expr(
            bounded_job::Column::CompletedAt,
            Expr::value(chrono::Utc::now().timestamp_millis()),
        )
        .filter(bounded_job::Column::JobId.eq(job_id))
        .filter(bounded_job::Column::SourceId.eq(source_id))
        .filter(bounded_job::Column::CompletedAt.is_null())
        .exec(conn)
        .await?;

    Ok(())
}

/// Fails if any of `job_ids` has an unfinished bounded load.
///
/// A [`BoundedLoadTracker`] counts progress against the actors captured when the load starts,
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
