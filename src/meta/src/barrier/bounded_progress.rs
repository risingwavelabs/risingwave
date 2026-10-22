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
use risingwave_meta_model::bounded_job::BoundedJobStatus;
use risingwave_meta_model::{JobStatus, bounded_job, object, streaming_job};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use sea_orm::prelude::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, JoinType, QueryFilter, QuerySelect, RelationTrait,
};

use crate::MetaResult;
use crate::barrier::load_progress::{LoadProgress, LoadReport};
use crate::controller::fragment::InflightFragmentInfo;

/// Lifecycle actions applied only after the checkpoint carrying their progress commits.
#[derive(Debug)]
pub(super) enum BoundedLoadAction {
    ListFinish {
        database_id: DatabaseId,
        source_id: SourceId,
    },
    Complete {
        job_id: JobId,
        source_id: SourceId,
    },
    Fail {
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
///
/// Any actor may fail the load before it completes:
///
///   Listing | Reading -> Failed
#[derive(Debug, Clone, Copy)]
enum Phase {
    Listing,
    Reading,
    Completed,
    Failed,
}

#[derive(Debug)]
pub(super) struct BoundedLoadTracker {
    pub source_id: SourceId,
    /// Actors that hand out work to readers and must finish listing
    /// Empty for sources that read their pinned input directly.
    listers: HashSet<ActorId>,
    /// Actors that consume the pinned input
    readers: HashSet<ActorId>,
    list_finished: HashSet<ActorId>,
    read_finished: HashSet<ActorId>,
    /// Set once any lister or reader reports `Failed`.
    failed: bool,
    phase: Phase,
}

impl BoundedLoadTracker {
    /// Builds the tracker for the load of `source_id`. `fragments` must contain the source.
    pub fn new(source_id: SourceId, fragments: &HashMap<FragmentId, InflightFragmentInfo>) -> Self {
        let mut listers = HashSet::new();
        let mut readers = HashSet::new();

        for (fragment, role) in bounded_roles(fragments, source_id) {
            let actors = fragment.actors.keys().copied();

            match role {
                BoundedRole::Lister => listers.extend(actors),
                BoundedRole::Reader => readers.extend(actors),
            }
        }

        assert!(
            !readers.is_empty(),
            "bounded source {source_id} has no reader actors"
        );

        let phase = if listers.is_empty() {
            Phase::Reading
        } else {
            Phase::Listing
        };

        Self {
            source_id,
            listers,
            readers,
            list_finished: HashSet::new(),
            read_finished: HashSet::new(),
            failed: false,
            phase,
        }
    }

    /// Records a progress report from a lister or reader.
    ///
    /// Reports are ignored once the load has completed or failed.
    pub fn apply_progress(&mut self, report: &LoadReport) {
        if self.is_completed() || self.is_failed() {
            return;
        }

        let actor_id = report.actor_id;
        match report.progress {
            LoadProgress::ListFinished if self.is_listing() && self.listers.contains(&actor_id) => {
                self.list_finished.insert(actor_id);
            }
            LoadProgress::ReaderDone if self.is_reading() && self.readers.contains(&actor_id) => {
                self.read_finished.insert(actor_id);
            }
            LoadProgress::Failed
                if self.listers.contains(&actor_id) || self.readers.contains(&actor_id) =>
            {
                self.failed = true;
            }
            _ => tracing::warn!(?report, "ignore unexpected bounded load progress"),
        }
    }

    /// Returns `true` once any actor has reported `Failed`.
    /// Returns `true` at most once per load.
    pub fn take_failed(&mut self) -> bool {
        if self.failed && !self.is_failed() {
            self.phase = Phase::Failed;
            true
        } else {
            false
        }
    }

    /// Returns `true` once every lister has reported `ListFinished`.
    /// Returns `true` at most once per load, and never when there are no listers.
    pub fn take_list_finished(&mut self) -> bool {
        if self.is_listing() && self.list_finished.len() == self.listers.len() {
            self.phase = Phase::Reading;
            true
        } else {
            false
        }
    }

    /// Returns `true` once every reader has reported `ReaderDone` after listing finished, if any.
    /// Returns `true` at most once per load.
    pub fn take_completed(&mut self) -> bool {
        if self.is_reading() && self.read_finished.len() == self.readers.len() {
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

    fn is_failed(&self) -> bool {
        matches!(self.phase, Phase::Failed)
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
        .filter(bounded_job::Column::Status.eq(BoundedJobStatus::Loading));

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

/// Returns the unfinished bounded loads whose streaming job is created, with their database. Only
/// these have actors to receive `Load { Start }`
pub(crate) async fn load_startable_bounded_jobs(
    conn: &impl ConnectionTrait,
) -> MetaResult<Vec<(JobId, SourceId, DatabaseId)>> {
    Ok(bounded_job::Entity::find()
        .select_only()
        .column(bounded_job::Column::JobId)
        .column(bounded_job::Column::SourceId)
        .column(object::Column::DatabaseId)
        .join(JoinType::InnerJoin, bounded_job::Relation::Job.def())
        .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
        .filter(bounded_job::Column::Status.eq(BoundedJobStatus::Loading))
        .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
        .into_tuple::<(JobId, SourceId, Option<DatabaseId>)>()
        .all(conn)
        .await?
        .into_iter()
        .filter_map(|(job_id, source_id, database_id)| Some((job_id, source_id, database_id?)))
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
            bounded_job::Column::Status,
            Expr::value(BoundedJobStatus::Completed),
        )
        .col_expr(
            bounded_job::Column::CompletedAt,
            Expr::value(chrono::Utc::now().timestamp_millis()),
        )
        .filter(bounded_job::Column::JobId.eq(job_id))
        .filter(bounded_job::Column::SourceId.eq(source_id))
        .filter(bounded_job::Column::Status.eq(BoundedJobStatus::Loading))
        .exec(conn)
        .await?;

    Ok(())
}

/// Marks the bounded load of `job_id` as failed. A failed load is not restarted on recovery and no
/// longer blocks changes to the job.
pub(crate) async fn mark_bounded_job_failed(
    conn: &impl ConnectionTrait,
    job_id: JobId,
    source_id: SourceId,
) -> MetaResult<()> {
    bounded_job::Entity::update_many()
        .col_expr(
            bounded_job::Column::Status,
            Expr::value(BoundedJobStatus::Failed),
        )
        .filter(bounded_job::Column::JobId.eq(job_id))
        .filter(bounded_job::Column::SourceId.eq(source_id))
        .filter(bounded_job::Column::Status.eq(BoundedJobStatus::Loading))
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
        .filter(bounded_job::Column::Status.eq(BoundedJobStatus::Loading))
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

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::FragmentTypeMask;
    use risingwave_common::id::WorkerId;
    use risingwave_connector::source::iceberg::IcebergBoundedScan;
    use risingwave_connector::source::ingest_mode::{BoundedInput, IngestMode};
    use risingwave_meta_model::fragment::DistributionType;
    use risingwave_pb::plan_common::PbSourceIngestMode;
    use risingwave_pb::stream_plan::{
        PbStreamNode, SourceNode, StreamFsFetch, StreamFsFetchNode, StreamSource,
    };

    use super::*;
    use crate::controller::fragment::InflightActorInfo;

    const SOURCE_ID: SourceId = SourceId::new(10);

    /// A source fragment with a fetch fragment yields listers and readers, starting in `Listing`.
    #[test]
    fn test_two_phase_roles() {
        let tracker = BoundedLoadTracker::new(SOURCE_ID, &two_phase());

        assert!(tracker.is_listing());
        assert_eq!(tracker.listers, HashSet::from([1.into(), 2.into()]));
        assert_eq!(tracker.readers, HashSet::from([3.into(), 4.into()]));
    }

    /// A source fragment without a fetch fragment becomes the readers, starting in `Reading`.
    #[test]
    fn test_one_phase_roles() {
        let tracker = BoundedLoadTracker::new(SOURCE_ID, &one_phase());

        assert!(tracker.is_reading());
        assert!(tracker.listers.is_empty());
        assert_eq!(tracker.readers, HashSet::from([1.into(), 2.into()]));
    }

    /// List finish fires once after every lister finishes, then completion fires once after every
    /// reader is done.
    #[test]
    fn test_two_phase_lifecycle() {
        let mut tracker = BoundedLoadTracker::new(SOURCE_ID, &two_phase());

        assert!(tracker.is_listing());

        tracker.apply_progress(&list_finished(1));
        assert!(!tracker.take_list_finished());

        tracker.apply_progress(&list_finished(2));
        assert!(tracker.take_list_finished());
        assert!(!tracker.take_list_finished());
        assert!(tracker.is_reading());

        tracker.apply_progress(&reader_done(3));
        assert!(!tracker.take_completed());

        tracker.apply_progress(&reader_done(4));
        assert!(tracker.take_completed());
        assert!(!tracker.take_completed());
        assert!(tracker.is_completed());
    }

    /// List finish never fires, and completion fires once after every reader is done.
    #[test]
    fn test_one_phase_lifecycle() {
        let mut tracker = BoundedLoadTracker::new(SOURCE_ID, &one_phase());

        assert!(!tracker.take_list_finished());

        tracker.apply_progress(&reader_done(1));
        assert!(!tracker.take_completed());

        tracker.apply_progress(&reader_done(2));
        assert!(!tracker.take_list_finished());
        assert!(tracker.take_completed());
        assert!(!tracker.take_completed());
    }

    /// A failure from any lister or reader fails the load once, and blocks list finish and
    /// completion.
    #[test]
    fn test_failed_lifecycle() {
        let mut tracker = BoundedLoadTracker::new(SOURCE_ID, &two_phase());

        tracker.apply_progress(&failed(1));
        assert!(tracker.take_failed());
        assert!(!tracker.take_failed());
        assert!(tracker.is_failed());

        tracker.apply_progress(&list_finished(1));
        tracker.apply_progress(&list_finished(2));
        assert!(!tracker.take_list_finished());

        let mut tracker = BoundedLoadTracker::new(SOURCE_ID, &one_phase());

        tracker.apply_progress(&reader_done(1));
        tracker.apply_progress(&reader_done(2));
        tracker.apply_progress(&failed(2));
        assert!(tracker.take_failed());
        assert!(!tracker.take_completed());
    }

    fn bounded_mode() -> PbSourceIngestMode {
        IngestMode::Bounded(BoundedInput::Iceberg(IcebergBoundedScan::Snapshot {
            snapshot_id: 1,
        }))
        .to_proto()
    }

    fn source_node(source_id: SourceId) -> PbStreamNode {
        PbStreamNode {
            node_body: Some(NodeBody::Source(Box::new(SourceNode {
                source_inner: Some(StreamSource {
                    source_id,
                    ingest_mode: Some(bounded_mode()),
                    ..Default::default()
                }),
            }))),
            ..Default::default()
        }
    }

    fn fetch_node(source_id: SourceId) -> PbStreamNode {
        PbStreamNode {
            node_body: Some(NodeBody::StreamFsFetch(Box::new(StreamFsFetchNode {
                node_inner: Some(StreamFsFetch {
                    source_id,
                    ingest_mode: Some(bounded_mode()),
                    ..Default::default()
                }),
            }))),
            ..Default::default()
        }
    }

    fn fragment(fragment_id: u32, actor_ids: &[u32], nodes: PbStreamNode) -> InflightFragmentInfo {
        InflightFragmentInfo {
            fragment_id: fragment_id.into(),
            distribution_type: DistributionType::Hash,
            fragment_type_mask: FragmentTypeMask::empty(),
            vnode_count: 0,
            nodes,
            actors: actor_ids
                .iter()
                .map(|actor_id| {
                    (
                        (*actor_id).into(),
                        InflightActorInfo {
                            worker_id: WorkerId::new(1),
                            vnode_bitmap: None,
                            splits: vec![],
                        },
                    )
                })
                .collect(),
            state_table_ids: HashSet::new(),
        }
    }

    fn fragments(
        fragments: impl IntoIterator<Item = InflightFragmentInfo>,
    ) -> HashMap<FragmentId, InflightFragmentInfo> {
        fragments
            .into_iter()
            .map(|fragment| (fragment.fragment_id, fragment))
            .collect()
    }

    /// Fragment 1 lists with actors 1-2, fragment 2 reads with actors 3-4.
    fn two_phase() -> HashMap<FragmentId, InflightFragmentInfo> {
        fragments([
            fragment(1, &[1, 2], source_node(SOURCE_ID)),
            fragment(2, &[3, 4], fetch_node(SOURCE_ID)),
        ])
    }

    /// Fragment 1 reads with actors 1-2.
    fn one_phase() -> HashMap<FragmentId, InflightFragmentInfo> {
        fragments([fragment(1, &[1, 2], source_node(SOURCE_ID))])
    }

    fn report(actor_id: u32, progress: LoadProgress) -> LoadReport {
        LoadReport {
            actor_id: actor_id.into(),
            source_id: SOURCE_ID,
            progress,
        }
    }

    fn list_finished(actor_id: u32) -> LoadReport {
        report(actor_id, LoadProgress::ListFinished)
    }

    fn reader_done(actor_id: u32) -> LoadReport {
        report(actor_id, LoadProgress::ReaderDone)
    }

    fn failed(actor_id: u32) -> LoadReport {
        report(actor_id, LoadProgress::Failed)
    }
}
