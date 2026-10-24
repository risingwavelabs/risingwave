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
use std::mem::take;

use risingwave_common::bail;
use risingwave_common::id::{ActorId, DatabaseId, JobId, SourceId};
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
#[derive(Debug, PartialEq, Eq)]
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
    job_id: JobId,
    database_id: DatabaseId,
    /// Actors that hand out work to readers and must finish listing
    /// Empty for sources that read their pinned input directly.
    listers: HashSet<ActorId>,
    /// Actors that consume the pinned input
    readers: HashSet<ActorId>,
    list_finished: HashSet<ActorId>,
    read_finished: HashSet<ActorId>,
    phase: Phase,
    /// Actions of the collected barriers, applied once their checkpoint commits.
    actions: Vec<BoundedLoadAction>,
}

impl BoundedLoadTracker {
    pub fn new<'a>(
        database_id: DatabaseId,
        job_id: JobId,
        source_id: SourceId,
        fragments: impl IntoIterator<Item = &'a InflightFragmentInfo>,
    ) -> Self {
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
            database_id,
            job_id,
            source_id,
            listers,
            readers,
            list_finished: HashSet::new(),
            read_finished: HashSet::new(),
            phase,
            actions: vec![],
        }
    }

    pub fn apply_progress(&mut self, report: &LoadReport) {
        let actor_id = report.actor_id;

        let action = match (self.phase, report.progress) {
            (Phase::Listing | Phase::Reading, LoadProgress::Failed)
                if self.listers.contains(&actor_id) || self.readers.contains(&actor_id) =>
            {
                self.phase = Phase::Failed;

                Some(BoundedLoadAction::Fail {
                    job_id: self.job_id,
                    source_id: self.source_id,
                })
            }
            (Phase::Listing, LoadProgress::ListFinished) if self.listers.contains(&actor_id) => {
                self.list_finished.insert(actor_id);

                (self.list_finished.len() == self.listers.len()).then(|| {
                    self.phase = Phase::Reading;

                    BoundedLoadAction::ListFinish {
                        database_id: self.database_id,
                        source_id: self.source_id,
                    }
                })
            }
            (Phase::Reading, LoadProgress::ReaderDone) if self.readers.contains(&actor_id) => {
                self.read_finished.insert(actor_id);

                (self.read_finished.len() == self.readers.len()).then(|| {
                    self.phase = Phase::Completed;

                    BoundedLoadAction::Complete {
                        job_id: self.job_id,
                        source_id: self.source_id,
                    }
                })
            }
            // Other actors keep reporting after a failure, e.g. failing on the same cause.
            (Phase::Failed, _) => None,
            _ => {
                tracing::warn!(?report, "ignore unexpected bounded load progress");
                None
            }
        };

        self.actions.extend(action);
    }

    /// Takes the queued actions, once their checkpoint commits.
    pub fn take_actions(&mut self) -> Vec<BoundedLoadAction> {
        take(&mut self.actions)
    }
}

#[derive(Clone, Copy)]
enum BoundedRole {
    Lister,
    Reader,
}

/// Returns the fragments taking part in the bounded load of `source_id`, with their roles.
fn bounded_roles<'a>(
    fragments: impl IntoIterator<Item = &'a InflightFragmentInfo>,
    source_id: SourceId,
) -> Vec<(&'a InflightFragmentInfo, BoundedRole)> {
    let mut sources = vec![];
    let mut fetches = vec![];

    for fragment in fragments {
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

    const DATABASE_ID: DatabaseId = DatabaseId::new(1);
    const JOB_ID: JobId = JobId::new(2);
    const SOURCE_ID: SourceId = SourceId::new(10);

    /// A source fragment with a fetch fragment yields listers and readers, starting in `Listing`.
    #[test]
    fn test_two_phase_roles() {
        let tracker = BoundedLoadTracker::new(DATABASE_ID, JOB_ID, SOURCE_ID, &two_phase());

        assert!(matches!(tracker.phase, Phase::Listing));
        assert_eq!(tracker.listers, HashSet::from([1.into(), 2.into()]));
        assert_eq!(tracker.readers, HashSet::from([3.into(), 4.into()]));
    }

    /// A source fragment without a fetch fragment becomes the readers, starting in `Reading`.
    #[test]
    fn test_one_phase_roles() {
        let tracker = BoundedLoadTracker::new(DATABASE_ID, JOB_ID, SOURCE_ID, &one_phase());

        assert!(matches!(tracker.phase, Phase::Reading));
        assert!(tracker.listers.is_empty());
        assert_eq!(tracker.readers, HashSet::from([1.into(), 2.into()]));
    }

    /// List finish fires once after every lister finishes, then completion fires once after every
    /// reader is done.
    #[test]
    fn test_two_phase_lifecycle() {
        let mut tracker = BoundedLoadTracker::new(DATABASE_ID, JOB_ID, SOURCE_ID, &two_phase());

        tracker.apply_progress(&list_finished(1));
        assert!(tracker.take_actions().is_empty());

        tracker.apply_progress(&list_finished(2));
        tracker.apply_progress(&list_finished(2));
        assert_eq!(tracker.take_actions(), vec![list_finish()]);

        tracker.apply_progress(&reader_done(3));
        assert!(tracker.take_actions().is_empty());

        tracker.apply_progress(&reader_done(4));
        tracker.apply_progress(&reader_done(4));
        assert_eq!(tracker.take_actions(), vec![complete()]);
    }

    /// List finish never fires, and completion fires once after every reader is done.
    #[test]
    fn test_one_phase_lifecycle() {
        let mut tracker = BoundedLoadTracker::new(DATABASE_ID, JOB_ID, SOURCE_ID, &one_phase());

        tracker.apply_progress(&reader_done(1));
        assert!(tracker.take_actions().is_empty());

        tracker.apply_progress(&reader_done(2));
        tracker.apply_progress(&reader_done(2));
        assert_eq!(tracker.take_actions(), vec![complete()]);
    }

    /// A failure from any lister or reader fails the load once, and blocks list finish and
    /// completion.
    #[test]
    fn test_failed_lifecycle() {
        let mut tracker = BoundedLoadTracker::new(DATABASE_ID, JOB_ID, SOURCE_ID, &two_phase());

        tracker.apply_progress(&failed(1));
        tracker.apply_progress(&failed(2));
        tracker.apply_progress(&list_finished(1));
        tracker.apply_progress(&list_finished(2));
        assert_eq!(tracker.take_actions(), vec![fail()]);

        let mut tracker = BoundedLoadTracker::new(DATABASE_ID, JOB_ID, SOURCE_ID, &one_phase());

        tracker.apply_progress(&reader_done(1));
        tracker.apply_progress(&failed(2));
        tracker.apply_progress(&reader_done(2));
        assert_eq!(tracker.take_actions(), vec![fail()]);
    }

    fn list_finish() -> BoundedLoadAction {
        BoundedLoadAction::ListFinish {
            database_id: DATABASE_ID,
            source_id: SOURCE_ID,
        }
    }

    fn complete() -> BoundedLoadAction {
        BoundedLoadAction::Complete {
            job_id: JOB_ID,
            source_id: SOURCE_ID,
        }
    }

    fn fail() -> BoundedLoadAction {
        BoundedLoadAction::Fail {
            job_id: JOB_ID,
            source_id: SOURCE_ID,
        }
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

    /// Fragment 1 lists with actors 1-2, fragment 2 reads with actors 3-4.
    fn two_phase() -> Vec<InflightFragmentInfo> {
        vec![
            fragment(1, &[1, 2], source_node(SOURCE_ID)),
            fragment(2, &[3, 4], fetch_node(SOURCE_ID)),
        ]
    }

    /// Fragment 1 reads with actors 1-2.
    fn one_phase() -> Vec<InflightFragmentInfo> {
        vec![fragment(1, &[1, 2], source_node(SOURCE_ID))]
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
