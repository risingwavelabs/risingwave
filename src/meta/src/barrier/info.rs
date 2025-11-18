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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::mem::replace;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RawRwLock;
use parking_lot::lock_api::RwLockReadGuard;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{DatabaseId, FragmentTypeFlag, FragmentTypeMask, TableId};
use risingwave_common::id::JobId;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_mut;
use risingwave_connector::source::{SplitImpl, SplitMetaData};
use risingwave_meta_model::WorkerId;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::meta::PbFragmentWorkerSlotMapping;
use risingwave_pb::meta::subscribe_response::Operation;
use risingwave_pb::stream_plan::PbUpstreamSinkInfo;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use tracing::{info, warn};

use crate::MetaResult;
use crate::barrier::edge_builder::{FragmentEdgeBuildResult, FragmentEdgeBuilder};
use crate::barrier::progress::{CreateMviewProgressTracker, StagingCommitInfo};
use crate::barrier::rpc::ControlStreamManager;
use crate::barrier::{BarrierKind, Command, CreateStreamingJobType, TracedEpoch};
use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::controller::utils::rebuild_fragment_mapping;
use crate::manager::NotificationManagerRef;
use crate::model::{ActorId, BackfillUpstreamType, FragmentId, StreamJobFragments};

#[derive(Debug, Clone)]
pub struct SharedFragmentInfo {
    pub fragment_id: FragmentId,
    pub job_id: JobId,
    pub distribution_type: DistributionType,
    pub actors: HashMap<ActorId, InflightActorInfo>,
    pub vnode_count: usize,
    pub fragment_type_mask: FragmentTypeMask,
}

impl From<(&InflightFragmentInfo, JobId)> for SharedFragmentInfo {
    fn from(pair: (&InflightFragmentInfo, JobId)) -> Self {
        let (info, job_id) = pair;

        let InflightFragmentInfo {
            fragment_id,
            distribution_type,
            fragment_type_mask,
            actors,
            vnode_count,
            ..
        } = info;

        Self {
            fragment_id: *fragment_id,
            job_id,
            distribution_type: *distribution_type,
            fragment_type_mask: *fragment_type_mask,
            actors: actors.clone(),
            vnode_count: *vnode_count,
        }
    }
}

#[derive(Default, Debug)]
pub struct SharedActorInfosInner {
    info: HashMap<DatabaseId, HashMap<FragmentId, SharedFragmentInfo>>,
}

impl SharedActorInfosInner {
    pub fn get_fragment(&self, fragment_id: FragmentId) -> Option<&SharedFragmentInfo> {
        self.info
            .values()
            .find_map(|database| database.get(&fragment_id))
    }

    pub fn iter_over_fragments(&self) -> impl Iterator<Item = (&FragmentId, &SharedFragmentInfo)> {
        self.info.values().flatten()
    }
}

#[derive(Clone, educe::Educe)]
#[educe(Debug)]
pub struct SharedActorInfos {
    inner: Arc<parking_lot::RwLock<SharedActorInfosInner>>,
    #[educe(Debug(ignore))]
    notification_manager: NotificationManagerRef,
}

impl SharedActorInfos {
    pub fn read_guard(&self) -> RwLockReadGuard<'_, RawRwLock, SharedActorInfosInner> {
        self.inner.read()
    }

    pub fn list_assignments(&self) -> HashMap<ActorId, Vec<SplitImpl>> {
        let core = self.inner.read();
        core.iter_over_fragments()
            .flat_map(|(_, fragment)| {
                fragment
                    .actors
                    .iter()
                    .map(|(actor_id, info)| (*actor_id, info.splits.clone()))
            })
            .collect()
    }

    /// Migrates splits from previous actors to the new actors for a rescheduled fragment.
    ///
    /// Very occasionally split removal may happen during scaling, in which case we need to
    /// use the old splits for reallocation instead of the latest splits (which may be missing),
    /// so that we can resolve the split removal in the next command.
    pub fn migrate_splits_for_source_actors(
        &self,
        fragment_id: FragmentId,
        prev_actor_ids: &[ActorId],
        curr_actor_ids: &[ActorId],
    ) -> MetaResult<HashMap<ActorId, Vec<SplitImpl>>> {
        let guard = self.read_guard();

        let prev_splits = prev_actor_ids
            .iter()
            .flat_map(|actor_id| {
                // Note: File Source / Iceberg Source doesn't have splits assigned by meta.
                guard
                    .get_fragment(fragment_id)
                    .and_then(|info| info.actors.get(actor_id))
                    .map(|actor| actor.splits.clone())
                    .unwrap_or_default()
            })
            .map(|split| (split.id(), split))
            .collect();

        let empty_actor_splits = curr_actor_ids
            .iter()
            .map(|actor_id| (*actor_id, vec![]))
            .collect();

        let diff = crate::stream::source_manager::reassign_splits(
            fragment_id,
            empty_actor_splits,
            &prev_splits,
            // pre-allocate splits is the first time getting splits, and it does not have scale-in scene
            std::default::Default::default(),
        )
        .unwrap_or_default();

        Ok(diff)
    }
}

impl SharedActorInfos {
    pub(crate) fn new(notification_manager: NotificationManagerRef) -> Self {
        Self {
            inner: Arc::new(Default::default()),
            notification_manager,
        }
    }

    pub(super) fn remove_database(&self, database_id: DatabaseId) {
        if let Some(database) = self.inner.write().info.remove(&database_id) {
            let mapping = database
                .into_values()
                .map(|fragment| rebuild_fragment_mapping(&fragment))
                .collect_vec();
            if !mapping.is_empty() {
                self.notification_manager
                    .notify_fragment_mapping(Operation::Delete, mapping);
            }
        }
    }

    pub(super) fn retain_databases(&self, database_ids: impl IntoIterator<Item = DatabaseId>) {
        let database_ids: HashSet<_> = database_ids.into_iter().collect();

        let mut mapping = Vec::new();
        for fragment in self
            .inner
            .write()
            .info
            .extract_if(|database_id, _| !database_ids.contains(database_id))
            .flat_map(|(_, fragments)| fragments.into_values())
        {
            mapping.push(rebuild_fragment_mapping(&fragment));
        }
        if !mapping.is_empty() {
            self.notification_manager
                .notify_fragment_mapping(Operation::Delete, mapping);
        }
    }

    pub(super) fn recover_database(
        &self,
        database_id: DatabaseId,
        fragments: impl Iterator<Item = (&InflightFragmentInfo, JobId)>,
    ) {
        let mut remaining_fragments: HashMap<_, _> = fragments
            .map(|info @ (fragment, _)| (fragment.fragment_id, info))
            .collect();
        // delete the fragments that exist previously, but not included in the recovered fragments
        let mut writer = self.start_writer(database_id);
        let database = writer.write_guard.info.entry(database_id).or_default();
        for (_, fragment) in database.extract_if(|fragment_id, fragment_info| {
            if let Some(info) = remaining_fragments.remove(fragment_id) {
                let info = info.into();
                writer
                    .updated_fragment_mapping
                    .get_or_insert_default()
                    .push(rebuild_fragment_mapping(&info));
                *fragment_info = info;
                false
            } else {
                true
            }
        }) {
            writer
                .deleted_fragment_mapping
                .get_or_insert_default()
                .push(rebuild_fragment_mapping(&fragment));
        }
        for (fragment_id, info) in remaining_fragments {
            let info = info.into();
            writer
                .added_fragment_mapping
                .get_or_insert_default()
                .push(rebuild_fragment_mapping(&info));
            database.insert(fragment_id, info);
        }
        writer.finish();
    }

    pub(super) fn upsert(
        &self,
        database_id: DatabaseId,
        infos: impl IntoIterator<Item = (&InflightFragmentInfo, JobId)>,
    ) {
        let mut writer = self.start_writer(database_id);
        writer.upsert(infos);
        writer.finish();
    }

    pub(super) fn start_writer(&self, database_id: DatabaseId) -> SharedActorInfoWriter<'_> {
        SharedActorInfoWriter {
            database_id,
            write_guard: self.inner.write(),
            notification_manager: &self.notification_manager,
            added_fragment_mapping: None,
            updated_fragment_mapping: None,
            deleted_fragment_mapping: None,
        }
    }
}

pub(super) struct SharedActorInfoWriter<'a> {
    database_id: DatabaseId,
    write_guard: parking_lot::RwLockWriteGuard<'a, SharedActorInfosInner>,
    notification_manager: &'a NotificationManagerRef,
    added_fragment_mapping: Option<Vec<PbFragmentWorkerSlotMapping>>,
    updated_fragment_mapping: Option<Vec<PbFragmentWorkerSlotMapping>>,
    deleted_fragment_mapping: Option<Vec<PbFragmentWorkerSlotMapping>>,
}

impl SharedActorInfoWriter<'_> {
    pub(super) fn upsert(
        &mut self,
        infos: impl IntoIterator<Item = (&InflightFragmentInfo, JobId)>,
    ) {
        let database = self.write_guard.info.entry(self.database_id).or_default();
        for info @ (fragment, _) in infos {
            match database.entry(fragment.fragment_id) {
                Entry::Occupied(mut entry) => {
                    let info = info.into();
                    self.updated_fragment_mapping
                        .get_or_insert_default()
                        .push(rebuild_fragment_mapping(&info));
                    entry.insert(info);
                }
                Entry::Vacant(entry) => {
                    let info = info.into();
                    self.added_fragment_mapping
                        .get_or_insert_default()
                        .push(rebuild_fragment_mapping(&info));
                    entry.insert(info);
                }
            }
        }
    }

    pub(super) fn remove(&mut self, info: &InflightFragmentInfo) {
        if let Some(database) = self.write_guard.info.get_mut(&self.database_id)
            && let Some(fragment) = database.remove(&info.fragment_id)
        {
            self.deleted_fragment_mapping
                .get_or_insert_default()
                .push(rebuild_fragment_mapping(&fragment));
        }
    }

    pub(super) fn finish(self) {
        if let Some(mapping) = self.added_fragment_mapping {
            self.notification_manager
                .notify_fragment_mapping(Operation::Add, mapping);
        }
        if let Some(mapping) = self.updated_fragment_mapping {
            self.notification_manager
                .notify_fragment_mapping(Operation::Update, mapping);
        }
        if let Some(mapping) = self.deleted_fragment_mapping {
            self.notification_manager
                .notify_fragment_mapping(Operation::Delete, mapping);
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct BarrierInfo {
    pub prev_epoch: TracedEpoch,
    pub curr_epoch: TracedEpoch,
    pub kind: BarrierKind,
}

impl BarrierInfo {
    pub(super) fn prev_epoch(&self) -> u64 {
        self.prev_epoch.value().0
    }
}

#[derive(Debug, Clone)]
pub(crate) enum CommandFragmentChanges {
    NewFragment {
        job_id: JobId,
        info: InflightFragmentInfo,
        /// Whether the fragment already exists before added. This is used
        /// when snapshot backfill is finished and add its fragment info
        /// back to the database.
        is_existing: bool,
    },
    AddNodeUpstream(PbUpstreamSinkInfo),
    DropNodeUpstream(Vec<FragmentId>),
    ReplaceNodeUpstream(
        /// old `fragment_id` -> new `fragment_id`
        HashMap<FragmentId, FragmentId>,
    ),
    Reschedule {
        new_actors: HashMap<ActorId, InflightActorInfo>,
        actor_update_vnode_bitmap: HashMap<ActorId, Bitmap>,
        to_remove: HashSet<ActorId>,
        actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
    },
    RemoveFragment,
    SplitAssignment {
        actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
    },
}

#[derive(Clone, Debug)]
pub enum SubscriberType {
    Subscription(u64),
    SnapshotBackfill,
}

#[derive(Debug, Clone)]
pub(super) enum CreateStreamingJobStatus {
    Init,
    Creating(CreateMviewProgressTracker),
    Created,
}

#[derive(Debug, Clone)]
pub(super) struct InflightStreamingJobInfo {
    pub job_id: JobId,
    pub fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
    pub subscribers: HashMap<u32, SubscriberType>,
    pub status: CreateStreamingJobStatus,
}

impl InflightStreamingJobInfo {
    pub fn fragment_infos(&self) -> impl Iterator<Item = &InflightFragmentInfo> + '_ {
        self.fragment_infos.values()
    }

    pub fn snapshot_backfill_actor_ids(
        fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
    ) -> impl Iterator<Item = ActorId> + '_ {
        fragment_infos
            .values()
            .filter(|fragment| {
                fragment
                    .fragment_type_mask
                    .contains(FragmentTypeFlag::SnapshotBackfillStreamScan)
            })
            .flat_map(|fragment| fragment.actors.keys().copied())
    }

    pub fn tracking_progress_actor_ids(
        fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
    ) -> Vec<(ActorId, BackfillUpstreamType)> {
        StreamJobFragments::tracking_progress_actor_ids_impl(
            fragment_infos
                .values()
                .map(|fragment| (fragment.fragment_type_mask, fragment.actors.keys().copied())),
        )
    }
}

impl<'a> IntoIterator for &'a InflightStreamingJobInfo {
    type Item = &'a InflightFragmentInfo;

    type IntoIter = impl Iterator<Item = &'a InflightFragmentInfo> + 'a;

    fn into_iter(self) -> Self::IntoIter {
        self.fragment_infos()
    }
}

#[derive(Debug, Clone)]
pub struct InflightDatabaseInfo {
    database_id: DatabaseId,
    jobs: HashMap<JobId, InflightStreamingJobInfo>,
    fragment_location: HashMap<FragmentId, JobId>,
    pub(super) shared_actor_infos: SharedActorInfos,
}

impl InflightDatabaseInfo {
    pub fn fragment_infos(&self) -> impl Iterator<Item = &InflightFragmentInfo> + '_ {
        self.jobs.values().flat_map(|job| job.fragment_infos())
    }

    pub fn contains_job(&self, job_id: JobId) -> bool {
        self.jobs.contains_key(&job_id)
    }

    pub fn fragment(&self, fragment_id: FragmentId) -> &InflightFragmentInfo {
        let job_id = self.fragment_location[&fragment_id];
        self.jobs
            .get(&job_id)
            .expect("should exist")
            .fragment_infos
            .get(&fragment_id)
            .expect("should exist")
    }

    pub fn gen_ddl_progress(&self) -> impl Iterator<Item = (JobId, DdlProgress)> + '_ {
        self.jobs
            .iter()
            .filter_map(|(job_id, job)| match &job.status {
                CreateStreamingJobStatus::Init => None,
                CreateStreamingJobStatus::Creating(tracker) => {
                    Some((*job_id, tracker.gen_ddl_progress()))
                }
                CreateStreamingJobStatus::Created => None,
            })
    }

    pub(super) fn apply_collected_command(
        &mut self,
        command: Option<&Command>,
        resps: impl Iterator<Item = &BarrierCompleteResponse>,
        version_stats: &HummockVersionStats,
    ) {
        if let Some(Command::CreateStreamingJob { info, job_type, .. }) = command {
            match job_type {
                CreateStreamingJobType::Normal | CreateStreamingJobType::SinkIntoTable(_) => {
                    let job_id = info.streaming_job.id();
                    if let Some(job_info) = self.jobs.get_mut(&job_id) {
                        let CreateStreamingJobStatus::Init = replace(
                            &mut job_info.status,
                            CreateStreamingJobStatus::Creating(CreateMviewProgressTracker::new(
                                info,
                                version_stats,
                            )),
                        ) else {
                            unreachable!("should be init before collect the first barrier")
                        };
                    } else {
                        info!(%job_id, "newly create job get cancelled before first barrier is collected")
                    }
                }
                CreateStreamingJobType::SnapshotBackfill(_) => {
                    // The progress of SnapshotBackfill won't be tracked here
                }
            }
        }
        for progress in resps.flat_map(|resp| &resp.create_mview_progress) {
            let Some(job_id) = self.fragment_location.get(&progress.fragment_id) else {
                warn!(
                    "update the progress of an non-existent creating streaming job: {progress:?}, which could be cancelled"
                );
                continue;
            };
            let CreateStreamingJobStatus::Creating(tracker) =
                &mut self.jobs.get_mut(job_id).expect("should exist").status
            else {
                warn!("update the progress of an created streaming job: {progress:?}");
                continue;
            };
            tracker.apply_progress(progress, version_stats);
        }
    }

    fn iter_creating_job_tracker(&self) -> impl Iterator<Item = &CreateMviewProgressTracker> {
        self.jobs.values().filter_map(|job| match &job.status {
            CreateStreamingJobStatus::Init => None,
            CreateStreamingJobStatus::Creating(tracker) => Some(tracker),
            CreateStreamingJobStatus::Created => None,
        })
    }

    fn iter_mut_creating_job_tracker(
        &mut self,
    ) -> impl Iterator<Item = &mut CreateMviewProgressTracker> {
        self.jobs
            .values_mut()
            .filter_map(|job| match &mut job.status {
                CreateStreamingJobStatus::Init => None,
                CreateStreamingJobStatus::Creating(tracker) => Some(tracker),
                CreateStreamingJobStatus::Created => None,
            })
    }

    pub(super) fn has_pending_finished_jobs(&self) -> bool {
        self.iter_creating_job_tracker()
            .any(|tracker| tracker.is_finished())
    }

    pub(super) fn take_pending_backfill_nodes(&mut self) -> Vec<FragmentId> {
        self.iter_mut_creating_job_tracker()
            .flat_map(|tracker| tracker.take_pending_backfill_nodes())
            .collect()
    }

    pub(super) fn take_staging_commit_info(&mut self) -> StagingCommitInfo {
        let mut finished_jobs = vec![];
        let mut table_ids_to_truncate = vec![];
        for job in self.jobs.values_mut() {
            if let CreateStreamingJobStatus::Creating(tracker) = &mut job.status {
                let (is_finished, truncate_table_ids) = tracker.collect_staging_commit_info();
                table_ids_to_truncate.extend(truncate_table_ids);
                if is_finished {
                    let CreateStreamingJobStatus::Creating(tracker) =
                        replace(&mut job.status, CreateStreamingJobStatus::Created)
                    else {
                        unreachable!()
                    };
                    finished_jobs.push(tracker.into_tracking_job());
                }
            }
        }
        StagingCommitInfo {
            finished_jobs,
            table_ids_to_truncate,
        }
    }

    pub fn fragment_subscribers(&self, fragment_id: FragmentId) -> impl Iterator<Item = u32> + '_ {
        let job_id = self.fragment_location[&fragment_id];
        self.jobs[&job_id].subscribers.keys().copied()
    }

    pub fn job_subscribers(&self, job_id: JobId) -> impl Iterator<Item = u32> + '_ {
        self.jobs[&job_id].subscribers.keys().copied()
    }

    pub fn max_subscription_retention(&self) -> HashMap<TableId, u64> {
        self.jobs
            .iter()
            .filter_map(|(job_id, info)| {
                info.subscribers
                    .values()
                    .filter_map(|subscriber| match subscriber {
                        SubscriberType::Subscription(retention) => Some(*retention),
                        SubscriberType::SnapshotBackfill => None,
                    })
                    .max()
                    .map(|max_subscription| (job_id.as_mv_table_id(), max_subscription))
            })
            .collect()
    }

    pub fn register_subscriber(
        &mut self,
        job_id: JobId,
        subscriber_id: u32,
        subscriber: SubscriberType,
    ) {
        self.jobs
            .get_mut(&job_id)
            .expect("should exist")
            .subscribers
            .try_insert(subscriber_id, subscriber)
            .expect("non duplicate");
    }

    pub fn unregister_subscriber(
        &mut self,
        job_id: JobId,
        subscriber_id: u32,
    ) -> Option<SubscriberType> {
        self.jobs
            .get_mut(&job_id)
            .expect("should exist")
            .subscribers
            .remove(&subscriber_id)
    }

    fn fragment_mut(&mut self, fragment_id: FragmentId) -> (&mut InflightFragmentInfo, JobId) {
        let job_id = self.fragment_location[&fragment_id];
        let fragment = self
            .jobs
            .get_mut(&job_id)
            .expect("should exist")
            .fragment_infos
            .get_mut(&fragment_id)
            .expect("should exist");
        (fragment, job_id)
    }

    fn empty_inner(database_id: DatabaseId, shared_actor_infos: SharedActorInfos) -> Self {
        Self {
            database_id,
            jobs: Default::default(),
            fragment_location: Default::default(),
            shared_actor_infos,
        }
    }

    pub fn empty(database_id: DatabaseId, shared_actor_infos: SharedActorInfos) -> Self {
        // remove the database because it's empty.
        shared_actor_infos.remove_database(database_id);
        Self::empty_inner(database_id, shared_actor_infos)
    }

    pub fn recover(
        database_id: DatabaseId,
        jobs: impl Iterator<Item = InflightStreamingJobInfo>,
        shared_actor_infos: SharedActorInfos,
    ) -> Self {
        let mut info = Self::empty_inner(database_id, shared_actor_infos);
        for job in jobs {
            info.add_existing(job);
        }
        info
    }

    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    pub fn add_existing(&mut self, job: InflightStreamingJobInfo) {
        let InflightStreamingJobInfo {
            job_id,
            fragment_infos,
            subscribers,
            status,
        } = job;
        self.jobs
            .try_insert(
                job.job_id,
                InflightStreamingJobInfo {
                    job_id,
                    subscribers,
                    fragment_infos: Default::default(), // fill in later in apply_add
                    status,
                },
            )
            .expect("non-duplicate");
        self.apply_add(fragment_infos.into_iter().map(|(fragment_id, info)| {
            (
                fragment_id,
                CommandFragmentChanges::NewFragment {
                    job_id: job.job_id,
                    info,
                    is_existing: true,
                },
            )
        }))
    }

    /// Apply some actor changes before issuing a barrier command, if the command contains any new added actors, we should update
    /// the info correspondingly.
    pub(crate) fn pre_apply(
        &mut self,
        new_job_id: Option<JobId>,
        fragment_changes: &HashMap<FragmentId, CommandFragmentChanges>,
    ) {
        if let Some(job_id) = new_job_id {
            self.jobs
                .try_insert(
                    job_id,
                    InflightStreamingJobInfo {
                        job_id,
                        fragment_infos: Default::default(),
                        subscribers: Default::default(), // no subscriber for newly create job
                        status: CreateStreamingJobStatus::Init,
                    },
                )
                .expect("non-duplicate");
        }
        self.apply_add(
            fragment_changes
                .iter()
                .map(|(fragment_id, change)| (*fragment_id, change.clone())),
        )
    }

    fn apply_add(
        &mut self,
        fragment_changes: impl Iterator<Item = (FragmentId, CommandFragmentChanges)>,
    ) {
        {
            let shared_infos = self.shared_actor_infos.clone();
            let mut shared_actor_writer = shared_infos.start_writer(self.database_id);
            for (fragment_id, change) in fragment_changes {
                match change {
                    CommandFragmentChanges::NewFragment {
                        job_id,
                        info,
                        is_existing,
                    } => {
                        let fragment_infos = self.jobs.get_mut(&job_id).expect("should exist");
                        if !is_existing {
                            shared_actor_writer.upsert([(&info, job_id)]);
                        }
                        fragment_infos
                            .fragment_infos
                            .try_insert(fragment_id, info)
                            .expect("non duplicate");
                        self.fragment_location
                            .try_insert(fragment_id, job_id)
                            .expect("non duplicate");
                    }
                    CommandFragmentChanges::Reschedule {
                        new_actors,
                        actor_update_vnode_bitmap,
                        actor_splits,
                        ..
                    } => {
                        let (info, _) = self.fragment_mut(fragment_id);
                        let actors = &mut info.actors;
                        for (actor_id, new_vnodes) in actor_update_vnode_bitmap {
                            actors
                                .get_mut(&actor_id)
                                .expect("should exist")
                                .vnode_bitmap = Some(new_vnodes);
                        }
                        for (actor_id, actor) in new_actors {
                            actors
                                .try_insert(actor_id as _, actor)
                                .expect("non-duplicate");
                        }
                        for (actor_id, splits) in actor_splits {
                            actors.get_mut(&actor_id).expect("should exist").splits = splits;
                        }

                        // info will be upserted into shared_actor_infos in post_apply stage
                    }
                    CommandFragmentChanges::RemoveFragment => {}
                    CommandFragmentChanges::ReplaceNodeUpstream(replace_map) => {
                        let mut remaining_fragment_ids: HashSet<_> =
                            replace_map.keys().cloned().collect();
                        let (info, _) = self.fragment_mut(fragment_id);
                        visit_stream_node_mut(&mut info.nodes, |node| {
                            if let NodeBody::Merge(m) = node
                                && let Some(new_upstream_fragment_id) =
                                    replace_map.get(&m.upstream_fragment_id)
                            {
                                if !remaining_fragment_ids.remove(&m.upstream_fragment_id) {
                                    if cfg!(debug_assertions) {
                                        panic!(
                                            "duplicate upstream fragment: {:?} {:?}",
                                            m, replace_map
                                        );
                                    } else {
                                        warn!(?m, ?replace_map, "duplicate upstream fragment");
                                    }
                                }
                                m.upstream_fragment_id = *new_upstream_fragment_id;
                            }
                        });
                        if cfg!(debug_assertions) {
                            assert!(
                                remaining_fragment_ids.is_empty(),
                                "non-existing fragment to replace: {:?} {:?} {:?}",
                                remaining_fragment_ids,
                                info.nodes,
                                replace_map
                            );
                        } else {
                            warn!(?remaining_fragment_ids, node = ?info.nodes, ?replace_map, "non-existing fragment to replace");
                        }
                    }
                    CommandFragmentChanges::AddNodeUpstream(new_upstream_info) => {
                        let (info, _) = self.fragment_mut(fragment_id);
                        let mut injected = false;
                        visit_stream_node_mut(&mut info.nodes, |node| {
                            if let NodeBody::UpstreamSinkUnion(u) = node {
                                if cfg!(debug_assertions) {
                                    let current_upstream_fragment_ids = u
                                        .init_upstreams
                                        .iter()
                                        .map(|upstream| upstream.upstream_fragment_id)
                                        .collect::<HashSet<_>>();
                                    if current_upstream_fragment_ids
                                        .contains(&new_upstream_info.upstream_fragment_id)
                                    {
                                        panic!(
                                            "duplicate upstream fragment: {:?} {:?}",
                                            u, new_upstream_info
                                        );
                                    }
                                }
                                u.init_upstreams.push(new_upstream_info.clone());
                                injected = true;
                            }
                        });
                        assert!(injected, "should inject upstream into UpstreamSinkUnion");
                    }
                    CommandFragmentChanges::DropNodeUpstream(drop_upstream_fragment_ids) => {
                        let (info, _) = self.fragment_mut(fragment_id);
                        let mut removed = false;
                        visit_stream_node_mut(&mut info.nodes, |node| {
                            if let NodeBody::UpstreamSinkUnion(u) = node {
                                if cfg!(debug_assertions) {
                                    let current_upstream_fragment_ids = u
                                        .init_upstreams
                                        .iter()
                                        .map(|upstream| upstream.upstream_fragment_id)
                                        .collect::<HashSet<FragmentId>>();
                                    for drop_fragment_id in &drop_upstream_fragment_ids {
                                        if !current_upstream_fragment_ids.contains(drop_fragment_id)
                                        {
                                            panic!(
                                                "non-existing upstream fragment to drop: {:?} {:?} {:?}",
                                                u, drop_upstream_fragment_ids, drop_fragment_id
                                            );
                                        }
                                    }
                                }
                                u.init_upstreams.retain(|upstream| {
                                    !drop_upstream_fragment_ids
                                        .contains(&upstream.upstream_fragment_id)
                                });
                                removed = true;
                            }
                        });
                        assert!(removed, "should remove upstream from UpstreamSinkUnion");
                    }
                    CommandFragmentChanges::SplitAssignment { actor_splits } => {
                        let (info, job_id) = self.fragment_mut(fragment_id);
                        let actors = &mut info.actors;
                        for (actor_id, splits) in actor_splits {
                            actors.get_mut(&actor_id).expect("should exist").splits = splits;
                        }
                        shared_actor_writer.upsert([(&*info, job_id)]);
                    }
                }
            }
            shared_actor_writer.finish();
        }
    }

    pub(super) fn build_edge(
        &self,
        command: Option<&Command>,
        control_stream_manager: &ControlStreamManager,
    ) -> Option<FragmentEdgeBuildResult> {
        let (info, replace_job, new_upstream_sink) = match command {
            None => {
                return None;
            }
            Some(command) => match command {
                Command::Flush
                | Command::Pause
                | Command::Resume
                | Command::DropStreamingJobs { .. }
                | Command::MergeSnapshotBackfillStreamingJobs(_)
                | Command::RescheduleFragment { .. }
                | Command::SourceChangeSplit { .. }
                | Command::Throttle(_)
                | Command::CreateSubscription { .. }
                | Command::DropSubscription { .. }
                | Command::ConnectorPropsChange(_)
                | Command::StartFragmentBackfill { .. }
                | Command::Refresh { .. }
                | Command::ListFinish { .. }
                | Command::LoadFinish { .. } => {
                    return None;
                }
                Command::CreateStreamingJob { info, job_type, .. } => {
                    let new_upstream_sink = if let CreateStreamingJobType::SinkIntoTable(
                        new_upstream_sink,
                    ) = job_type
                    {
                        Some(new_upstream_sink)
                    } else {
                        None
                    };
                    (Some(info), None, new_upstream_sink)
                }
                Command::ReplaceStreamJob(replace_job) => (None, Some(replace_job), None),
            },
        };
        // `existing_fragment_ids` consists of
        //  - keys of `info.upstream_fragment_downstreams`, which are the `fragment_id` the upstream fragment of the newly created job
        //  - keys of `replace_job.upstream_fragment_downstreams`, which are the `fragment_id` of upstream fragment of replace_job,
        // if the upstream fragment previously exists
        //  - keys of `replace_upstream`, which are the `fragment_id` of downstream fragments that will update their upstream fragments,
        // if creating a new sink-into-table
        //  - should contain the `fragment_id` of the downstream table.
        let existing_fragment_ids = info
            .into_iter()
            .flat_map(|info| info.upstream_fragment_downstreams.keys())
            .chain(replace_job.into_iter().flat_map(|replace_job| {
                replace_job
                    .upstream_fragment_downstreams
                    .keys()
                    .filter(|fragment_id| {
                        info.map(|info| {
                            !info
                                .stream_job_fragments
                                .fragments
                                .contains_key(fragment_id)
                        })
                        .unwrap_or(true)
                    })
                    .chain(replace_job.replace_upstream.keys())
            }))
            .chain(
                new_upstream_sink
                    .into_iter()
                    .map(|ctx| &ctx.new_sink_downstream.downstream_fragment_id),
            )
            .cloned();
        let new_fragment_infos = info
            .into_iter()
            .flat_map(|info| {
                info.stream_job_fragments
                    .new_fragment_info(&info.init_split_assignment)
            })
            .chain(replace_job.into_iter().flat_map(|replace_job| {
                replace_job
                    .new_fragments
                    .new_fragment_info(&replace_job.init_split_assignment)
                    .chain(
                        replace_job
                            .auto_refresh_schema_sinks
                            .as_ref()
                            .into_iter()
                            .flat_map(|sinks| {
                                sinks.iter().map(|sink| {
                                    (sink.new_fragment.fragment_id, sink.new_fragment_info())
                                })
                            }),
                    )
            }))
            .collect_vec();
        let mut builder = FragmentEdgeBuilder::new(
            existing_fragment_ids
                .map(|fragment_id| self.fragment(fragment_id))
                .chain(new_fragment_infos.iter().map(|(_, info)| info)),
            control_stream_manager,
        );
        if let Some(info) = info {
            builder.add_relations(&info.upstream_fragment_downstreams);
            builder.add_relations(&info.stream_job_fragments.downstreams);
        }
        if let Some(replace_job) = replace_job {
            builder.add_relations(&replace_job.upstream_fragment_downstreams);
            builder.add_relations(&replace_job.new_fragments.downstreams);
        }
        if let Some(new_upstream_sink) = new_upstream_sink {
            let sink_fragment_id = new_upstream_sink.sink_fragment_id;
            let new_sink_downstream = &new_upstream_sink.new_sink_downstream;
            builder.add_edge(sink_fragment_id, new_sink_downstream);
        }
        if let Some(replace_job) = replace_job {
            for (fragment_id, fragment_replacement) in &replace_job.replace_upstream {
                for (original_upstream_fragment_id, new_upstream_fragment_id) in
                    fragment_replacement
                {
                    builder.replace_upstream(
                        *fragment_id,
                        *original_upstream_fragment_id,
                        *new_upstream_fragment_id,
                    );
                }
            }
        }
        Some(builder.build())
    }

    /// Apply some actor changes after the barrier command is collected, if the command contains any actors that are dropped, we should
    /// remove that from the snapshot correspondingly.
    pub(crate) fn post_apply(
        &mut self,
        fragment_changes: &HashMap<FragmentId, CommandFragmentChanges>,
    ) {
        let inner = self.shared_actor_infos.clone();
        let mut shared_actor_writer = inner.start_writer(self.database_id);
        {
            for (fragment_id, changes) in fragment_changes {
                match changes {
                    CommandFragmentChanges::NewFragment { .. } => {}
                    CommandFragmentChanges::Reschedule { to_remove, .. } => {
                        let job_id = self.fragment_location[fragment_id];
                        let info = self
                            .jobs
                            .get_mut(&job_id)
                            .expect("should exist")
                            .fragment_infos
                            .get_mut(fragment_id)
                            .expect("should exist");
                        for actor_id in to_remove {
                            assert!(info.actors.remove(&(*actor_id as _)).is_some());
                        }
                        shared_actor_writer.upsert([(&*info, job_id)]);
                    }
                    CommandFragmentChanges::RemoveFragment => {
                        let job_id = self
                            .fragment_location
                            .remove(fragment_id)
                            .expect("should exist");
                        let job = self.jobs.get_mut(&job_id).expect("should exist");
                        let fragment = job
                            .fragment_infos
                            .remove(fragment_id)
                            .expect("should exist");
                        shared_actor_writer.remove(&fragment);
                        if job.fragment_infos.is_empty() {
                            self.jobs.remove(&job_id).expect("should exist");
                        }
                    }
                    CommandFragmentChanges::ReplaceNodeUpstream(_)
                    | CommandFragmentChanges::AddNodeUpstream(_)
                    | CommandFragmentChanges::DropNodeUpstream(_)
                    | CommandFragmentChanges::SplitAssignment { .. } => {}
                }
            }
        }
        shared_actor_writer.finish();
    }
}

impl InflightFragmentInfo {
    /// Returns actor list to collect in the target worker node.
    pub(crate) fn actor_ids_to_collect(
        infos: impl IntoIterator<Item = &Self>,
    ) -> HashMap<WorkerId, HashSet<ActorId>> {
        let mut ret: HashMap<_, HashSet<_>> = HashMap::new();
        for (actor_id, actor) in infos.into_iter().flat_map(|info| info.actors.iter()) {
            assert!(
                ret.entry(actor.worker_id)
                    .or_default()
                    .insert(*actor_id as _)
            )
        }
        ret
    }

    pub fn existing_table_ids<'a>(
        infos: impl IntoIterator<Item = &'a Self> + 'a,
    ) -> impl Iterator<Item = TableId> + 'a {
        infos
            .into_iter()
            .flat_map(|info| info.state_table_ids.iter().cloned())
    }

    pub fn contains_worker(infos: impl IntoIterator<Item = &Self>, worker_id: WorkerId) -> bool {
        infos.into_iter().any(|fragment| {
            fragment
                .actors
                .values()
                .any(|actor| (actor.worker_id) == worker_id)
        })
    }
}

impl InflightDatabaseInfo {
    pub fn contains_worker(&self, worker_id: WorkerId) -> bool {
        InflightFragmentInfo::contains_worker(self.fragment_infos(), worker_id)
    }

    pub fn existing_table_ids(&self) -> impl Iterator<Item = TableId> + '_ {
        InflightFragmentInfo::existing_table_ids(self.fragment_infos())
    }
}
