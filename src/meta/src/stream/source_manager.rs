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

use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use risingwave_common::catalog::TableId;
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_connector::dispatch_source_prop;
use risingwave_connector::error::ConnectorResult;
use risingwave_connector::source::{
    ConnectorProperties, SourceEnumeratorContext, SourceEnumeratorInfo, SourceProperties,
    SplitEnumerator, SplitId, SplitImpl, SplitMetaData,
};
use risingwave_pb::catalog::Source;
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use risingwave_pb::stream_plan::Dispatcher;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio::{select, time};

use crate::barrier::{BarrierScheduler, Command};
use crate::manager::{MetadataManager, SourceId};
use crate::model::{ActorId, FragmentId, TableFragments};
use crate::rpc::metrics::MetaMetrics;
use crate::MetaResult;

pub type SourceManagerRef = Arc<SourceManager>;
pub type SplitAssignment = HashMap<FragmentId, HashMap<ActorId, Vec<SplitImpl>>>;
pub type ThrottleConfig = HashMap<FragmentId, HashMap<ActorId, Option<u32>>>;

/// `SourceManager` keeps fetching the latest split metadata from the external source services ([`ConnectorSourceWorker::tick`]),
/// and sends a split assignment command if split changes detected ([`Self::tick`]).
pub struct SourceManager {
    pub paused: Mutex<()>,
    barrier_scheduler: BarrierScheduler,
    core: Mutex<SourceManagerCore>,
    metrics: Arc<MetaMetrics>,
}

const MAX_FAIL_CNT: u32 = 10;

struct SharedSplitMap {
    splits: Option<BTreeMap<SplitId, SplitImpl>>,
}

type SharedSplitMapRef = Arc<Mutex<SharedSplitMap>>;

/// `ConnectorSourceWorker` keeps fetching the latest split metadata from the external source service ([`Self::tick`]),
/// and maintains it in `current_splits`.
struct ConnectorSourceWorker<P: SourceProperties> {
    source_id: SourceId,
    source_name: String,
    current_splits: SharedSplitMapRef,
    enumerator: P::SplitEnumerator,
    period: Duration,
    metrics: Arc<MetaMetrics>,
    connector_properties: P,
    fail_cnt: u32,
    source_is_up: LabelGuardedIntGauge<2>,
}

fn extract_prop_from_existing_source(source: &Source) -> ConnectorResult<ConnectorProperties> {
    let mut properties = ConnectorProperties::extract(source.with_properties.clone(), false)?;
    properties.init_from_pb_source(source);
    Ok(properties)
}
fn extract_prop_from_new_source(source: &Source) -> ConnectorResult<ConnectorProperties> {
    let mut properties = ConnectorProperties::extract(source.with_properties.clone(), true)?;
    properties.init_from_pb_source(source);
    Ok(properties)
}

const DEFAULT_SOURCE_WORKER_TICK_INTERVAL: Duration = Duration::from_secs(30);

impl<P: SourceProperties> ConnectorSourceWorker<P> {
    /// Recreate the `SplitEnumerator` to establish a new connection to the external source service.
    async fn refresh(&mut self) -> MetaResult<()> {
        let enumerator = P::SplitEnumerator::new(
            self.connector_properties.clone(),
            Arc::new(SourceEnumeratorContext {
                metrics: self.metrics.source_enumerator_metrics.clone(),
                info: SourceEnumeratorInfo {
                    source_id: self.source_id,
                },
            }),
        )
        .await
        .context("failed to create SplitEnumerator")?;
        self.enumerator = enumerator;
        self.fail_cnt = 0;
        tracing::info!("refreshed source enumerator: {}", self.source_name);
        Ok(())
    }

    /// On creation, connection to the external source service will be established, but `splits`
    /// will not be updated until `tick` is called.
    pub async fn create(
        source: &Source,
        connector_properties: P,
        period: Duration,
        splits: Arc<Mutex<SharedSplitMap>>,
        metrics: Arc<MetaMetrics>,
    ) -> MetaResult<Self> {
        let enumerator = P::SplitEnumerator::new(
            connector_properties.clone(),
            Arc::new(SourceEnumeratorContext {
                metrics: metrics.source_enumerator_metrics.clone(),
                info: SourceEnumeratorInfo {
                    source_id: source.id,
                },
            }),
        )
        .await
        .context("failed to create SplitEnumerator")?;

        let source_is_up = metrics
            .source_is_up
            .with_guarded_label_values(&[source.id.to_string().as_str(), &source.name]);

        Ok(Self {
            source_id: source.id,
            source_name: source.name.clone(),
            current_splits: splits,
            enumerator,
            period,
            metrics,
            connector_properties,
            fail_cnt: 0,
            source_is_up,
        })
    }

    pub async fn run(
        &mut self,
        mut sync_call_rx: UnboundedReceiver<oneshot::Sender<MetaResult<()>>>,
    ) {
        let mut interval = time::interval(self.period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            select! {
                biased;
                tx = sync_call_rx.borrow_mut().recv() => {
                    if let Some(tx) = tx {
                        let _ = tx.send(self.tick().await);
                    }
                }
                _ = interval.tick() => {
                    if self.fail_cnt > MAX_FAIL_CNT {
                        if let Err(e) = self.refresh().await {
                            tracing::error!(error = %e.as_report(), "error happened when refresh from connector source worker");
                        }
                    }
                    if let Err(e) = self.tick().await {
                        tracing::error!(error = %e.as_report(), "error happened when tick from connector source worker");
                    }
                }
            }
        }
    }

    /// Uses [`SplitEnumerator`] to fetch the latest split metadata from the external source service.
    async fn tick(&mut self) -> MetaResult<()> {
        let source_is_up = |res: i64| {
            self.source_is_up.set(res);
        };
        let splits = self.enumerator.list_splits().await.map_err(|e| {
            source_is_up(0);
            self.fail_cnt += 1;
            e
        })?;
        source_is_up(1);
        self.fail_cnt = 0;
        let mut current_splits = self.current_splits.lock().await;
        current_splits.splits.replace(
            splits
                .into_iter()
                .map(|split| (split.id(), P::Split::into(split)))
                .collect(),
        );

        Ok(())
    }
}

/// Handle for a running [`ConnectorSourceWorker`].
struct ConnectorSourceWorkerHandle {
    handle: JoinHandle<()>,
    sync_call_tx: UnboundedSender<oneshot::Sender<MetaResult<()>>>,
    splits: SharedSplitMapRef,
    enable_scale_in: bool,
}

impl ConnectorSourceWorkerHandle {
    async fn discovered_splits(&self) -> Option<BTreeMap<SplitId, SplitImpl>> {
        self.splits.lock().await.splits.clone()
    }
}

pub struct SourceManagerCore {
    metadata_manager: MetadataManager,

    /// Managed source loops
    managed_sources: HashMap<SourceId, ConnectorSourceWorkerHandle>,
    /// Fragments associated with each source
    source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    /// `source_id` -> `(fragment_id, upstream_fragment_id)`
    backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,

    /// Splits assigned per actor
    actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
}

impl SourceManagerCore {
    fn new(
        metadata_manager: MetadataManager,
        managed_sources: HashMap<SourceId, ConnectorSourceWorkerHandle>,
        source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,
        actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
    ) -> Self {
        Self {
            metadata_manager,
            managed_sources,
            source_fragments,
            backfill_fragments,
            actor_splits,
        }
    }

    /// Checks whether the external source metadata has changed,
    /// and re-assigns splits if there's a diff.
    ///
    /// `self.actor_splits` will not be updated. It will be updated by `Self::apply_source_change`,
    /// after the mutation barrier has been collected.
    async fn reassign_splits(&self) -> MetaResult<SplitAssignment> {
        let mut split_assignment: SplitAssignment = HashMap::new();

        for (source_id, handle) in &self.managed_sources {
            let source_fragment_ids = match self.source_fragments.get(source_id) {
                Some(fragment_ids) if !fragment_ids.is_empty() => fragment_ids,
                _ => {
                    continue;
                }
            };
            let backfill_fragment_ids = self.backfill_fragments.get(source_id);

            let Some(discovered_splits) = handle.discovered_splits().await else {
                return Ok(split_assignment);
            };
            if discovered_splits.is_empty() {
                tracing::warn!("No splits discovered for source {}", source_id);
            }

            for &fragment_id in source_fragment_ids {
                let actors = match self
                    .metadata_manager
                    .get_running_actors_of_fragment(fragment_id)
                    .await
                {
                    Ok(actors) => {
                        if actors.is_empty() {
                            tracing::warn!("No actors found for fragment {}", fragment_id);
                            continue;
                        }
                        actors
                    }
                    Err(err) => {
                        tracing::warn!(error = %err.as_report(), "Failed to get the actor of the fragment, maybe the fragment doesn't exist anymore");
                        continue;
                    }
                };

                let prev_actor_splits: HashMap<_, _> = actors
                    .into_iter()
                    .map(|actor_id| {
                        (
                            actor_id,
                            self.actor_splits
                                .get(&actor_id)
                                .cloned()
                                .unwrap_or_default(),
                        )
                    })
                    .collect();

                if let Some(new_assignment) = reassign_splits(
                    fragment_id,
                    prev_actor_splits,
                    &discovered_splits,
                    SplitDiffOptions {
                        enable_scale_in: handle.enable_scale_in,
                    },
                ) {
                    split_assignment.insert(fragment_id, new_assignment);
                }
            }

            if let Some(backfill_fragment_ids) = backfill_fragment_ids {
                // align splits for backfill fragments with its upstream source fragment
                for (fragment_id, upstream_fragment_id) in backfill_fragment_ids {
                    let Some(upstream_assignment) = split_assignment.get(upstream_fragment_id)
                    else {
                        // upstream fragment unchanged, do not update backfill fragment too
                        continue;
                    };
                    let actors = match self
                        .metadata_manager
                        .get_running_actors_and_upstream_actors_of_fragment(*fragment_id)
                        .await
                    {
                        Ok(actors) => {
                            if actors.is_empty() {
                                tracing::warn!("No actors found for fragment {}", fragment_id);
                                continue;
                            }
                            actors
                        }
                        Err(err) => {
                            tracing::warn!(error = %err.as_report(),"Failed to get the actor of the fragment, maybe the fragment doesn't exist anymore");
                            continue;
                        }
                    };
                    split_assignment.insert(
                        *fragment_id,
                        align_backfill_splits(
                            actors,
                            upstream_assignment,
                            *fragment_id,
                            *upstream_fragment_id,
                        )?,
                    );
                }
            }
        }

        Ok(split_assignment)
    }

    fn apply_source_change(
        &mut self,
        added_source_fragments: Option<HashMap<SourceId, BTreeSet<FragmentId>>>,
        added_backfill_fragments: Option<HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>>,
        split_assignment: Option<SplitAssignment>,
        dropped_actors: Option<HashSet<ActorId>>,
    ) {
        if let Some(source_fragments) = added_source_fragments {
            for (source_id, mut fragment_ids) in source_fragments {
                self.source_fragments
                    .entry(source_id)
                    .or_default()
                    .append(&mut fragment_ids);
            }
        }
        if let Some(backfill_fragments) = added_backfill_fragments {
            for (source_id, mut fragment_ids) in backfill_fragments {
                self.backfill_fragments
                    .entry(source_id)
                    .or_default()
                    .append(&mut fragment_ids);
            }
        }

        if let Some(assignment) = split_assignment {
            for (_, actor_splits) in assignment {
                for (actor_id, splits) in actor_splits {
                    // override previous splits info
                    self.actor_splits.insert(actor_id, splits);
                }
            }
        }

        if let Some(dropped_actors) = dropped_actors {
            for actor_id in &dropped_actors {
                self.actor_splits.remove(actor_id);
            }
        }
    }

    fn drop_source_fragments(
        &mut self,
        source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        removed_actors: &HashSet<ActorId>,
    ) {
        for (source_id, fragment_ids) in source_fragments {
            if let Entry::Occupied(mut entry) = self.source_fragments.entry(source_id) {
                let managed_fragment_ids = entry.get_mut();
                for fragment_id in &fragment_ids {
                    managed_fragment_ids.remove(fragment_id);
                }

                if managed_fragment_ids.is_empty() {
                    entry.remove();
                }
            }
        }

        for actor_id in removed_actors {
            self.actor_splits.remove(actor_id);
        }
    }
}

/// Note: the `PartialEq` and `Ord` impl just compares the number of splits.
#[derive(Debug)]
struct ActorSplitsAssignment<T: SplitMetaData> {
    actor_id: ActorId,
    splits: Vec<T>,
}

impl<T: SplitMetaData + Clone> Eq for ActorSplitsAssignment<T> {}

impl<T: SplitMetaData + Clone> PartialEq<Self> for ActorSplitsAssignment<T> {
    fn eq(&self, other: &Self) -> bool {
        self.splits.len() == other.splits.len()
    }
}

impl<T: SplitMetaData + Clone> PartialOrd<Self> for ActorSplitsAssignment<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: SplitMetaData + Clone> Ord for ActorSplitsAssignment<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Note: this is reversed order, to make BinaryHeap a min heap.
        other.splits.len().cmp(&self.splits.len())
    }
}

#[derive(Debug)]
struct SplitDiffOptions {
    enable_scale_in: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for SplitDiffOptions {
    fn default() -> Self {
        SplitDiffOptions {
            enable_scale_in: false,
        }
    }
}

/// Reassigns splits if there are new splits or dropped splits,
/// i.e., `actor_splits` and `discovered_splits` differ.
///
/// The existing splits will remain unmoved in their currently assigned actor.
///
/// If an actor has an upstream actor, it should be a backfill executor,
/// and its splits should be aligned with the upstream actor. `reassign_splits` should not be used in this case.
/// Use `align_backfill_splits` instead.
///
/// - `fragment_id`: just for logging
///
/// ## Different connectors' behavior of split change
///
/// ### Kafka and Pulsar
/// They only support increasing the number of splits via adding new empty splits.
/// Old data is not moved.
///
/// ### Kinesis
/// It supports *pairwise* shard split and merge.
///
/// In both cases, old data remain in the old shard(s) and the old shard is still available.
/// New data are routed to the new shard(s).
/// After the retention period has expired, the old shard will become `EXPIRED` and isn't
/// listed any more. In other words, the total number of shards will first increase and then decrease.
///
/// See also:
/// - [Kinesis resharding doc](https://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-after-resharding.html#kinesis-using-sdk-java-resharding-data-routing)
/// - An example of how the shards can be like: <https://stackoverflow.com/questions/72272034/list-shard-show-more-shards-than-provisioned>
fn reassign_splits<T>(
    fragment_id: FragmentId,
    actor_splits: HashMap<ActorId, Vec<T>>,
    discovered_splits: &BTreeMap<SplitId, T>,
    opts: SplitDiffOptions,
) -> Option<HashMap<ActorId, Vec<T>>>
where
    T: SplitMetaData + Clone,
{
    // if no actors, return
    if actor_splits.is_empty() {
        return None;
    }

    let prev_split_ids: HashSet<_> = actor_splits
        .values()
        .flat_map(|splits| splits.iter().map(SplitMetaData::id))
        .collect();

    tracing::trace!(fragment_id, prev_split_ids = ?prev_split_ids, "previous splits");
    tracing::trace!(fragment_id, prev_split_ids = ?discovered_splits.keys(), "discovered splits");

    let discovered_split_ids: HashSet<_> = discovered_splits.keys().cloned().collect();

    let dropped_splits: HashSet<_> = prev_split_ids
        .difference(&discovered_split_ids)
        .cloned()
        .collect();

    if !dropped_splits.is_empty() {
        if opts.enable_scale_in {
            tracing::info!(fragment_id, dropped_spltis = ?dropped_splits, "new dropped splits");
        } else {
            tracing::warn!(fragment_id, dropped_spltis = ?dropped_splits, "split dropping happened, but it is not allowed");
        }
    }

    let new_discovered_splits: BTreeSet<_> = discovered_split_ids
        .into_iter()
        .filter(|split_id| !prev_split_ids.contains(split_id))
        .collect();

    if opts.enable_scale_in {
        // if we support scale in, no more splits are discovered, and no splits are dropped, return
        // we need to check if discovered_split_ids is empty, because if it is empty, we need to
        // handle the case of scale in to zero (like deleting all objects from s3)
        if dropped_splits.is_empty()
            && new_discovered_splits.is_empty()
            && !discovered_splits.is_empty()
        {
            return None;
        }
    } else {
        // if we do not support scale in, and no more splits are discovered, return
        if new_discovered_splits.is_empty() && !discovered_splits.is_empty() {
            return None;
        }
    }

    tracing::info!(fragment_id, new_discovered_splits = ?new_discovered_splits, "new discovered splits");

    let mut heap = BinaryHeap::with_capacity(actor_splits.len());

    for (actor_id, mut splits) in actor_splits {
        if opts.enable_scale_in {
            splits.retain(|split| !dropped_splits.contains(&split.id()));
        }

        heap.push(ActorSplitsAssignment { actor_id, splits })
    }

    for split_id in new_discovered_splits {
        // ActorSplitsAssignment's Ord is reversed, so this is min heap, i.e.,
        // we get the assignment with the least splits here.

        // Note: If multiple actors have the same number of splits, it will be randomly picked.
        // When the number of source actors is larger than the number of splits,
        // It's possible that the assignment is uneven.
        // e.g., https://github.com/risingwavelabs/risingwave/issues/14324#issuecomment-1875033158
        // TODO: We should make the assignment rack-aware to make sure it's even.
        let mut peek_ref = heap.peek_mut().unwrap();
        peek_ref
            .splits
            .push(discovered_splits.get(&split_id).cloned().unwrap());
    }

    Some(
        heap.into_iter()
            .map(|ActorSplitsAssignment { actor_id, splits }| (actor_id, splits))
            .collect(),
    )
}

pub fn validate_assignment(assignment: &mut HashMap<ActorId, Vec<SplitImpl>>) {
    // check if one split is assign to multiple actors
    let mut split_to_actor = HashMap::new();
    for (actor_id, splits) in &mut *assignment {
        let _ = splits.iter().map(|split| {
            split_to_actor
                .entry(split.id())
                .or_insert_with(Vec::new)
                .push(*actor_id)
        });
    }

    for (split_id, actor_ids) in &mut split_to_actor {
        if actor_ids.len() > 1 {
            tracing::warn!(split_id = ?split_id, actor_ids = ?actor_ids, "split is assigned to multiple actors");
        }
        // keep the first actor and remove the rest from the assignment
        for actor_id in actor_ids.iter().skip(1) {
            assignment
                .get_mut(actor_id)
                .unwrap()
                .retain(|split| split.id() != *split_id);
        }
    }
}

fn align_backfill_splits(
    backfill_actors: impl IntoIterator<Item = (ActorId, Vec<ActorId>)>,
    upstream_assignment: &HashMap<ActorId, Vec<SplitImpl>>,
    fragment_id: FragmentId,
    upstream_fragment_id: FragmentId,
) -> anyhow::Result<HashMap<ActorId, Vec<SplitImpl>>> {
    backfill_actors
        .into_iter()
        .map(|(actor_id, upstream_actor_id)| {
            let err = || anyhow::anyhow!("source backfill actor should have one upstream actor, fragment_id: {fragment_id}, upstream_fragment_id: {upstream_fragment_id}, actor_id: {actor_id}, upstream_assignment: {upstream_assignment:?}, upstream_actor_id: {upstream_actor_id:?}");
            if upstream_actor_id.len() != 1 {
                return Err(err());
            }
            let Some(splits) = upstream_assignment.get(&upstream_actor_id[0]) else {
                return Err(err());
            };
            Ok((
                actor_id,
                splits.clone(),
            ))
        })
        .collect()
}

impl SourceManager {
    const DEFAULT_SOURCE_TICK_INTERVAL: Duration = Duration::from_secs(10);
    const DEFAULT_SOURCE_TICK_TIMEOUT: Duration = Duration::from_secs(10);

    pub async fn new(
        barrier_scheduler: BarrierScheduler,
        metadata_manager: MetadataManager,
        metrics: Arc<MetaMetrics>,
    ) -> MetaResult<Self> {
        let mut managed_sources = HashMap::new();
        {
            let sources = metadata_manager.list_sources().await?;
            for source in sources {
                Self::create_source_worker_async(source, &mut managed_sources, metrics.clone())?
            }
        }

        let mut actor_splits = HashMap::new();
        let mut source_fragments = HashMap::new();
        let mut backfill_fragments = HashMap::new();

        match &metadata_manager {
            MetadataManager::V1(mgr) => {
                for table_fragments in mgr
                    .fragment_manager
                    .get_fragment_read_guard()
                    .await
                    .table_fragments()
                    .values()
                {
                    source_fragments.extend(table_fragments.stream_source_fragments());
                    backfill_fragments.extend(table_fragments.source_backfill_fragments()?);
                    actor_splits.extend(table_fragments.actor_splits.clone());
                }
            }
            MetadataManager::V2(mgr) => {
                source_fragments = mgr
                    .catalog_controller
                    .load_source_fragment_ids()
                    .await?
                    .into_iter()
                    .map(|(source_id, fragment_ids)| {
                        (
                            source_id as SourceId,
                            fragment_ids.into_iter().map(|id| id as _).collect(),
                        )
                    })
                    .collect();
                backfill_fragments = mgr
                    .catalog_controller
                    .load_backfill_fragment_ids()
                    .await?
                    .into_iter()
                    .map(|(source_id, fragment_ids)| {
                        (
                            source_id as SourceId,
                            fragment_ids
                                .into_iter()
                                .map(|(id, up_id)| (id as _, up_id as _))
                                .collect(),
                        )
                    })
                    .collect();
                actor_splits = mgr
                    .catalog_controller
                    .load_actor_splits()
                    .await?
                    .into_iter()
                    .map(|(actor_id, splits)| {
                        (
                            actor_id as ActorId,
                            splits
                                .to_protobuf()
                                .splits
                                .iter()
                                .map(|split| SplitImpl::try_from(split).unwrap())
                                .collect(),
                        )
                    })
                    .collect();
            }
        }

        let core = Mutex::new(SourceManagerCore::new(
            metadata_manager,
            managed_sources,
            source_fragments,
            backfill_fragments,
            actor_splits,
        ));

        Ok(Self {
            barrier_scheduler,
            core,
            paused: Mutex::new(()),
            metrics,
        })
    }

    pub async fn drop_source_fragments_v2(
        &self,
        source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        removed_actors: HashSet<ActorId>,
    ) {
        let mut core = self.core.lock().await;
        core.drop_source_fragments(source_fragments, &removed_actors);
    }

    /// For dropping MV.
    pub async fn drop_source_fragments(&self, table_fragments: &[TableFragments]) {
        let mut core = self.core.lock().await;

        // Extract the fragments that include source operators.
        let source_fragments = table_fragments
            .iter()
            .flat_map(|table_fragments| table_fragments.stream_source_fragments())
            .collect::<HashMap<_, _>>();

        let fragments = table_fragments
            .iter()
            .flat_map(|table_fragments| &table_fragments.fragments)
            .collect::<BTreeMap<_, _>>();

        let dropped_actors = source_fragments
            .values()
            .flatten()
            .flat_map(|fragment_id| fragments.get(fragment_id).unwrap().get_actors())
            .map(|actor| actor.get_actor_id())
            .collect::<HashSet<_>>();

        core.drop_source_fragments(source_fragments, &dropped_actors);
    }

    /// Updates states after split change (`post_collect` barrier) or scaling (`post_apply_reschedule`).
    pub async fn apply_source_change(
        &self,
        added_source_fragments: Option<HashMap<SourceId, BTreeSet<FragmentId>>>,
        added_backfill_fragments: Option<HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>>,
        split_assignment: Option<SplitAssignment>,
        dropped_actors: Option<HashSet<ActorId>>,
    ) {
        let mut core = self.core.lock().await;
        core.apply_source_change(
            added_source_fragments,
            added_backfill_fragments,
            split_assignment,
            dropped_actors,
        );
    }

    /// Migrates splits from previous actors to the new actors for a rescheduled fragment.
    ///
    /// Very occasionally split removal may happen
    /// during scaling, in which case we need to use the old splits for reallocation instead of the
    /// latest splits (which may be missing), so that we can resolve the split removal in the next
    /// command.
    pub async fn migrate_splits(
        &self,
        fragment_id: FragmentId,
        prev_actor_ids: &[ActorId],
        curr_actor_ids: &[ActorId],
    ) -> MetaResult<HashMap<ActorId, Vec<SplitImpl>>> {
        let core = self.core.lock().await;

        let prev_splits = prev_actor_ids
            .iter()
            .flat_map(|actor_id| core.actor_splits.get(actor_id).unwrap())
            .map(|split| (split.id(), split.clone()))
            .collect();

        let empty_actor_splits = curr_actor_ids
            .iter()
            .map(|actor_id| (*actor_id, vec![]))
            .collect();

        let diff = reassign_splits(
            fragment_id,
            empty_actor_splits,
            &prev_splits,
            // pre-allocate splits is the first time getting splits and it does not have scale in scene
            SplitDiffOptions::default(),
        )
        .unwrap_or_default();

        Ok(diff)
    }

    /// Allocates splits to actors for a newly created source executor.
    pub async fn allocate_splits(&self, table_id: &TableId) -> MetaResult<SplitAssignment> {
        let core = self.core.lock().await;
        let table_fragments = core
            .metadata_manager
            .get_job_fragments_by_id(table_id)
            .await?;

        let source_fragments = table_fragments.stream_source_fragments();

        let mut assigned = HashMap::new();

        for (source_id, fragments) in source_fragments {
            let handle = core
                .managed_sources
                .get(&source_id)
                .with_context(|| format!("could not find source {}", source_id))?;

            if handle.splits.lock().await.splits.is_none() {
                // force refresh source
                let (tx, rx) = oneshot::channel();
                handle
                    .sync_call_tx
                    .send(tx)
                    .ok()
                    .context("failed to send sync call")?;
                rx.await
                    .ok()
                    .context("failed to receive sync call response")??;
            }

            let splits = handle.discovered_splits().await.unwrap();

            if splits.is_empty() {
                tracing::warn!("no splits detected for source {}", source_id);
                continue;
            }

            for fragment_id in fragments {
                let empty_actor_splits = table_fragments
                    .fragments
                    .get(&fragment_id)
                    .unwrap()
                    .actors
                    .iter()
                    .map(|actor| (actor.actor_id, vec![]))
                    .collect();

                if let Some(diff) = reassign_splits(
                    fragment_id,
                    empty_actor_splits,
                    &splits,
                    SplitDiffOptions::default(),
                ) {
                    assigned.insert(fragment_id, diff);
                }
            }
        }

        Ok(assigned)
    }

    /// Allocates splits to actors for a newly created `SourceBackfill` executor.
    ///
    /// Unlike [`Self::allocate_splits`], which creates a new assignment,
    /// this method aligns the splits for backfill fragments with its upstream source fragment ([`align_backfill_splits`]).
    pub async fn allocate_splits_for_backfill(
        &self,
        table_id: &TableId,
        dispatchers: &HashMap<ActorId, Vec<Dispatcher>>,
    ) -> MetaResult<SplitAssignment> {
        let core = self.core.lock().await;
        let table_fragments = core
            .metadata_manager
            .get_job_fragments_by_id(table_id)
            .await?;

        let upstream_assignment = &core.actor_splits;
        let source_backfill_fragments = table_fragments.source_backfill_fragments()?;

        let mut assigned = HashMap::new();

        for (_source_id, fragments) in source_backfill_fragments {
            for (fragment_id, upstream_fragment_id) in fragments {
                let upstream_actors = core
                    .metadata_manager
                    .get_running_actors_of_fragment(upstream_fragment_id)
                    .await?;
                let mut backfill_actors = vec![];
                for upstream_actor in upstream_actors {
                    if let Some(dispatchers) = dispatchers.get(&upstream_actor) {
                        let err = || {
                            anyhow::anyhow!(
                            "source backfill fragment's upstream fragment should have one dispatcher, fragment_id: {fragment_id}, upstream_fragment_id: {upstream_fragment_id}, upstream_actor: {upstream_actor}, dispatchers: {dispatchers:?}",
                            fragment_id = fragment_id,
                            upstream_fragment_id = upstream_fragment_id,
                            upstream_actor = upstream_actor,
                            dispatchers = dispatchers
                        )
                        };
                        if dispatchers.len() != 1 || dispatchers[0].downstream_actor_id.len() != 1 {
                            return Err(err().into());
                        }

                        backfill_actors
                            .push((dispatchers[0].downstream_actor_id[0], vec![upstream_actor]));
                    }
                }
                assigned.insert(
                    fragment_id,
                    align_backfill_splits(
                        backfill_actors,
                        upstream_assignment,
                        fragment_id,
                        upstream_fragment_id,
                    )?,
                );
            }
        }

        Ok(assigned)
    }

    /// register connector worker for source.
    pub async fn register_source(&self, source: &Source) -> MetaResult<()> {
        let mut core = self.core.lock().await;
        if core.managed_sources.contains_key(&source.get_id()) {
            tracing::warn!("source {} already registered", source.get_id());
        } else {
            Self::create_source_worker(source, &mut core.managed_sources, self.metrics.clone())
                .await
                .context("failed to create source worker")?;
        }
        Ok(())
    }

    /// Unregister connector worker for source.
    pub async fn unregister_sources(&self, source_ids: Vec<SourceId>) {
        let mut core = self.core.lock().await;
        for source_id in source_ids {
            if let Some(handle) = core.managed_sources.remove(&source_id) {
                handle.handle.abort();
            }
        }
    }

    /// Used on startup ([`Self::new`]). Failed sources will not block meta startup.
    fn create_source_worker_async(
        source: Source,
        managed_sources: &mut HashMap<SourceId, ConnectorSourceWorkerHandle>,
        metrics: Arc<MetaMetrics>,
    ) -> MetaResult<()> {
        tracing::info!("spawning new watcher for source {}", source.id);

        let splits = Arc::new(Mutex::new(SharedSplitMap { splits: None }));
        let current_splits_ref = splits.clone();
        let source_id = source.id;

        let connector_properties = extract_prop_from_existing_source(&source)?;
        let enable_scale_in = connector_properties.enable_split_scale_in();
        let (sync_call_tx, sync_call_rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = tokio::spawn(async move {
            let mut ticker = time::interval(Self::DEFAULT_SOURCE_TICK_INTERVAL);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            dispatch_source_prop!(connector_properties, prop, {
                let mut worker = loop {
                    ticker.tick().await;

                    match ConnectorSourceWorker::create(
                        &source,
                        prop.deref().clone(),
                        DEFAULT_SOURCE_WORKER_TICK_INTERVAL,
                        current_splits_ref.clone(),
                        metrics.clone(),
                    )
                    .await
                    {
                        Ok(worker) => {
                            break worker;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e.as_report(), "failed to create source worker");
                        }
                    }
                };

                worker.run(sync_call_rx).await
            });
        });

        managed_sources.insert(
            source_id,
            ConnectorSourceWorkerHandle {
                handle,
                sync_call_tx,
                splits,
                enable_scale_in,
            },
        );
        Ok(())
    }

    /// Used when registering new sources (`Self::register_source`).
    ///
    /// It will call `ConnectorSourceWorker::tick()` to fetch split metadata once before returning.
    async fn create_source_worker(
        source: &Source,
        managed_sources: &mut HashMap<SourceId, ConnectorSourceWorkerHandle>,
        metrics: Arc<MetaMetrics>,
    ) -> MetaResult<()> {
        tracing::info!("spawning new watcher for source {}", source.id);

        let splits = Arc::new(Mutex::new(SharedSplitMap { splits: None }));
        let current_splits_ref = splits.clone();
        let source_id = source.id;

        let connector_properties = extract_prop_from_new_source(source)?;
        let enable_scale_in = connector_properties.enable_split_scale_in();
        let (sync_call_tx, sync_call_rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = dispatch_source_prop!(connector_properties, prop, {
            let mut worker = ConnectorSourceWorker::create(
                source,
                *prop,
                DEFAULT_SOURCE_WORKER_TICK_INTERVAL,
                current_splits_ref.clone(),
                metrics,
            )
            .await?;

            // if fail to fetch meta info, will refuse to create source

            // todo: make the timeout configurable, longer than `properties.sync.call.timeout`
            // in kafka
            tokio::time::timeout(Self::DEFAULT_SOURCE_TICK_TIMEOUT, worker.tick())
                .await
                .ok()
                .with_context(|| {
                    format!(
                        "failed to fetch meta info for source {}, timeout {:?}",
                        source.id,
                        Self::DEFAULT_SOURCE_TICK_TIMEOUT
                    )
                })??;

            tokio::spawn(async move { worker.run(sync_call_rx).await })
        });

        managed_sources.insert(
            source_id,
            ConnectorSourceWorkerHandle {
                handle,
                sync_call_tx,
                splits,
                enable_scale_in,
            },
        );

        Ok(())
    }

    pub async fn list_assignments(&self) -> HashMap<ActorId, Vec<SplitImpl>> {
        let core = self.core.lock().await;
        core.actor_splits.clone()
    }

    /// Checks whether the external source metadata has changed, and sends a split assignment command
    /// if it has.
    ///
    /// This is also how a newly created `SourceExecutor` is initialized.
    /// (force `tick` in `Self::create_source_worker`)
    ///
    /// The command will first updates `SourceExecutor`'s splits, and finally calls `Self::apply_source_change`
    /// to update states in `SourceManager`.
    async fn tick(&self) -> MetaResult<()> {
        let split_assignment = {
            let core_guard = self.core.lock().await;
            core_guard.reassign_splits().await?
        };

        if !split_assignment.is_empty() {
            let command = Command::SourceSplitAssignment(split_assignment);
            tracing::info!(command = ?command, "pushing down split assignment command");
            self.barrier_scheduler.run_command(command).await?;
        }

        Ok(())
    }

    pub async fn run(&self) -> MetaResult<()> {
        let mut ticker = time::interval(Self::DEFAULT_SOURCE_TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let _pause_guard = self.paused.lock().await;
            if let Err(e) = self.tick().await {
                tracing::error!(
                    error = %e.as_report(),
                    "error happened while running source manager tick",
                );
            }
        }
    }
}

pub fn build_actor_connector_splits(
    splits: &HashMap<ActorId, Vec<SplitImpl>>,
) -> HashMap<u32, ConnectorSplits> {
    splits
        .iter()
        .map(|(&actor_id, splits)| {
            (
                actor_id,
                ConnectorSplits {
                    splits: splits.iter().map(ConnectorSplit::from).collect(),
                },
            )
        })
        .collect()
}

pub fn build_actor_split_impls(
    actor_splits: &HashMap<u32, ConnectorSplits>,
) -> HashMap<ActorId, Vec<SplitImpl>> {
    actor_splits
        .iter()
        .map(|(actor_id, ConnectorSplits { splits })| {
            (
                *actor_id,
                splits
                    .iter()
                    .map(|split| SplitImpl::try_from(split).unwrap())
                    .collect(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};

    use risingwave_common::types::JsonbVal;
    use risingwave_connector::error::ConnectorResult;
    use risingwave_connector::source::test_source::TestSourceSplit;
    use risingwave_connector::source::{SplitId, SplitImpl, SplitMetaData};
    use serde::{Deserialize, Serialize};

    use super::validate_assignment;
    use crate::model::{ActorId, FragmentId};
    use crate::stream::source_manager::{reassign_splits, SplitDiffOptions};
    use crate::stream::SplitAssignment;

    #[derive(Debug, Copy, Clone, Serialize, Deserialize)]
    struct TestSplit {
        id: u32,
    }

    impl SplitMetaData for TestSplit {
        fn id(&self) -> SplitId {
            format!("{}", self.id).into()
        }

        fn encode_to_json(&self) -> JsonbVal {
            serde_json::to_value(*self).unwrap().into()
        }

        fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
            serde_json::from_value(value.take()).map_err(Into::into)
        }

        fn update_offset(&mut self, _last_read_offset: String) -> ConnectorResult<()> {
            Ok(())
        }
    }

    fn check_all_splits(
        discovered_splits: &BTreeMap<SplitId, TestSplit>,
        diff: &HashMap<ActorId, Vec<TestSplit>>,
    ) {
        let mut split_ids: HashSet<_> = discovered_splits.keys().cloned().collect();

        for splits in diff.values() {
            for split in splits {
                assert!(split_ids.remove(&split.id()))
            }
        }

        assert!(split_ids.is_empty());
    }

    #[test]
    fn test_drop_splits() {
        let mut actor_splits: HashMap<ActorId, _> = HashMap::new();
        actor_splits.insert(0, vec![TestSplit { id: 0 }, TestSplit { id: 1 }]);
        actor_splits.insert(1, vec![TestSplit { id: 2 }, TestSplit { id: 3 }]);
        actor_splits.insert(2, vec![TestSplit { id: 4 }, TestSplit { id: 5 }]);

        let mut prev_split_to_actor = HashMap::new();
        for (actor_id, splits) in &actor_splits {
            for split in splits {
                prev_split_to_actor.insert(split.id(), *actor_id);
            }
        }

        let discovered_splits: BTreeMap<SplitId, TestSplit> = (1..5)
            .map(|i| {
                let split = TestSplit { id: i };
                (split.id(), split)
            })
            .collect();

        let opts = SplitDiffOptions {
            enable_scale_in: true,
        };

        let prev_split_ids: HashSet<_> = actor_splits
            .values()
            .flat_map(|splits| splits.iter().map(|split| split.id()))
            .collect();

        let diff = reassign_splits(
            FragmentId::default(),
            actor_splits,
            &discovered_splits,
            opts,
        )
        .unwrap();
        check_all_splits(&discovered_splits, &diff);

        let mut after_split_to_actor = HashMap::new();
        for (actor_id, splits) in &diff {
            for split in splits {
                after_split_to_actor.insert(split.id(), *actor_id);
            }
        }

        let discovered_split_ids: HashSet<_> = discovered_splits.keys().cloned().collect();

        let retained_split_ids: HashSet<_> =
            prev_split_ids.intersection(&discovered_split_ids).collect();

        for retained_split_id in retained_split_ids {
            assert_eq!(
                prev_split_to_actor.get(retained_split_id),
                after_split_to_actor.get(retained_split_id)
            )
        }
    }

    #[test]
    fn test_drop_splits_to_empty() {
        let mut actor_splits: HashMap<ActorId, _> = HashMap::new();
        actor_splits.insert(0, vec![TestSplit { id: 0 }]);

        let discovered_splits: BTreeMap<SplitId, TestSplit> = BTreeMap::new();

        let opts = SplitDiffOptions {
            enable_scale_in: true,
        };

        let diff = reassign_splits(
            FragmentId::default(),
            actor_splits,
            &discovered_splits,
            opts,
        )
        .unwrap();

        assert!(!diff.is_empty())
    }

    #[test]
    fn test_validate_assignment() {
        let mut fragment_assignment: SplitAssignment;
        let test_assignment: HashMap<ActorId, Vec<SplitImpl>> = maplit::hashmap! {
            0 => vec![SplitImpl::Test(
                TestSourceSplit {id: "1".into(), properties: Default::default(), offset: Default::default()}
            ), SplitImpl::Test(
                TestSourceSplit {id: "2".into(), properties: Default::default(), offset: Default::default()}
            )],
            1 => vec![SplitImpl::Test(
                TestSourceSplit {id: "3".into(), properties: Default::default(), offset: Default::default()}
            )],
            2 => vec![SplitImpl::Test(
                TestSourceSplit {id: "1".into(), properties: Default::default(), offset: Default::default()}
            )],
        };
        fragment_assignment = maplit::hashmap! {
            1 => test_assignment,
        };

        fragment_assignment.iter_mut().for_each(|(_, assignment)| {
            validate_assignment(assignment);
        });

        {
            let mut split_to_actor = HashMap::new();
            for actor_to_splits in fragment_assignment.values() {
                for (actor_id, splits) in actor_to_splits {
                    let _ = splits.iter().map(|split| {
                        split_to_actor
                            .entry(split.id())
                            .or_insert_with(Vec::new)
                            .push(*actor_id)
                    });
                }
            }

            for actor_ids in split_to_actor.values() {
                assert_eq!(actor_ids.len(), 1);
            }
        }
    }

    #[test]
    fn test_reassign_splits() {
        let actor_splits = HashMap::new();
        let discovered_splits: BTreeMap<SplitId, TestSplit> = BTreeMap::new();
        assert!(reassign_splits(
            FragmentId::default(),
            actor_splits,
            &discovered_splits,
            Default::default()
        )
        .is_none());

        let actor_splits = (0..3).map(|i| (i, vec![])).collect();
        let discovered_splits: BTreeMap<SplitId, TestSplit> = BTreeMap::new();
        let diff = reassign_splits(
            FragmentId::default(),
            actor_splits,
            &discovered_splits,
            Default::default(),
        )
        .unwrap();
        assert_eq!(diff.len(), 3);
        for splits in diff.values() {
            assert!(splits.is_empty())
        }

        let actor_splits = (0..3).map(|i| (i, vec![])).collect();
        let discovered_splits: BTreeMap<SplitId, TestSplit> = (0..3)
            .map(|i| {
                let split = TestSplit { id: i };
                (split.id(), split)
            })
            .collect();

        let diff = reassign_splits(
            FragmentId::default(),
            actor_splits,
            &discovered_splits,
            Default::default(),
        )
        .unwrap();
        assert_eq!(diff.len(), 3);
        for splits in diff.values() {
            assert_eq!(splits.len(), 1);
        }

        check_all_splits(&discovered_splits, &diff);

        let actor_splits = (0..3).map(|i| (i, vec![TestSplit { id: i }])).collect();
        let discovered_splits: BTreeMap<SplitId, TestSplit> = (0..5)
            .map(|i| {
                let split = TestSplit { id: i };
                (split.id(), split)
            })
            .collect();

        let diff = reassign_splits(
            FragmentId::default(),
            actor_splits,
            &discovered_splits,
            Default::default(),
        )
        .unwrap();
        assert_eq!(diff.len(), 3);
        for splits in diff.values() {
            let len = splits.len();
            assert!(len == 1 || len == 2);
        }

        check_all_splits(&discovered_splits, &diff);

        let mut actor_splits: HashMap<ActorId, Vec<TestSplit>> =
            (0..3).map(|i| (i, vec![TestSplit { id: i }])).collect();
        actor_splits.insert(3, vec![]);
        actor_splits.insert(4, vec![]);

        let discovered_splits: BTreeMap<SplitId, TestSplit> = (0..5)
            .map(|i| {
                let split = TestSplit { id: i };
                (split.id(), split)
            })
            .collect();

        let diff = reassign_splits(
            FragmentId::default(),
            actor_splits,
            &discovered_splits,
            Default::default(),
        )
        .unwrap();
        assert_eq!(diff.len(), 5);
        for splits in diff.values() {
            assert_eq!(splits.len(), 1);
        }

        check_all_splits(&discovered_splits, &diff);
    }
}
