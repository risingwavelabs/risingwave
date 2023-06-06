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

use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_connector::source::{
    ConnectorProperties, SplitEnumeratorImpl, SplitId, SplitImpl, SplitMetaData,
};
use risingwave_pb::catalog::Source;
use risingwave_pb::connector_service::table_schema::Column;
use risingwave_pb::connector_service::TableSchema;
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio::{select, time};

use crate::barrier::{BarrierScheduler, Command};
use crate::manager::{CatalogManagerRef, FragmentManagerRef, SourceId};
use crate::model::{ActorId, FragmentId, TableFragments};
use crate::rpc::metrics::MetaMetrics;
use crate::storage::MetaStore;
use crate::MetaResult;

pub type SourceManagerRef<S> = Arc<SourceManager<S>>;
pub type SplitAssignment = HashMap<FragmentId, HashMap<ActorId, Vec<SplitImpl>>>;

pub struct SourceManager<S: MetaStore> {
    pub(crate) paused: Mutex<()>,
    barrier_scheduler: BarrierScheduler<S>,
    core: Mutex<SourceManagerCore<S>>,
    connector_rpc_endpoint: Option<String>,
    metrics: Arc<MetaMetrics>,
}

const MAX_FAIL_CNT: u32 = 10;

struct SharedSplitMap {
    splits: Option<BTreeMap<SplitId, SplitImpl>>,
}

type SharedSplitMapRef = Arc<Mutex<SharedSplitMap>>;

struct ConnectorSourceWorker {
    source_id: SourceId,
    source_name: String,
    current_splits: SharedSplitMapRef,
    enumerator: SplitEnumeratorImpl,
    period: Duration,
    metrics: Arc<MetaMetrics>,
    connector_properties: ConnectorProperties,
    fail_cnt: u32,
}

impl ConnectorSourceWorker {
    async fn refresh(&mut self) -> MetaResult<()> {
        let enumerator = SplitEnumeratorImpl::create(self.connector_properties.clone()).await?;
        self.enumerator = enumerator;
        self.fail_cnt = 0;
        tracing::info!("refreshed source enumerator: {}", self.source_name);
        Ok(())
    }

    pub async fn create(
        connector_rpc_endpoint: &Option<String>,
        source: &Source,
        period: Duration,
        metrics: Arc<MetaMetrics>,
    ) -> MetaResult<Self> {
        let mut properties = ConnectorProperties::extract(source.properties.clone())?;
        // init cdc properties
        if let Some(endpoint) = connector_rpc_endpoint {
            let table_schema = Self::extract_source_schema(source);
            properties.init_properties_for_cdc(source.id, endpoint.to_string(), Some(table_schema));
        }
        let enumerator = SplitEnumeratorImpl::create(properties.clone()).await?;
        let splits = Arc::new(Mutex::new(SharedSplitMap { splits: None }));
        Ok(Self {
            source_id: source.id,
            source_name: source.name.clone(),
            current_splits: splits,
            enumerator,
            period,
            metrics,
            connector_properties: properties,
            fail_cnt: 0,
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
                            tracing::error!("error happened when refresh from connector source worker: {}", e.to_string());
                        }
                    }
                    if let Err(e) = self.tick().await {
                        tracing::error!("error happened when tick from connector source worker: {}", e.to_string());
                    }
                }
            }
        }
    }

    async fn tick(&mut self) -> MetaResult<()> {
        let source_is_up = |res: i64| {
            self.metrics
                .source_is_up
                .with_label_values(&[self.source_id.to_string().as_str(), &self.source_name])
                .set(res);
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
                .map(|split| (split.id(), split))
                .collect(),
        );

        Ok(())
    }

    fn extract_source_schema(source: &Source) -> TableSchema {
        let pk_indices = source
            .pk_column_ids
            .iter()
            .map(|&id| {
                source
                    .columns
                    .iter()
                    .position(|col| col.column_desc.as_ref().unwrap().column_id == id)
                    .unwrap() as u32
            })
            .collect_vec();

        TableSchema {
            columns: source
                .columns
                .iter()
                .flat_map(|col| &col.column_desc)
                .map(|col| Column {
                    name: col.name.clone(),
                    data_type: col.column_type.as_ref().unwrap().type_name,
                })
                .collect(),
            pk_indices,
        }
    }
}

struct ConnectorSourceWorkerHandle {
    handle: JoinHandle<()>,
    sync_call_tx: UnboundedSender<oneshot::Sender<MetaResult<()>>>,
    splits: SharedSplitMapRef,
}

impl ConnectorSourceWorkerHandle {
    async fn discovered_splits(&self) -> Option<BTreeMap<SplitId, SplitImpl>> {
        self.splits.lock().await.splits.clone()
    }
}

pub struct SourceManagerCore<S: MetaStore> {
    fragment_manager: FragmentManagerRef<S>,

    /// Managed source loops
    managed_sources: HashMap<SourceId, ConnectorSourceWorkerHandle>,
    /// Fragments associated with each source
    source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    /// Revert index for source_fragments
    fragment_sources: HashMap<FragmentId, SourceId>,

    /// Splits assigned per actor
    actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
}

impl<S> SourceManagerCore<S>
where
    S: MetaStore,
{
    fn new(
        fragment_manager: FragmentManagerRef<S>,
        managed_sources: HashMap<SourceId, ConnectorSourceWorkerHandle>,
        source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
    ) -> Self {
        let mut fragment_sources = HashMap::new();
        for (source_id, fragment_ids) in &source_fragments {
            for fragment_id in fragment_ids {
                fragment_sources.insert(*fragment_id, *source_id);
            }
        }

        Self {
            fragment_manager,
            managed_sources,
            source_fragments,
            fragment_sources,
            actor_splits,
        }
    }

    async fn diff(&self) -> MetaResult<SplitAssignment> {
        let mut split_assignment: SplitAssignment = HashMap::new();

        for (source_id, handle) in &self.managed_sources {
            let fragment_ids = match self.source_fragments.get(source_id) {
                Some(fragment_ids) if !fragment_ids.is_empty() => fragment_ids,
                _ => {
                    continue;
                }
            };

            if let Some(discovered_splits) = handle.discovered_splits().await {
                for fragment_id in fragment_ids {
                    let actor_ids = match self
                        .fragment_manager
                        .get_running_actors_of_fragment(*fragment_id)
                        .await
                    {
                        Ok(actor_ids) => actor_ids,
                        Err(err) => {
                            tracing::warn!("Failed to get the actor of the fragment {}, maybe the fragment doesn't exist anymore", err.to_string());
                            continue;
                        }
                    };

                    let prev_actor_splits: HashMap<_, _> = actor_ids
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

                    if let Some(change) = diff_splits(
                        prev_actor_splits,
                        &discovered_splits,
                        SplitDiffOptions::default(),
                    ) {
                        split_assignment.insert(*fragment_id, change);
                    }
                }
            }
        }

        Ok(split_assignment)
    }

    pub fn apply_source_change(
        &mut self,
        source_fragments: Option<HashMap<SourceId, BTreeSet<FragmentId>>>,
        split_assignment: Option<SplitAssignment>,
        dropped_actors: Option<HashSet<ActorId>>,
    ) {
        if let Some(source_fragments) = source_fragments {
            for (source_id, mut fragment_ids) in source_fragments {
                for fragment_id in &fragment_ids {
                    self.fragment_sources.insert(*fragment_id, source_id);
                }

                self.source_fragments
                    .entry(source_id)
                    .or_insert_with(BTreeSet::default)
                    .append(&mut fragment_ids);
            }
        }

        if let Some(assignment) = split_assignment {
            for (_, actor_splits) in assignment {
                for (actor_id, splits) in actor_splits {
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

    pub fn drop_source_change(
        &mut self,
        source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        actor_splits: &HashSet<ActorId>,
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

            for fragment_id in &fragment_ids {
                self.fragment_sources.remove(fragment_id);
            }
        }

        for actor_id in actor_splits {
            self.actor_splits.remove(actor_id);
        }
    }

    pub fn get_actor_splits(&self) -> HashMap<ActorId, Vec<SplitImpl>> {
        self.actor_splits.clone()
    }
}

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
        other.splits.len().partial_cmp(&self.splits.len())
    }
}

impl<T: SplitMetaData + Clone> Ord for ActorSplitsAssignment<T> {
    fn cmp(&self, other: &Self) -> Ordering {
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
            enable_scale_in: true,
        }
    }
}

fn diff_splits<T>(
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

    if discovered_splits.is_empty() {
        tracing::warn!("no splits discovered");
    }

    let prev_split_ids: HashSet<_> = actor_splits
        .values()
        .flat_map(|splits| splits.iter().map(SplitMetaData::id))
        .collect();

    tracing::debug!("previous splits {:?}", prev_split_ids);
    tracing::debug!("discovered splits {:?}", discovered_splits.keys());

    let discovered_split_ids: HashSet<_> = discovered_splits.keys().cloned().collect();

    let dropped_splits: HashSet<_> = prev_split_ids
        .difference(&discovered_split_ids)
        .cloned()
        .collect();

    if !dropped_splits.is_empty() {
        if opts.enable_scale_in {
            tracing::debug!("dropping splits {:?}", dropped_splits);
        } else {
            tracing::warn!(
                "dropping splits {:?} happened, but it is not allowed",
                dropped_splits
            );
        }
    }

    let new_discovered_splits: HashSet<_> = discovered_split_ids
        .into_iter()
        .filter(|split_id| !prev_split_ids.contains(split_id))
        .collect();

    tracing::debug!("new created splits {:?}", new_discovered_splits);

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

    let mut heap = BinaryHeap::with_capacity(actor_splits.len());

    for (actor_id, mut splits) in actor_splits {
        if opts.enable_scale_in {
            splits.drain_filter(|split| dropped_splits.contains(&split.id()));
        }

        heap.push(ActorSplitsAssignment { actor_id, splits })
    }

    for split_id in new_discovered_splits {
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

impl<S> SourceManager<S>
where
    S: MetaStore,
{
    const SOURCE_TICK_INTERVAL: Duration = Duration::from_secs(10);

    pub async fn new(
        connector_rpc_endpoint: Option<String>,
        barrier_scheduler: BarrierScheduler<S>,
        catalog_manager: CatalogManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        metrics: Arc<MetaMetrics>,
    ) -> MetaResult<Self> {
        let mut managed_sources = HashMap::new();
        {
            let sources = catalog_manager.list_sources().await;

            for source in sources {
                Self::create_source_worker(
                    &connector_rpc_endpoint,
                    &source,
                    &mut managed_sources,
                    false,
                    metrics.clone(),
                )
                .await?
            }
        }

        let mut actor_splits = HashMap::new();
        let mut source_fragments = HashMap::new();
        for table_fragments in fragment_manager.list_table_fragments().await? {
            source_fragments.extend(table_fragments.stream_source_fragments());
            actor_splits.extend(table_fragments.actor_splits.clone());
        }

        let core = Mutex::new(SourceManagerCore::new(
            fragment_manager,
            managed_sources,
            source_fragments,
            actor_splits,
        ));

        Ok(Self {
            barrier_scheduler,
            core,
            paused: Mutex::new(()),
            connector_rpc_endpoint,
            metrics,
        })
    }

    pub async fn drop_source_change(&self, table_fragments_vec: &[TableFragments]) {
        let mut core = self.core.lock().await;

        // Extract the fragments that include source operators.
        let source_fragments = table_fragments_vec
            .iter()
            .flat_map(|table_fragments| table_fragments.stream_source_fragments())
            .collect::<HashMap<_, _>>();

        let fragments = table_fragments_vec
            .iter()
            .flat_map(|table_fragments| &table_fragments.fragments)
            .collect::<BTreeMap<_, _>>();

        let dropped_actors = source_fragments
            .values()
            .flatten()
            .flat_map(|fragment_id| fragments.get(fragment_id).unwrap().get_actors())
            .map(|actor| actor.get_actor_id())
            .collect::<HashSet<_>>();

        core.drop_source_change(source_fragments, &dropped_actors);
    }

    pub async fn apply_source_change(
        &self,
        source_fragments: Option<HashMap<SourceId, BTreeSet<FragmentId>>>,
        split_assignment: Option<SplitAssignment>,
        dropped_actors: Option<HashSet<ActorId>>,
    ) {
        let mut core = self.core.lock().await;
        core.apply_source_change(source_fragments, split_assignment, dropped_actors);
    }

    // After introducing the remove function for split, there may be a very occasional split removal
    // during scaling, in which case we need to use the old splits for reallocation instead of the
    // latest splits (which may be missing), so that we can resolve the split removal in the next
    // command.
    pub async fn reallocate_splits(
        &self,
        prev_actor_ids: &[ActorId],
        curr_actor_ids: &[ActorId],
    ) -> MetaResult<HashMap<ActorId, Vec<SplitImpl>>> {
        let core = self.core.lock().await;

        let prev_splits = prev_actor_ids
            .iter()
            .flat_map(|actor_id| core.actor_splits.get(actor_id).unwrap())
            .cloned()
            .collect_vec();

        let empty_actor_splits = curr_actor_ids
            .iter()
            .map(|actor_id| (*actor_id, vec![]))
            .collect();

        let prev_splits = prev_splits
            .into_iter()
            .map(|split| (split.id(), split))
            .collect();

        let diff = diff_splits(
            empty_actor_splits,
            &prev_splits,
            SplitDiffOptions::default(),
        )
        .unwrap_or_default();

        Ok(diff)
    }

    pub async fn pre_allocate_splits(&self, table_id: &TableId) -> MetaResult<SplitAssignment> {
        let core = self.core.lock().await;
        let table_fragments = core
            .fragment_manager
            .select_table_fragments_by_table_id(table_id)
            .await?;

        let source_fragments = table_fragments.stream_source_fragments();

        let mut assigned = HashMap::new();

        for (source_id, fragments) in source_fragments {
            let handle = core
                .managed_sources
                .get(&source_id)
                .ok_or_else(|| anyhow!("could not found source {}", source_id))?;

            if handle.splits.lock().await.splits.is_none() {
                // force refresh source
                let (tx, rx) = oneshot::channel();
                handle
                    .sync_call_tx
                    .send(tx)
                    .map_err(|e| anyhow!(e.to_string()))?;
                rx.await.map_err(|e| anyhow!(e.to_string()))??;
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

                if let Some(diff) =
                    diff_splits(empty_actor_splits, &splits, SplitDiffOptions::default())
                {
                    assigned.insert(fragment_id, diff);
                }
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
            Self::create_source_worker(
                &self.connector_rpc_endpoint,
                source,
                &mut core.managed_sources,
                true,
                self.metrics.clone(),
            )
            .await?;
        }
        Ok(())
    }

    async fn create_source_worker(
        connector_rpc_endpoint: &Option<String>,
        source: &Source,
        managed_sources: &mut HashMap<SourceId, ConnectorSourceWorkerHandle>,
        force_tick: bool,
        metrics: Arc<MetaMetrics>,
    ) -> MetaResult<()> {
        let mut worker = ConnectorSourceWorker::create(
            connector_rpc_endpoint,
            source,
            Duration::from_secs(10),
            metrics,
        )
        .await?;
        let current_splits_ref = worker.current_splits.clone();
        tracing::info!("spawning new watcher for source {}", source.id);

        // don't force tick in process of recovery. One source down should not lead to meta recovery
        // failure.
        if force_tick {
            // if fail to fetch meta info, will refuse to create source
            worker.tick().await?;
        }

        let (sync_call_tx, sync_call_rx) = tokio::sync::mpsc::unbounded_channel();

        let handle = tokio::spawn(async move { worker.run(sync_call_rx).await });

        managed_sources.insert(
            source.id,
            ConnectorSourceWorkerHandle {
                handle,
                sync_call_tx,
                splits: current_splits_ref,
            },
        );

        Ok(())
    }

    /// unregister connector worker for source.
    pub async fn unregister_sources(&self, source_ids: Vec<SourceId>) {
        let mut core = self.core.lock().await;
        for source_id in source_ids {
            if let Some(handle) = core.managed_sources.remove(&source_id) {
                handle.handle.abort();
            }
        }
    }

    pub async fn list_assignments(&self) -> HashMap<ActorId, Vec<SplitImpl>> {
        let core = self.core.lock().await;
        core.actor_splits.clone()
    }

    async fn tick(&self) -> MetaResult<()> {
        let diff = {
            let core_guard = self.core.lock().await;
            core_guard.diff().await?
        };

        if !diff.is_empty() {
            let command = Command::SourceSplitAssignment(diff);
            tracing::debug!("pushing down command {:#?}", command);
            self.barrier_scheduler.run_command(command).await?;
        }

        Ok(())
    }

    pub async fn run(&self) -> MetaResult<()> {
        let mut ticker = time::interval(Self::SOURCE_TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let _pause_guard = self.paused.lock().await;
            if let Err(e) = self.tick().await {
                tracing::error!(
                    "error happened while running source manager tick: {}",
                    e.to_string()
                );
            }
        }
    }

    pub async fn get_actor_splits(&self) -> HashMap<ActorId, Vec<SplitImpl>> {
        self.core.lock().await.get_actor_splits()
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

    use anyhow::anyhow;
    use risingwave_common::types::JsonbVal;
    use risingwave_connector::source::{SplitId, SplitMetaData};
    use serde::{Deserialize, Serialize};

    use crate::model::ActorId;
    use crate::stream::source_manager::{diff_splits, SplitDiffOptions};

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

        fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
            serde_json::from_value(value.take()).map_err(|e| anyhow!(e))
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

        let diff = diff_splits(actor_splits, &discovered_splits, opts).unwrap();
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

        let diff = diff_splits(actor_splits, &discovered_splits, opts).unwrap();

        assert!(!diff.is_empty())
    }

    #[test]
    fn test_diff_splits() {
        let actor_splits = HashMap::new();
        let discovered_splits: BTreeMap<SplitId, TestSplit> = BTreeMap::new();
        assert!(diff_splits(actor_splits, &discovered_splits, Default::default()).is_none());

        let actor_splits = (0..3).map(|i| (i, vec![])).collect();
        let discovered_splits: BTreeMap<SplitId, TestSplit> = BTreeMap::new();
        let diff = diff_splits(actor_splits, &discovered_splits, Default::default()).unwrap();
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

        let diff = diff_splits(actor_splits, &discovered_splits, Default::default()).unwrap();
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

        let diff = diff_splits(actor_splits, &discovered_splits, Default::default()).unwrap();
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

        let diff = diff_splits(actor_splits, &discovered_splits, Default::default()).unwrap();
        assert_eq!(diff.len(), 5);
        for splits in diff.values() {
            assert_eq!(splits.len(), 1);
        }

        check_all_splits(&discovered_splits, &diff);
    }
}
