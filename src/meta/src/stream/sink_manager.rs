// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::BorrowMut;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::error::{internal_error, Result, ToRwResult};
use risingwave_common::try_match_expand;
use risingwave_connector::{ConnectorProperties, SplitEnumeratorImpl, SplitImpl};
use risingwave_pb::catalog::sink::Info;
use risingwave_pb::catalog::Sink;
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::WorkerType;
use risingwave_pb::stream_service::{
    CreateSinkRequest as ComputeNodeCreateSinkRequest,
    DropSinkRequest as ComputeNodeDropSinkRequest,
};
use risingwave_rpc_client::StreamClient;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio::{select, time};

use crate::barrier::BarrierManagerRef;
use crate::cluster::ClusterManagerRef;
use crate::manager::{CatalogManagerRef, MetaSrvEnv, SinkId};
use crate::model::{ActorId, FragmentId};
use crate::storage::MetaStore;
use crate::stream::FragmentManagerRef;

pub type SinkManagerRef<S> = Arc<SinkManager<S>>;

#[allow(dead_code)]
pub struct SinkManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    cluster_manager: ClusterManagerRef<S>,
    catalog_manager: CatalogManagerRef<S>,
    barrier_manager: BarrierManagerRef<S>,
    core: Arc<Mutex<SinkManagerCore<S>>>,
}

pub struct SharedSplitMap {
    splits: Option<BTreeMap<String, SplitImpl>>,
}

type SharedSplitMapRef = Arc<Mutex<SharedSplitMap>>;

#[allow(dead_code)]
pub struct ConnectorSinkWorker {
    sink_id: SinkId,
    current_splits: SharedSplitMapRef,
    enumerator: SplitEnumeratorImpl,
    period: Duration,
}

impl ConnectorSinkWorker {
    pub async fn create(sink: &Sink, period: Duration) -> Result<Self> {
        let sink_id = sink.get_id();
        let info = sink
            .info
            .clone()
            .ok_or_else(|| internal_error("sink info is empty"))?;
        let stream_sink_info = try_match_expand!(info, Info::StreamSink)?;
        let properties =
            ConnectorProperties::extract(stream_sink_info.properties).to_rw_result()?;
        let enumerator = SplitEnumeratorImpl::create(properties)
            .await
            .to_rw_result()?;
        let current_splits = Arc::new(Mutex::new(SharedSplitMap { splits: None }));
        Ok(Self {
            sink_id,
            current_splits,
            enumerator,
            period,
        })
    }

    pub async fn run(&mut self, mut sync_call_rx: UnboundedReceiver<oneshot::Sender<Result<()>>>) {
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
                    if let Err(e) = self.tick().await {
                        log::error!("error happened when tick from connector sink worker: {}", e.to_string());
                    }
                }
            }
        }
    }

    async fn tick(&mut self) -> Result<()> {
        let splits = self.enumerator.list_splits().await.to_rw_result()?;
        let mut current_splits = self.current_splits.lock().await;
        current_splits.splits.replace(
            splits
                .into_iter()
                .map(|split| (split.id(), split))
                .collect(),
        );

        Ok(())
    }
}

pub struct ConnectorSinkWorkerHandle {
    handle: JoinHandle<()>,
    sync_call_tx: UnboundedSender<oneshot::Sender<Result<()>>>,
    splits: SharedSplitMapRef,
}

pub struct SinkManagerCore<S: MetaStore> {
    pub fragment_manager: FragmentManagerRef<S>,
    pub managed_sinks: HashMap<SinkId, ConnectorSinkWorkerHandle>,
    pub sink_fragments: HashMap<SinkId, Vec<FragmentId>>,
    pub actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
}

impl<S> SinkManagerCore<S>
where
    S: MetaStore,
{
    fn new(fragment_manager: FragmentManagerRef<S>) -> Self {
        Self {
            fragment_manager,
            managed_sinks: HashMap::new(),
            sink_fragments: HashMap::new(),
            actor_splits: HashMap::new(),
        }
    }

    #[allow(dead_code)]
    async fn diff(&mut self) -> Result<HashMap<ActorId, Vec<SplitImpl>>> {
        // first, list all fragment, so that we can get `FragmentId` -> `Vec<ActorId>` map
        let table_frags = self.fragment_manager.list_table_fragments().await?;
        let mut frag_actors: HashMap<FragmentId, Vec<ActorId>> = HashMap::new();
        for table_frag in table_frags {
            for (frag_id, mut frag) in table_frag.fragments {
                let mut actors = frag.actors.iter_mut().map(|x| x.actor_id).collect_vec();
                frag_actors
                    .entry(frag_id)
                    .or_insert(vec![])
                    .append(&mut actors);
            }
        }

        // then we diff the splits
        let mut changed_actors: HashMap<ActorId, Vec<SplitImpl>> = HashMap::new();

        for (sink_id, ConnectorSinkWorkerHandle { splits, .. }) in &self.managed_sinks {
            let frag_ids = match self.sink_fragments.get(sink_id) {
                Some(fragment_ids) if !fragment_ids.is_empty() => fragment_ids,
                _ => {
                    continue;
                }
            };

            let discovered_splits = {
                let splits_guard = splits.lock().await;
                match splits_guard.splits.clone() {
                    None => continue,
                    Some(splits) => splits,
                }
            };

            for frag_id in frag_ids {
                let actor_ids = match frag_actors.remove(frag_id) {
                    None => {
                        // target fragment has gone?
                        continue;
                    }
                    Some(actors) => actors,
                };

                let mut prev_splits = HashMap::new();
                for actor_id in actor_ids {
                    prev_splits.insert(
                        actor_id,
                        self.actor_splits
                            .get(&actor_id)
                            .cloned()
                            .unwrap_or_default(),
                    );
                }

                let diff = diff_splits(prev_splits, &discovered_splits);
                if let Some(change) = diff {
                    for (actor_id, splits) in change {
                        changed_actors.insert(actor_id, splits);
                    }
                }
            }
        }

        Ok(changed_actors)
    }

    pub async fn patch_diff(
        &mut self,
        sink_fragments: Option<HashMap<SinkId, Vec<FragmentId>>>,
        actor_splits: Option<HashMap<ActorId, Vec<SplitImpl>>>,
    ) -> Result<()> {
        if let Some(sink_fragments) = sink_fragments {
            for (sink_id, mut fragment_ids) in sink_fragments {
                self.sink_fragments
                    .entry(sink_id)
                    .or_insert(vec![])
                    .append(&mut fragment_ids);
            }
        }

        if let Some(actor_splits) = actor_splits {
            for (actor_id, splits) in actor_splits {
                self.actor_splits.insert(actor_id, splits);
                // TODO store state
            }
        }

        Ok(())
    }
}

fn diff_splits(
    mut prev_actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
    discovered_splits: &BTreeMap<String, SplitImpl>,
) -> Option<HashMap<ActorId, Vec<SplitImpl>>> {
    let prev_split_ids: HashSet<_> = prev_actor_splits
        .values()
        .flat_map(|splits| splits.iter().map(SplitImpl::id))
        .collect();

    if discovered_splits
        .keys()
        .all(|split_id| prev_split_ids.contains(split_id))
    {
        return None;
    }

    let mut new_discovered_splits = HashSet::new();
    for (split_id, split) in discovered_splits {
        if !prev_split_ids.contains(split_id) {
            new_discovered_splits.insert(split.id());
        }
    }

    let mut result = HashMap::new();

    let mut actors = prev_actor_splits.keys().cloned().collect_vec();

    // sort actors
    actors.sort();

    let actor_len = actors.len();

    for (index, split_id) in new_discovered_splits.into_iter().enumerate() {
        let target_actor_id = actors[index % actor_len];
        let split = discovered_splits.get(&split_id).unwrap().clone();

        result
            .entry(target_actor_id)
            .or_insert_with(|| prev_actor_splits.remove(&target_actor_id).unwrap());

        result.get_mut(&target_actor_id).unwrap().push(split);
    }

    Some(result)
}

impl<S> SinkManager<S>
where
    S: MetaStore,
{
    pub async fn new(
        env: MetaSrvEnv<S>,
        cluster_manager: ClusterManagerRef<S>,
        barrier_manager: BarrierManagerRef<S>,
        catalog_manager: CatalogManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
    ) -> Result<Self> {
        let core = Arc::new(Mutex::new(SinkManagerCore::new(fragment_manager)));

        Ok(Self {
            env,
            cluster_manager,
            catalog_manager,
            barrier_manager,
            core,
        })
    }

    pub async fn patch_update(
        &self,
        sink_fragments: Option<HashMap<SinkId, Vec<FragmentId>>>,
        actor_splits: Option<HashMap<ActorId, Vec<SplitImpl>>>,
    ) {
        let mut core = self.core.lock().await;
        let _ = core.patch_diff(sink_fragments, actor_splits).await;
    }

    pub async fn pre_allocate_splits(
        &self,
        table_id: &TableId,
        sink_fragments: HashMap<SinkId, Vec<FragmentId>>,
    ) -> Result<HashMap<ActorId, Vec<SplitImpl>>> {
        let core = self.core.lock().await;
        let table_fragments = core
            .fragment_manager
            .select_table_fragments_by_table_id(table_id)
            .await?;

        let mut assigned = HashMap::new();

        for (sink_id, fragments) in sink_fragments {
            let handle = core
                .managed_sinks
                .get(&sink_id)
                .ok_or_else(|| internal_error(format!("could not found sink {}", sink_id)))?;

            if handle.splits.lock().await.splits.is_none() {
                // force refresh sink
                let (tx, rx) = oneshot::channel();
                handle.sync_call_tx.send(tx).to_rw_result()?;
                rx.await.map_err(|e| internal_error(e.to_string()))??;
            }

            if let Some(splits) = &handle.splits.lock().await.splits {
                for fragment_id in fragments {
                    let empty_actor_splits = table_fragments
                        .fragments
                        .get(&fragment_id)
                        .ok_or_else(|| internal_error(format!("could not found sink {}", sink_id)))?
                        .actors
                        .iter()
                        .map(|actor| (actor.actor_id, vec![]))
                        .collect();

                    assigned.extend(diff_splits(empty_actor_splits, splits).unwrap());
                }
            } else {
                unreachable!();
            }
        }

        Ok(assigned)
    }

    async fn all_stream_clients(&self) -> Result<impl Iterator<Item = StreamClient>> {
        // FIXME: there is gap between the compute node activate itself and sink ddl operation,
        // create/drop sink(non-stateful sink like TableSink) before the compute node
        // activate itself will cause an inconsistent state. This situation will happen when some
        // compute node scale in.
        let all_compute_nodes = self
            .cluster_manager
            .list_worker_node(WorkerType::ComputeNode, Some(Running))
            .await;

        let all_stream_clients = try_join_all(
            all_compute_nodes
                .iter()
                .map(|worker| self.env.stream_client_pool().get(worker)),
        )
        .await?
        .into_iter();

        Ok(all_stream_clients)
    }

    /// Broadcast the create sink request to all compute nodes.
    pub async fn create_sink(&self, sink: &Sink) -> Result<()> {
        let futures = self
            .all_stream_clients()
            .await?
            .into_iter()
            .map(|mut client| {
                let request = ComputeNodeCreateSinkRequest {
                    sink: Some(sink.clone()),
                };
                async move { client.create_sink(request).await.to_rw_result() }
            });

        // ignore response body, always none
        let _ = try_join_all(futures).await?;

        let mut core = self.core.lock().await;
        if core.managed_sinks.contains_key(&sink.get_id()) {
            log::warn!("sink {} already registered", sink.get_id());
            return Ok(());
        }

        if let Some(Info::StreamSink(_)) = sink.info {
            let mut worker = ConnectorSinkWorker::create(sink, Duration::from_secs(10)).await?;
            let current_splits_ref = worker.current_splits.clone();
            log::info!("Spawning new watcher for sink {}", sink.id);

            let (sync_call_tx, sync_call_rx) = tokio::sync::mpsc::unbounded_channel();

            let handle = tokio::spawn(async move { worker.run(sync_call_rx).await });
            core.managed_sinks.insert(
                sink.id,
                ConnectorSinkWorkerHandle {
                    handle,
                    sync_call_tx,
                    splits: current_splits_ref,
                },
            );
        }

        Ok(())
    }

    pub async fn drop_sink(&self, sink_id: SinkId) -> Result<()> {
        let futures = self
            .all_stream_clients()
            .await?
            .into_iter()
            .map(|mut client| {
                let request = ComputeNodeDropSinkRequest { sink_id };
                async move { client.drop_sink(request).await.to_rw_result() }
            });
        let _responses: Vec<_> = try_join_all(futures).await?;

        let mut core = self.core.lock().await;
        if let Some(handle) = core.managed_sinks.remove(&sink_id) {
            handle.handle.abort();
        }

        Ok(())
    }

    async fn tick(&self) -> Result<()> {
        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        let mut ticker = time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            self.tick().await?;
        }
    }
}
