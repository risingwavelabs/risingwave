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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::error::ErrorCode::{ConnectorError, InternalError};
use risingwave_common::error::{internal_error, Result, RwError, ToRwResult};
use risingwave_common::{ensure, try_match_expand};
use risingwave_connector::{ConnectorProperties, SplitEnumeratorImpl, SplitImpl};
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::{Source};
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::WorkerType;
use risingwave_pb::stream_service::{
    CreateSourceRequest as ComputeNodeCreateSourceRequest,
    DropSourceRequest as ComputeNodeDropSourceRequest,
};
use risingwave_rpc_client::StreamClient;
use tokio::sync::{Mutex};
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::MissedTickBehavior;

use crate::barrier::BarrierManagerRef;
use crate::cluster::ClusterManagerRef;
use crate::manager::{CatalogManagerRef, MetaSrvEnv, SourceId};
use crate::model::{ActorId, FragmentId};
use crate::storage::MetaStore;
use crate::stream::FragmentManagerRef;

pub type SourceManagerRef<S> = Arc<SourceManager<S>>;

pub struct SourceManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    cluster_manager: ClusterManagerRef<S>,
    catalog_manager: CatalogManagerRef<S>,
    barrier_manager: BarrierManagerRef<S>,
    core: Arc<Mutex<SourceManagerCore<S>>>,
}

pub struct SharedSplitMap {
    splits: Option<BTreeMap<String, SplitImpl>>,
}

type SharedSplitMapRef = Arc<Mutex<SharedSplitMap>>;

pub struct ConnectorSourceWorker {
    source_id: SourceId,
    current_splits: SharedSplitMapRef,
    enumerator: SplitEnumeratorImpl,
    period: Duration,
}

impl ConnectorSourceWorker {
    pub async fn create(source: &Source, period: Duration) -> Result<Self> {
        let source_id = source.get_id();
        let info = source
            .info
            .clone()
            .ok_or_else(|| internal_error("source info is empty"))?;
        let stream_source_info = try_match_expand!(info, Info::StreamSource)?;
        let properties =
            ConnectorProperties::extract(stream_source_info.properties).to_rw_result()?;
        let enumerator = SplitEnumeratorImpl::create(properties)
            .await
            .to_rw_result()?;
        let current_splits = Arc::new(Mutex::new(SharedSplitMap { splits: None }));
        Ok(Self {
            source_id,
            current_splits,
            enumerator,
            period,
        })
    }

    pub async fn run(&mut self) {
        let mut interval = time::interval(self.period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;

            let splits = match self.enumerator.list_splits().await {
                Ok(s) => s,
                Err(e) => {
                    log::error!(
                        "error happened when fetching split from source {}, {}",
                        self.source_id,
                        e.to_string()
                    );
                    continue;
                }
            };

            let mut current_splits = self.current_splits.lock().await;
            current_splits.splits.replace(
                splits
                    .into_iter()
                    .map(|split| (split.id(), split))
                    .collect(),
            );
        }
    }
}

pub struct SourceManagerCore<S: MetaStore> {
    pub fragment_manager: FragmentManagerRef<S>,

    // may fetch from catalog
    pub managed_sources: HashMap<SourceId, (SharedSplitMapRef, JoinHandle<()>)>,

    // may fetch from catalog
    pub source_fragments: HashMap<SourceId, Vec<FragmentId>>,

    // should store
    pub actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
}

impl<S> SourceManagerCore<S>
    where
        S: MetaStore,
{
    fn new(fragment_manager: FragmentManagerRef<S>) -> Self {
        Self {
            fragment_manager,
            managed_sources: HashMap::new(),
            source_fragments: HashMap::new(),
            actor_splits: HashMap::new(),
        }
    }

    // async fn register_source(&self, source: &Source) -> Result<()> {}

    // async fn diff(&mut self) -> Result<HashMap<ActorId, Vec<SplitImpl>>> {
    //     // first, list all fragment, so that we can get `FragmentId` -> `Vec<ActorId>` map
    //     let table_frags = self.fragment_manager.list_table_fragments().await?;
    //     let mut frag_actors: HashMap<FragmentId, Vec<ActorId>> = HashMap::new();
    //     for table_frag in table_frags {
    //         for (frag_id, mut frag) in table_frag.fragments {
    //             let mut actors = frag.actors.iter_mut().map(|x| x.actor_id).collect_vec();
    //             frag_actors
    //                 .entry(frag_id)
    //                 .or_insert(vec![])
    //                 .append(&mut actors);
    //         }
    //     }
    //
    //     // then we diff the splits
    //     let mut changed_actors: HashMap<ActorId, Vec<SplitImpl>> = HashMap::new();
    //
    //     for (source_id, (splits, _)) in &self.managed_sources {
    //         let frag_ids = match self.source_fragments.get(source_id) {
    //             Some(frags) if !frags.is_empty() => frags,
    //             _ => {
    //                 // TODO, no fragment is bound with such source_id, should drop that source
    //                 continue;
    //             }
    //         };
    //
    //         let splits_guard = splits.lock().await;
    //         let discovered_splits = match &*splits_guard {
    //             None => continue,
    //             Some(splits) => splits,
    //         };
    //
    //         for frag_id in frag_ids {
    //             let actor_ids = match frag_actors.remove(frag_id) {
    //                 None => {
    //                     // target fragment has gone?
    //                     continue;
    //                 }
    //                 Some(actors) => actors,
    //             };
    //
    //             let mut prev_splits = HashMap::new();
    //             for actor_id in actor_ids {
    //                 prev_splits.insert(
    //                     actor_id,
    //                     self.actor_splits
    //                         .get(&actor_id)
    //                         .cloned()
    //                         .unwrap_or_default(),
    //                 );
    //             }
    //
    //             let diff = Self::diff_splits(prev_splits, discovered_splits)?;
    //             if let Some(change) = diff {
    //                 for (actor_id, splits) in change {
    //                     changed_actors.insert(actor_id, splits);
    //                 }
    //             }
    //         }
    //     }
    //
    //     Ok(changed_actors)
    // }

    fn diff_splits(
        mut prev_actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
        discovered_splits: &BTreeMap<String, SplitImpl>,
    ) -> Result<Option<HashMap<ActorId, Vec<SplitImpl>>>> {
        let prev_split_ids: HashSet<_> = prev_actor_splits
            .values()
            .flat_map(|splits| splits.iter().map(SplitImpl::id))
            .collect();

        if discovered_splits
            .keys()
            .all(|split_id| prev_split_ids.contains(split_id))
        {
            return Ok(None);
        }

        let mut new_discovered_splits = HashSet::new();
        for (split_id, split) in discovered_splits {
            if !prev_split_ids.contains(split_id) {
                new_discovered_splits.insert(split.id());
            }
        }

        let mut result = HashMap::new();

        let actors = prev_actor_splits.keys().cloned().collect_vec();
        let actor_len = actors.len();

        for (index, split_id) in new_discovered_splits.into_iter().enumerate() {
            let target_actor_id = actors[index % actor_len];
            let split = discovered_splits.get(&split_id).unwrap().clone();

            result
                .entry(target_actor_id)
                .or_insert_with(|| prev_actor_splits.remove(&target_actor_id).unwrap());

            result.get_mut(&target_actor_id).unwrap().push(split);
        }

        Ok(Some(result))
    }
}

impl<S> SourceManager<S>
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
        let core = Arc::new(Mutex::new(SourceManagerCore::new(fragment_manager)));

        Ok(Self {
            env,
            cluster_manager,
            catalog_manager,
            barrier_manager,
            core,
        })
    }

    pub async fn register_source(
        &self,
        source_fragments: HashMap<SourceId, Vec<FragmentId>>,
        affiliated_source: Option<Source>,
    ) -> Result<()> {
        let mut core = self.core.lock().await;

        let source_ref = &affiliated_source;

        if let Some(source) = source_ref {
            ensure!(
                source_fragments.len() == 1,
                "only one source should exist when creating materialized source"
            );
            let mut source_entry = source_fragments.into_iter().next().unwrap();
            ensure!(source_entry.0 == source.id);
            core.source_fragments
                .entry(source.id)
                .or_insert(vec![])
                .append(&mut source_entry.1);
        } else {
            for (source_id, mut fragments) in source_fragments {
                core.source_fragments
                    .entry(source_id)
                    .or_insert(vec![])
                    .append(&mut fragments);
            }
        }

        Ok(())
    }

    async fn fetch_splits_for_source(&self, source: &Source) -> Result<Vec<SplitImpl>> {
        let info = match source.get_info()? {
            Info::StreamSource(s) => s,
            _ => {
                return Err(RwError::from(InternalError(
                    "for now we only support StreamSource in source manager".to_string(),
                )));
            }
        };

        let properties = ConnectorProperties::extract(info.properties.clone())
            .map_err(|e| RwError::from(ConnectorError(e.to_string())))?;

        SplitEnumeratorImpl::create(properties)
            .await
            .to_rw_result()?
            .list_splits()
            .await
            .to_rw_result()
    }

    /// Perform one-time split scheduling, using the round-robin method to assign splits to the
    /// source actor under the same fragment Note that the same Materialized View will have
    /// multiple identical Sources to join, so there will be multiple groups of Actors under same
    /// `SourceId`
    pub async fn schedule_split_for_actors(
        &self,
        actors: HashMap<SourceId, Vec<Vec<ActorId>>>,
        affiliated_source: Option<Source>,
    ) -> Result<HashMap<ActorId, Vec<SplitImpl>>> {
        let source_ref = &affiliated_source;
        let source_splits = try_join_all(actors.keys().map(|source_id| async move {
            if let Some(affiliated_source) = source_ref && *source_id == affiliated_source.get_id() {
                // we are creating materialized source
                self.fetch_splits_for_source(affiliated_source).await
            } else {
                let catalog_guard = self.catalog_manager.get_catalog_core_guard().await;
                let source = catalog_guard.get_source(*source_id).await?.ok_or_else(|| {
                    RwError::from(InternalError(format!(
                        "could not find source catalog for {}",
                        source_id
                    )))
                })?;
                self.fetch_splits_for_source(&source).await
            }
        }))
            .await?;
        let mut result = HashMap::new();

        for (splits, fragments) in source_splits.into_iter().zip_eq(actors.into_values()) {
            log::debug!("found {} splits", splits.len());
            for actors in fragments {
                let actor_count = actors.len();
                let mut chunks = vec![vec![]; actor_count];
                for (i, split) in splits.iter().enumerate() {
                    chunks[i % actor_count].push(split.clone());
                }

                actors
                    .into_iter()
                    .zip_eq(chunks)
                    .into_iter()
                    .for_each(|(actor_id, splits)| {
                        result.insert(actor_id, splits.to_vec());
                    })
            }
        }

        Ok(result)
    }

    async fn all_stream_clients(&self) -> Result<impl Iterator<Item=StreamClient>> {
        // FIXME: there is gap between the compute node activate itself and source ddl operation,
        // create/drop source(non-stateful source like TableSource) before the compute node
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

    /// Broadcast the create source request to all compute nodes.
    pub async fn create_source(&self, source: &Source) -> Result<()> {
        let futures = self
            .all_stream_clients()
            .await?
            .into_iter()
            .map(|mut client| {
                let request = ComputeNodeCreateSourceRequest {
                    source: Some(source.clone()),
                };
                async move { client.create_source(request).await.to_rw_result() }
            });

        // ignore response body, always none
        let _ = try_join_all(futures).await?;

        let mut core = self.core.lock().await;
        if core.managed_sources.contains_key(&source.get_id()) {
            log::warn!("source {} already registered", source.get_id());
            return Ok(());
        }

        let mut worker = ConnectorSourceWorker::create(source, Duration::from_secs(10)).await?;
        let current_splits_ref = worker.current_splits.clone();
        log::info!("Spawning new watcher for source {}", source.id);
        let handle = tokio::spawn(async move { worker.run().await });
        core.managed_sources
            .insert(source.id, (current_splits_ref, handle));

        Ok(())
    }

    pub async fn drop_source(&self, source_id: SourceId) -> Result<()> {
        let futures = self
            .all_stream_clients()
            .await?
            .into_iter()
            .map(|mut client| {
                let request = ComputeNodeDropSourceRequest { source_id };
                async move { client.drop_source(request).await.to_rw_result() }
            });
        let _responses: Vec<_> = try_join_all(futures).await?;

        let mut core = self.core.lock().await;
        if let Some((_, handle)) = core.managed_sources.remove(&source_id) {
            handle.abort()
        }

        Ok(())
    }

    async fn tick(&self) -> Result<()> {
        let mut core_guard = self.core.lock().await;

        for (id, x) in core_guard.managed_sources.iter() {
            let catalog_guard = self.catalog_manager.get_catalog_core_guard().await;
            let source = catalog_guard.get_source(*id).await?.ok_or_else(|| {
                RwError::from(InternalError(format!(
                    "could not find source catalog for {}",
                    id
                )))
            })?;


            let guard = x.0.lock().await;
            println!("source: {}", source.name);

            if let Some(splits) = &guard.splits {
                for (k, v) in splits {
                    println!("\tid: {}, split {:?}", k, v);
                }
            }
        }


        // let x = {
        //     let mut core_guard = self.core.lock().await;
        //     core_guard.diff().await
        // };
        //
        // println!("{:?}", x);
        //


        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        // todo: uncomment me when stable
        let mut ticker = time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            self.tick().await?;
        }
        Ok(())
    }
}
