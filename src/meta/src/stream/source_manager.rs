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

use std::borrow::{BorrowMut};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use risingwave_common::error::internal_error;

use futures::future::try_join_all;

use itertools::Itertools;

use tokio::sync::{Mutex, oneshot};
use tokio::time;

use risingwave_common::error::ErrorCode::{ConnectorError, InternalError};
use risingwave_common::error::{Result, RwError, ToRwResult};
use risingwave_common::try_match_expand;
use risingwave_connector::{ConnectorProperties, SplitEnumeratorImpl, SplitImpl};
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::{Source, StreamSourceInfo};
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::WorkerType;
use risingwave_pb::stream_service::{
    CreateSourceRequest as ComputeNodeCreateSourceRequest,
    DropSourceRequest as ComputeNodeDropSourceRequest,
};
use risingwave_rpc_client::StreamClient;

use crate::barrier::BarrierManagerRef;
use crate::cluster::ClusterManagerRef;
use crate::manager::{CatalogManagerRef, MetaSrvEnv, SourceId};
use crate::model::ActorId;
use crate::storage::MetaStore;

pub type SourceManagerRef<S> = Arc<SourceManager<S>>;

pub struct SourceManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    cluster_manager: ClusterManagerRef<S>,
    catalog_manager: CatalogManagerRef<S>,
    core: Arc<Mutex<SourceManagerCore>>,
}

pub struct ConnectorSourceWorker {
    source_id: SourceId,
    current_splits: Arc<Mutex<Option<Vec<SplitImpl>>>>,
    enumerator: SplitEnumeratorImpl,
    stop_tx: oneshot::Sender<()>,
}

impl ConnectorSourceWorker {
    pub async fn create(source_id: SourceId, info: &StreamSourceInfo, stop_tx: oneshot::Sender<()>) -> Result<Self> {
        let properties = ConnectorProperties::extract(info.properties.to_owned()).to_rw_result()?;
        let enumerator = SplitEnumeratorImpl::create(properties).await.to_rw_result()?;
        let current_splits = Arc::new(Mutex::new(None));
        Ok(Self { source_id, current_splits, enumerator, stop_tx })
    }

    pub async fn run(&mut self, mut stop_rx: oneshot::Receiver<()>) {
        let mut interval = time::interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                biased;
                _ = stop_rx.borrow_mut() => {
                    log::debug!("source worker {} stop signal received", self.source_id);
                    break;
                }

                _ = interval.tick() => {
                    let splits = match self.enumerator.list_splits().await {
                        Ok(s) => s,
                        Err(e) => {
                            log::error!("error happened when fetching split from source {}, {}", self.source_id, e.to_string());
                            continue;
                        }
                    };

                    {
                        let mut currend_splits_guard = self.current_splits.lock().await;
                        currend_splits_guard.replace(splits);
                    }

                }
            }
        }
    }
}

pub struct SourceManagerCore {
    pub managed_sources: HashMap<SourceId, Arc<Mutex<Option<Vec<SplitImpl>>>>>,
    pub actors: HashMap<SourceId, Vec<HashMap<ActorId, Vec<SplitImpl>>>>,
}

impl SourceManagerCore
{
    fn new() -> Self {
        Self {
            managed_sources: HashMap::new(),
            actors: Default::default(),
        }
    }

    async fn tick(&mut self) -> Result<()> {
        for (source_id, splits) in &self.managed_sources {
            let splits_guard = splits.lock().await;
            if splits_guard.is_none() {
                continue;
            }

            if let Some(splits) = splits_guard.as_ref() {
                let actor_group = match self.actors.get_mut(source_id) {
                    None => {
                        log::warn!("no actors bind to source {}", source_id);
                        continue;
                    }
                    Some(actor_group) => actor_group,
                };

                for actor_group_assigned_splits in actor_group {
                    let mut assigned_splits = HashSet::new();
                    let mut id_to_splits = HashMap::new();

                    for actor_splits in actor_group_assigned_splits.values() {
                        for split in actor_splits {
                            let id = split.id();
                            assigned_splits.insert(id.clone());
                            id_to_splits.insert(id, split);
                        }
                    }

                    let new_splits = splits.iter().filter(|split| !assigned_splits.contains(split.id().as_str())).cloned().collect_vec();

                    // todo: use heap
                    let actors = actor_group_assigned_splits.keys().collect_vec();
                    let actor_len = actors.len();

                    let mut assigned = HashMap::new();

                    for (i, split) in new_splits.into_iter().enumerate() {
                        assigned.entry(actors[i % actor_len].clone()).or_insert(vec![]).push(split)
                    }

                    // do something

                    for (k, mut vs) in assigned {
                        actor_group_assigned_splits.entry(k).or_insert(vec![]).append(&mut vs)
                    }
                }
            }
        }

        Ok(())
    }
}


impl<S> SourceManager<S>
    where
        S: MetaStore,
{
    pub async fn new(
        env: MetaSrvEnv<S>,
        cluster_manager: ClusterManagerRef<S>,
        _barrier_manager: BarrierManagerRef<S>,
        catalog_manager: CatalogManagerRef<S>,
    ) -> Result<Self> {
        let core = Arc::new(Mutex::new(SourceManagerCore::new()));

        Ok(Self {
            env,
            cluster_manager,
            catalog_manager,
            core,
        })
    }

    pub async fn register_source(&self, actors: HashMap<SourceId, Vec<Vec<ActorId>>>, affiliated_source: Option<Source>) -> Result<()> {
        let mut core = self.core.lock().await;

        let source_ref = &affiliated_source;

        for (source_id, actor_groups) in actors {
            if let std::collections::hash_map::Entry::Vacant(e) = core.managed_sources.entry(source_id) {
                let source = if let Some(affiliated_source) = source_ref && affiliated_source.get_id() == source_id {
                    affiliated_source.clone()
                } else {
                    let catalog_guard = self.catalog_manager.get_catalog_core_guard().await;
                    catalog_guard.get_source(source_id).await?.ok_or_else(|| {
                        internal_error(
                            format!("could not find source catalog for {}", source_id)
                        )
                    })?
                };

                let info = source.info.unwrap();
                let stream_source_info = try_match_expand!(info, Info::StreamSource)?;

                let (stop_tx, stop_rx) = oneshot::channel();
                let mut worker = ConnectorSourceWorker::create(source_id, &stream_source_info, stop_tx).await?;
                e.insert(worker.current_splits.clone());
                let _ = tokio::spawn(async move { worker.run(stop_rx).await });
            }

            let mut map = actor_groups
                .into_iter()
                .map(|actors|
                    actors
                        .into_iter()
                        .map(|actor| (actor, vec![])).collect() )
                .collect();
            core.actors.entry(source_id).or_insert(vec![]).append(&mut map);
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
        let _responses: Vec<_> = try_join_all(futures).await?;

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

        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        // todo: in the future, split change will be pushed as a long running service
        let mut interval = time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;

            let mut core_guard = self.core.lock().await;
            let _ = core_guard.tick().await;
        }
    }
}
