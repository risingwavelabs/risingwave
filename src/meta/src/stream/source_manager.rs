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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError, ToRwResult};
use risingwave_connector::{extract_split_enumerator, SplitImpl};
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::{Source, StreamSourceInfo};
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::WorkerType;
use risingwave_pb::data::barrier::Mutation;
use risingwave_pb::data::{SplitAssignMutation, Splits};
use risingwave_pb::stream_service::{
    CreateSourceRequest as ComputeNodeCreateSourceRequest,
    DropSourceRequest as ComputeNodeDropSourceRequest,
};
use tokio::sync::{oneshot, Mutex};
use tokio::time;

use crate::barrier::{BarrierManagerRef, Command};
use crate::cluster::ClusterManagerRef;
use crate::manager::{CatalogManagerRef, MetaSrvEnv, SourceId, StreamClient};
use crate::model::ActorId;
use crate::storage::MetaStore;

pub type SourceManagerRef<S> = Arc<SourceManager<S>>;

#[allow(dead_code)]
pub struct SourceManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    cluster_manager: ClusterManagerRef<S>,
    catalog_manager: CatalogManagerRef<S>,
    core: Arc<Mutex<SourceManagerCore<S>>>,
}

pub struct SourceDiscovery {
    current_splits: Arc<Mutex<Option<Vec<SplitImpl>>>>,
    last_assigned_splits: Vec<HashMap<ActorId, Vec<SplitImpl>>>,
    stop_tx: oneshot::Sender<()>,
}

impl SourceDiscovery {
    pub async fn create(source_id: SourceId, info: &StreamSourceInfo) -> Result<Self> {
        let mut enumerator = extract_split_enumerator(&info.properties).to_rw_result()?;
        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
        let splits = Arc::new(Mutex::new(None));
        let splits_clone = splits.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let new_splits = enumerator.list_splits().await.unwrap();
                        {
                            let mut splits = splits_clone.lock().await;
                            *splits = Some(new_splits);
                        }
                    }
                    _ = stop_rx.borrow_mut() => {
                        log::info!("source {} discovery loop stopped", source_id );
                        break
                    }
                }
            }
        });

        Ok(Self {
            current_splits: splits,
            last_assigned_splits: vec![],
            stop_tx,
        })
    }

    pub async fn stop(self) {
        self.stop_tx.send(()).unwrap();
    }
}

pub struct SourceManagerCore<S: MetaStore> {
    pub managed_sources: HashMap<SourceId, SourceDiscovery>,
    pub barrier_manager: BarrierManagerRef<S>,
    pub catalog_manager: CatalogManagerRef<S>,
    pub meta_srv_env: MetaSrvEnv<S>,
}

impl<S> SourceManagerCore<S>
where
    S: MetaStore,
{
    fn new(
        meta_srv_env: MetaSrvEnv<S>,
        barrier_manager: BarrierManagerRef<S>,
        catalog_manager: CatalogManagerRef<S>,
    ) -> Result<Self> {
        // todo: restore managed source from meta
        Ok(Self {
            managed_sources: HashMap::new(),
            barrier_manager,
            catalog_manager,
            meta_srv_env,
        })
    }

    async fn tick(&mut self) -> Result<()> {
        for mut source in self.managed_sources.values_mut() {
            let last_assign_splits = &source.last_assigned_splits;

            let current_splits = match {
                let guard = source.current_splits.lock().await;
                guard.clone()
            } {
                None => {
                    // source discovery is not ready
                    continue;
                }
                Some(splits) => splits,
            };

            let current_split_with_ids = current_splits
                .iter()
                .map(|split| (split.id(), split))
                .collect::<HashMap<_, _>>();

            let mut assign = HashMap::new();
            let mut current_assigned_splits = vec![];

            // TODO(peng): use metastore instead
            for last_splits in last_assign_splits {
                let mut grouped_splits = last_splits.clone();

                // for now we only check new splits
                let assigned_ids = last_splits
                    .iter()
                    .flat_map(|(_, splits)| splits.iter().map(|s| s.id()))
                    .collect::<HashSet<_>>();

                let new_created_ids = current_split_with_ids
                    .iter()
                    .filter(|(id, _)| !assigned_ids.contains(*id))
                    .collect_vec();

                let actor_ids = last_splits
                    .iter()
                    .map(|(actor_id, _)| actor_id)
                    .collect_vec();

                // todo: use heap
                for (i, (_, split)) in new_created_ids.iter().cloned().enumerate() {
                    let actor_id = actor_ids[i % actor_ids.len()];
                    assign
                        .entry(*actor_id)
                        .or_insert_with(Vec::new)
                        .push((**split).clone());

                    grouped_splits
                        .entry(*actor_id)
                        .or_insert_with(Vec::new)
                        .push((**split).clone());
                }

                current_assigned_splits.push(grouped_splits);
            }

            // todo: error handling
            if !assign.is_empty() {
                self.barrier_manager
                    .run_command(Command::Plain(Mutation::SplitAssign(SplitAssignMutation {
                        splits: assign
                            .into_iter()
                            .map(|(actor_id, assigned_splits)| {
                                (
                                    actor_id,
                                    Splits {
                                        splits: assigned_splits
                                            .into_iter()
                                            .map(|split| serde_json::to_vec(&split).unwrap())
                                            .collect(),
                                    },
                                )
                            })
                            .collect(),
                    })))
                    .await?;

                source.last_assigned_splits = current_assigned_splits;
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
        barrier_manager: BarrierManagerRef<S>,
        catalog_manager: CatalogManagerRef<S>,
    ) -> Result<Self> {
        Ok(Self {
            catalog_manager: catalog_manager.clone(),
            env: env.clone(),
            cluster_manager,
            core: Arc::new(Mutex::new(
                SourceManagerCore::new(env, barrier_manager, catalog_manager).unwrap(),
            )),
        })
    }

    async fn fetch_splits_for_source(&self, source_id: SourceId) -> Result<Vec<SplitImpl>> {
        let catalog_guard = self.catalog_manager.get_catalog_core_guard().await;
        let source = catalog_guard.get_source(source_id).await?.ok_or_else(|| {
            RwError::from(InternalError(format!(
                "could not find source catalog for {}",
                source_id
            )))
        })?;

        let info = match source.get_info()? {
            Info::StreamSource(s) => s,
            _ => {
                return Err(RwError::from(InternalError(
                    "for now we only support StreamSource in source manager".to_string(),
                )));
            }
        };

        extract_split_enumerator(&info.properties)
            .to_rw_result()?
            .list_splits()
            .await
            .to_rw_result()
    }

    pub async fn schedule_split_for_actors(
        &self,
        actors: HashMap<SourceId, Vec<Vec<ActorId>>>,
    ) -> Result<HashMap<ActorId, Vec<SplitImpl>>> {
        let source_splits = try_join_all(
            actors
                .keys()
                .map(|source_id| self.fetch_splits_for_source(*source_id)),
        )
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

    pub async fn unregister_source_discovery(&self, source_id: SourceId) -> Result<()> {
        let mut core = self.core.lock().await;

        if let Some(source) = core.managed_sources.remove(&source_id) {
            source.stop().await;
        }

        Ok(())
    }

    pub async fn register_source_discovery(
        &self,
        actors: HashMap<SourceId, Vec<Vec<ActorId>>>,
    ) -> Result<()> {
        let mut core = self.core.lock().await;

        for (source_id, actors) in &actors {
            if !core.managed_sources.contains_key(source_id) {
                let source = {
                    let catalog_core = core.catalog_manager.get_catalog_core_guard().await;
                    catalog_core.get_source(*source_id).await?.ok_or_else(|| {
                        RwError::from(InternalError(format!(
                            "source {} does not exists",
                            source_id,
                        )))
                    })?
                };

                let source_info = match source.get_info()?.clone() {
                    Info::StreamSource(s) => s,
                    _ => {
                        return Err(RwError::from(InternalError(
                            "only stream source is support".to_string(),
                        )));
                    }
                };

                let source_discovery = SourceDiscovery::create(*source_id, &source_info).await?;

                core.managed_sources.insert(*source_id, source_discovery);
            }

            let mut new_assign_splits = actors
                .iter()
                .map(|actor_ids| actor_ids.iter().map(|x| (*x, vec![])).collect())
                .collect_vec();

            core.managed_sources
                .get_mut(source_id)
                .unwrap()
                .last_assigned_splits
                .append(&mut new_assign_splits);
        }

        Ok(())
    }

    async fn all_stream_clients(&self) -> Result<impl Iterator<Item = StreamClient>> {
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
                .map(|worker| self.env.stream_clients().get(worker)),
        )
        .await?
        .into_iter();

        Ok(all_stream_clients)
    }

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
        let mut interval = time::interval(Duration::from_secs(10));

        loop {
            tokio::select! {
            _ = interval.tick() => {
                    let mut core = self.core.lock().await;
                    if let Err(e) = core.tick().await {
                        log::error!("source manager tick failed! {}", e);
                    }
                }
            }
        }
    }
}
