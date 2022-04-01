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

use std::collections::HashSet;

use futures::future::try_join_all;
use log::debug;
use risingwave_common::error::{ErrorCode, Result, RwError, ToRwResult};
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::{ActorInfo, WorkerType};
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BuildActorsRequest, CreateSourceRequest,
    ForceStopActorsRequest, UpdateActorsRequest,
};
use uuid::Uuid;

use crate::barrier::command::CommandContext;
use crate::barrier::info::BarrierActorInfo;
use crate::barrier::{Command, GlobalBarrierManager};
use crate::manager::Epoch;
use crate::storage::MetaStore;

impl<S> GlobalBarrierManager<S>
where
    S: MetaStore,
{
    /// Recovery the whole cluster from the latest epoch.
    pub(crate) async fn recovery(&self, prev_epoch: u64, prev_command: &Command) -> Epoch {
        let new_epoch = self.env.epoch_generator().generate();
        // Abort buffered schedules, they might be dirty already.
        self.scheduled_barriers.abort().await;

        // clean up the previous command dirty data.
        self.clean_up(prev_command).await;

        debug!("recovery start!");
        loop {
            tokio::time::sleep(Self::RECOVERY_RETRY_INTERVAL).await;

            let info = self.resolve_actor_info(None).await;

            // reset all compute nodes and wait for online, and create sources on failed nodes.
            match self.reset_and_wait_compute_nodes(&info).await {
                Ok(failed_node_ids) => {
                    if let Err(e) = self.create_sources(&info, failed_node_ids).await {
                        debug!("create_sources failed: {:?}", e);
                        continue;
                    }
                }
                Err(e) => {
                    debug!("reset_and_wait_compute_nodes failed: {:?}", e);
                    continue;
                }
            }

            // update and build all actors.
            if self.update_actors(&info).await.is_err() || self.build_actors(&info).await.is_err() {
                continue;
            }

            // checkpoint, used as init barrier to initialize all executors.
            let command_ctx = CommandContext::new(
                self.fragment_manager.clone(),
                self.env.stream_clients_ref(),
                &info,
                prev_epoch,
                new_epoch.into_inner(),
                Command::checkpoint(),
            );
            if self.inject_barrier(&command_ctx).await.is_err()
                || command_ctx.post_collect().await.is_err()
            {
                continue;
            }

            debug!("recovery success");
            break;
        }

        new_epoch
    }

    /// Clean up previous command dirty data. Currently, we only need to handle table fragments info
    /// for `CreateMaterializedView`. For `DropMaterializedView`, since we already response fail to
    /// frontend and the actors will be rebuild by follow recovery process, it's okay to retain
    /// it.
    async fn clean_up(&self, prev_command: &Command) {
        if let Some(table_id) = prev_command.creating_table_id() {
            while self
                .fragment_manager
                .drop_table_fragments(&table_id)
                .await
                .is_err()
            {
                tokio::time::sleep(Self::RECOVERY_RETRY_INTERVAL).await;
            }
        }
    }

    /// Create all sources in compute nodes.
    async fn create_sources(
        &self,
        info: &BarrierActorInfo,
        failed_node_ids: HashSet<u32>,
    ) -> Result<()> {
        // Attention, using catalog v2 here, it's not compatible with Java frontend.
        let sources = self.catalog_manager.list_sources().await?;

        for worker_node in info
            .node_map
            .values()
            .filter(|n| failed_node_ids.contains(&n.id))
        {
            let client = &self.env.stream_clients().get(worker_node).await?;
            let futures = sources.iter().map(|source| {
                let request = CreateSourceRequest {
                    source: Some(source.to_owned()),
                };
                async move {
                    client
                        .to_owned()
                        .create_source(request)
                        .await
                        .to_rw_result()
                }
            });

            let _response = try_join_all(futures).await?;
        }

        Ok(())
    }

    /// Update all actors in compute nodes.
    async fn update_actors(&self, info: &BarrierActorInfo) -> Result<()> {
        let mut actor_infos = vec![];
        for (node_id, actors) in &info.actor_map {
            let host = info
                .node_map
                .get(node_id)
                .ok_or_else(|| {
                    RwError::from(ErrorCode::InternalError(
                        "worker evicted, wait for online.".to_string(),
                    ))
                })?
                .host
                .clone();
            actor_infos.extend(actors.iter().map(|&actor_id| ActorInfo {
                actor_id,
                host: host.clone(),
            }));
        }

        let node_actors = self.fragment_manager.all_node_actors(false).await?;
        for (node_id, actors) in &info.actor_map {
            let node = info.node_map.get(node_id).unwrap();
            let client = self.env.stream_clients().get(node).await?;

            client
                .to_owned()
                .broadcast_actor_info_table(BroadcastActorInfoTableRequest {
                    info: actor_infos.clone(),
                })
                .await
                .to_rw_result_with(|| format!("failed to connect to {}", node_id))?;

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "update actors");
            client
                .to_owned()
                .update_actors(UpdateActorsRequest {
                    request_id,
                    actors: node_actors.get(node_id).cloned().unwrap_or_default(),
                    ..Default::default()
                })
                .await
                .to_rw_result_with(|| format!("failed to connect to {}", node_id))?;
        }

        Ok(())
    }

    /// Build all actors in compute nodes.
    async fn build_actors(&self, info: &BarrierActorInfo) -> Result<()> {
        for (node_id, actors) in &info.actor_map {
            let node = info.node_map.get(node_id).unwrap();
            let client = self.env.stream_clients().get(node).await?;

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "build actors");
            client
                .to_owned()
                .build_actors(BuildActorsRequest {
                    request_id,
                    actor_id: actors.to_owned(),
                })
                .await
                .to_rw_result_with(|| format!("failed to connect to {}", node_id))?;
        }

        Ok(())
    }

    /// Reset all compute nodes and wait for them to be online again.
    async fn reset_and_wait_compute_nodes(&self, info: &BarrierActorInfo) -> Result<HashSet<u32>> {
        let mut failed_worker_id = HashSet::<u32>::new();
        for worker_node in info.node_map.values() {
            // force shutdown actors on running compute nodes
            match self.env.stream_clients().get(worker_node).await {
                Ok(client) => {
                    match client
                        .to_owned()
                        .force_stop_actors(ForceStopActorsRequest {
                            request_id: String::new(),
                        })
                        .await
                    {
                        Ok(_) => {}
                        Err(err) => {
                            // this node has down, remove it from cluster manager.
                            debug!("failed to stop actors on {:?}: {}", worker_node, err);
                            self.cluster_manager
                                .deactivate_worker_node(worker_node.host.clone().unwrap())
                                .await?;
                            failed_worker_id.insert(worker_node.id);
                        }
                    }
                }
                Err(err) => {
                    debug!("failed to get client: {}", err);
                }
            }
        }
        debug!("currently stopped compute nodes: {:?}", failed_worker_id);
        loop {
            tokio::time::sleep(Self::RECOVERY_RETRY_INTERVAL).await;
            let all_nodes = self
                .cluster_manager
                .list_worker_node(WorkerType::ComputeNode, Some(Running))
                .await
                .iter()
                .map(|worker_node| worker_node.id)
                .collect::<HashSet<_>>();
            if info
                .node_map
                .keys()
                .all(|node_id| all_nodes.contains(node_id))
            {
                break;
            }
        }

        debug!("all compute nodes have been restarted.");
        Ok(failed_worker_id)
    }
}
