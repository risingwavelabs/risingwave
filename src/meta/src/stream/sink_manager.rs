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

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::try_join_all;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::catalog::Sink;
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::WorkerType;
use risingwave_pb::stream_service::{
    CreateSinkRequest as ComputeNodeCreateSinkRequest,
    DropSinkRequest as ComputeNodeDropSinkRequest,
};
use risingwave_rpc_client::{StreamClient, StreamClientPoolRef};
use tokio::sync::Mutex;

use crate::barrier::BarrierManagerRef;
use crate::cluster::ClusterManagerRef;
use crate::manager::{MetaSrvEnv, SinkId};
use crate::model::TableFragments;
use crate::storage::MetaStore;
use crate::stream::{FragmentManagerRef, Scheduler};

pub type SinkManagerRef<S> = Arc<SinkManager<S>>;

#[allow(dead_code)]
pub struct SinkManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    cluster_manager: ClusterManagerRef<S>,
    /// Manages definition and status of fragments and actors
    fragment_manager: FragmentManagerRef<S>,

    /// Broadcasts and collect barriers
    barrier_manager: BarrierManagerRef<S>,

    /// Schedules streaming actors into compute nodes
    scheduler: Scheduler<S>,

    /// Client Pool to stream service on compute nodes
    client_pool: StreamClientPoolRef,

    core: Arc<Mutex<SinkManagerCore<S>>>,
}

pub struct SinkManagerCore<S: MetaStore> {
    pub fragment_manager: FragmentManagerRef<S>,
    pub managed_sinks: HashMap<SinkId, String>,
}

impl<S> SinkManagerCore<S>
where
    S: MetaStore,
{
    fn new(
        fragment_manager: FragmentManagerRef<S>,
        managed_sinks: HashMap<SinkId, String>,
    ) -> Self {
        Self {
            fragment_manager,
            managed_sinks,
        }
    }
}

impl<S> SinkManager<S>
where
    S: MetaStore,
{
    pub async fn new(
        env: MetaSrvEnv<S>,
        cluster_manager: ClusterManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        barrier_manager: BarrierManagerRef<S>,
    ) -> Result<Self> {
        let managed_sinks = HashMap::new();
        let core = Arc::new(Mutex::new(SinkManagerCore::new(
            fragment_manager.clone(),
            managed_sinks,
        )));

        Ok(Self {
            env: env.clone(),
            cluster_manager: cluster_manager.clone(),
            fragment_manager,
            barrier_manager,
            scheduler: Scheduler::new(cluster_manager, env.hash_mapping_manager_ref()),
            client_pool: env.stream_client_pool_ref(),
            core,
        })
    }

    async fn all_stream_clients(&self) -> Result<impl Iterator<Item = StreamClient>> {
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
    pub async fn create_sink(&self, sink: &Sink, _table_fragments: TableFragments) -> Result<()> {
        // This scope guard does clean up jobs ASYNCHRONOUSLY before Err returns.
        // It MUST be cleared before Ok returns.
        let mut revert_funcs = scopeguard::guard(
            vec![],
            |revert_funcs: Vec<futures::future::BoxFuture<()>>| {
                tokio::spawn(async move {
                    for revert_func in revert_funcs {
                        revert_func.await;
                    }
                });
            },
        );

        let futures = self
            .all_stream_clients()
            .await?
            .into_iter()
            .map(|mut client| {
                let request = ComputeNodeCreateSinkRequest {
                    sink: Some(sink.clone()),
                };
                async move { client.create_sink(request).await.map_err(RwError::from) }
            });

        // ignore response body, always none
        let _ = try_join_all(futures).await?;

        let core = self.core.lock().await;
        if core.managed_sinks.contains_key(&sink.get_id()) {
            log::warn!("sink {} already registered", sink.get_id());
            revert_funcs.clear();
            return Ok(());
        }

        revert_funcs.clear();
        Ok(())
    }

    pub async fn drop_sink(&self, sink_id: SinkId) -> Result<()> {
        let futures = self
            .all_stream_clients()
            .await?
            .into_iter()
            .map(|mut client| {
                let request = ComputeNodeDropSinkRequest { sink_id };
                async move { client.drop_sink(request).await.map_err(RwError::from) }
            });
        let _responses: Vec<_> = try_join_all(futures).await?;

        Ok(())
    }
}
