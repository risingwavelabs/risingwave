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

#[allow(dead_code)]
pub struct ConnectorSinkWorker {
    sink_id: SinkId,
    enumerator: SplitEnumeratorImpl,
    period: Duration,
}

impl ConnectorSinkWorker {
    pub async fn create(sink: &Sink, period: Duration) -> Result<Self> {
        todo!();
    }

    pub async fn run(&mut self, mut sync_call_rx: UnboundedReceiver<oneshot::Sender<Result<()>>>) {
        todo!();
    }
}

pub struct ConnectorSinkWorkerHandle {
    handle: JoinHandle<()>,
    sync_call_tx: UnboundedSender<oneshot::Sender<Result<()>>>,
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
        todo!();
    }

    pub async fn patch_diff(
        &mut self,
        sink_fragments: Option<HashMap<SinkId, Vec<FragmentId>>>,
        actor_splits: Option<HashMap<ActorId, Vec<SplitImpl>>>,
    ) -> Result<()> {
        todo!();
    }
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
        todo!();
    }

    /// Broadcast the create sink request to all compute nodes.
    pub async fn create_sink(&self, sink: &Sink) -> Result<()> {
        todo!();
    }

    pub async fn drop_sink(&self, sink_id: SinkId) -> Result<()> {
        todo!();
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
