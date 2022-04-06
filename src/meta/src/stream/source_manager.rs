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

use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use anyhow::anyhow;

use futures::future::try_join_all;
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, oneshot};
use tokio::time;
use risingwave_common::error::{Result, RwError, ToRwResult};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_connector::{extract_split_enumerator, SplitEnumeratorImpl, SplitImpl};
use risingwave_pb::catalog::{Source, StreamSourceInfo};
use risingwave_pb::catalog::source::Info;
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_pb::stream_service::{
    CreateSourceRequest as ComputeNodeCreateSourceRequest,
    DropSourceRequest as ComputeNodeDropSourceRequest,
};

use crate::barrier::BarrierManagerRef;
use crate::cluster::ClusterManagerRef;
use crate::manager::{CatalogManagerRef, MetaSrvEnv, SourceId, StreamClient, TableId};
use crate::model::{ActorId, FragmentId};
use crate::storage::MetaStore;

pub type GlobalSourceManagerRef<S> = Arc<GlobalSourceManager<S>>;

#[allow(dead_code)]
pub struct GlobalSourceManager<S: MetaStore> {
    env: MetaSrvEnv<S>,

    cluster_manager: ClusterManagerRef<S>,
    barrier_manager: BarrierManagerRef<S>,

    catalog_manager: CatalogManagerRef<S>,

    core: Arc<Mutex<SourceManagerCore<S>>>,
}

pub struct SourceDiscovery {
    id: SourceId,
    current_splits: Arc<Mutex<Option<Vec<SplitImpl>>>>,
    stop_tx: oneshot::Sender<()>,
}

impl SourceDiscovery {
    async fn create(source_id: SourceId, info: &StreamSourceInfo) -> Result<Self> {
        let mut enumerator = extract_split_enumerator(&info.properties).to_rw_result()?;
        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
        let mut splits = Arc::new(Mutex::new(None));
        let mut splits_clone = splits.clone();

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
            id: source_id,
            current_splits: splits,
            stop_tx,
        })
    }
}

pub struct RegisterSourceContext {
    pub materialized_view_id: TableId,
    pub fragments: HashMap<SourceId, Vec<Fragment>>,
}

pub struct SourceManagerCore<S: MetaStore> {
    pub managed_sources: HashMap<SourceId, SourceDiscovery>,
    associated_sources: HashMap<TableId, HashMap<SourceId, Vec<Fragment>>>,
    meta_srv_env: MetaSrvEnv<S>,
}

impl<S> SourceManagerCore<S> where S: MetaStore {
    fn new() -> Result<Self> {
        todo!()
    }


    async fn tick(&mut self) -> Result<()> {
        for (mv_id, map) in self.associated_sources {
            for (source_id, frags) in map {
                let discovery = self.managed_sources.get(&source_id).unwrap();
                let current_splits = discovery.current_splits.lock().await.as_ref();
                match current_splits{
                    None => continue,
                    Some(splits) => {
                        
                    }
                }


            }
        }

        todo!()
    }


    fn diff_splits() -> Result<()> {
        todo!()
    }
}


impl<S> GlobalSourceManager<S>
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
            env,
            cluster_manager,
            barrier_manager,
            catalog_manager,
            core: Arc::new(Mutex::new(SourceManagerCore::new().unwrap())),
        })
    }

    async fn register_source_discovery(&mut self, ctx: RegisterSourceContext) -> Result<()> {
        let mut core = self.core.lock().await;

        if core.associated_sources.contains_key(&ctx.materialized_view_id) {
            return Err(anyhow!("mv {} already registered in source manager",ctx.materialized_view_id)).to_rw_result();
        }

        for source_id in ctx.fragments.keys() {
            if !core.managed_sources.contains_key(source_id) {
                let catalog_core = self.catalog_manager.get_catalog_core_guard().await;
                let source = catalog_core.get_source(source_id.clone()).await?
                    .ok_or(RwError::from(InternalError(format!(
                        "source {} does not exists",
                        source_id,
                    ))))?;

                let source_info = match source.get_info()?.clone() {
                    Info::StreamSource(s) => s,
                    _ => {
                        return Err(RwError::from(InternalError(
                            "only stream source is support".to_string(),
                        )));
                    }
                };

                core.managed_sources.insert(source_id.clone(), SourceDiscovery::create(source_id.clone(), &source_info).await?);
            }
        }

        let mut ctx = ctx;

        core.associated_sources.insert(ctx.materialized_view_id, std::mem::take(&mut ctx.fragments));

        Ok(())
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
        // todo: fill me
        Ok(())
    }
}
