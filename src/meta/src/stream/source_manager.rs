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
use itertools::Itertools;
use risingwave_common::error::ErrorCode::{ConnectorError, InternalError};
use risingwave_common::error::{Result, RwError, ToRwResult};
use risingwave_connector::{ConnectorProperties, SplitEnumeratorImpl, SplitImpl};
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::Source;
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
        Ok(Self {
            env,
            cluster_manager,
            catalog_manager,
        })
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
        Ok(())
    }
}
