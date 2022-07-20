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

use lazy_static;
use risingwave_common::error::{tonic_err, RwError};
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::notification_service_server::NotificationService;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{MetaSnapshot, SubscribeRequest, SubscribeResponse};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

use crate::cluster::{ClusterManagerRef, WorkerKey};
use crate::manager::{CatalogManagerRef, MetaSrvEnv, Notification, UserInfoManagerRef};
use crate::storage::MetaStore;

#[derive(Debug, Clone)]
pub enum SubscibeDataType {
    Node,
    Database,
    Schema,
    Source,
    Table,
    User,
}

lazy_static::lazy_static! {
static ref  COMPACTOR_DATA_TYPE: Vec<SubscibeDataType> = vec![SubscibeDataType::Table];

static ref FRONTEND_DATA_TYPE: Vec<SubscibeDataType> = vec![
    SubscibeDataType::Node,
    SubscibeDataType::Database,
    SubscibeDataType::Schema,
    SubscibeDataType::Source,
    SubscibeDataType::Table,
    SubscibeDataType::User,
];
}

pub struct NotificationServiceImpl<S: MetaStore> {
    env: MetaSrvEnv<S>,

    catalog_manager: CatalogManagerRef<S>,
    cluster_manager: ClusterManagerRef<S>,
    user_manager: UserInfoManagerRef<S>,
}

impl<S> NotificationServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(
        env: MetaSrvEnv<S>,
        catalog_manager: CatalogManagerRef<S>,
        cluster_manager: ClusterManagerRef<S>,
        user_manager: UserInfoManagerRef<S>,
    ) -> Self {
        Self {
            env,
            catalog_manager,
            cluster_manager,
            user_manager,
        }
    }

    async fn build_snapshot_by_type(
        &self,
        worker_type: WorkerType,
    ) -> Result<MetaSnapshot, RwError> {
        let catalog_guard = self.catalog_manager.get_catalog_core_guard().await;
        let (database, schema, table, source) = catalog_guard.get_catalog().await?;

        let cluster_guard = self.cluster_manager.get_cluster_core_guard().await;
        let nodes = cluster_guard.list_worker_node(WorkerType::ComputeNode, Some(Running));

        let user_guard = self.user_manager.get_user_core_guard().await;
        let users = user_guard
            .get_user_info()
            .values()
            .cloned()
            .collect::<Vec<_>>();

        // Send the snapshot on subscription. After that we will send only updates.
        let subscirbe_data_type = match worker_type {
            WorkerType::Frontend => FRONTEND_DATA_TYPE.clone(),
            WorkerType::Compactor => COMPACTOR_DATA_TYPE.clone(),
            _ => unreachable!(),
        };

        // to filter info by subscirbe_data_type
        let mut nodes = Some(nodes);
        let mut database = Some(database);
        let mut schema = Some(schema);
        let mut source = Some(source);
        let mut table = Some(table);
        let mut users = Some(users);

        let mut result = MetaSnapshot::default();
        for data_type in subscirbe_data_type {
            match data_type {
                SubscibeDataType::Node => result.nodes = nodes.take().unwrap(),
                SubscibeDataType::Database => result.database = database.take().unwrap(),
                SubscibeDataType::Schema => result.schema = schema.take().unwrap(),
                SubscibeDataType::Source => result.source = source.take().unwrap(),
                SubscibeDataType::Table => result.table = table.take().unwrap(),
                SubscibeDataType::User => result.users = users.take().unwrap(),
            }
        }

        Ok(result)
    }
}

#[async_trait::async_trait]
impl<S> NotificationService for NotificationServiceImpl<S>
where
    S: MetaStore,
{
    type SubscribeStream = UnboundedReceiverStream<Notification>;

    #[cfg_attr(coverage, no_coverage)]
    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let worker_type = req.get_worker_type().map_err(tonic_err)?;
        let host_address = req.get_host().map_err(tonic_err)?.clone();

        let (tx, rx) = mpsc::unbounded_channel();

        let meta_snapshot = self.build_snapshot_by_type(worker_type).await?;

        tx.send(Ok(SubscribeResponse {
            status: None,
            operation: Operation::Snapshot as i32,
            info: Some(Info::Snapshot(meta_snapshot)),
            version: self.env.notification_manager().current_version().await,
        }))
        .unwrap();

        self.env
            .notification_manager()
            .insert_sender(worker_type, WorkerKey(host_address), tx)
            .await;

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }
}
