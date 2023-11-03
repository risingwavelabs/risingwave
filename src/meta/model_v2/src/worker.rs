// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_pb::common::worker_node::PbState;
use risingwave_pb::common::{PbWorkerNode, PbWorkerType};
use sea_orm::entity::prelude::*;
use sea_orm::ActiveValue::Set;

use crate::{TransactionId, WorkerId};

#[derive(Clone, Debug, Hash, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(None)")]
pub enum WorkerType {
    #[sea_orm(string_value = "FRONTEND")]
    Frontend,
    #[sea_orm(string_value = "COMPUTE_NODE")]
    ComputeNode,
    #[sea_orm(string_value = "RISE_CTL")]
    RiseCtl,
    #[sea_orm(string_value = "COMPACTOR")]
    Compactor,
    #[sea_orm(string_value = "META")]
    Meta,
}

impl From<PbWorkerType> for WorkerType {
    fn from(worker_type: PbWorkerType) -> Self {
        match worker_type {
            PbWorkerType::Unspecified => unreachable!("unspecified worker type"),
            PbWorkerType::Frontend => Self::Frontend,
            PbWorkerType::ComputeNode => Self::ComputeNode,
            PbWorkerType::RiseCtl => Self::RiseCtl,
            PbWorkerType::Compactor => Self::Compactor,
            PbWorkerType::Meta => Self::Meta,
        }
    }
}

impl From<WorkerType> for PbWorkerType {
    fn from(worker_type: WorkerType) -> Self {
        match worker_type {
            WorkerType::Frontend => Self::Frontend,
            WorkerType::ComputeNode => Self::ComputeNode,
            WorkerType::RiseCtl => Self::RiseCtl,
            WorkerType::Compactor => Self::Compactor,
            WorkerType::Meta => Self::Meta,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(None)")]
pub enum WorkerStatus {
    #[sea_orm(string_value = "STARTING")]
    Starting,
    #[sea_orm(string_value = "RUNNING")]
    Running,
}

impl From<PbState> for WorkerStatus {
    fn from(state: PbState) -> Self {
        match state {
            PbState::Unspecified => unreachable!("unspecified worker status"),
            PbState::Starting => Self::Starting,
            PbState::Running => Self::Running,
        }
    }
}

impl From<WorkerStatus> for PbState {
    fn from(status: WorkerStatus) -> Self {
        match status {
            WorkerStatus::Starting => Self::Starting,
            WorkerStatus::Running => Self::Running,
        }
    }
}

impl From<&PbWorkerNode> for ActiveModel {
    fn from(worker: &PbWorkerNode) -> Self {
        let host = worker.host.clone().unwrap();
        Self {
            worker_id: Set(worker.id as _),
            worker_type: Set(worker.r#type().into()),
            host: Set(host.host),
            port: Set(host.port),
            status: Set(worker.state().into()),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "worker")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub worker_id: WorkerId,
    pub worker_type: WorkerType,
    pub host: String,
    pub port: i32,
    pub status: WorkerStatus,
    pub transaction_id: Option<TransactionId>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::worker_property::Entity")]
    WorkerProperty,
}

impl Related<super::worker_property::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::WorkerProperty.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
