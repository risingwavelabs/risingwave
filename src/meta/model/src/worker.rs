// Copyright 2025 RisingWave Labs
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

use std::fmt::Formatter;

use risingwave_pb::common::worker_node::PbState;
use risingwave_pb::common::{PbWorkerNode, PbWorkerType};
use sea_orm::entity::prelude::*;
use sea_orm::ActiveValue::Set;
use serde::de::{Error, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{TransactionId, WorkerId};

#[derive(Clone, Debug, Hash, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
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

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
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
            transaction_id: Set(worker.transactional_id.map(|id| id as _)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
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

const FIELDS: [&str; 7] = [
    "_id",
    "worker_type",
    "host",
    "port",
    "status",
    "transaction_id",
    "worker_property",
];

pub struct MongoDb {
    pub worker: Model,
    pub worker_property: Option<super::worker_property::MongoDb>,
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = match &self.worker_property {
            Some(worker_property) => {
                let mut state = serializer.serialize_struct("MongoDb", 7)?;
                state.serialize_field("worker_property", worker_property)?;
                state
            },
            None => {
                serializer.serialize_struct("MongoDb", 6)?;
            }
        };
        state.serialize_field("_id", &self.worker.worker_id)?;
        state.serialize_field("worker_type", &self.worker.worker_type)?;
        state.serialize_field("host", &self.worker.host)?;
        state.serialize_field("port", &self.worker.port)?;
        state.serialize_field("status", &self.worker.status)?;
        state.serialize_field("transaction_id", &self.worker.transaction_id)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for MongoDb {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MongoDbVisitor;
        impl<'de> Visitor<'de> for MongoDbVisitor {
            type Value = MongoDb;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("MongoDb")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut worker_id: Option<WorkerId> = None;
                let mut worker_type: Option<WorkerType> = None;
                let mut host: Option<String> = None;
                let mut port: Option<i32> = None;
                let mut status: Option<WorkerStatus> = None;
                let mut transaction_id = None;
                let mut worker_property = None;
                while let Some((key, value)) = map.next_entry()? {
                    match key {
                        "_id" => {
                            worker_id =
                                Some(i32::deserialize(value).unwrap())
                        }
                        "worker_type" => worker_type = Some(WorkerType::deserialize(value).unwrap()),
                        "host" => host = Some(value.to_string()),
                        "port" => port = Some(i32::deserialize(value).unwrap()),
                        "status" => status = Some(WorkerStatus::deserialize(value).unwrap()),
                        "transaction_id" => {
                            transaction_id =
                                Some(i32::deserialize(value).unwrap())
                        }
                        "worker_property" => {
                            worker_property = Some(
                                super::worker_property::MongoDb::deserialize(value)
                                    .unwrap(),
                            )
                        }
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let worker = Model {
                    worker_id: worker_id.ok_or_else(|| Error::missing_field("_id"))?,
                    worker_type: worker_type.ok_or_else(|| Error::missing_field("worker_type"))?,
                    host: host.ok_or_else(|| Error::missing_field("host"))?,
                    port: port.ok_or_else(|| Error::missing_field("port"))?,
                    status: status.ok_or_else(|| Error::missing_field("status"))?,
                    transaction_id,
                };
                Ok(Self::Value {
                    worker,
                    worker_property,
                })
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}
