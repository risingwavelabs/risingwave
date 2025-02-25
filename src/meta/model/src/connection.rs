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

use risingwave_pb::catalog::PbConnection;
use risingwave_pb::catalog::connection::PbInfo;
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{ConnectionId, ConnectionParams, PrivateLinkService};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "connection")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub connection_id: ConnectionId,
    pub name: String,

    // Private link service has been deprecated
    pub info: PrivateLinkService,
    pub params: ConnectionParams,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::ConnectionId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
    #[sea_orm(has_many = "super::sink::Entity")]
    Sink,
    #[sea_orm(has_many = "super::source::Entity")]
    Source,
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}

impl Related<super::sink::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Sink.def()
    }
}

impl Related<super::source::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Source.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

impl From<PbConnection> for ActiveModel {
    fn from(conn: PbConnection) -> Self {
        let Some(PbInfo::ConnectionParams(connection_params)) = conn.info else {
            unreachable!("private link has been deprecated.")
        };

        Self {
            connection_id: Set(conn.id as _),
            name: Set(conn.name),
            info: Set(PrivateLinkService::default()),
            params: Set(ConnectionParams::from(&connection_params)),
        }
    }
}
