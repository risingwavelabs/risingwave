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

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::TableId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum RefreshState {
    /// The table is refreshable and the current state is `Pending`.
    #[sea_orm(string_value = "IDLE")]
    Idle,
    /// The table is refreshable and the current state is `Refreshing` (`RefreshStart` barrier passed).
    #[sea_orm(string_value = "REFRESHING")]
    Refreshing,
    /// The table is refreshable and the current state is `Finishing`. (`LoadFinish` barrier passed).
    #[sea_orm(string_value = "FINISHING")]
    Finishing,
}

impl From<RefreshState> for risingwave_pb::catalog::RefreshState {
    fn from(refresh_state: RefreshState) -> Self {
        match refresh_state {
            RefreshState::Idle => Self::Idle,
            RefreshState::Refreshing => Self::Refreshing,
            RefreshState::Finishing => Self::Finishing,
        }
    }
}

impl From<risingwave_pb::catalog::RefreshState> for RefreshState {
    fn from(refresh_state: risingwave_pb::catalog::RefreshState) -> Self {
        match refresh_state {
            risingwave_pb::catalog::RefreshState::Idle => Self::Idle,
            risingwave_pb::catalog::RefreshState::Refreshing => Self::Refreshing,
            risingwave_pb::catalog::RefreshState::Finishing => Self::Finishing,
            risingwave_pb::catalog::RefreshState::Unspecified => {
                unreachable!("Unspecified refresh state")
            }
        }
    }
}

impl std::fmt::Display for RefreshState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefreshState::Idle => write!(f, "IDLE"),
            RefreshState::Refreshing => write!(f, "REFRESHING"),
            RefreshState::Finishing => write!(f, "FINISHING"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "refresh_job")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub table_id: TableId,
    pub job_create_time: DateTime,
    pub last_trigger_time: Option<DateTime>,
    pub trigger_interval_secs: Option<i64>,
    pub current_status: RefreshState,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::table::Entity",
        from = "Column::TableId",
        to = "super::table::Column::TableId",
        on_update = "Cascade",
        on_delete = "Cascade"
    )]
    Table,
}

impl Related<super::table::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Table.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
