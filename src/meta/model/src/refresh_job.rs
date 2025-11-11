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

#[derive(
    Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize, Copy, Hash,
)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum RefreshJobStatus {
    #[sea_orm(string_value = "IDLE")]
    Idle,
    #[sea_orm(string_value = "RUNNING")]
    Running,
    #[sea_orm(string_value = "DISABLED")]
    Disabled,
}

impl RefreshJobStatus {
    pub fn is_active(self) -> bool {
        matches!(self, RefreshJobStatus::Idle | RefreshJobStatus::Running)
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
    pub current_status: RefreshJobStatus,
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
