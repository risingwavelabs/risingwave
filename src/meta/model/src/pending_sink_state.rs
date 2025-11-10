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

use crate::{Epoch, SinkId};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum SinkState {
    #[sea_orm(string_value = "PENDING")]
    Pending,
    #[sea_orm(string_value = "COMMITTED")]
    Committed,
    #[sea_orm(string_value = "ABORTED")]
    Aborted,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "pending_sink_state")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub sink_id: SinkId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub epoch: Epoch,
    pub sink_state: SinkState,
    pub metadata: Vec<u8>,
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::sink::Entity",
        from = "Column::SinkId",
        to = "super::sink::Column::SinkId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Sink,
}
