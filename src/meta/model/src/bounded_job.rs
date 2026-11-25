// Copyright 2026 RisingWave Labs
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

use crate::{JobId, SourceId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum BoundedJobStatus {
    #[sea_orm(string_value = "LOADING")]
    Loading,
    #[sea_orm(string_value = "COMPLETED")]
    Completed,
    #[sea_orm(string_value = "FAILED")]
    Failed,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "bounded_job")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub job_id: JobId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub source_id: SourceId,
    pub status: BoundedJobStatus,
    /// Unix timestamp in milliseconds; set only when the load completed.
    pub completed_at: Option<i64>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::JobId",
        to = "super::object::Column::Oid",
        on_delete = "Cascade"
    )]
    Job,
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::SourceId",
        to = "super::object::Column::Oid",
        on_delete = "Cascade"
    )]
    Source,
}

impl ActiveModelBehavior for ActiveModel {}
