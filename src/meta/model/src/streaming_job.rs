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

use std::collections::HashMap;

use sea_orm::FromJsonQueryResult;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{CreateType, JobStatus, StreamingParallelism};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "streaming_job")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub job_id: i32,
    pub job_status: JobStatus,
    pub create_type: CreateType,
    pub timezone: Option<String>,
    pub parallelism: StreamingParallelism,
    pub max_parallelism: i32,
    pub specific_resource_group: Option<String>,
}

/// This data structure contains an adjacency list of
/// backfill nodes.
/// Each edge represents a backfill order.
/// For instance, given:
/// `BackfillOrders[1] = [2, 3, 4]`
/// It means that node 1 must be backfilled before nodes 2, 3, and 4.
/// Concretely, these node ids are the fragment ids.
/// This is because each fragment will only have 1 stream scan,
/// and stream scan corresponds to a backfill node.
#[derive(Clone, Debug, PartialEq, Eq, FromJsonQueryResult, Serialize, Deserialize, Default)]
pub struct BackfillOrders(pub HashMap<u32, Vec<u32>>);

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::JobId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
