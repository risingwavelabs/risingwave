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

use sea_orm::entity::prelude::*;

use crate::model_v2::I32Array;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "fragment")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub fragment_id: i32,
    pub table_id: i32,
    pub fragment_type_mask: i32,
    pub distribution_type: String,
    pub stream_node: Json,
    pub vnode_mapping: Option<Json>,
    pub state_table_ids: Option<I32Array>,
    pub upstream_fragment_id: Option<I32Array>,
    pub dispatcher_type: Option<String>,
    pub dist_key_indices: Option<I32Array>,
    pub output_indices: Option<I32Array>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::actor::Entity")]
    Actor,
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::TableId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
}

impl Related<super::actor::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Actor.def()
    }
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
