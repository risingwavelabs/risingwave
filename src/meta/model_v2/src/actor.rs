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

use crate::{
    ActorId, ActorStatus, ActorUpstreamActors, ConnectorSplits, Dispatchers, FragmentId,
    VnodeBitmap,
};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "actor")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub actor_id: ActorId,
    pub fragment_id: FragmentId,
    pub status: ActorStatus,
    pub splits: Option<ConnectorSplits>,
    pub parallel_unit_id: i32,
    pub upstream_actor_ids: ActorUpstreamActors,
    pub dispatchers: Dispatchers,
    pub vnode_bitmap: Option<VnodeBitmap>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::fragment::Entity",
        from = "Column::FragmentId",
        to = "super::fragment::Column::FragmentId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Fragment,
}

impl Related<super::fragment::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Fragment.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
