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

use risingwave_pb::stream_plan::{PbDispatcher, PbDispatcherType};
use sea_orm::entity::prelude::*;

use crate::{ActorId, ActorMapping, FragmentId, I32Array};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(None)")]
pub enum DispatcherType {
    #[sea_orm(string_value = "HASH")]
    Hash,
    #[sea_orm(string_value = "BROADCAST")]
    Broadcast,
    #[sea_orm(string_value = "SIMPLE")]
    Simple,
    #[sea_orm(string_value = "NO_SHUFFLE")]
    NoShuffle,
}

impl From<PbDispatcherType> for DispatcherType {
    fn from(val: PbDispatcherType) -> Self {
        match val {
            PbDispatcherType::Unspecified => unreachable!(),
            PbDispatcherType::Hash => DispatcherType::Hash,
            PbDispatcherType::Broadcast => DispatcherType::Broadcast,
            PbDispatcherType::Simple => DispatcherType::Simple,
            PbDispatcherType::NoShuffle => DispatcherType::NoShuffle,
        }
    }
}

impl From<DispatcherType> for PbDispatcherType {
    fn from(val: DispatcherType) -> Self {
        match val {
            DispatcherType::Hash => PbDispatcherType::Hash,
            DispatcherType::Broadcast => PbDispatcherType::Broadcast,
            DispatcherType::Simple => PbDispatcherType::Simple,
            DispatcherType::NoShuffle => PbDispatcherType::NoShuffle,
        }
    }
}

impl From<(u32, PbDispatcher)> for Model {
    fn from((actor_id, dispatcher): (u32, PbDispatcher)) -> Self {
        Self {
            id: 0,
            actor_id: actor_id as _,
            dispatcher_type: dispatcher.r#type().into(),
            dist_key_indices: dispatcher.dist_key_indices.into(),
            output_indices: dispatcher.output_indices.into(),
            hash_mapping: dispatcher.hash_mapping.map(ActorMapping),
            dispatcher_id: dispatcher.dispatcher_id as _,
            downstream_actor_ids: dispatcher.downstream_actor_id.into(),
        }
    }
}

impl From<Model> for PbDispatcher {
    fn from(model: Model) -> Self {
        Self {
            r#type: PbDispatcherType::from(model.dispatcher_type) as _,
            dist_key_indices: model.dist_key_indices.into_u32_array(),
            output_indices: model.output_indices.into_u32_array(),
            hash_mapping: model.hash_mapping.map(|mapping| mapping.into_inner()),
            dispatcher_id: model.dispatcher_id as _,
            downstream_actor_id: model.downstream_actor_ids.into_u32_array(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "actor_dispatcher")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub actor_id: ActorId,
    pub dispatcher_type: DispatcherType,
    pub dist_key_indices: I32Array,
    pub output_indices: I32Array,
    pub hash_mapping: Option<ActorMapping>,
    pub dispatcher_id: FragmentId,
    pub downstream_actor_ids: I32Array,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::actor::Entity",
        from = "Column::ActorId",
        to = "super::actor::Column::ActorId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Actor,
    #[sea_orm(
        belongs_to = "super::fragment::Entity",
        from = "Column::DispatcherId",
        to = "super::fragment::Column::FragmentId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Fragment,
}

impl Related<super::actor::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Actor.def()
    }
}

impl Related<super::fragment::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Fragment.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
