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

use risingwave_pb::hummock::LevelHandler as PbLevelHandler;
use sea_orm::entity::prelude::*;

use crate::CompactionGroupId;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "compaction_status")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub compaction_group_id: CompactionGroupId,
    pub status: LevelHandlers,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

crate::derive_array_from_blob!(LevelHandlers, PbLevelHandler, PbLevelHandlerArray);
