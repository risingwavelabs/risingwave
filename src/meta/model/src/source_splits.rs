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



use crate::{ConnectorSplits, SourceId, source_splits};



#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "source_splits")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub source_id: SourceId,
    pub splits: Option<ConnectorSplits>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::source::Entity",
        from = "Column::SourceId",
        to = "super::source::Column::SourceId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    SourceId,
}

impl Related<super::source::Entity> for source_splits::Entity {
    fn to() -> RelationDef {
        source_splits::Relation::SourceId.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
