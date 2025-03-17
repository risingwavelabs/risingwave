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

use risingwave_pb::hummock::HummockPinnedSnapshot;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{Epoch, WorkerId};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize, Default)]
#[sea_orm(table_name = "hummock_pinned_snapshot")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub context_id: WorkerId,
    pub min_pinned_snapshot: Epoch,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

impl From<Model> for HummockPinnedSnapshot {
    fn from(value: Model) -> Self {
        Self {
            context_id: value.context_id as _,
            minimal_pinned_snapshot: value.min_pinned_snapshot as _,
        }
    }
}
