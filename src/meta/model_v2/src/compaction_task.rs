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

use risingwave_pb::hummock::{CompactTask as PbCompactTask, CompactTaskAssignment};
use sea_orm::entity::prelude::*;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{CompactionTaskId, WorkerId};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "compaction_task")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: CompactionTaskId,
    pub task: CompactionTask,
    pub context_id: WorkerId,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

crate::derive_from_json_struct!(CompactionTask, PbCompactTask);

impl From<Model> for CompactTaskAssignment {
    fn from(value: Model) -> Self {
        Self {
            compact_task: Some(value.task.0),
            context_id: value.context_id as _,
        }
    }
}
