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

use std::collections::HashMap;

use risingwave_pb::hummock::{GroupDelta as PbGroupDelta, HummockVersionDelta};
use sea_orm::entity::prelude::*;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{CompactionGroupId, Epoch, HummockSstableObjectId, HummockVersionId};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize, Default)]
#[sea_orm(table_name = "hummock_version_delta")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: HummockVersionId,
    pub prev_id: HummockVersionId,
    pub group_deltas: GroupDeltas,
    pub max_committed_epoch: Epoch,
    pub safe_epoch: Epoch,
    pub trivial_move: bool,
    pub gc_object_ids: SstableObjectIds,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

crate::derive_from_json_struct!(SstableObjectIds, Vec<HummockSstableObjectId>);

impl From<Vec<u64>> for SstableObjectIds {
    fn from(value: Vec<u64>) -> Self {
        Self(value.into_iter().map(|id| id as _).collect())
    }
}

crate::derive_from_json_struct!(GroupDeltas, HashMap<CompactionGroupId, Vec<PbGroupDelta>>);

impl From<Model> for HummockVersionDelta {
    fn from(value: Model) -> Self {
        use risingwave_pb::hummock::hummock_version_delta::GroupDeltas as PbGroupDeltas;
        Self {
            id: value.id as _,
            prev_id: value.prev_id as _,
            group_deltas: value
                .group_deltas
                .0
                .into_iter()
                .map(|(cg_id, group_deltas)| (cg_id as _, PbGroupDeltas { group_deltas }))
                .collect(),
            max_committed_epoch: value.max_committed_epoch as _,
            safe_epoch: value.safe_epoch as _,
            trivial_move: value.trivial_move as _,
            gc_object_ids: value
                .gc_object_ids
                .into_inner()
                .into_iter()
                .map(|id| id as _)
                .collect(),
        }
    }
}
