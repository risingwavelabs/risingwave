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

use risingwave_pb::hummock::HummockVersionDelta;
use sea_orm::entity::prelude::*;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{Epoch, HummockVersionId};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Default)]
#[sea_orm(table_name = "hummock_version_delta")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: HummockVersionId,
    pub prev_id: HummockVersionId,
    pub max_committed_epoch: Epoch,
    pub safe_epoch: Epoch,
    pub trivial_move: bool,
    pub full_version_delta: FullVersionDelta,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

crate::derive_from_json_struct!(FullVersionDelta, HummockVersionDelta);

impl From<Model> for HummockVersionDelta {
    fn from(value: Model) -> Self {
        let ret = value.full_version_delta.into_inner();
        assert_eq!(value.id, ret.id as i64);
        assert_eq!(value.prev_id, ret.prev_id as i64);
        assert_eq!(value.max_committed_epoch, ret.max_committed_epoch as i64);
        assert_eq!(value.safe_epoch, ret.safe_epoch as i64);
        assert_eq!(value.trivial_move, ret.trivial_move);
        ret
    }
}
