// Copyright 2024 RisingWave Labs
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

use risingwave_pb::hummock::PbHummockVersionDelta;
use sea_orm::entity::prelude::*;

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

crate::derive_from_blob!(FullVersionDelta, PbHummockVersionDelta);

impl From<Model> for PbHummockVersionDelta {
    fn from(value: Model) -> Self {
        let ret = value.full_version_delta.to_protobuf();
        assert_eq!(value.id, ret.id as i64);
        assert_eq!(value.prev_id, ret.prev_id as i64);
        assert_eq!(value.trivial_move, ret.trivial_move);
        ret
    }
}
