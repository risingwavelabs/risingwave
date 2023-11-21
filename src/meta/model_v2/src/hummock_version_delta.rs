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

use std::fmt::Formatter;

use risingwave_pb::hummock::HummockVersionDelta;
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
    pub serialized_payload: SerializedHummockVersionDelta,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Clone, PartialEq, Eq, DeriveValueType)]
pub struct SerializedHummockVersionDelta(#[sea_orm] Vec<u8>);

impl SerializedHummockVersionDelta {
    fn get_delta(&self) -> HummockVersionDelta {
        prost::Message::decode(&*self.0).expect("should be able to decode")
    }

    pub fn from_delta(delta: &HummockVersionDelta) -> Self {
        Self(prost::Message::encode_to_vec(delta))
    }
}

impl std::fmt::Debug for SerializedHummockVersionDelta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.get_delta().fmt(f)
    }
}

impl Default for SerializedHummockVersionDelta {
    fn default() -> Self {
        Self::from_delta(&HummockVersionDelta::default())
    }
}

impl From<Model> for HummockVersionDelta {
    fn from(value: Model) -> Self {
        let ret = value.serialized_payload.get_delta();
        assert_eq!(value.id, ret.id as i64);
        assert_eq!(value.prev_id, ret.prev_id as i64);
        assert_eq!(value.max_committed_epoch, ret.max_committed_epoch as i64);
        assert_eq!(value.safe_epoch, ret.safe_epoch as i64);
        assert_eq!(value.trivial_move, ret.trivial_move);
        ret
    }
}
