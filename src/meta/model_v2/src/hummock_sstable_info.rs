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

use risingwave_pb::hummock::PbSstableInfo;
use sea_orm::entity::prelude::*;
use sea_orm::{DeriveEntityModel, DeriveRelation, EnumIter};

use crate::HummockSstableObjectId;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Default)]
#[sea_orm(table_name = "hummock_sstable_info")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub sst_id: HummockSstableObjectId,
    pub object_id: HummockSstableObjectId,
    pub sstable_info: SstableInfo,
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

crate::derive_from_blob!(SstableInfo, PbSstableInfo);

impl From<Model> for PbSstableInfo {
    fn from(value: Model) -> Self {
        let ret = value.sstable_info.to_protobuf();
        assert_eq!(value.object_id, ret.object_id as i64);
        assert_eq!(value.sst_id, ret.sst_id as i64);
        ret
    }
}
