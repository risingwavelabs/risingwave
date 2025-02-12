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

use std::fmt::Formatter;
use risingwave_pb::hummock::PbSstableInfo;
use sea_orm::entity::prelude::*;
use sea_orm::{DeriveEntityModel, DeriveRelation, EnumIter};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use crate::HummockSstableObjectId;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Default)]
#[sea_orm(table_name = "hummock_sstable_info")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub sst_id: HummockSstableObjectId,
    pub object_id: HummockSstableObjectId,
    pub sstable_info: SstableInfoV2Backend,
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

crate::derive_from_blob!(SstableInfoV2Backend, PbSstableInfo);

const FIELDS: [&str; 3] = [
    "_id",
    "object_id",
    "sstable_info",
];

pub struct MongoDb {
    pub hummock_sstable_info: Model
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("MongoDb", 3)?;
        state.serialize_field("_id", &self.hummock_sstable_info.sst_id)?;
        state.serialize_field("object_id", &self.hummock_sstable_info.object_id)?;
        state.serialize_field("sstable_info", &self.hummock_sstable_info.sstable_info)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for MongoDb {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MongoDbVisitor;
        impl<'de> Visitor<'de> for MongoDbVisitor {
            type Value = MongoDb;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("MongoDb")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut sst_id: Option<HummockSstableObjectId> = None;
                let mut object_id: Option<HummockSstableObjectId> = None;
                let mut sstable_info: Option<SstableInfoV2Backend> = None;
                while let Some((key, value)) = map.next_entry()? {
                    match key {
                        "_id" => sst_id = Some(i64::deserialize(value).unwrap()),
                        "object_id" => object_id = Some(i64::deserialize(value).unwrap()),
                        "sstable_info" => sstable_info = Some(SstableInfoV2Backend::deserialize(value).unwrap()),
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let hummock_sstable_info = Model {
                    sst_id: sst_id.ok_or_else(|| Error::missing_field("_id"))?,
                    object_id: object_id.ok_or_else(|| Error::missing_field("object_id"))?,
                    sstable_info: sstable_info.ok_or_else(|| Error::missing_field("sstable_info"))?,
                };
                Ok(Self::Value { hummock_sstable_info })
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}
