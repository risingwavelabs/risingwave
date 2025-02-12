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
use risingwave_pb::hummock::HummockPinnedVersion;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use crate::{HummockVersionId, WorkerId};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize, Default)]
#[sea_orm(table_name = "hummock_pinned_version")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub context_id: WorkerId,
    pub min_pinned_id: HummockVersionId,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

impl From<Model> for HummockPinnedVersion {
    fn from(value: Model) -> Self {
        Self {
            context_id: value.context_id as _,
            min_pinned_id: value.min_pinned_id as _,
        }
    }
}

const FIELDS: [&str; 2] = [
    "_id",
    "min_pinned_id",
];

pub struct MongoDb {
    hummock_pinned_version: Model
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("MongoDb", 2)?;
        state.serialize_field("_id", &self.hummock_pinned_version.context_id)?;
        state.serialize_field("min_pinned_id", &self.hummock_pinned_version.min_pinned_id)?;
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
                let mut context_id: Option<WorkerId> = None;
                let mut min_pinned_id: Option<HummockVersionId> = None;
                while let Some((key, value)) = map.next_entry()? {
                    match key {
                        "_id" => context_id = Some(i32::deserialize(value).unwrap()),
                        "min_pinned_id" => min_pinned_id = Some(i64::deserialize(value).unwrap()),
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let hummock_pinned_version = Model {
                    context_id: context_id.ok_or_else(|| Error::missing_field("_id"))?,
                    min_pinned_id: min_pinned_id.ok_or_else(|| Error::missing_field("min_pinned_id"))?,
                };
                Ok(Self::Value {hummock_pinned_version})
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}
