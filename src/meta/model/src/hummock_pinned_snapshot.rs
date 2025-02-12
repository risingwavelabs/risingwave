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
use risingwave_pb::hummock::HummockPinnedSnapshot;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, MapAccess, Visitor};
use serde::ser::SerializeStruct;
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

const FIELDS: [&str; 2] = [
    "_id",
    "min_pinned_snapshot",
];

pub struct MongoDb {
    hummock_pinned_snapshot: Model
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("MongoDb", 2)?;
        state.serialize_field("_id", &self.hummock_pinned_snapshot.context_id)?;
        state.serialize_field("min_pinned_snapshot", &self.hummock_pinned_snapshot.min_pinned_snapshot)?;
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
                let mut min_pinned_snapshot: Option<Epoch> = None;
                while let Some((key, value)) = map.next_entry()? {
                    match key {
                        "_id" => context_id = Some(i32::deserialize(value).unwrap()),
                        "min_pinned_snapshot" => min_pinned_snapshot = Some(i64::deserialize(value).unwrap()),
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let hummock_pinned_snapshot = Model {
                    context_id: context_id.ok_or_else(|| Error::missing_field("_id"))?,
                    min_pinned_snapshot: min_pinned_snapshot.ok_or_else(|| Error::missing_field("min_pinned_snapshot"))?,
                };
                Ok(Self::Value {hummock_pinned_snapshot})
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}
