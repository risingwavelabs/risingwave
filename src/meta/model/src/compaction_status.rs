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

use risingwave_pb::hummock::LevelHandler as PbLevelHandler;
use sea_orm::entity::prelude::*;

use crate::CompactionGroupId;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "compaction_status")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub compaction_group_id: CompactionGroupId,
    pub status: LevelHandlers,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

crate::derive_array_from_blob!(LevelHandlers, PbLevelHandler, PbLevelHandlerArray);

const FIELDS: [&str; 2] = [
    "_id",
    "status",
];

pub struct MongoDb {
    pub copaction_status: Model
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("MongoDb", 2)?;
        state.serialize_field("_id", &self.cluster.cluster_id)?;
        state.serialize_field("created_at", &self.cluster.created_at)?;
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
                let mut cluster_id: Option<Uuid> = None;
                let mut created_at: Option<DateTime> = None;
                while let Some((key, value)) = map.next_entry()? {
                    match key {
                        "_id" => cluster_id = Some(Uuid::deserialize(value).unwrap()),
                        "created_at" => created_at = Some(DateTime::deserialize(value).unwrap()),
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let cluster = Model {
                    cluster_id: cluster_id.ok_or_else(|| Error::missing_field("_id"))?,
                    created_at: created_at.ok_or_else(|| Error::missing_field("created_at"))?,
                };
                Ok(Self::Value { cluster })
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}
