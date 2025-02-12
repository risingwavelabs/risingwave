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
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, MapAccess, Visitor};
use serde::ser::SerializeStruct;

pub const COMPACTION_TASK_ID: &str = "compaction_task";
pub const COMPACTION_GROUP_ID: &str = "compaction_group";
pub const SSTABLE_OBJECT_ID: &str = "sstable_object";
pub const META_BACKUP_ID: &str = "meta_backup";
/// The read & write of now is different from other sequences. It merely reuses the hummock_sequence table.
pub const HUMMOCK_NOW: &str = "now";

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Default, Serialize, Deserialize)]
#[sea_orm(table_name = "hummock_sequence")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub name: String,
    pub seq: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

const FIELDS: [&str; 2] = [
    "_id",
    "seq",
];

pub struct MongoDb {
    hummock_sequence: Model
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("MongoDb", 2)?;
        state.serialize_field("_id", &self.hummock_sequence.name)?;
        state.serialize_field("seq", &self.hummock_sequence.seq)?;
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
                let mut name: Option<String> = None;
                let mut seq: Option<i64> = None;
                while let Some((key, value)) = map.next_entry()? {
                    match key {
                        "_id" => name = Some(value.to_string()),
                        "seq" => seq = Some(i64::deserialize(value).unwrap()),
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let hummock_sequence = Model {
                    name: name.ok_or_else(|| Error::missing_field("_id"))?,
                    seq: seq.ok_or_else(|| Error::missing_field("seq"))?,
                };
                Ok(Self::Value {hummock_sequence})
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}
