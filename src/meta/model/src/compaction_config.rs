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
use risingwave_pb::hummock::CompactionConfig as PbCompactionConfig;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use crate::CompactionGroupId;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize, Default)]
#[sea_orm(table_name = "compaction_config")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub compaction_group_id: CompactionGroupId,
    pub config: CompactionConfig,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

crate::derive_from_blob!(CompactionConfig, PbCompactionConfig);

const FIELDS: [&str; 2] = [
    "_id",
    "config",
];

pub struct MongoDb {
    pub compaction_config: Model
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("MongoDb", 2)?;
        state.serialize_field("_id", &self.compaction_config.compaction_group_id)?;
        state.serialize_field("config", &self.compaction_config.config)?;
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
                let mut compaction_group_id: Option<CompactionGroupId> = None;
                let mut config: Option<CompactionConfig> = None;
                while let Some((key, value)) = map.next_entry()? {
                    match key {
                        "_id" => compaction_group_id = Some(<CompactionGroupId as std::str::FromStr>::from_str(value).unwrap()),
                        "config" => config = Some(CompactionConfig::deserialize(value).unwrap()),
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let compaction_config = Model {
                    compaction_group_id: compaction_group_id.ok_or_else(|| Error::missing_field("_id"))?,
                    config: config.ok_or_else(|| Error::missing_field("config"))?,
                };
                Ok(Self::Value { compaction_config })
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}