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

use std::collections::HashMap;
use std::fmt::Formatter;
use risingwave_pb::hummock::{HummockVersionStats, TableStats as PbTableStats};
use sea_orm::entity::prelude::*;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use crate::HummockVersionId;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize, Default)]
#[sea_orm(table_name = "hummock_version_stats")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: HummockVersionId,
    pub stats: TableStats,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Clone, Debug, PartialEq, Eq, FromJsonQueryResult, Serialize, Deserialize, Default)]
pub struct TableStats(pub HashMap<u32, PbTableStats>);

impl From<Model> for HummockVersionStats {
    fn from(value: Model) -> Self {
        Self {
            hummock_version_id: value.id as _,
            table_stats: value.stats.0,
        }
    }
}

const FIELDS: [&str; 2] = [
    "_id",
    "stats",
];

pub struct MongoDb {
    hummock_version_stats: Model
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("MongoDb", 2)?;
        state.serialize_field("_id", &self.hummock_version_stats.id)?;
        state.serialize_field("stats", &self.hummock_version_stats.stats)?;
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
                let mut id: Option<HummockVersionId> = None;
                let mut stats: Option<TableStats> = None;
                while let Some((key, value)) = map.next_entry()? {
                    match key {
                        "_id" => {
                            id =
                                Some(i64::deserialize(value).unwrap())
                        }
                        "stats" => stats = Some(TableStats::deserialize(value).unwrap()),
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let hummock_version_stats = Model {
                    id: id.ok_or_else(|| Error::missing_field("_id"))?,
                    stats: stats.ok_or_else(|| Error::missing_field("stats"))?,
                };
                Ok(Self::Value {hummock_version_stats})
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}
