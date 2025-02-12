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

#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum VersionCategory {
    #[sea_orm(string_value = "NOTIFICATION")]
    Notification,
    #[sea_orm(string_value = "TABLE_REVISION")]
    TableRevision,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "catalog_version")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub name: VersionCategory,
    pub version: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

const FIELDS: [&str; 2] = [
    "_id",
    "version",
];

pub struct MongoDb {
    pub catalog_version: Model
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("MongoDb", 2)?;
        state.serialize_field("_id", &self.catalog_version.name)?;
        state.serialize_field("version", &self.catalog_version.version)?;
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
                let mut name: Option<VersionCategory> = None;
                let mut version: Option<i64> = None;
                while let Some((key, value)) = map.next_entry()? {
                    match key {
                        "_id" => name = Some(VersionCategory::deserialize(value).unwrap()),
                        "version" => version = Some(i64::deserialize(value).unwrap()),
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let catalog_version = Model {
                    name: name.ok_or_else(|| Error::missing_field("_id"))?,
                    version: version.ok_or_else(|| Error::missing_field("version"))?,
                };
                Ok(Self::Value { catalog_version })
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}
