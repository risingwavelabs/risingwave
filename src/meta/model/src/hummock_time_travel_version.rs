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
use risingwave_pb::hummock::PbHummockVersion;
use sea_orm::entity::prelude::*;
use sea_orm::{DeriveEntityModel, DeriveRelation, EnumIter};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use crate::HummockVersionId;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Default)]
#[sea_orm(table_name = "hummock_time_travel_version")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub version_id: HummockVersionId,
    pub version: HummockVersion,
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

crate::derive_from_blob!(HummockVersion, PbHummockVersion);

const FIELDS: [&str; 2] = [
    "_id",
    "version",
];

pub struct MongoDb {
    pub hummock_time_travel_version: Model,
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("MongoDb", 2)?;
        state.serialize_field("_id", &self.hummock_time_travel_version.version_id)?;
        state.serialize_field("version", &self.hummock_time_travel_version.version)?;
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
                let mut version_id: Option<HummockVersionId> = None;
                let mut version: Option<HummockVersion> = None;
                while let Some((key, value)) = map.next_entry()? {
                    match key {
                        "_id" => version_id = Some(<HummockVersionId as std::str::FromStr>::from_str(value).unwrap()),
                        "version" => version = Some(HummockVersion::deserialize(value).unwrap()),
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let hummock_time_travel_version = Model {
                    version_id: version_id.ok_or_else(|| Error::missing_field("_id"))?,
                    version: version.ok_or_else(|| Error::missing_field("version"))?,
                };
                Ok(Self::Value { hummock_time_travel_version })
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}
