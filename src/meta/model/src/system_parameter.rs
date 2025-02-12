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

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "system_parameter")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub name: String,
    pub value: String,
    pub is_mutable: bool,
    pub description: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

const FIELDS: [&str; 4] = [
    "_id",
    "value",
    "is_mutable",
    "description",
];

pub struct MongoDb{
    system_parameter: Model
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = match &self.system_parameter.description {
            Some(description) => {
                let mut state = serializer.serialize_struct("MongoDb", 4)?;;
                state.serialize_field("description", description)?;
                state
            },
            None => serializer.serialize_struct("MongoDb", 3)?
        };
        state.serialize_field("_id", &self.system_parameter.name)?;
        state.serialize_field("value", &self.system_parameter.value)?;
        state.serialize_field("is_mutable", &self.system_parameter.is_mutable)?;
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
                let mut value: Option<String> = None;
                let mut is_mutable: Option<bool> = None;
                let mut description: Option<String> = None;
                while let Some((key, val)) = map.next_entry()? {
                    match key {
                        "_id" => name = Some(val.to_string()),
                        "value" => value = Some(val.to_string()),
                        "is_mutable" => is_mutable = Some(value.parse::<bool>().unwrap()),
                        "description" => description = Some(val.to_string()),
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let system_parameter = Model {
                    name: name.ok_or_else(|| Error::missing_field("_id"))?,
                    value: value.ok_or_else(|| Error::missing_field("value"))?,
                    is_mutable: is_mutable.ok_or_else(|| Error::missing_field("is_mutable"))?,
                    description,
                };
                Ok(Self::Value { system_parameter })
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}
