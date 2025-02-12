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
use risingwave_pb::catalog::PbView;
use sea_orm::entity::prelude::*;
use sea_orm::ActiveValue::Set;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use crate::{FieldArray, Property, ViewId};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "view")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub view_id: ViewId,
    pub name: String,
    pub properties: Property,
    pub definition: String,
    pub columns: FieldArray,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::ViewId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

impl From<PbView> for ActiveModel {
    fn from(view: PbView) -> Self {
        Self {
            view_id: Set(view.id as _),
            name: Set(view.name),
            properties: Set(Property(view.properties)),
            definition: Set(view.sql),
            columns: Set(view.columns.into()),
        }
    }
}

const FIELDS: [&str; 5] = [
    "_id",
    "name",
    "properties",
    "definition",
    "columns",
];
pub struct MongoDb {
    pub view: Model,
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("MongoDb", 5)?;
        state.serialize_field("_id", &self.view.view_id)?;
        state.serialize_field("name", &self.view.name)?;
        state.serialize_field("properties", &self.view.properties)?;
        state.serialize_field("definition", &self.view.definition)?;
        state.serialize_field("columns", &self.view.columns)?;
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
                let mut view_id: Option<ViewId> = None;
                let mut name: Option<String> = None;
                let mut properties: Option<Property> = None;
                let mut definition: Option<String> = None;
                let mut columns: Option<FieldArray> = None;
                while let Some((key, value)) = map.next_entry()? {
                    match key {
                        "_id" => {
                            view_id =
                                Some(<ViewId as std::str::FromStr>::from_str(value).unwrap())
                        }
                        "name" => name = Some(value.to_string()),
                        "properties" => properties = Some(Property::deserialize(value).unwrap()),
                        "definition" => definition = Some(value.to_string()),
                        "columns" => columns = Some(FieldArray::deserialize(value).unwrap()),
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let view = Model {
                    view_id: view_id.ok_or_else(|| Error::missing_field("_id"))?,
                    name: name.ok_or_else(|| Error::missing_field("name"))?,
                    properties: properties.ok_or_else(|| Error::missing_field("properties"))?,
                    definition: definition.ok_or_else(|| Error::missing_field("definition"))?,
                    columns: columns.ok_or_else(|| Error::missing_field("columns"))?,
                };
                Ok(Self::Value { view })
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}
