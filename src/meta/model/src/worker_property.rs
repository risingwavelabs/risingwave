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
use crate::WorkerId;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "worker_property")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub worker_id: WorkerId,
    pub parallelism: i32,
    pub is_streaming: bool,
    pub is_serving: bool,
    pub is_unschedulable: bool,
    pub internal_rpc_host_addr: Option<String>,
    pub resource_group: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::worker::Entity",
        from = "Column::WorkerId",
        to = "super::worker::Column::WorkerId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Worker,
}

impl Related<super::worker::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Worker.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

const FIELDS: [&str; 6] = [
    "parallelism",
    "is_streaming",
    "is_serving",
    "is_unschedulable",
    "internal_rpc_host_addr",
    "resource_group",
];

pub struct MongoDb {
    pub worker_property: Model,
}

impl Serialize for MongoDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let count = 4 + if self.worker_property.internal_rpc_host_addr.is_some() {1} else {0} + if self.worker_property.resource_group.is_some() {1} else {0} ;

        let mut state = serializer.serialize_struct("MongoDb", count)?;
        state.serialize_field("parallelism", &self.worker_property.parallelism)?;
        state.serialize_field("is_streaming", &self.worker_property.is_streaming)?;
        state.serialize_field("is_serving", &self.worker_property.is_serving)?;
        state.serialize_field("is_unschedulable", &self.worker_property.is_unschedulable)?;
        if self.worker_property.internal_rpc_host_addr.is_some(){
            state.serialize_field("internal_rpc_host_addr", &self.worker_property.internal_rpc_host_addr)?;
        }
        if self.worker_property.resource_group.is_some(){
            state.serialize_field("resource_group", &self.worker_property.resource_group)?;
        }
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
                let mut parallelism: Option<i32> = None;
                let mut is_streaming: Option<bool> = None;
                let mut is_serving: Option<bool> = None;
                let mut is_unschedulable: Option<bool> = None;
                let mut internal_rpc_host_addr: Option<String> = None;
                let mut resource_group: Option<String> = None;
                while let Some((key, value)) = map.next_entry()? {
                    match key {
                        "parallelism" => {
                            parallelism =
                                Some(<i32 as std::str::FromStr>::from_str(value).unwrap())
                        }
                        "is_streaming" => is_streaming = Some(<bool as std::str::FromStr>::from_str(value).unwrap()),
                        "is_serving" => is_serving = Some(<bool as std::str::FromStr>::from_str(value).unwrap()),
                        "is_unschedulable" => is_unschedulable = Some(<bool as std::str::FromStr>::from_str(value).unwrap()),
                        "internal_rpc_host_addr" => internal_rpc_host_addr = Some(value.to_string()),
                        "resource_group" => resource_group = Some(value.to_string()),
                        x => return Err(Error::unknown_field(x, &FIELDS)),
                    }
                }

                let worker_property = Model {
                    worker_id: Default::default(),
                    parallelism: parallelism.ok_or_else(|| Error::missing_field("parallelism"))?,
                    is_streaming: is_streaming.ok_or_else(|| Error::missing_field("is_streaming"))?,
                    is_serving: is_serving.ok_or_else(|| Error::missing_field("is_serving"))?,
                    is_unschedulable: is_unschedulable.ok_or_else(|| Error::missing_field("is_unschedulable"))?,
                    internal_rpc_host_addr,
                    resource_group,
                };
                Ok(Self::Value { worker_property })
            }
        }
        deserializer.deserialize_map(MongoDbVisitor {})
    }
}
