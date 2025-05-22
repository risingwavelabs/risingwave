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

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "allow_alter_connector_props")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub connector_name: String,
    #[sea_orm(primary_key)]
    pub object_type: String,
    pub allow_alter_props: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Copy, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum AlterPropsObjectType {
    #[sea_orm(string_value = "SINK")]
    Sink,
    #[sea_orm(string_value = "SOURCE")]
    Source,
}

impl Model {
    pub fn get_object_type(&self) -> AlterPropsObjectType {
        match self.object_type.as_str() {
            "SINK" => AlterPropsObjectType::Sink,
            "SOURCE" => AlterPropsObjectType::Source,
            _ => panic!("Invalid object type: {}", self.object_type),
        }
    }

    pub fn set_object_type(&mut self, obj_type: AlterPropsObjectType) {
        self.object_type = match obj_type {
            AlterPropsObjectType::Sink => "SINK".to_string(),
            AlterPropsObjectType::Source => "SOURCE".to_string(),
        };
    }
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
