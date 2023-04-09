// Copyright 2023 RisingWave Labs
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

use risingwave_common::catalog::Field;
use risingwave_pb::catalog::PbView;

use super::{RelationCatalog, ViewId};
use crate::user::UserId;
use crate::WithOptions;

#[derive(Clone, Debug)]
pub struct ViewCatalog {
    pub id: ViewId,
    pub name: String,

    pub owner: UserId,
    pub properties: WithOptions,
    pub sql: String,
    pub columns: Vec<Field>,
}

impl From<&PbView> for ViewCatalog {
    fn from(view: &PbView) -> Self {
        ViewCatalog {
            id: view.id,
            name: view.name.clone(),
            owner: view.owner,
            properties: WithOptions::new(view.properties.clone()),
            sql: view.sql.clone(),
            columns: view.columns.iter().map(|f| f.into()).collect(),
        }
    }
}

impl ViewCatalog {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the SQL statement that can be used to create this view.
    pub fn create_sql(&self) -> String {
        format!("CREATE VIEW {} AS {}", self.name, self.sql)
    }
}

impl RelationCatalog for ViewCatalog {
    fn owner(&self) -> UserId {
        self.owner
    }
}
