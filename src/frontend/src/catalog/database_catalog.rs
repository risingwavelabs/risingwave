// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use itertools::Itertools;
use risingwave_pb::catalog::{Database as ProstDatabase, Schema as ProstSchema};

use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::{DatabaseId, SchemaId};
#[derive(Clone, Debug)]
pub struct DatabaseCatalog {
    id: DatabaseId,
    #[allow(dead_code)]
    name: String,
    schema_by_name: HashMap<String, SchemaCatalog>,
    schema_name_by_id: HashMap<SchemaId, String>,
}

impl DatabaseCatalog {
    pub fn create_schema(&mut self, proto: ProstSchema) {
        let name = proto.name.clone();
        let id = proto.id;
        let schema = (&proto).into();
        self.schema_by_name
            .try_insert(name.clone(), schema)
            .unwrap();
        self.schema_name_by_id.try_insert(id, name).unwrap();
    }

    pub fn drop_schema(&mut self, schema_id: SchemaId) {
        let name = self.schema_name_by_id.remove(&schema_id).unwrap();
        self.schema_by_name.remove(&name).unwrap();
    }

    pub fn get_all_schema_names(&self) -> Vec<String> {
        self.schema_by_name.keys().cloned().collect_vec()
    }

    pub fn get_schema_by_name(&self, name: &str) -> Option<&SchemaCatalog> {
        self.schema_by_name.get(name)
    }

    pub fn get_schema_mut(&mut self, schema_id: SchemaId) -> Option<&mut SchemaCatalog> {
        let name = self.schema_name_by_id.get(&schema_id).unwrap();
        self.schema_by_name.get_mut(name)
    }

    pub fn id(&self) -> DatabaseId {
        self.id
    }
}
impl From<&ProstDatabase> for DatabaseCatalog {
    fn from(db: &ProstDatabase) -> Self {
        Self {
            id: db.id,
            name: db.name.clone(),
            schema_by_name: HashMap::new(),
            schema_name_by_id: HashMap::new(),
        }
    }
}
