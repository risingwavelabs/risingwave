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

use itertools::Itertools;
use risingwave_pb::catalog::{PbDatabase, PbSchema};
use risingwave_pb::user::grant_privilege::Object;

use super::OwnedByUserCatalog;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::{DatabaseId, SchemaId, TableId};
use crate::user::UserId;

#[derive(Clone, Debug)]
pub struct DatabaseCatalog {
    id: DatabaseId,
    pub name: String,
    schema_by_name: HashMap<String, SchemaCatalog>,
    schema_name_by_id: HashMap<SchemaId, String>,
    pub owner: u32,
    pub resource_group: String,
    pub barrier_interval_ms: Option<u32>,
    pub checkpoint_frequency: Option<u64>,
}

impl DatabaseCatalog {
    pub fn create_schema(&mut self, proto: &PbSchema) {
        let name = proto.name.clone();
        let id = proto.id;
        let schema = proto.into();
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

    pub fn iter_all_table_ids(&self) -> impl Iterator<Item = TableId> + '_ {
        self.schema_by_name
            .values()
            .flat_map(|schema| schema.iter_all().map(|t| t.id()))
    }

    pub fn iter_schemas(&self) -> impl Iterator<Item = &SchemaCatalog> {
        self.schema_by_name.values()
    }

    pub fn iter_schemas_mut(&mut self) -> impl Iterator<Item = &mut SchemaCatalog> {
        self.schema_by_name.values_mut()
    }

    pub fn get_schema_by_name(&self, name: &str) -> Option<&SchemaCatalog> {
        self.schema_by_name.get(name)
    }

    pub fn get_schema_by_id(&self, schema_id: &SchemaId) -> Option<&SchemaCatalog> {
        self.schema_by_name
            .get(self.schema_name_by_id.get(schema_id)?)
    }

    pub fn get_schema_mut(&mut self, schema_id: SchemaId) -> Option<&mut SchemaCatalog> {
        let name = self.schema_name_by_id.get(&schema_id).unwrap();
        self.schema_by_name.get_mut(name)
    }

    pub fn get_grant_object_by_oid(&self, oid: u32) -> Option<Object> {
        for schema in self.schema_by_name.values() {
            let object = schema.get_grant_object_by_oid(oid);
            if object.is_some() {
                return object;
            }
        }
        None
    }

    pub fn update_schema(&mut self, prost: &PbSchema) {
        let id = prost.id;
        let name = prost.name.clone();

        let old_schema_name = self.schema_name_by_id.get(&id).unwrap().to_owned();
        if old_schema_name != name {
            let mut schema = self.schema_by_name.remove(&old_schema_name).unwrap();
            schema.name.clone_from(&name);
            schema.database_id = prost.database_id;
            schema.owner = prost.owner;
            self.schema_by_name.insert(name.clone(), schema);
            self.schema_name_by_id.insert(id, name);
        } else {
            let schema = self.get_schema_mut(id).unwrap();
            schema.name.clone_from(&name);
            schema.database_id = prost.database_id;
            schema.owner = prost.owner;
        };
    }

    pub fn id(&self) -> DatabaseId {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn to_prost(&self) -> PbDatabase {
        PbDatabase {
            id: self.id,
            name: self.name.clone(),
            owner: self.owner,
            resource_group: self.resource_group.clone(),
            barrier_interval_ms: self.barrier_interval_ms,
            checkpoint_frequency: self.checkpoint_frequency,
        }
    }
}

impl OwnedByUserCatalog for DatabaseCatalog {
    fn owner(&self) -> UserId {
        self.owner
    }
}

impl From<&PbDatabase> for DatabaseCatalog {
    fn from(db: &PbDatabase) -> Self {
        Self {
            id: db.id,
            name: db.name.clone(),
            schema_by_name: HashMap::new(),
            schema_name_by_id: HashMap::new(),
            owner: db.owner,
            resource_group: db.resource_group.clone(),
            barrier_interval_ms: db.barrier_interval_ms,
            checkpoint_frequency: db.checkpoint_frequency,
        }
    }
}
