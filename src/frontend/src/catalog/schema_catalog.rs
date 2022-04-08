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
use risingwave_common::catalog::TableId;
use risingwave_meta::manager::SourceId;
use risingwave_pb::catalog::{Schema as ProstSchema, Source as ProstSource, Table as ProstTable};

use super::source_catalog::SourceCatalog;
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::SchemaId;

#[derive(Clone, Debug)]
pub struct SchemaCatalog {
    id: SchemaId,
    #[allow(dead_code)]
    name: String,
    table_by_name: HashMap<String, TableCatalog>,
    table_name_by_id: HashMap<TableId, String>,
    source_by_name: HashMap<String, SourceCatalog>,
    source_name_by_id: HashMap<SourceId, String>,
}

impl SchemaCatalog {
    pub fn create_table(&mut self, prost: &ProstTable) {
        let name = prost.name.clone();
        let id = prost.id.into();
        let table = prost.into();

        self.table_by_name.try_insert(name.clone(), table).unwrap();
        self.table_name_by_id.try_insert(id, name).unwrap();
    }
    pub fn drop_table(&mut self, id: TableId) {
        let name = self.table_name_by_id.remove(&id).unwrap();
        self.table_by_name.remove(&name).unwrap();
    }

    pub fn create_source(&mut self, prost: ProstSource) {
        let name = prost.name.clone();
        let id = prost.id;

        self.source_by_name
            .try_insert(name.clone(), SourceCatalog::from(&prost))
            .unwrap();
        self.source_name_by_id.try_insert(id, name).unwrap();
    }
    pub fn drop_source(&mut self, id: SourceId) {
        let name = self.source_name_by_id.remove(&id).unwrap();
        self.source_by_name.remove(&name).unwrap();
    }

    // Use associated source to filter table.
    pub fn get_all_table_names(&self) -> Vec<String> {
        self.table_by_name
            .iter()
            .filter(|(_, v)| v.associated_source_id.is_some())
            .map(|(k, _)| k.clone())
            .collect_vec()
    }

    // Use associated source to filter mv.
    pub fn get_all_mv_names(&self) -> Vec<String> {
        self.table_by_name
            .iter()
            .filter(|(_, v)| v.associated_source_id.is_none())
            .map(|(k, _)| k.clone())
            .collect_vec()
    }

    pub fn get_table_by_name(&self, table_name: &str) -> Option<&TableCatalog> {
        self.table_by_name.get(table_name)
    }
    pub fn get_source_by_name(&self, source_name: &str) -> Option<&SourceCatalog> {
        self.source_by_name.get(source_name)
    }

    pub fn id(&self) -> SchemaId {
        self.id
    }
}

impl From<&ProstSchema> for SchemaCatalog {
    fn from(schema: &ProstSchema) -> Self {
        Self {
            id: schema.id,
            name: schema.name.clone(),
            table_by_name: HashMap::new(),
            table_name_by_id: HashMap::new(),
            source_by_name: HashMap::new(),
            source_name_by_id: HashMap::new(),
        }
    }
}
