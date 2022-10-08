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

use risingwave_common::catalog::{valid_table_name, IndexId, TableId, PG_CATALOG_SCHEMA_NAME};
use risingwave_pb::catalog::{
    Index as ProstIndex, Schema as ProstSchema, Sink as ProstSink, Source as ProstSource,
    Table as ProstTable,
};

use super::source_catalog::SourceCatalog;
use crate::catalog::index_catalog::IndexCatalog;
use crate::catalog::sink_catalog::SinkCatalog;
use crate::catalog::system_catalog::SystemCatalog;
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::SchemaId;

pub type SourceId = u32;
pub type SinkId = u32;

#[derive(Clone, Debug)]
pub struct SchemaCatalog {
    id: SchemaId,
    name: String,
    table_by_name: HashMap<String, TableCatalog>,
    table_name_by_id: HashMap<TableId, String>,
    source_by_name: HashMap<String, SourceCatalog>,
    source_name_by_id: HashMap<SourceId, String>,
    sink_by_name: HashMap<String, SinkCatalog>,
    sink_name_by_id: HashMap<SinkId, String>,
    index_by_name: HashMap<String, IndexCatalog>,
    index_name_by_id: HashMap<IndexId, String>,

    // This field only available when schema is "pg_catalog". Meanwhile, others will be empty.
    system_table_by_name: HashMap<String, SystemCatalog>,
    owner: u32,
}

impl SchemaCatalog {
    pub fn create_table(&mut self, prost: &ProstTable) {
        let name = prost.name.clone();
        let id = prost.id.into();
        let table: TableCatalog = prost.into();

        self.table_by_name.try_insert(name.clone(), table).unwrap();
        self.table_name_by_id.try_insert(id, name).unwrap();
    }

    pub fn create_sys_table(&mut self, sys_table: SystemCatalog) {
        assert_eq!(self.name, PG_CATALOG_SCHEMA_NAME);
        self.system_table_by_name
            .try_insert(sys_table.name.clone(), sys_table)
            .unwrap();
    }

    pub fn update_table(&mut self, prost: &ProstTable) {
        let name = prost.name.clone();
        let id = prost.id.into();
        let table: TableCatalog = prost.into();

        self.table_by_name.insert(name.clone(), table);
        self.table_name_by_id.insert(id, name);
    }

    pub fn drop_table(&mut self, id: TableId) {
        let name = self.table_name_by_id.remove(&id).unwrap();
        self.table_by_name.remove(&name).unwrap();
    }

    pub fn create_index(&mut self, prost: &ProstIndex) {
        let name = prost.name.clone();
        let id = prost.id.into();

        let index_table = self.get_table_by_id(&prost.index_table_id.into()).unwrap();
        let primary_table = self
            .get_table_by_id(&prost.primary_table_id.into())
            .unwrap();
        let index: IndexCatalog = IndexCatalog::build_from(prost, index_table, primary_table);

        self.index_by_name.try_insert(name.clone(), index).unwrap();
        self.index_name_by_id.try_insert(id, name).unwrap();
    }

    pub fn drop_index(&mut self, id: IndexId) {
        let name = self.index_name_by_id.remove(&id).unwrap();
        self.index_by_name.remove(&name).unwrap();
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

    pub fn create_sink(&mut self, prost: ProstSink) {
        let name = prost.name.clone();
        let id = prost.id;

        self.sink_by_name
            .try_insert(name.clone(), SinkCatalog::from(&prost))
            .unwrap();
        self.sink_name_by_id.try_insert(id, name).unwrap();
    }

    pub fn drop_sink(&mut self, id: SinkId) {
        let name = self.sink_name_by_id.remove(&id).unwrap();
        self.sink_by_name.remove(&name).unwrap();
    }

    pub fn iter_table(&self) -> impl Iterator<Item = &TableCatalog> {
        self.table_by_name
            .iter()
            .filter(|(_, v)| {
                // Internally, a table with an associated source can be
                // MATERIALIZED SOURCE or TABLE.
                v.associated_source_id.is_some()
                    && self.get_source_by_name(v.name()).unwrap().is_table()
            })
            .map(|(_, v)| v)
    }

    /// Iterate all materialized views, excluding the indices.
    pub fn iter_mv(&self) -> impl Iterator<Item = &TableCatalog> {
        self.table_by_name
            .iter()
            .filter(|(_, v)| v.associated_source_id.is_none() && valid_table_name(&v.name))
            .map(|(_, v)| v)
    }

    /// Iterate all indices
    pub fn iter_index(&self) -> impl Iterator<Item = &IndexCatalog> {
        self.index_by_name.iter().map(|(_, v)| v)
    }

    /// Iterate all sources, including the materialized sources.
    pub fn iter_source(&self) -> impl Iterator<Item = &SourceCatalog> {
        self.source_by_name
            .iter()
            .filter(|(_, v)| v.is_stream())
            .map(|(_, v)| v)
    }

    /// Iterate the materialized sources.
    pub fn iter_materialized_source(&self) -> impl Iterator<Item = &SourceCatalog> {
        self.source_by_name
            .iter()
            .filter(|(name, v)| v.is_stream() && self.table_by_name.get(*name).is_some())
            .map(|(_, v)| v)
    }

    pub fn iter_sink(&self) -> impl Iterator<Item = &SinkCatalog> {
        self.sink_by_name.iter().map(|(_, v)| v)
    }

    pub fn iter_system_tables(&self) -> impl Iterator<Item = &SystemCatalog> {
        self.system_table_by_name.iter().map(|(_, v)| v)
    }

    pub fn get_table_by_name(&self, table_name: &str) -> Option<&TableCatalog> {
        self.table_by_name.get(table_name)
    }

    pub fn get_table_by_id(&self, table_id: &TableId) -> Option<&TableCatalog> {
        self.table_by_name.get(self.table_name_by_id.get(table_id)?)
    }

    pub fn get_source_by_name(&self, source_name: &str) -> Option<&SourceCatalog> {
        self.source_by_name.get(source_name)
    }

    pub fn get_sink_by_name(&self, sink_name: &str) -> Option<&SinkCatalog> {
        self.sink_by_name.get(sink_name)
    }

    pub fn get_index_by_name(&self, index_name: &str) -> Option<&IndexCatalog> {
        self.index_by_name.get(index_name)
    }

    pub fn get_index_by_id(&self, index_id: &IndexId) -> Option<&IndexCatalog> {
        self.index_by_name.get(self.index_name_by_id.get(index_id)?)
    }

    pub fn get_system_table_by_name(&self, table_name: &str) -> Option<&SystemCatalog> {
        self.system_table_by_name.get(table_name)
    }

    pub fn get_table_name_by_id(&self, table_id: TableId) -> Option<String> {
        self.table_name_by_id.get(&table_id).cloned()
    }

    pub fn id(&self) -> SchemaId {
        self.id
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn owner(&self) -> u32 {
        self.owner
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
            sink_by_name: HashMap::new(),
            sink_name_by_id: HashMap::new(),
            index_by_name: HashMap::new(),
            index_name_by_id: HashMap::new(),
            system_table_by_name: HashMap::new(),
            owner: schema.owner,
        }
    }
}
