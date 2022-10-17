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

use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::sync::Arc;

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
    table_by_name: HashMap<String, Arc<TableCatalog>>,
    table_by_id: HashMap<TableId, Arc<TableCatalog>>,
    source_by_name: HashMap<String, Arc<SourceCatalog>>,
    source_by_id: HashMap<SourceId, Arc<SourceCatalog>>,
    sink_by_name: HashMap<String, Arc<SinkCatalog>>,
    sink_by_id: HashMap<SinkId, Arc<SinkCatalog>>,
    index_by_name: HashMap<String, Arc<IndexCatalog>>,
    index_by_id: HashMap<IndexId, Arc<IndexCatalog>>,
    indexes_by_table_id: HashMap<TableId, Vec<Arc<IndexCatalog>>>,

    // This field only available when schema is "pg_catalog". Meanwhile, others will be empty.
    system_table_by_name: HashMap<String, SystemCatalog>,
    owner: u32,
}

impl SchemaCatalog {
    pub fn create_table(&mut self, prost: &ProstTable) {
        let name = prost.name.clone();
        let id = prost.id.into();
        let table: TableCatalog = prost.into();
        let table_ref = Arc::new(table);

        self.table_by_name
            .try_insert(name, table_ref.clone())
            .unwrap();
        self.table_by_id.try_insert(id, table_ref).unwrap();
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
        let table_ref = Arc::new(table);

        self.table_by_name.insert(name, table_ref.clone());
        self.table_by_id.insert(id, table_ref);
    }

    pub fn drop_table(&mut self, id: TableId) {
        let table_ref = self.table_by_id.remove(&id).unwrap();
        self.table_by_name.remove(&table_ref.name).unwrap();
        self.indexes_by_table_id.remove(&table_ref.id);
    }

    pub fn create_index(&mut self, prost: &ProstIndex) {
        let name = prost.name.clone();
        let id = prost.id.into();

        let index_table = self.get_table_by_id(&prost.index_table_id.into()).unwrap();
        let primary_table = self
            .get_table_by_id(&prost.primary_table_id.into())
            .unwrap();
        let index: IndexCatalog = IndexCatalog::build_from(prost, index_table, primary_table);
        let index_ref = Arc::new(index);

        self.index_by_name
            .try_insert(name, index_ref.clone())
            .unwrap();
        self.index_by_id.try_insert(id, index_ref.clone()).unwrap();
        match self.indexes_by_table_id.entry(index_ref.primary_table.id) {
            Occupied(mut entry) => {
                entry.get_mut().push(index_ref);
            }
            Vacant(entry) => {
                entry.insert(vec![index_ref]);
            }
        };
    }

    pub fn drop_index(&mut self, id: IndexId) {
        let index_ref = self.index_by_id.remove(&id).unwrap();
        self.index_by_name.remove(&index_ref.name).unwrap();
        match self.indexes_by_table_id.entry(index_ref.primary_table.id) {
            Occupied(mut entry) => {
                let pos = entry
                    .get_mut()
                    .iter()
                    .position(|x| x.id == index_ref.id)
                    .unwrap();
                entry.get_mut().remove(pos);
            }
            Vacant(_entry) => unreachable!(),
        };
    }

    pub fn create_source(&mut self, prost: ProstSource) {
        let name = prost.name.clone();
        let id = prost.id;
        let source = SourceCatalog::from(&prost);
        let source_ref = Arc::new(source);

        self.source_by_name
            .try_insert(name, source_ref.clone())
            .unwrap();
        self.source_by_id.try_insert(id, source_ref).unwrap();
    }

    pub fn drop_source(&mut self, id: SourceId) {
        let source_ref = self.source_by_id.remove(&id).unwrap();
        self.source_by_name.remove(&source_ref.name).unwrap();
    }

    pub fn create_sink(&mut self, prost: ProstSink) {
        let name = prost.name.clone();
        let id = prost.id;
        let sink = SinkCatalog::from(&prost);
        let sink_ref = Arc::new(sink);

        self.sink_by_name
            .try_insert(name, sink_ref.clone())
            .unwrap();
        self.sink_by_id.try_insert(id, sink_ref).unwrap();
    }

    pub fn drop_sink(&mut self, id: SinkId) {
        let sink_ref = self.sink_by_id.remove(&id).unwrap();
        self.sink_by_name.remove(&sink_ref.name).unwrap();
    }

    pub fn iter_table(&self) -> impl Iterator<Item = &Arc<TableCatalog>> {
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
    pub fn iter_mv(&self) -> impl Iterator<Item = &Arc<TableCatalog>> {
        self.table_by_name
            .iter()
            .filter(|(_, v)| {
                v.associated_source_id.is_none() && valid_table_name(&v.name) && !v.is_index
            })
            .map(|(_, v)| v)
    }

    /// Iterate all indices
    pub fn iter_index(&self) -> impl Iterator<Item = &Arc<IndexCatalog>> {
        self.index_by_name.iter().map(|(_, v)| v)
    }

    /// Iterate all sources, including the materialized sources.
    pub fn iter_source(&self) -> impl Iterator<Item = &Arc<SourceCatalog>> {
        self.source_by_name
            .iter()
            .filter(|(_, v)| v.is_stream())
            .map(|(_, v)| v)
    }

    /// Iterate the materialized sources.
    pub fn iter_materialized_source(&self) -> impl Iterator<Item = &Arc<SourceCatalog>> {
        self.source_by_name
            .iter()
            .filter(|(name, v)| v.is_stream() && self.table_by_name.get(*name).is_some())
            .map(|(_, v)| v)
    }

    pub fn iter_sink(&self) -> impl Iterator<Item = &Arc<SinkCatalog>> {
        self.sink_by_name.iter().map(|(_, v)| v)
    }

    pub fn iter_system_tables(&self) -> impl Iterator<Item = &SystemCatalog> {
        self.system_table_by_name.iter().map(|(_, v)| v)
    }

    pub fn get_table_by_name(&self, table_name: &str) -> Option<&Arc<TableCatalog>> {
        self.table_by_name.get(table_name)
    }

    pub fn get_table_by_id(&self, table_id: &TableId) -> Option<&Arc<TableCatalog>> {
        self.table_by_id.get(table_id)
    }

    pub fn get_source_by_name(&self, source_name: &str) -> Option<&Arc<SourceCatalog>> {
        self.source_by_name.get(source_name)
    }

    pub fn get_sink_by_name(&self, sink_name: &str) -> Option<&Arc<SinkCatalog>> {
        self.sink_by_name.get(sink_name)
    }

    pub fn get_index_by_name(&self, index_name: &str) -> Option<&Arc<IndexCatalog>> {
        self.index_by_name.get(index_name)
    }

    pub fn get_index_by_id(&self, index_id: &IndexId) -> Option<&Arc<IndexCatalog>> {
        self.index_by_id.get(index_id)
    }

    pub fn get_indexes_by_table_id(&self, table_id: &TableId) -> Vec<Arc<IndexCatalog>> {
        self.indexes_by_table_id
            .get(table_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn get_system_table_by_name(&self, table_name: &str) -> Option<&SystemCatalog> {
        self.system_table_by_name.get(table_name)
    }

    pub fn get_table_name_by_id(&self, table_id: TableId) -> Option<String> {
        self.table_by_id
            .get(&table_id)
            .map(|table| table.name.clone())
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
            table_by_id: HashMap::new(),
            source_by_name: HashMap::new(),
            source_by_id: HashMap::new(),
            sink_by_name: HashMap::new(),
            sink_by_id: HashMap::new(),
            index_by_name: HashMap::new(),
            index_by_id: HashMap::new(),
            indexes_by_table_id: HashMap::new(),
            system_table_by_name: HashMap::new(),
            owner: schema.owner,
        }
    }
}
