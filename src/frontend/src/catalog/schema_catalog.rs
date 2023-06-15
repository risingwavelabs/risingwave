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

use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::catalog::{valid_table_name, FunctionId, IndexId, TableId};
use risingwave_common::types::DataType;
use risingwave_connector::sink::catalog::SinkCatalog;
use risingwave_pb::catalog::{
    PbConnection, PbFunction, PbIndex, PbSchema, PbSink, PbSource, PbTable, PbView,
};

use crate::catalog::connection_catalog::ConnectionCatalog;
use crate::catalog::function_catalog::FunctionCatalog;
use crate::catalog::index_catalog::IndexCatalog;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::system_catalog::SystemCatalog;
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::view_catalog::ViewCatalog;
use crate::catalog::{ConnectionId, SchemaId, SinkId, SourceId, ViewId};

#[derive(Clone, Debug)]
pub struct SchemaCatalog {
    pub(crate) id: SchemaId,
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
    view_by_name: HashMap<String, Arc<ViewCatalog>>,
    view_by_id: HashMap<ViewId, Arc<ViewCatalog>>,
    function_by_name: HashMap<String, HashMap<Vec<DataType>, Arc<FunctionCatalog>>>,
    function_by_id: HashMap<FunctionId, Arc<FunctionCatalog>>,
    connection_by_name: HashMap<String, Arc<ConnectionCatalog>>,
    connection_by_id: HashMap<ConnectionId, Arc<ConnectionCatalog>>,

    // This field is currently used only for `show connections`
    connection_source_ref: HashMap<ConnectionId, Vec<SourceId>>,
    // This field is currently used only for `show connections`
    connection_sink_ref: HashMap<ConnectionId, Vec<SinkId>>,
    // This field only available when schema is "pg_catalog". Meanwhile, others will be empty.
    system_table_by_name: HashMap<String, Arc<SystemCatalog>>,
    owner: u32,
}

impl SchemaCatalog {
    pub fn create_table(&mut self, prost: &PbTable) -> Arc<TableCatalog> {
        let name = prost.name.clone();
        let id = prost.id.into();
        let table: TableCatalog = prost.into();
        let table_ref = Arc::new(table);

        self.table_by_name
            .try_insert(name, table_ref.clone())
            .unwrap();
        self.table_by_id.try_insert(id, table_ref.clone()).unwrap();
        table_ref
    }

    pub fn create_sys_table(&mut self, sys_table: Arc<SystemCatalog>) {
        self.system_table_by_name
            .try_insert(sys_table.name.clone(), sys_table)
            .unwrap();
    }

    pub fn update_table(&mut self, prost: &PbTable) -> Arc<TableCatalog> {
        let name = prost.name.clone();
        let id = prost.id.into();
        let table: TableCatalog = prost.into();
        let table_ref = Arc::new(table);

        let old_table = self.table_by_id.get(&id).unwrap();
        // check if table name get updated.
        if old_table.name() != name {
            self.table_by_name.remove(old_table.name());
        }
        self.table_by_name.insert(name, table_ref.clone());
        self.table_by_id.insert(id, table_ref.clone());
        table_ref
    }

    pub fn update_index(&mut self, prost: &PbIndex) {
        let name = prost.name.clone();
        let id = prost.id.into();
        let old_index = self.index_by_id.get(&id).unwrap();
        let index_table = self.get_table_by_id(&prost.index_table_id.into()).unwrap();
        let primary_table = self
            .get_table_by_id(&prost.primary_table_id.into())
            .unwrap();
        let index: IndexCatalog = IndexCatalog::build_from(prost, index_table, primary_table);
        let index_ref = Arc::new(index);

        // check if index name get updated.
        if old_index.name != name {
            self.index_by_name.remove(&old_index.name);
        }
        self.index_by_name.insert(name, index_ref.clone());
        self.index_by_id.insert(id, index_ref.clone());

        match self.indexes_by_table_id.entry(index_ref.primary_table.id) {
            Occupied(mut entry) => {
                let pos = entry
                    .get()
                    .iter()
                    .position(|x| x.id == index_ref.id)
                    .unwrap();
                *entry.get_mut().get_mut(pos).unwrap() = index_ref;
            }
            Vacant(_entry) => {
                unreachable!()
            }
        };
    }

    pub fn drop_table(&mut self, id: TableId) {
        let table_ref = self.table_by_id.remove(&id).unwrap();
        self.table_by_name.remove(&table_ref.name).unwrap();
        self.indexes_by_table_id.remove(&table_ref.id);
    }

    pub fn create_index(&mut self, prost: &PbIndex) {
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

    pub fn create_source(&mut self, prost: &PbSource) {
        let name = prost.name.clone();
        let id = prost.id;
        let source = SourceCatalog::from(prost);
        let source_ref = Arc::new(source);

        if let Some(connection_id) = source_ref.connection_id {
            self.connection_source_ref
                .entry(connection_id)
                .and_modify(|sources| sources.push(source_ref.id))
                .or_insert(vec![source_ref.id]);
        }

        self.source_by_name
            .try_insert(name, source_ref.clone())
            .unwrap();
        self.source_by_id.try_insert(id, source_ref).unwrap();
    }

    pub fn drop_source(&mut self, id: SourceId) {
        let source_ref = self.source_by_id.remove(&id).unwrap();
        self.source_by_name.remove(&source_ref.name).unwrap();
        if let Some(connection_id) = source_ref.connection_id {
            if let Occupied(mut e) = self.connection_source_ref.entry(connection_id) {
                let source_ids = e.get_mut();
                source_ids.retain_mut(|sid| *sid != id);
                if source_ids.is_empty() {
                    e.remove_entry();
                }
            }
        }
    }

    pub fn update_source(&mut self, prost: &PbSource) {
        let name = prost.name.clone();
        let id = prost.id;
        let source = SourceCatalog::from(prost);
        let source_ref = Arc::new(source);

        let old_source = self.source_by_id.get(&id).unwrap();
        // check if source name get updated.
        if old_source.name != name {
            self.source_by_name.remove(&old_source.name);
        }

        self.source_by_name.insert(name, source_ref.clone());
        self.source_by_id.insert(id, source_ref);
    }

    pub fn create_sink(&mut self, prost: &PbSink) {
        let name = prost.name.clone();
        let id = prost.id;
        let sink = SinkCatalog::from(prost);
        let sink_ref = Arc::new(sink);

        if let Some(connection_id) = sink_ref.connection_id {
            self.connection_sink_ref
                .entry(connection_id.0)
                .and_modify(|sinks| sinks.push(id))
                .or_insert(vec![id]);
        }

        self.sink_by_name
            .try_insert(name, sink_ref.clone())
            .unwrap();
        self.sink_by_id.try_insert(id, sink_ref).unwrap();
    }

    pub fn drop_sink(&mut self, id: SinkId) {
        let sink_ref = self.sink_by_id.remove(&id).unwrap();
        self.sink_by_name.remove(&sink_ref.name).unwrap();
        if let Some(connection_id) = sink_ref.connection_id {
            if let Occupied(mut e) = self.connection_sink_ref.entry(connection_id.0) {
                let sink_ids = e.get_mut();
                sink_ids.retain_mut(|sid| *sid != id);
                if sink_ids.is_empty() {
                    e.remove_entry();
                }
            }
        }
    }

    pub fn update_sink(&mut self, prost: &PbSink) {
        let name = prost.name.clone();
        let id = prost.id;
        let sink = SinkCatalog::from(prost);
        let sink_ref = Arc::new(sink);

        let old_sink = self.sink_by_id.get(&id).unwrap();
        // check if sink name get updated.
        if old_sink.name != name {
            self.sink_by_name.remove(&old_sink.name);
        }

        self.sink_by_name.insert(name, sink_ref.clone());
        self.sink_by_id.insert(id, sink_ref);
    }

    pub fn create_view(&mut self, prost: &PbView) {
        let name = prost.name.clone();
        let id = prost.id;
        let view = ViewCatalog::from(prost);
        let view_ref = Arc::new(view);

        self.view_by_name
            .try_insert(name, view_ref.clone())
            .unwrap();
        self.view_by_id.try_insert(id, view_ref).unwrap();
    }

    pub fn drop_view(&mut self, id: ViewId) {
        let view_ref = self.view_by_id.remove(&id).unwrap();
        self.view_by_name.remove(&view_ref.name).unwrap();
    }

    pub fn update_view(&mut self, prost: &PbView) {
        let name = prost.name.clone();
        let id = prost.id;
        let view = ViewCatalog::from(prost);
        let view_ref = Arc::new(view);

        let old_view = self.view_by_id.get(&id).unwrap();
        // check if view name get updated.
        if old_view.name != name {
            self.view_by_name.remove(&old_view.name);
        }

        self.view_by_name.insert(name, view_ref.clone());
        self.view_by_id.insert(id, view_ref);
    }

    pub fn create_function(&mut self, prost: &PbFunction) {
        let name = prost.name.clone();
        let id = prost.id;
        let function = FunctionCatalog::from(prost);
        let args = function.arg_types.clone();
        let function_ref = Arc::new(function);

        self.function_by_name
            .entry(name)
            .or_default()
            .try_insert(args, function_ref.clone())
            .expect("function already exists with same argument types");
        self.function_by_id
            .try_insert(id.into(), function_ref)
            .expect("function id exists");
    }

    pub fn drop_function(&mut self, id: FunctionId) {
        let function_ref = self
            .function_by_id
            .remove(&id)
            .expect("function not found by id");
        self.function_by_name
            .get_mut(&function_ref.name)
            .expect("function not found by name")
            .remove(&function_ref.arg_types)
            .expect("function not found by argument types");
    }

    pub fn create_connection(&mut self, prost: &PbConnection) {
        let name = prost.name.clone();
        let id = prost.id;
        let connection = ConnectionCatalog::from(prost);
        let connection_ref = Arc::new(connection);
        self.connection_by_name
            .try_insert(name, connection_ref.clone())
            .unwrap();
        self.connection_by_id
            .try_insert(id, connection_ref)
            .unwrap();
    }

    pub fn drop_connection(&mut self, connection_id: ConnectionId) {
        let connection_ref = self
            .connection_by_id
            .remove(&connection_id)
            .expect("connection not found by id");
        self.connection_by_name
            .remove(&connection_ref.name)
            .expect("connection not found by name");
    }

    pub fn iter_all(&self) -> impl Iterator<Item = &Arc<TableCatalog>> {
        self.table_by_name.values()
    }

    pub fn iter_table(&self) -> impl Iterator<Item = &Arc<TableCatalog>> {
        self.table_by_name
            .iter()
            .filter(|(_, v)| v.is_table())
            .map(|(_, v)| v)
    }

    pub fn iter_internal_table(&self) -> impl Iterator<Item = &Arc<TableCatalog>> {
        self.table_by_name
            .iter()
            .filter(|(_, v)| v.is_internal_table())
            .map(|(_, v)| v)
    }

    pub fn iter_valid_table(&self) -> impl Iterator<Item = &Arc<TableCatalog>> {
        self.table_by_name
            .iter()
            .filter_map(|(key, v)| valid_table_name(key).then_some(v))
    }

    /// Iterate all materialized views, excluding the indices.
    pub fn iter_mv(&self) -> impl Iterator<Item = &Arc<TableCatalog>> {
        self.table_by_name
            .iter()
            .filter(|(_, v)| v.is_mview() && valid_table_name(&v.name))
            .map(|(_, v)| v)
    }

    /// Iterate all indices
    pub fn iter_index(&self) -> impl Iterator<Item = &Arc<IndexCatalog>> {
        self.index_by_name.values()
    }

    /// Iterate all sources
    pub fn iter_source(&self) -> impl Iterator<Item = &Arc<SourceCatalog>> {
        self.source_by_name.values()
    }

    pub fn iter_sink(&self) -> impl Iterator<Item = &Arc<SinkCatalog>> {
        self.sink_by_name.values()
    }

    pub fn iter_view(&self) -> impl Iterator<Item = &Arc<ViewCatalog>> {
        self.view_by_name.values()
    }

    pub fn iter_function(&self) -> impl Iterator<Item = &Arc<FunctionCatalog>> {
        self.function_by_name.values().flat_map(|v| v.values())
    }

    pub fn iter_connections(&self) -> impl Iterator<Item = &Arc<ConnectionCatalog>> {
        self.connection_by_name.values()
    }

    pub fn iter_system_tables(&self) -> impl Iterator<Item = &Arc<SystemCatalog>> {
        self.system_table_by_name.values()
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

    pub fn get_source_by_id(&self, source_id: &SourceId) -> Option<&Arc<SourceCatalog>> {
        self.source_by_id.get(source_id)
    }

    pub fn get_sink_by_name(&self, sink_name: &str) -> Option<&Arc<SinkCatalog>> {
        self.sink_by_name.get(sink_name)
    }

    pub fn get_sink_by_id(&self, sink_id: &SinkId) -> Option<&Arc<SinkCatalog>> {
        self.sink_by_id.get(sink_id)
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

    pub fn get_system_table_by_name(&self, table_name: &str) -> Option<&Arc<SystemCatalog>> {
        self.system_table_by_name.get(table_name)
    }

    pub fn get_table_name_by_id(&self, table_id: TableId) -> Option<String> {
        self.table_by_id
            .get(&table_id)
            .map(|table| table.name.clone())
    }

    pub fn get_view_by_name(&self, view_name: &str) -> Option<&Arc<ViewCatalog>> {
        self.view_by_name.get(view_name)
    }

    pub fn get_function_by_name_args(
        &self,
        name: &str,
        args: &[DataType],
    ) -> Option<&Arc<FunctionCatalog>> {
        self.function_by_name.get(name)?.get(args)
    }

    pub fn get_functions_by_name(&self, name: &str) -> Option<Vec<&Arc<FunctionCatalog>>> {
        let functions = self.function_by_name.get(name)?;
        if functions.is_empty() {
            return None;
        }
        Some(functions.values().collect())
    }

    pub fn get_connection_by_name(&self, connection_name: &str) -> Option<&Arc<ConnectionCatalog>> {
        self.connection_by_name.get(connection_name)
    }

    /// get all sources referencing the connection
    pub fn get_source_ids_by_connection(
        &self,
        connection_id: ConnectionId,
    ) -> Option<Vec<SourceId>> {
        self.connection_source_ref
            .get(&connection_id)
            .map(|c| c.to_owned())
    }

    /// get all sinks referencing the connection
    pub fn get_sink_ids_by_connection(&self, connection_id: ConnectionId) -> Option<Vec<SinkId>> {
        self.connection_sink_ref
            .get(&connection_id)
            .map(|s| s.to_owned())
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

impl From<&PbSchema> for SchemaCatalog {
    fn from(schema: &PbSchema) -> Self {
        Self {
            id: schema.id,
            owner: schema.owner,
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
            view_by_name: HashMap::new(),
            view_by_id: HashMap::new(),
            function_by_name: HashMap::new(),
            function_by_id: HashMap::new(),
            connection_by_name: HashMap::new(),
            connection_by_id: HashMap::new(),
            connection_source_ref: HashMap::new(),
            connection_sink_ref: HashMap::new(),
        }
    }
}
