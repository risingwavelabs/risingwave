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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_pb::catalog::{Database, Index, Schema, Sink, Source, Table};

use super::{DatabaseId, RelationId, SchemaId, SinkId, SourceId};
use crate::manager::{MetaSrvEnv, TableId};
use crate::model::MetadataModel;
use crate::storage::MetaStore;
use crate::MetaResult;

pub type Catalog = (
    Vec<Database>,
    Vec<Schema>,
    Vec<Table>,
    Vec<Source>,
    Vec<Sink>,
    Vec<Index>,
);

type DatabaseKey = String;
type SchemaKey = (DatabaseId, String);
type TableKey = (DatabaseId, SchemaId, String);
type SourceKey = (DatabaseId, SchemaId, String);
type SinkKey = (DatabaseId, SchemaId, String);
type IndexKey = (DatabaseId, SchemaId, String);
type RelationKey = (DatabaseId, SchemaId, String);

/// [`DatabaseManager`] caches meta catalog information and maintains dependent relationship
/// between tables.
pub struct DatabaseManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    /// Cached database key information.
    databases: HashSet<DatabaseKey>,
    /// Cached schema key information.
    schemas: HashSet<SchemaKey>,
    /// Cached source key information.
    sources: HashSet<SourceKey>,
    /// Cached sink key information.
    sinks: HashSet<SinkKey>,
    /// Cached table key information.
    tables: HashSet<TableKey>,
    /// Cached index key information.
    indexes: HashSet<IndexKey>,
    /// Relation refer count mapping.
    // TODO(zehua): avoid key conflicts after distinguishing table's and source's id generator.
    pub(super) relation_ref_count: HashMap<RelationId, usize>,

    // In-progress creation tracker
    in_progress_creation_tracker: HashSet<RelationKey>,
    // In-progress creating streaming job tracker: this is a temporary workaround to avoid clean up
    // creating streaming jobs.
    in_progress_creation_streaming_job: HashSet<TableId>,
    // In-progress creating tables, including internal tables.
    in_progress_creating_tables: HashMap<TableId, Table>,
}

impl<S> DatabaseManager<S>
where
    S: MetaStore,
{
    pub async fn new(env: MetaSrvEnv<S>) -> MetaResult<Self> {
        let databases = Database::list(env.meta_store()).await?;
        let schemas = Schema::list(env.meta_store()).await?;
        let sources = Source::list(env.meta_store()).await?;
        let sinks = Sink::list(env.meta_store()).await?;
        let tables = Table::list(env.meta_store()).await?;
        let indexes = Index::list(env.meta_store()).await?;

        let mut relation_ref_count = HashMap::new();

        let databases = HashSet::from_iter(databases.into_iter().map(|database| (database.name)));
        let schemas = HashSet::from_iter(
            schemas
                .into_iter()
                .map(|schema| (schema.database_id, schema.name)),
        );
        let sources = HashSet::from_iter(
            sources
                .into_iter()
                .map(|source| (source.database_id, source.schema_id, source.name)),
        );
        let sinks = HashSet::from_iter(
            sinks
                .into_iter()
                .map(|sink| (sink.database_id, sink.schema_id, sink.name)),
        );
        let indexes = HashSet::from_iter(
            indexes
                .into_iter()
                .map(|index| (index.database_id, index.schema_id, index.name)),
        );
        let tables = HashSet::from_iter(tables.into_iter().map(|table| {
            for depend_relation_id in &table.dependent_relations {
                relation_ref_count
                    .entry(*depend_relation_id)
                    .and_modify(|e| *e += 1)
                    .or_insert(1);
            }
            (table.database_id, table.schema_id, table.name)
        }));

        Ok(Self {
            env,
            databases,
            schemas,
            sources,
            sinks,
            tables,
            indexes,
            relation_ref_count,
            in_progress_creation_tracker: HashSet::default(),
            in_progress_creation_streaming_job: HashSet::default(),
            in_progress_creating_tables: HashMap::default(),
        })
    }

    pub async fn get_catalog(&self) -> MetaResult<Catalog> {
        Ok((
            Database::list(self.env.meta_store()).await?,
            Schema::list(self.env.meta_store()).await?,
            Table::list(self.env.meta_store()).await?,
            Source::list(self.env.meta_store()).await?,
            Sink::list(self.env.meta_store()).await?,
            Index::list(self.env.meta_store()).await?,
        ))
    }

    pub fn list_creating_tables(&self) -> Vec<Table> {
        self.in_progress_creating_tables
            .values()
            .cloned()
            .collect_vec()
    }

    pub async fn list_sources(&self) -> MetaResult<Vec<Source>> {
        Source::list(self.env.meta_store())
            .await
            .map_err(Into::into)
    }

    pub async fn list_source_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<SourceId>> {
        let sources = Source::list(self.env.meta_store()).await?;
        Ok(sources
            .iter()
            .filter(|s| s.schema_id == schema_id)
            .map(|s| s.id)
            .collect())
    }

    pub async fn list_stream_job_ids(&self) -> MetaResult<impl Iterator<Item = RelationId> + '_> {
        let tables = Table::list(self.env.meta_store()).await?;
        let sinks = Sink::list(self.env.meta_store()).await?;
        let indexes = Index::list(self.env.meta_store()).await?;
        Ok(tables
            .into_iter()
            .map(|t| t.id)
            .chain(sinks.into_iter().map(|s| s.id))
            .chain(indexes.into_iter().map(|i| i.id)))
    }

    pub fn has_database(&self, database: &Database) -> bool {
        self.databases.contains(database.get_name())
    }

    pub fn add_database(&mut self, database: &Database) {
        self.databases.insert(database.name.clone());
    }

    pub fn drop_database(&mut self, database: &Database) -> bool {
        self.databases.remove(database.get_name())
    }

    pub fn has_schema(&self, schema: &Schema) -> bool {
        self.schemas
            .contains(&(schema.database_id, schema.name.clone()))
    }

    pub fn add_schema(&mut self, schema: &Schema) {
        self.schemas
            .insert((schema.database_id, schema.name.clone()));
    }

    pub fn drop_schema(&mut self, schema: &Schema) -> bool {
        self.schemas
            .remove(&(schema.database_id, schema.name.clone()))
    }

    pub fn has_table(&self, table: &Table) -> bool {
        self.tables
            .contains(&(table.database_id, table.schema_id, table.name.clone()))
    }

    pub fn add_table(&mut self, table: &Table) {
        self.tables
            .insert((table.database_id, table.schema_id, table.name.clone()));
    }

    pub fn drop_table(&mut self, table: &Table) -> bool {
        self.tables
            .remove(&(table.database_id, table.schema_id, table.name.clone()))
    }

    pub fn has_source(&self, source: &Source) -> bool {
        self.sources
            .contains(&(source.database_id, source.schema_id, source.name.clone()))
    }

    pub fn add_source(&mut self, source: &Source) {
        self.sources
            .insert((source.database_id, source.schema_id, source.name.clone()));
    }

    pub fn drop_source(&mut self, source: &Source) -> bool {
        self.sources
            .remove(&(source.database_id, source.schema_id, source.name.clone()))
    }

    pub async fn get_source(&self, id: SourceId) -> MetaResult<Option<Source>> {
        Source::select(self.env.meta_store(), &id)
            .await
            .map_err(Into::into)
    }

    pub fn has_sink(&self, sink: &Sink) -> bool {
        self.sinks
            .contains(&(sink.database_id, sink.schema_id, sink.name.clone()))
    }

    pub fn add_sink(&mut self, sink: &Sink) {
        self.sinks
            .insert((sink.database_id, sink.schema_id, sink.name.clone()));
    }

    pub fn drop_sink(&mut self, sink: &Sink) -> bool {
        self.sinks
            .remove(&(sink.database_id, sink.schema_id, sink.name.clone()))
    }

    pub async fn get_sink(&self, id: SinkId) -> MetaResult<Option<Sink>> {
        Sink::select(self.env.meta_store(), &id)
            .await
            .map_err(Into::into)
    }

    pub fn has_index(&self, index: &Index) -> bool {
        self.indexes
            .contains(&(index.database_id, index.schema_id, index.name.clone()))
    }

    pub fn add_index(&mut self, index: &Index) {
        self.indexes
            .insert((index.database_id, index.schema_id, index.name.clone()));
    }

    pub fn drop_index(&mut self, index: &Index) -> bool {
        self.indexes
            .remove(&(index.database_id, index.schema_id, index.name.clone()))
    }

    pub fn get_ref_count(&self, relation_id: RelationId) -> Option<usize> {
        self.relation_ref_count.get(&relation_id).cloned()
    }

    pub fn increase_ref_count(&mut self, relation_id: RelationId) {
        *self.relation_ref_count.entry(relation_id).or_insert(0) += 1;
    }

    pub fn decrease_ref_count(&mut self, relation_id: RelationId) {
        match self.relation_ref_count.entry(relation_id) {
            Entry::Occupied(mut o) => {
                *o.get_mut() -= 1;
                if *o.get() == 0 {
                    o.remove_entry();
                }
            }
            Entry::Vacant(_) => unreachable!(),
        }
    }

    pub fn has_in_progress_creation(&self, relation: &RelationKey) -> bool {
        self.in_progress_creation_tracker
            .contains(&relation.clone())
    }

    pub fn mark_creating(&mut self, relation: &RelationKey) {
        self.in_progress_creation_tracker.insert(relation.clone());
    }

    pub fn mark_creating_streaming_job(&mut self, table_id: TableId) {
        self.in_progress_creation_streaming_job.insert(table_id);
    }

    pub fn unmark_creating(&mut self, relation: &RelationKey) {
        self.in_progress_creation_tracker.remove(&relation.clone());
    }

    pub fn unmark_creating_streaming_job(&mut self, table_id: TableId) {
        self.in_progress_creation_streaming_job.remove(&table_id);
    }

    pub fn all_creating_streaming_jobs(&self) -> impl Iterator<Item = TableId> + '_ {
        self.in_progress_creation_streaming_job.iter().cloned()
    }

    pub fn mark_creating_tables(&mut self, tables: &[Table]) {
        self.in_progress_creating_tables
            .extend(tables.iter().map(|t| (t.id, t.clone())));
    }

    pub fn unmark_creating_tables(&mut self, table_ids: &[TableId]) {
        for id in table_ids {
            self.in_progress_creating_tables.remove(id);
        }
    }
}
