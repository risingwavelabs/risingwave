// Copyright 2026 RisingWave Labs
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

//! Keeps the summarized definition of materialized views across queries.
//!
//! Whether a view is usable and what its definition is depend on the catalog only, so both are
//! kept until the catalog changes. Any change drops everything: tracking which change affects
//! which view would be finer, but a single missed dependency would leave a stale definition
//! behind.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;
use risingwave_common::acl::AclMode;
use risingwave_common::catalog::{CatalogVersion, TableId};
use risingwave_pb::common::PbObjectType;

use super::definition::{Unusable, summarize_definition};
use super::summary::BlockSummary;
use crate::catalog::TableCatalog;
use crate::catalog::index_catalog::{IndexType, TableIndex};
use crate::catalog::root_catalog::Catalog;
use crate::session::SessionImpl;

/// A materialized view that may answer a query block.
pub struct Candidate {
    pub table: Arc<TableCatalog>,
    pub indexes: Vec<Arc<TableIndex>>,
    /// The definition of the view. Its output columns are the visible columns of `table`.
    pub summary: Arc<BlockSummary>,
}

/// The views to consider for a set of tables.
#[derive(Default)]
pub struct Candidates {
    pub usable: Vec<Candidate>,
    /// Views over these tables that cannot be used, with the reason.
    pub unusable: Vec<(String, Unusable)>,
}

#[derive(Clone)]
enum Definition {
    /// Some query is summarizing the definition right now.
    Building,
    Ready(Result<Arc<BlockSummary>, Unusable>),
}

#[derive(Default)]
struct Inner {
    /// The catalog version that everything below is derived from.
    version: Option<CatalogVersion>,
    /// For every table, the materialized views that read it.
    views_by_table: HashMap<TableId, Vec<TableId>>,
    /// For every materialized view, the tables and materialized views it reads.
    tables_by_view: HashMap<TableId, Vec<TableId>>,
    /// Filled on first use.
    definitions: HashMap<TableId, Definition>,
}

impl Inner {
    fn new(catalog: &Catalog) -> Self {
        let mut inner = Inner {
            version: Some(catalog.version()),
            ..Default::default()
        };
        for dependency in catalog.iter_object_dependencies() {
            if !matches!(
                dependency.referenced_object_type,
                PbObjectType::Table | PbObjectType::Mview
            ) {
                continue;
            }
            let view_id = dependency.object_id.as_table_id();
            if !catalog
                .get_any_table_by_id(view_id)
                .is_ok_and(|table| table.is_mview())
            {
                continue;
            }
            let table_id = dependency.referenced_object_id.as_table_id();
            inner
                .views_by_table
                .entry(table_id)
                .or_default()
                .push(view_id);
            inner
                .tables_by_view
                .entry(view_id)
                .or_default()
                .push(table_id);
        }
        inner
    }
}

/// One per frontend node.
#[derive(Default)]
pub struct CandidateCache {
    inner: Mutex<Inner>,
}

/// Forgets that a definition is being summarized if summarizing it unwinds.
struct BuildGuard<'a> {
    cache: &'a CandidateCache,
    view_id: TableId,
}

impl Drop for BuildGuard<'_> {
    fn drop(&mut self) {
        let mut inner = self.cache.inner.lock();
        if matches!(
            inner.definitions.get(&self.view_id),
            Some(Definition::Building)
        ) {
            inner.definitions.remove(&self.view_id);
        }
    }
}

impl CandidateCache {
    /// Returns the materialized views that read nothing but (some of) `tables`, and that the
    /// user of the session may read.
    pub fn candidates(&self, session: &SessionImpl, tables: &HashSet<TableId>) -> Candidates {
        let env = session.env();
        // Holding the catalog keeps it from changing until the definitions are stored.
        let catalog = env.catalog_reader().read_guard();
        let user_reader = env.user_info_reader().read_guard();
        let Some(user) = user_reader.get_user_by_name(&session.user_name()) else {
            return Candidates::default();
        };

        let mut result = Candidates::default();
        let mut to_build = vec![];
        {
            let mut inner = self.inner.lock();
            if inner.version != Some(catalog.version()) {
                *inner = Inner::new(&catalog);
            }
            // Ordered, to try the views in the same order every time.
            let view_ids: BTreeSet<TableId> = tables
                .iter()
                .filter_map(|table| inner.views_by_table.get(table))
                .flatten()
                .filter(|view| {
                    inner.tables_by_view[*view]
                        .iter()
                        .all(|table| tables.contains(table))
                })
                .copied()
                .collect();
            for view_id in view_ids {
                let Ok(view) = catalog.get_any_table_by_id(view_id) else {
                    continue;
                };
                // Privileges are not part of the cached state.
                if !(user.is_super
                    || user.id == view.owner
                    || user.has_privilege(view.id, AclMode::Select))
                {
                    continue;
                }
                match inner.definitions.get(&view_id) {
                    Some(Definition::Ready(Ok(summary))) => {
                        result
                            .usable
                            .push(candidate(&catalog, view, summary.clone()));
                    }
                    Some(Definition::Ready(Err(reason))) => {
                        result.unusable.push((view.name.clone(), reason.clone()));
                    }
                    // Do not wait: the query is fine without the view.
                    Some(Definition::Building) => result.unusable.push((
                        view.name.clone(),
                        "it is being analyzed by another query".to_owned(),
                    )),
                    None => {
                        inner.definitions.insert(view_id, Definition::Building);
                        to_build.push(view.clone());
                    }
                }
            }
        }

        // Binding and planning a definition takes a while, so it runs without the cache locked.
        // Marking the view as being built makes concurrent queries bind it once in total.
        for view in to_build {
            let guard = BuildGuard {
                cache: self,
                view_id: view.id,
            };
            let definition = summarize_definition(env, &catalog, &view).map(Arc::new);
            self.inner
                .lock()
                .definitions
                .insert(view.id, Definition::Ready(definition.clone()));
            drop(guard);
            match definition {
                Ok(summary) => result.usable.push(candidate(&catalog, &view, summary)),
                Err(reason) => result.unusable.push((view.name.clone(), reason)),
            }
        }
        result.usable.sort_by_key(|candidate| candidate.table.id);
        result
    }
}

fn candidate(catalog: &Catalog, view: &Arc<TableCatalog>, summary: Arc<BlockSummary>) -> Candidate {
    let indexes = catalog
        .get_schema_by_id(view.database_id, view.schema_id)
        .map(|schema| schema.get_indexes_by_table_id(view.id, false))
        .unwrap_or_default()
        .into_iter()
        .filter_map(|index| match &index.index_type {
            IndexType::Table(index) => Some(index.clone()),
            IndexType::Vector(_) => None,
        })
        .collect();
    Candidate {
        table: view.clone(),
        indexes,
        summary,
    }
}
