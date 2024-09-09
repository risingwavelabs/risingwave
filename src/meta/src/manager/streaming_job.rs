// Copyright 2024 RisingWave Labs
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

use std::collections::HashSet;

use risingwave_common::catalog::TableVersionId;
use risingwave_common::current_cluster_version;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::catalog::{CreateType, Index, PbSource, Sink, Table};
use risingwave_pb::ddl_service::TableJobType;
use strum::{EnumDiscriminants, EnumIs};

use super::{get_refed_secret_ids_from_sink, get_refed_secret_ids_from_source};
use crate::model::FragmentId;
use crate::MetaResult;

// This enum is used in order to re-use code in `DdlServiceImpl` for creating MaterializedView and
// Sink.
#[derive(Debug, Clone, EnumDiscriminants, EnumIs)]
pub enum StreamingJob {
    MaterializedView(Table),
    Sink(Sink, Option<(Table, Option<PbSource>)>),
    Table(Option<PbSource>, Table, TableJobType),
    Index(Index, Table),
    Source(PbSource),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DdlType {
    MaterializedView,
    Sink,
    Table(TableJobType),
    Index,
    Source,
}

impl From<&StreamingJob> for DdlType {
    fn from(job: &StreamingJob) -> Self {
        match job {
            StreamingJob::MaterializedView(_) => DdlType::MaterializedView,
            StreamingJob::Sink(_, _) => DdlType::Sink,
            StreamingJob::Table(_, _, ty) => DdlType::Table(*ty),
            StreamingJob::Index(_, _) => DdlType::Index,
            StreamingJob::Source(_) => DdlType::Source,
        }
    }
}

#[cfg(test)]
#[allow(clippy::derivable_impls)]
impl Default for DdlType {
    fn default() -> Self {
        // This should not be used by mock services,
        // so we can just pick an arbitrary default variant.
        DdlType::MaterializedView
    }
}

impl StreamingJob {
    pub fn mark_created(&mut self) {
        let created_at_epoch = Some(Epoch::now().0);
        let created_at_cluster_version = Some(current_cluster_version());
        match self {
            StreamingJob::MaterializedView(table) => {
                table.created_at_epoch = created_at_epoch;
                table.created_at_cluster_version = created_at_cluster_version;
            }
            StreamingJob::Sink(table, _) => table.created_at_epoch = created_at_epoch,
            StreamingJob::Table(source, table, ..) => {
                table.created_at_epoch = created_at_epoch;
                table
                    .created_at_cluster_version
                    .clone_from(&created_at_cluster_version);
                if let Some(source) = source {
                    source.created_at_epoch = created_at_epoch;
                    source.created_at_cluster_version = created_at_cluster_version;
                }
            }
            StreamingJob::Index(index, _) => {
                index.created_at_epoch = created_at_epoch;
                index.created_at_cluster_version = created_at_cluster_version;
            }
            StreamingJob::Source(source) => {
                source.created_at_epoch = created_at_epoch;
                source.created_at_cluster_version = created_at_cluster_version;
            }
        }
    }

    pub fn mark_initialized(&mut self) {
        let initialized_at_epoch = Some(Epoch::now().0);
        let initialized_at_cluster_version = Some(current_cluster_version());
        match self {
            StreamingJob::MaterializedView(table) => {
                table.initialized_at_epoch = initialized_at_epoch;
                table.initialized_at_cluster_version = initialized_at_cluster_version;
            }
            StreamingJob::Sink(table, _) => {
                table.initialized_at_epoch = initialized_at_epoch;
                table.initialized_at_cluster_version = initialized_at_cluster_version;
            }
            StreamingJob::Table(source, table, ..) => {
                table.initialized_at_epoch = initialized_at_epoch;
                table
                    .initialized_at_cluster_version
                    .clone_from(&initialized_at_cluster_version);

                if let Some(source) = source {
                    source.initialized_at_epoch = initialized_at_epoch;
                    source.initialized_at_cluster_version = initialized_at_cluster_version;
                }
            }
            StreamingJob::Index(index, _) => {
                index.initialized_at_epoch = initialized_at_epoch;
                index.initialized_at_cluster_version = initialized_at_cluster_version;
            }
            StreamingJob::Source(source) => {
                source.initialized_at_epoch = initialized_at_epoch;
                source.initialized_at_cluster_version = initialized_at_cluster_version;
            }
        }
    }
}

impl StreamingJob {
    pub fn set_id(&mut self, id: u32) {
        match self {
            Self::MaterializedView(table) => table.id = id,
            Self::Sink(sink, _) => sink.id = id,
            Self::Table(_, table, ..) => table.id = id,
            Self::Index(index, index_table) => {
                index.id = id;
                index.index_table_id = id;
                index_table.id = id;
            }
            StreamingJob::Source(src) => {
                src.id = id;
            }
        }
    }

    /// Set the fragment id where the table is materialized.
    pub fn set_table_fragment_id(&mut self, id: FragmentId) {
        match self {
            Self::MaterializedView(table) | Self::Index(_, table) | Self::Table(_, table, ..) => {
                table.fragment_id = id;
            }
            Self::Sink(_, _) | Self::Source(_) => {}
        }
    }

    pub fn set_table_vnode_count(&mut self, vnode_count: usize) {
        match self {
            Self::MaterializedView(table) | Self::Index(_, table) | Self::Table(_, table, ..) => {
                table.maybe_vnode_count = Some(vnode_count as u32);
            }
            Self::Sink(_, _) | Self::Source(_) => {}
        }
    }

    /// Set the fragment id where the table dml is received.
    pub fn set_dml_fragment_id(&mut self, id: Option<FragmentId>) {
        match self {
            Self::Table(_, table, ..) => {
                table.dml_fragment_id = id;
            }
            Self::MaterializedView(_) | Self::Index(_, _) | Self::Sink(_, _) => {}
            Self::Source(_) => {}
        }
    }

    pub fn id(&self) -> u32 {
        match self {
            Self::MaterializedView(table) => table.id,
            Self::Sink(sink, _) => sink.id,
            Self::Table(_, table, ..) => table.id,
            Self::Index(index, _) => index.id,
            Self::Source(source) => source.id,
        }
    }

    pub fn mv_table(&self) -> Option<u32> {
        match self {
            Self::MaterializedView(table) => Some(table.id),
            Self::Sink(_, _) => None,
            Self::Table(_, table, ..) => Some(table.id),
            Self::Index(_, table) => Some(table.id),
            Self::Source(_) => None,
        }
    }

    /// Returns the reference to the [`Table`] of the job if it exists.
    pub fn table(&self) -> Option<&Table> {
        match self {
            Self::MaterializedView(table) | Self::Index(_, table) | Self::Table(_, table, ..) => {
                Some(table)
            }
            Self::Sink(_, _) | Self::Source(_) => None,
        }
    }

    pub fn schema_id(&self) -> u32 {
        match self {
            Self::MaterializedView(table) => table.schema_id,
            Self::Sink(sink, _) => sink.schema_id,
            Self::Table(_, table, ..) => table.schema_id,
            Self::Index(index, _) => index.schema_id,
            Self::Source(source) => source.schema_id,
        }
    }

    pub fn database_id(&self) -> u32 {
        match self {
            Self::MaterializedView(table) => table.database_id,
            Self::Sink(sink, _) => sink.database_id,
            Self::Table(_, table, ..) => table.database_id,
            Self::Index(index, _) => index.database_id,
            Self::Source(source) => source.database_id,
        }
    }

    pub fn name(&self) -> String {
        match self {
            Self::MaterializedView(table) => table.name.clone(),
            Self::Sink(sink, _) => sink.name.clone(),
            Self::Table(_, table, ..) => table.name.clone(),
            Self::Index(index, _) => index.name.clone(),
            Self::Source(source) => source.name.clone(),
        }
    }

    pub fn owner(&self) -> u32 {
        match self {
            StreamingJob::MaterializedView(mv) => mv.owner,
            StreamingJob::Sink(sink, _) => sink.owner,
            StreamingJob::Table(_, table, ..) => table.owner,
            StreamingJob::Index(index, _) => index.owner,
            StreamingJob::Source(source) => source.owner,
        }
    }

    pub fn job_type_str(&self) -> &'static str {
        match self {
            StreamingJob::MaterializedView(_) => "materialized view",
            StreamingJob::Sink(_, _) => "sink",
            StreamingJob::Table(_, _, _) => "table",
            StreamingJob::Index(_, _) => "index",
            StreamingJob::Source(_) => "source",
        }
    }

    pub fn definition(&self) -> String {
        match self {
            Self::MaterializedView(table) => table.definition.clone(),
            Self::Table(_, table, ..) => table.definition.clone(),
            Self::Index(_, table) => table.definition.clone(),
            Self::Sink(sink, _) => sink.definition.clone(),
            Self::Source(source) => source.definition.clone(),
        }
    }

    /// Returns the [`TableVersionId`] if this job is `Table`.
    pub fn table_version_id(&self) -> Option<TableVersionId> {
        if let Self::Table(_, table, ..) = self {
            Some(
                table
                    .get_version()
                    .expect("table must be versioned")
                    .version,
            )
        } else {
            None
        }
    }

    pub fn create_type(&self) -> CreateType {
        match self {
            Self::MaterializedView(table) => {
                table.get_create_type().unwrap_or(CreateType::Foreground)
            }
            Self::Sink(s, _) => s.get_create_type().unwrap_or(CreateType::Foreground),
            _ => CreateType::Foreground,
        }
    }

    // TODO: record all objects instead.
    pub fn dependent_relations(&self) -> Vec<u32> {
        match self {
            StreamingJob::MaterializedView(table) => table.dependent_relations.clone(),
            StreamingJob::Sink(sink, _) => sink.dependent_relations.clone(),
            StreamingJob::Table(_, table, _) => table.dependent_relations.clone(),
            StreamingJob::Index(index, index_table) => {
                assert_eq!(index.primary_table_id, index_table.dependent_relations[0]);
                vec![]
            }
            StreamingJob::Source(_) => vec![],
        }
    }

    // Get the secret ids that are referenced by this job.
    pub fn dependent_secret_ids(&self) -> MetaResult<HashSet<u32>> {
        match self {
            StreamingJob::Sink(sink, _) => Ok(get_refed_secret_ids_from_sink(sink)),
            StreamingJob::Table(source, _, _) => {
                if let Some(source) = source {
                    get_refed_secret_ids_from_source(source)
                } else {
                    Ok(HashSet::new())
                }
            }
            StreamingJob::Source(source) => get_refed_secret_ids_from_source(source),
            StreamingJob::MaterializedView(_) | StreamingJob::Index(_, _) => Ok(HashSet::new()),
        }
    }

    pub fn is_source_job(&self) -> bool {
        matches!(self, StreamingJob::Source(_))
    }
}
