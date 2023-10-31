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

use std::collections::HashMap;

use risingwave_common::catalog::TableVersionId;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::catalog::{CreateType, Index, PbSource, Sink, Table};
use risingwave_pb::ddl_service::TableJobType;

use crate::model::FragmentId;

// This enum is used in order to re-use code in `DdlServiceImpl` for creating MaterializedView and
// Sink.
#[derive(Debug, Clone)]
pub enum StreamingJob {
    MaterializedView(Table),
    Sink(Sink),
    Table(Option<PbSource>, Table, TableJobType),
    Index(Index, Table),
    Source(PbSource),
}

impl StreamingJob {
    pub fn mark_created(&mut self) {
        let created_at_epoch = Some(Epoch::now().0);
        match self {
            StreamingJob::MaterializedView(table) => table.created_at_epoch = created_at_epoch,
            StreamingJob::Sink(table) => table.created_at_epoch = created_at_epoch,
            StreamingJob::Table(source, table, ..) => {
                table.created_at_epoch = created_at_epoch;
                if let Some(source) = source {
                    source.created_at_epoch = created_at_epoch;
                }
            }
            StreamingJob::Index(index, _) => {
                index.created_at_epoch = created_at_epoch;
            }
            StreamingJob::Source(source) => {
                source.created_at_epoch = created_at_epoch;
            }
        }
    }

    pub fn mark_initialized(&mut self) {
        let initialized_at_epoch = Some(Epoch::now().0);
        match self {
            StreamingJob::MaterializedView(table) => {
                table.initialized_at_epoch = initialized_at_epoch
            }
            StreamingJob::Sink(table) => table.initialized_at_epoch = initialized_at_epoch,
            StreamingJob::Table(source, table, ..) => {
                table.initialized_at_epoch = initialized_at_epoch;
                if let Some(source) = source {
                    source.initialized_at_epoch = initialized_at_epoch;
                }
            }
            StreamingJob::Index(index, _) => {
                index.initialized_at_epoch = initialized_at_epoch;
            }
            StreamingJob::Source(source) => {
                source.initialized_at_epoch = initialized_at_epoch;
            }
        }
    }
}

impl StreamingJob {
    pub fn set_id(&mut self, id: u32) {
        match self {
            Self::MaterializedView(table) => table.id = id,
            Self::Sink(sink) => sink.id = id,
            Self::Table(_, table, ..) => table.id = id,
            Self::Index(index, index_table) => {
                index.id = id;
                index.index_table_id = id;
                index_table.id = id;
            }
            StreamingJob::Source(_) => {
                // The id of source is set in `DdlServiceImpl::create_source`,
                // so do nothing here.
                unreachable!()
            }
        }
    }

    /// Set the fragment id where the table is materialized.
    pub fn set_table_fragment_id(&mut self, id: FragmentId) {
        match self {
            Self::MaterializedView(table) | Self::Index(_, table) | Self::Table(_, table, ..) => {
                table.fragment_id = id;
            }
            Self::Sink(_) | Self::Source(_) => {}
        }
    }

    /// Set the fragment id where the table dml is received.
    pub fn set_dml_fragment_id(&mut self, id: Option<FragmentId>) {
        match self {
            Self::Table(_, table, ..) => {
                table.dml_fragment_id = id;
            }
            Self::MaterializedView(_) | Self::Index(_, _) | Self::Sink(_) => {}
            Self::Source(_) => {}
        }
    }

    pub fn id(&self) -> u32 {
        match self {
            Self::MaterializedView(table) => table.id,
            Self::Sink(sink) => sink.id,
            Self::Table(_, table, ..) => table.id,
            Self::Index(index, _) => index.id,
            Self::Source(source) => source.id,
        }
    }

    pub fn mv_table(&self) -> Option<u32> {
        match self {
            Self::MaterializedView(table) => Some(table.id),
            Self::Sink(_sink) => None,
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
            Self::Sink(_) | Self::Source(_) => None,
        }
    }

    pub fn schema_id(&self) -> u32 {
        match self {
            Self::MaterializedView(table) => table.schema_id,
            Self::Sink(sink) => sink.schema_id,
            Self::Table(_, table, ..) => table.schema_id,
            Self::Index(index, _) => index.schema_id,
            Self::Source(source) => source.schema_id,
        }
    }

    pub fn database_id(&self) -> u32 {
        match self {
            Self::MaterializedView(table) => table.database_id,
            Self::Sink(sink) => sink.database_id,
            Self::Table(_, table, ..) => table.database_id,
            Self::Index(index, _) => index.database_id,
            Self::Source(source) => source.database_id,
        }
    }

    pub fn name(&self) -> String {
        match self {
            Self::MaterializedView(table) => table.name.clone(),
            Self::Sink(sink) => sink.name.clone(),
            Self::Table(_, table, ..) => table.name.clone(),
            Self::Index(index, _) => index.name.clone(),
            Self::Source(source) => source.name.clone(),
        }
    }

    pub fn owner(&self) -> u32 {
        match self {
            StreamingJob::MaterializedView(mv) => mv.owner,
            StreamingJob::Sink(sink) => sink.owner,
            StreamingJob::Table(_, table, ..) => table.owner,
            StreamingJob::Index(index, _) => index.owner,
            StreamingJob::Source(source) => source.owner,
        }
    }

    pub fn definition(&self) -> String {
        match self {
            Self::MaterializedView(table) => table.definition.clone(),
            Self::Table(_, table, ..) => table.definition.clone(),
            Self::Index(_, table) => table.definition.clone(),
            Self::Sink(sink) => sink.definition.clone(),
            Self::Source(source) => source.definition.clone(),
        }
    }

    pub fn properties(&self) -> HashMap<String, String> {
        match self {
            Self::MaterializedView(table) => table.properties.clone(),
            Self::Sink(sink) => sink.properties.clone(),
            Self::Table(_, table, ..) => table.properties.clone(),
            Self::Index(_, index_table) => index_table.properties.clone(),
            Self::Source(source) => source.properties.clone(),
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
            _ => CreateType::Foreground,
        }
    }

    pub fn table_job_type(&self) -> Option<TableJobType> {
        if let Self::Table(.., sub_type) = self {
            Some(*sub_type)
        } else {
            None
        }
    }

    pub fn is_source_job(&self) -> bool {
        matches!(self, StreamingJob::Source(_))
    }
}
