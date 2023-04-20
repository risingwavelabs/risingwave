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
use risingwave_pb::catalog::{Index, Sink, Source, Table};

use crate::model::FragmentId;

// This enum is used in order to re-use code in `DdlServiceImpl` for creating MaterializedView and
// Sink.
#[derive(Debug, Clone)]
pub enum StreamingJob {
    MaterializedView(Table),
    Sink(Sink),
    Table(Option<Source>, Table),
    Index(Index, Table),
}

impl StreamingJob {
    pub fn set_id(&mut self, id: u32) {
        match self {
            Self::MaterializedView(table) => table.id = id,
            Self::Sink(sink) => sink.id = id,
            Self::Table(_, table) => table.id = id,
            Self::Index(index, index_table) => {
                index.id = id;
                index.index_table_id = id;
                index_table.id = id;
            }
        }
    }

    /// Set the fragment id where the table is materialized.
    pub fn set_table_fragment_id(&mut self, id: FragmentId) {
        match self {
            Self::MaterializedView(table) | Self::Index(_, table) | Self::Table(_, table) => {
                table.fragment_id = id;
            }
            Self::Sink(_) => {}
        }
    }

    pub fn id(&self) -> u32 {
        match self {
            Self::MaterializedView(table) => table.id,
            Self::Sink(sink) => sink.id,
            Self::Table(_, table) => table.id,
            Self::Index(index, _) => index.id,
        }
    }

    pub fn mv_table(&self) -> Option<u32> {
        match self {
            Self::MaterializedView(table) => Some(table.id),
            Self::Sink(_sink) => None,
            Self::Table(_, table) => Some(table.id),
            Self::Index(_, table) => Some(table.id),
        }
    }

    /// Returns the reference to the [`Table`] of the job if it exists.
    pub fn table(&self) -> Option<&Table> {
        match self {
            Self::MaterializedView(table) | Self::Index(_, table) | Self::Table(_, table) => {
                Some(table)
            }
            Self::Sink(_) => None,
        }
    }

    pub fn schema_id(&self) -> u32 {
        match self {
            Self::MaterializedView(table) => table.schema_id,
            Self::Sink(sink) => sink.schema_id,
            Self::Table(_, table) => table.schema_id,
            Self::Index(index, _) => index.schema_id,
        }
    }

    pub fn database_id(&self) -> u32 {
        match self {
            Self::MaterializedView(table) => table.database_id,
            Self::Sink(sink) => sink.database_id,
            Self::Table(_, table) => table.database_id,
            Self::Index(index, _) => index.database_id,
        }
    }

    pub fn name(&self) -> String {
        match self {
            Self::MaterializedView(table) => table.name.clone(),
            Self::Sink(sink) => sink.name.clone(),
            Self::Table(_, table) => table.name.clone(),
            Self::Index(index, _) => index.name.clone(),
        }
    }

    pub fn mview_definition(&self) -> String {
        match self {
            Self::MaterializedView(table) => table.definition.clone(),
            Self::Table(_, table) => table.definition.clone(),
            Self::Index(_, table) => table.definition.clone(),
            Self::Sink(sink) => sink.definition.clone(),
        }
    }

    pub fn properties(&self) -> HashMap<String, String> {
        match self {
            Self::MaterializedView(table) => table.properties.clone(),
            Self::Sink(sink) => sink.properties.clone(),
            Self::Table(_, table) => table.properties.clone(),
            Self::Index(_, index_table) => index_table.properties.clone(),
        }
    }

    /// Returns the [`TableVersionId`] if this job is `Table`.
    pub fn table_version_id(&self) -> Option<TableVersionId> {
        if let Self::Table(_, table) = self {
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
}
