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

use risingwave_pb::catalog::{Index, Sink, Source, Table};

// This enum is used in order to re-use code in `DdlServiceImpl` for creating MaterializedView and
// Sink.
#[derive(Debug)]
pub enum StreamingJob {
    Table(Table),
    Sink(Sink),
    MaterializedSource(Source, Table),
    Index(Index, Table),
}

impl StreamingJob {
    pub fn set_id(&mut self, id: u32) {
        match self {
            Self::Table(table) => table.id = id,
            Self::Sink(sink) => sink.id = id,
            Self::MaterializedSource(_, table) => table.id = id,
            Self::Index(index, index_table) => {
                index.id = id;
                index.index_table_id = id;
                index_table.id = id;
            }
        }
    }

    pub fn id(&self) -> u32 {
        match self {
            Self::Table(table) => table.id,
            Self::Sink(sink) => sink.id,
            Self::MaterializedSource(_, table) => table.id,
            Self::Index(index, _) => index.id,
        }
    }

    pub fn set_dependent_relations(&mut self, dependent_relations: Vec<u32>) {
        match self {
            Self::Table(table) => table.dependent_relations = dependent_relations,
            Self::Sink(sink) => sink.dependent_relations = dependent_relations,
            Self::Index(_, index_table) => index_table.dependent_relations = dependent_relations,
            _ => {}
        }
    }

    pub fn schema_id(&self) -> u32 {
        match self {
            Self::Table(table) => table.schema_id,
            Self::Sink(sink) => sink.schema_id,
            Self::MaterializedSource(_, table) => table.schema_id,
            Self::Index(index, _) => index.schema_id,
        }
    }

    pub fn database_id(&self) -> u32 {
        match self {
            Self::Table(table) => table.database_id,
            Self::Sink(sink) => sink.database_id,
            Self::MaterializedSource(_, table) => table.database_id,
            Self::Index(index, _) => index.database_id,
        }
    }

    pub fn name(&self) -> String {
        match self {
            Self::Table(table) => table.name.clone(),
            Self::Sink(sink) => sink.name.clone(),
            Self::MaterializedSource(_, table) => table.name.clone(),
            Self::Index(index, _) => index.name.clone(),
        }
    }

    pub fn properties(&self) -> HashMap<String, String> {
        match self {
            Self::Table(table) => table.properties.clone(),
            Self::Sink(sink) => sink.properties.clone(),
            Self::MaterializedSource(_, table) => table.properties.clone(),
            Self::Index(_, index_table) => index_table.properties.clone(),
        }
    }
}
