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

use risingwave_pb::catalog::{Sink, Table};

// This enum is used in order to re-use code in `DdlServiceImpl` for creating MaterializedView and Sink.
pub enum Relation {
    Table(Table),
    Sink(Sink),
}

impl Relation {
    pub fn set_id(&mut self, id: u32) {
        match self {
            Self::Table(table) => table.id = id,
            Self::Sink(sink) => sink.id = id,
        }
    }

    pub fn set_dependent_relations(&mut self, dependent_relations: Vec<u32>) {
        match self {
            Self::Table(table) => table.dependent_relations = dependent_relations,
            Self::Sink(sink) => sink.dependent_relations = dependent_relations,
        }
    }

    pub fn schema_id(&self) -> u32 {
        match self {
            Self::Table(table) => table.schema_id,
            Self::Sink(sink) => sink.schema_id,
        }
    }

    pub fn database_id(&self) -> u32 {
        match self {
            Self::Table(table) => table.database_id,
            Self::Sink(sink) => sink.database_id,
        }
    }

    pub fn name(&self) -> String {
        match self {
            Self::Table(table) => table.name.clone(),
            Self::Sink(sink) => sink.name.clone(),
        }
    }

    pub fn properties(&self) -> HashMap<String, String> {
        match self {
            Self::Table(table) => table.properties.clone(),
            Self::Sink(sink) => sink.properties.clone(),
        }
    }
}
