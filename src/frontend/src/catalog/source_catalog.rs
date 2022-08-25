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

use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::Source as ProstSource;
use risingwave_pb::stream_plan::source_node::SourceType;

use super::column_catalog::ColumnCatalog;
use super::{ColumnId, SourceId};

pub mod with_options {
    pub const APPEND_ONLY: &str = "appendonly";
    pub const CONNECTOR: &str = "connector";
}

pub const KAFKA_CONNECTOR: &str = "kafka";

/// this struct `SourceCatalog` is used in frontend and compared with `ProstSource` it only maintain
/// information which will be used during optimization.
#[derive(Clone, Debug)]
pub struct SourceCatalog {
    pub id: SourceId,
    pub name: String,
    pub columns: Vec<ColumnCatalog>,
    pub pk_col_ids: Vec<ColumnId>,
    pub source_type: SourceType,
    pub append_only: bool,
    pub owner: u32,
}

impl From<&ProstSource> for SourceCatalog {
    fn from(prost: &ProstSource) -> Self {
        let id = prost.id;
        let name = prost.name.clone();
        let (source_type, prost_columns, pk_col_ids, with_options) = match &prost.info {
            Some(Info::StreamSource(source)) => (
                SourceType::Source,
                source.columns.clone(),
                source
                    .pk_column_ids
                    .clone()
                    .into_iter()
                    .map(Into::into)
                    .collect(),
                source.properties.clone(),
            ),
            Some(Info::TableSource(source)) => (
                SourceType::Table,
                source.columns.clone(),
                source
                    .pk_column_ids
                    .clone()
                    .into_iter()
                    .map(Into::into)
                    .collect(),
                source.properties.clone(),
            ),
            None => unreachable!(),
        };
        let columns = prost_columns.into_iter().map(ColumnCatalog::from).collect();

        let append_only = check_append_only(&with_options);
        let owner = prost.owner;

        Self {
            id,
            name,
            columns,
            pk_col_ids,
            source_type,
            append_only,
            owner,
        }
    }
}

fn check_append_only(with_options: &HashMap<String, String>) -> bool {
    if let Some(val) = with_options.get(with_options::APPEND_ONLY) {
        if val.to_lowercase() == "true" {
            return true;
        }
    }
    if let Some(val) = with_options.get(with_options::CONNECTOR) {
        // Kafka source is append-only
        if val.to_lowercase() == KAFKA_CONNECTOR {
            return true;
        }
    }
    false
}
