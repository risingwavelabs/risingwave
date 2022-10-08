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

use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::{Source as ProstSource, StreamSourceInfo, TableSourceInfo};

use super::column_catalog::ColumnCatalog;
use super::{ColumnId, SourceId};
use crate::WithOptions;

pub const KAFKA_CONNECTOR: &str = "kafka";

#[derive(Clone, Debug)]
pub enum SourceCatalogInfo {
    StreamSource(StreamSourceInfo),
    TableSource(TableSourceInfo),
}

/// this struct `SourceCatalog` is used in frontend and compared with `ProstSource` it only maintain
/// information which will be used during optimization.
#[derive(Clone, Debug)]
pub struct SourceCatalog {
    pub id: SourceId,
    pub name: String,
    pub columns: Vec<ColumnCatalog>,
    pub pk_col_ids: Vec<ColumnId>,
    pub append_only: bool,
    pub owner: u32,
    pub info: SourceCatalogInfo,
}

impl SourceCatalog {
    pub fn is_table(&self) -> bool {
        matches!(self.info, SourceCatalogInfo::TableSource(_))
    }

    pub fn is_stream(&self) -> bool {
        matches!(self.info, SourceCatalogInfo::StreamSource(_))
    }
}

impl From<&ProstSource> for SourceCatalog {
    fn from(prost: &ProstSource) -> Self {
        let id = prost.id;
        let name = prost.name.clone();
        let (prost_columns, pk_col_ids, with_options, info) = match &prost.info {
            Some(Info::StreamSource(source)) => (
                source.columns.clone(),
                source
                    .pk_column_ids
                    .clone()
                    .into_iter()
                    .map(Into::into)
                    .collect(),
                WithOptions::new(source.properties.clone()),
                SourceCatalogInfo::StreamSource(source.clone()),
            ),
            Some(Info::TableSource(source)) => (
                source.columns.clone(),
                source
                    .pk_column_ids
                    .clone()
                    .into_iter()
                    .map(Into::into)
                    .collect(),
                WithOptions::new(source.properties.clone()),
                SourceCatalogInfo::TableSource(source.clone()),
            ),
            None => unreachable!(),
        };
        let columns = prost_columns.into_iter().map(ColumnCatalog::from).collect();

        let append_only = with_options.append_only();
        let owner = prost.owner;

        Self {
            id,
            name,
            columns,
            pk_col_ids,
            append_only,
            owner,
            info,
        }
    }
}
