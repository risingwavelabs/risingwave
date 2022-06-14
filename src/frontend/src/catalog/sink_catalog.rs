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
use itertools::Itertools;
use risingwave_pb::catalog::sink::Info;
use risingwave_pb::catalog::Sink as ProstSink;
use risingwave_pb::stream_plan::sink_node::SinkType;

use super::column_catalog::ColumnCatalog;
use super::{ColumnId, SinkId, TABLE_SINK_PK_COLID};

#[expect(non_snake_case, non_upper_case_globals)]
pub mod WithOptions {
    pub const AppenOnly: &str = "appendonly";
}

/// this struct `SinkCatalog` is used in frontend and compared with `ProstSink` it only maintain
/// information which will be used during optimization.
#[derive(Clone, Debug)]
pub struct SinkCatalog {
    pub id: SinkId,
    pub name: String,
    pub columns: Vec<ColumnCatalog>,
    pub pk_col_ids: Vec<ColumnId>,
    pub sink_type: SinkType,
    pub append_only: bool,
}

impl SinkCatalog {
    /// Extract `field_descs` from `column_desc` and add in sink catalog.
    pub fn flatten(mut self) -> Self {
        let mut catalogs = vec![];
        for col in &self.columns {
            // Extract `field_descs` and return `column_catalogs`.
            catalogs.append(
                &mut col
                    .column_desc
                    .flatten()
                    .into_iter()
                    .map(|c| ColumnCatalog {
                        column_desc: c,
                        is_hidden: col.is_hidden,
                    })
                    .collect_vec(),
            )
        }
        self.columns = catalogs.clone();
        self
    }
}

impl From<&ProstSink> for SinkCatalog {
    fn from(prost: &ProstSink) -> Self {
        let id = prost.id;
        let name = prost.name.clone();
        let (sink_type, prost_columns, pk_col_ids, with_options) = match &prost.info {
            Some(Info::StreamSink(sink)) => (
                SinkType::Sink,
                sink.columns.clone(),
                sink.pk_column_ids
                    .iter()
                    .map(|id| ColumnId::new(*id))
                    .collect(),
                sink.properties.clone(),
            ),
            Some(Info::TableSink(sink)) => (
                SinkType::Table,
                sink.columns.clone(),
                vec![TABLE_SINK_PK_COLID],
                sink.properties.clone(),
            ),
            None => unreachable!(),
        };
        let columns = prost_columns.into_iter().map(ColumnCatalog::from).collect();

        // parse options in WITH clause
        let mut append_only = false;
        if let Some(val) = with_options.get(WithOptions::AppenOnly) {
            if val.to_lowercase() == "true" {
                append_only = true;
            }
        }

        Self {
            id,
            name,
            columns,
            pk_col_ids,
            sink_type,
            append_only,
        }
    }
}
