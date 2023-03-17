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

use std::collections::BTreeMap;

use risingwave_common::catalog::ColumnCatalog;
use risingwave_pb::catalog::{PbSource, StreamSourceInfo, WatermarkDesc};

use super::{ColumnId, RelationCatalog, SourceId};
use crate::user::UserId;
use crate::WithOptions;

/// This struct `SourceCatalog` is used in frontend.
/// Compared with `PbSource`, it only maintains information used during optimization.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceCatalog {
    pub id: SourceId,
    pub name: String,
    pub columns: Vec<ColumnCatalog>,
    pub pk_col_ids: Vec<ColumnId>,
    pub append_only: bool,
    pub owner: UserId,
    pub info: StreamSourceInfo,
    pub row_id_index: Option<usize>,
    pub properties: BTreeMap<String, String>,
    pub watermark_descs: Vec<WatermarkDesc>,
}

impl From<&PbSource> for SourceCatalog {
    fn from(prost: &PbSource) -> Self {
        let id = prost.id;
        let name = prost.name.clone();
        let prost_columns = prost.columns.clone();
        let pk_col_ids = prost
            .pk_column_ids
            .clone()
            .into_iter()
            .map(Into::into)
            .collect();
        let with_options = WithOptions::new(prost.properties.clone());
        let columns = prost_columns.into_iter().map(ColumnCatalog::from).collect();
        let row_id_index = prost.row_id_index.map(|idx| idx as _);

        let append_only = row_id_index.is_some();
        let owner = prost.owner;
        let watermark_descs = prost.get_watermark_descs().clone();

        Self {
            id,
            name,
            columns,
            pk_col_ids,
            append_only,
            owner,
            info: prost.info.clone().unwrap(),
            row_id_index,
            properties: with_options.into_inner(),
            watermark_descs,
        }
    }
}

impl RelationCatalog for SourceCatalog {
    fn owner(&self) -> UserId {
        self.owner
    }
}
