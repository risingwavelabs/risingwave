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

use itertools::Itertools;
use risingwave_common::catalog::RW_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub const RW_HUMMOCK_META_CONFIGS: BuiltinTable = BuiltinTable {
    name: "rw_hummock_meta_configs",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Varchar, "config_name"),
        (DataType::Varchar, "config_value"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub async fn read_hummock_meta_configs(&self) -> Result<Vec<OwnedRow>> {
        let configs = self
            .meta_client
            .list_hummock_meta_configs()
            .await?
            .into_iter()
            .sorted()
            .map(|(k, v)| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Utf8(k.into())),
                    Some(ScalarImpl::Utf8(v.into())),
                ])
            })
            .collect_vec();
        Ok(configs)
    }
}
