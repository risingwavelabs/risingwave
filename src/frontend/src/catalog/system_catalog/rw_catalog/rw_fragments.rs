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

use std::sync::LazyLock;

use itertools::Itertools;
use risingwave_common::array::ListValue;
use risingwave_common::catalog::RW_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl, SystemCatalogColumnsDef};

pub static RW_FRAGMENTS_COLUMNS: LazyLock<Vec<SystemCatalogColumnsDef<'_>>> = LazyLock::new(|| {
    vec![
        (DataType::Int32, "fragment_id"),
        (DataType::Int32, "table_id"),
        (DataType::Varchar, "distribution_type"),
        (DataType::List(Box::new(DataType::Int32)), "state_table_ids"),
        (
            DataType::List(Box::new(DataType::Int32)),
            "upstream_fragment_ids",
        ),
    ]
});

pub static RW_FRAGMENTS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "rw_fragments",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &RW_FRAGMENTS_COLUMNS,
    pk: &[0],
});

impl SysCatalogReaderImpl {
    pub async fn read_rw_fragment_distributions_info(&self) -> Result<Vec<OwnedRow>> {
        let distributions = self.meta_client.list_fragment_distribution().await?;

        Ok(distributions
            .into_iter()
            .map(|distribution| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(distribution.fragment_id as i32)),
                    Some(ScalarImpl::Int32(distribution.table_id as i32)),
                    Some(ScalarImpl::Utf8(
                        distribution.distribution_type().as_str_name().into(),
                    )),
                    Some(ScalarImpl::List(ListValue::new(
                        distribution
                            .state_table_ids
                            .into_iter()
                            .map(|id| Some(ScalarImpl::Int32(id as i32)))
                            .collect_vec(),
                    ))),
                    Some(ScalarImpl::List(ListValue::new(
                        distribution
                            .upstream_fragment_ids
                            .into_iter()
                            .map(|id| Some(ScalarImpl::Int32(id as i32)))
                            .collect_vec(),
                    ))),
                ])
            })
            .collect_vec())
    }
}
