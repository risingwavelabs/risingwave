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
use risingwave_common::util::epoch::Epoch;

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub const RW_DDL_PROGRESS: BuiltinTable = BuiltinTable {
    name: "rw_ddl_progress",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int64, "ddl_id"),
        (DataType::Varchar, "ddl_statement"),
        (DataType::Varchar, "progress"),
        (DataType::Timestamptz, "initialized_at"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub async fn read_ddl_progress(&self) -> Result<Vec<OwnedRow>> {
        let ddl_progresses = self.meta_client.list_ddl_progress().await?;

        let table_ids = ddl_progresses
            .iter()
            .map(|progress| progress.id as u32)
            .collect_vec();

        let tables = self.meta_client.get_tables(&table_ids).await?;

        let ddl_progress = ddl_progresses
            .into_iter()
            .map(|s| {
                let initialized_at = tables
                    .get(&(s.id as u32))
                    .and_then(|table| table.initialized_at_epoch.map(Epoch::from));

                OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(s.id as i64)),
                    Some(ScalarImpl::Utf8(s.statement.into())),
                    Some(ScalarImpl::Utf8(s.progress.into())),
                    initialized_at.map(|e| e.as_scalar()),
                ])
            })
            .collect_vec();
        Ok(ddl_progress)
    }
}
