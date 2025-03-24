// Copyright 2025 RisingWave Labs
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
use risingwave_common::types::{Fields, Timestamptz};
use risingwave_common::util::epoch::Epoch;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwDdlProgress {
    #[primary_key]
    ddl_id: i64,
    ddl_statement: String,
    progress: String,
    initialized_at: Option<Timestamptz>,
}

#[system_catalog(table, "rw_catalog.rw_ddl_progress")]
async fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<RwDdlProgress>> {
    let ddl_progresses = reader.meta_client.get_ddl_progress().await?;

    let table_ids = ddl_progresses
        .iter()
        .map(|progress| progress.id as u32)
        .collect_vec();

    let tables = reader.meta_client.get_tables(&table_ids, false).await?;

    let ddl_progress = ddl_progresses
        .into_iter()
        .map(|s| {
            let initialized_at = tables
                .get(&(s.id as u32))
                .and_then(|table| table.initialized_at_epoch.map(Epoch::from));

            RwDdlProgress {
                ddl_id: s.id as i64,
                ddl_statement: s.statement.clone(),
                progress: s.progress.clone(),
                initialized_at: initialized_at.map(|e| *e.as_scalar().as_timestamptz()),
            }
        })
        .collect();
    Ok(ddl_progress)
}
