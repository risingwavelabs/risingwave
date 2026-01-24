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

use risingwave_common::types::{Fields, Timestamptz};
use risingwave_common::util::epoch::Epoch;
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::ddl_service::PbBackfillType;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwDdlProgress {
    #[primary_key]
    ddl_id: i64,
    ddl_statement: String,
    create_type: String,
    progress: String,
    initialized_at: Timestamptz,
    is_serverless_backfill: bool,
    backfill_type: String,
}

#[system_catalog(table, "rw_catalog.rw_ddl_progress")]
async fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<RwDdlProgress>> {
    let ddl_progresses = reader.meta_client.get_ddl_progress().await?;

    let ddl_progress = ddl_progresses
        .into_iter()
        .map(|s| RwDdlProgress {
            ddl_id: s.id as i64,
            backfill_type: s
                .get_backfill_type()
                .unwrap_or(PbBackfillType::Unspecified)
                .as_str_name()
                .to_owned(),
            ddl_statement: s.statement,
            create_type: s.create_type,
            progress: s.progress,
            initialized_at: *Epoch::from_unix_millis(s.initialized_at_time_millis as _)
                .as_scalar()
                .as_timestamptz(),
            is_serverless_backfill: s.is_serverless_backfill,
        })
        .collect();
    Ok(ddl_progress)
}
