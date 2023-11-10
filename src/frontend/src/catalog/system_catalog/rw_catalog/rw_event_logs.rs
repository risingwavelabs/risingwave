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
use risingwave_common::types::{DataType, ScalarImpl, Timestamptz};

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub const RW_EVENT_LOGS: BuiltinTable = BuiltinTable {
    name: "rw_event_logs",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Varchar, "unique_id"),
        (DataType::Timestamptz, "timestamp"),
        (DataType::Varchar, "event_type"),
        (DataType::Varchar, "info"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub async fn read_event_logs(&self) -> Result<Vec<OwnedRow>> {
        let configs = self
            .meta_client
            .list_event_log()
            .await?
            .into_iter()
            .sorted_by(|a, b| a.timestamp.cmp(&b.timestamp))
            .map(|e| {
                let event_type = e.event_type().as_str_name();
                let ts = Timestamptz::from_millis(e.timestamp.unwrap() as i64).unwrap();
                OwnedRow::new(vec![
                    Some(ScalarImpl::Utf8(e.unique_id.unwrap().into())),
                    Some(ScalarImpl::Timestamptz(ts)),
                    Some(ScalarImpl::Utf8(event_type.into())),
                    Some(ScalarImpl::Utf8(e.info.into())),
                ])
            })
            .collect_vec();
        Ok(configs)
    }
}
