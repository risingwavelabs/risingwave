// Copyright 2024 RisingWave Labs
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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;

/// The catalog `pg_settings` stores settings.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-settings.html`]
#[derive(Fields)]
struct PgSetting {
    name: String,
    setting: String,
    short_desc: String,
}

#[system_catalog(table, "pg_catalog.pg_settings")]
fn read_pg_settings(reader: &SysCatalogReaderImpl) -> Vec<PgSetting> {
    let config_reader = reader.config.read();
    let all_variables = config_reader.show_all();

    all_variables
        .iter()
        .map(|info| PgSetting {
            name: info.name.clone(),
            setting: info.setting.clone(),
            short_desc: info.description.clone(),
        })
        .collect()
}
