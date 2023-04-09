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

use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::SystemCatalogColumnsDef;

/// The catalog `pg_settings` stores settings.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-settings.html`]
pub const PG_SETTINGS_TABLE_NAME: &str = "pg_settings";
pub const PG_SETTINGS_COLUMNS: &[SystemCatalogColumnsDef<'_>] =
    &[(DataType::Varchar, "name"), (DataType::Varchar, "setting")];

pub static PG_SETTINGS_DATA_ROWS: LazyLock<Vec<OwnedRow>> = LazyLock::new(Vec::new);
