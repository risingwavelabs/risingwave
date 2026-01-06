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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

/// The catalog `pg_extension` stores information about the installed extensions. See Section 38.17
/// for details about extensions.
///
/// Reference: <https://www.postgresql.org/docs/current/catalog-pg-extension.html>.
/// Currently, we don't have any type of extension.
#[system_catalog(view, "pg_catalog.pg_extension")]
#[derive(Fields)]
struct PgExtension {
    oid: i32,
    extname: String,
    extowner: i32,
    extnamespace: i32,
    extrelocatable: bool,
    extversion: String,
    extconfig: Vec<i32>,
    extcondition: Vec<String>,
}
