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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

/// The catalog `pg_language` registers languages in which you can write functions or stored procedures.
/// Ref: `https://www.postgresql.org/docs/current/catalog-pg-language.html`
/// This is introduced only for pg compatibility and is not used in our system.
#[system_catalog(view, "pg_catalog.pg_language")]
#[derive(Fields)]
struct PgLanguage {
    #[primary_key]
    oid: i32,
    lanname: String,
    lanowner: i32,
    lanispl: bool,
    lanpltrusted: bool,
    lanplcallfoid: i32,
    laninline: i32,
    lanvalidator: i32,
    lanacl: Vec<String>,
}
