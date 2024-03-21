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

pub const PG_KEYWORDS_TABLE_NAME: &str = "pg_keywords";
pub const PG_GET_KEYWORDS_FUNC_NAME: &str = "pg_get_keywords";

/// The catalog `pg_keywords` stores keywords. `pg_get_keywords` returns the content of this table.
/// Ref: [`https://www.postgresql.org/docs/15/functions-info.html`]
// TODO: change to read reserved keywords here
#[system_catalog(view, "pg_catalog.pg_keywords")]
#[derive(Fields)]
struct PgKeywords {
    word: String,
    catcode: String,
    catdesc: String,
}
