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

// The code is same as `expr/impl/src/table_function/pg_get_keywords.rs`.

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;
use risingwave_sqlparser::keywords::{
    ALL_KEYWORDS_INDEX, RESERVED_FOR_COLUMN_ALIAS, RESERVED_FOR_COLUMN_OR_TABLE_NAME,
};

use crate::catalog::system_catalog::SysCatalogReaderImpl;

/// The catalog `pg_keywords` stores keywords. `pg_get_keywords` returns the content of this table.
/// Ref: [`https://www.postgresql.org/docs/15/functions-info.html`]
///
/// # Example
///
/// ```slt
/// query TTT
/// select * from pg_keywords where word = 'add';
/// ----
/// add U unreserved
/// ```
#[derive(Fields)]
struct PgKeywords {
    #[primary_key]
    word: String,
    catcode: char,
    catdesc: &'static str,
}

#[system_catalog(table, "pg_catalog.pg_keywords")]
fn read_pg_keywords(_reader: &SysCatalogReaderImpl) -> Vec<PgKeywords> {
    ALL_KEYWORDS_INDEX
        .iter()
        .map(|keyword| {
            // FIXME: The current category is not correct. Many are different from the PostgreSQL.
            let catcode = if !RESERVED_FOR_COLUMN_OR_TABLE_NAME.contains(keyword) {
                'U'
            } else if !RESERVED_FOR_COLUMN_ALIAS.contains(keyword) {
                'C'
            } else {
                'R'
            };
            let catdesc = match catcode {
                'U' => "unreserved",
                'C' => "unreserved (cannot be function or type name)",
                'T' => "reserved (can be function or type name)",
                'R' => "reserved",
                _ => unreachable!(),
            };
            PgKeywords {
                word: keyword.to_string().to_lowercase(),
                catcode,
                catdesc,
            }
        })
        .collect()
}
