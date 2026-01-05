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

use risingwave_expr::function;
use risingwave_sqlparser::keywords::{
    ALL_KEYWORDS_INDEX, RESERVED_FOR_COLUMN_ALIAS, RESERVED_FOR_COLUMN_OR_TABLE_NAME,
};

/// Returns a set of records describing the SQL keywords recognized by the server.
///
/// The word column contains the keyword.
///
/// The catcode column contains a category code:
/// - U for an unreserved keyword
/// - C for a keyword that can be a column name
/// - T for a keyword that can be a type or function name
/// - R for a fully reserved keyword.
///
/// The catdesc column contains a possibly-localized string describing the keyword's category.
///
/// ```slt
/// query TTT
/// select * from pg_get_keywords() where word = 'add';
/// ----
/// add U unreserved
/// ```
#[function("pg_get_keywords() -> setof struct<word varchar, catcode varchar, catdesc varchar>")]
fn pg_get_keywords() -> impl Iterator<Item = (Box<str>, &'static str, &'static str)> {
    ALL_KEYWORDS_INDEX.iter().map(|keyword| {
        // FIXME: The current category is not correct. Many are different from the PostgreSQL.
        let catcode = if !RESERVED_FOR_COLUMN_OR_TABLE_NAME.contains(keyword) {
            "U"
        } else if !RESERVED_FOR_COLUMN_ALIAS.contains(keyword) {
            "C"
        } else {
            "R"
        };
        let catdesc = match catcode {
            "U" => "unreserved",
            "C" => "unreserved (cannot be function or type name)",
            "T" => "reserved (can be function or type name)",
            "R" => "reserved",
            _ => unreachable!(),
        };
        (keyword.to_string().to_lowercase().into(), catcode, catdesc)
    })
}
