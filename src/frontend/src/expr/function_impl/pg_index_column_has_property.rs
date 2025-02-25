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

use risingwave_expr::{Result, capture_context, function};

use super::context::{CATALOG_READER, DB_NAME};
use crate::catalog::CatalogReader;

/// Tests whether an index column has the named property.
///
/// `index` is the OID of the index.
/// `column` is the column number (1-based) within the index.
///
/// NULL is returned if the property name is not known or does not apply to the particular object,
/// or if the OID or column number does not identify a valid object.
///
/// # Supported Properties
///
/// - `asc`: Does the column sort in ascending order on a forward scan?
/// - `desc`: Does the column sort in descending order on a forward scan?
/// - `nulls_first`: Does the column sort with nulls first on a forward scan?
/// - `nulls_last`: Does the column sort with nulls last on a forward scan?
///
/// # Examples
///
/// ```slt
/// statement ok
/// create table t(a int, b int);
///
/// statement ok
/// create index i on t (a asc, b desc);
///
/// query B
/// select pg_index_column_has_property('i'::regclass, 1, 'asc');
/// ----
/// t
///
/// query B
/// select pg_index_column_has_property('i'::regclass, 1, 'DESC');
/// ----
/// f
///
/// query B
/// select pg_index_column_has_property('i'::regclass, 1, 'nulls_FIRST');
/// ----
/// f
///
/// query B
/// select pg_index_column_has_property('i'::regclass, 1, 'nulls_last');
/// ----
/// t
///
/// query B
/// select pg_index_column_has_property('i'::regclass, 2, 'asc');
/// ----
/// f
///
/// query B
/// select pg_index_column_has_property('i'::regclass, 2, 'desc');
/// ----
/// t
///
/// query B
/// select pg_index_column_has_property('i'::regclass, 2, 'nulls_first');
/// ----
/// t
///
/// query B
/// select pg_index_column_has_property('i'::regclass, 2, 'nulls_last');
/// ----
/// f
///
/// query B
/// select pg_index_column_has_property('i'::regclass, 1, 'gg');    -- invalid property
/// ----
/// NULL
///
/// query B
/// select pg_index_column_has_property('i'::regclass, 0, 'asc');   -- column 0 does not exist
/// ----
/// NULL
///
/// statement ok
/// drop index i;
///
/// statement ok
/// drop table t;
/// ```
#[function("pg_index_column_has_property(int4, int4, varchar) -> boolean")]
fn pg_index_column_has_property(index: i32, column: i32, property: &str) -> Result<Option<bool>> {
    pg_index_column_has_property_impl_captured(index, column, property)
}

#[capture_context(CATALOG_READER, DB_NAME)]
fn pg_index_column_has_property_impl(
    catalog: &CatalogReader,
    db_name: &str,
    index_id: i32,
    column_idx: i32,
    property: &str,
    // `Result` is not necessary for this function, but it's required by `capture_context`.
) -> Result<Option<bool>> {
    let catalog_reader = catalog.read_guard();
    let Ok(index) = catalog_reader.get_index_by_id(db_name, index_id as u32) else {
        return Ok(None);
    };
    let Some(properties) = index.get_column_properties((column_idx - 1) as usize) else {
        return Ok(None);
    };
    Ok(match property.to_lowercase().as_str() {
        "asc" => Some(!properties.is_desc),
        "desc" => Some(properties.is_desc),
        "nulls_first" => Some(properties.nulls_first),
        "nulls_last" => Some(!properties.nulls_first),
        _ => None,
    })
}
