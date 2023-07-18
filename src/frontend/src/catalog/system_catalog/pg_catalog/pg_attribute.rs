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

use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::BuiltinView;

/// The catalog `pg_attribute` stores information about table columns. There will be exactly one
/// `pg_attribute` row for every column in every table in the database. (There will also be
/// attribute entries for indexes, and indeed all objects that have `pg_class` entries.)
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-attribute.html`]
///
/// In RisingWave, we simply make it contain the columns of the view and all the columns of the
/// tables that are not internal tables.
pub static PG_ATTRIBUTE: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_attribute",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "attrelid"),
        (DataType::Varchar, "attname"),
        (DataType::Int32, "atttypid"),
        (DataType::Int16, "attlen"),
        (DataType::Int16, "attnum"),
        (DataType::Boolean, "attnotnull"),
        (DataType::Boolean, "attisdropped"),
        (DataType::Varchar, "attidentity"),
        // From https://www.postgresql.org/docs/current/catalog-pg-attribute.html
        // The value will generally be -1 for types that do not need
        (DataType::Int32, "atttypmod"),
    ],
    sql: "SELECT c.id AS attrelid, \
                 c.name AS attname, \
                 c.type_oid AS atttypid, \
                 c.type_len AS attlen, \
                 c.position AS attnum, \
                 false AS attnotnull, \
                 false AS attisdropped, \
                 ''::varchar AS attidentity, \
                 -1 AS atttypmod \
           FROM rw_catalog.rw_columns c\
    "
    .to_string(),
});
