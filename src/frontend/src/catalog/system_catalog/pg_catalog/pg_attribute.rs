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

use risingwave_common::types::DataType;

use crate::catalog::system_catalog::SystemCatalogColumnsDef;

/// The catalog `pg_attribute` stores information about table columns. There will be exactly one
/// `pg_attribute` row for every column in every table in the database. (There will also be
/// attribute entries for indexes, and indeed all objects that have `pg_class` entries.)
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-attribute.html`]
///
/// In RisingWave, we simply make it contain the columns of the view and all the columns of the
/// tables that are not internal tables.
pub const PG_ATTRIBUTE_TABLE_NAME: &str = "pg_attribute";
pub const PG_ATTRIBUTE_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "attrelid"),
    (DataType::Varchar, "attname"),
    (DataType::Int32, "atttypid"),
    (DataType::Int16, "attlen"),
    (DataType::Int16, "attnum"),
    (DataType::Boolean, "attnotnull"),
    (DataType::Boolean, "attisdropped"),
    (DataType::Int32, "atttypmod"),
];
