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

/// The view `pg_views` provides access to useful information about each view in the database.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-views.html`]
pub static PG_VIEWS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_views",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Varchar, "schemaname"),
        (DataType::Varchar, "viewname"),
        (DataType::Varchar, "viewowner"),
        (DataType::Varchar, "definition"),
    ],
    sql: "SELECT s.name AS schemaname, \
                 v.name AS viewname, \
                 pg_catalog.pg_get_userbyid(v.owner) AS viewowner, \
                 v.definition AS definition \
             FROM rw_catalog.rw_views v \
             JOIN rw_catalog.rw_schemas s \
             ON v.schema_id = s.id"
        .into(),
});
