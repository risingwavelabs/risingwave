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

/// The view `pg_matviews` provides access to useful information about each materialized view in the
/// database.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-matviews.html`]
pub static PG_MATVIEWS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_matviews",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Varchar, "schemaname"),
        (DataType::Varchar, "matviewname"),
        (DataType::Int32, "matviewowner"),
        (DataType::Varchar, "definition"),
        // Below are some columns that PostgreSQL doesn't have.
        // TODO: these field is only exist in RW and used by cloud side, need to remove it and let
        // cloud switch to use `rw_catalog.rw_relation_info`.
        (DataType::Int32, "matviewid"),
        (DataType::Varchar, "matviewtimezone"), /* The timezone used to interpret ambiguous
                                                 * dates/timestamps as tstz */
        (DataType::Varchar, "matviewgraph"), /* materialized view graph is json encoded fragment
                                              * infos. */
    ],
    sql: "SELECT schemaname, \
                i.relationname AS matviewname, \
                i.relationowner AS matviewowner, \
                definition, \
                i.relationid AS matviewid, \
                i.relationtimezone AS matviewtimezone, \
                i.fragments AS matviewgraph \
           FROM rw_catalog.rw_relation_info i \
           WHERE i.relationtype = 'MATERIALIZED VIEW'"
        .to_string(),
});
