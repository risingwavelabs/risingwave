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

/// The view `pg_matviews` provides access to useful information about each materialized view in the
/// database.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-matviews.html`]
pub const PG_MATVIEWS_TABLE_NAME: &str = "pg_matviews";
pub const PG_MATVIEWS_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Varchar, "schemaname"),
    (DataType::Varchar, "matviewname"),
    (DataType::Int32, "matviewowner"),
    // TODO: add index back when open create index doc again.
    // (DataType::Boolean, "hasindexes"),
    (DataType::Varchar, "definition"),
    // Below are some columns that PostgreSQL doesn't have.
    (DataType::Int32, "matviewid"),
    (DataType::Varchar, "matviewtimezone"), /* The timezone used to interpret ambiguous
                                             * dates/timestamps as tstz */
    (DataType::Varchar, "matviewgraph"), // materialized view graph is json encoded fragment infos.
];
