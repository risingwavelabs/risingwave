// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::types::DataType;

use crate::catalog::pg_catalog::PgCatalogColumnsDef;

/// The catalog `pg_matviews_info` contains the information about the matviews.
pub const PG_MATVIEWS_INFO_TABLE_NAME: &str = "pg_matviews_info";
pub const PG_MATVIEWS_INFO_COLUMNS: &[PgCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "matviewid"),
    (DataType::Varchar, "matviewname"),
    (DataType::Varchar, "matviewschema"),
    (DataType::Int32, "matviewowner"),
    // TODO: add index back when open create index doc again.
    // (DataType::Boolean, "hasindexes"),
    (DataType::Varchar, "matviewgraph"), // materialized view graph is json encoded fragment infos.
    (DataType::Varchar, "definition"),
];
