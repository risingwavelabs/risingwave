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

/// The catalog `pg_description` stores description.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-description.html`]
pub static PG_DESCRIPTION: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "pg_description",
        schema: PG_CATALOG_SCHEMA_NAME,
        columns: &[
            (DataType::Int32, "objoid"),
            (DataType::Int32, "classoid"),
            (DataType::Int32, "objsubid"),
            (DataType::Varchar, "description"),
        ],
        sql: "SELECT id AS objoid, NULL::integer AS classoid, 0 AS objsubid, NULL AS description FROM rw_catalog.rw_tables \
        UNION ALL SELECT id AS objoid, NULL::integer AS classoid, 0 AS objsubid, NULL AS description FROM rw_catalog.rw_materialized_views \
        UNION ALL SELECT id AS objoid, NULL::integer AS classoid, 0 AS objsubid, NULL AS description FROM rw_catalog.rw_views \
        UNION ALL SELECT id AS objoid, NULL::integer AS classoid, 0 AS objsubid, NULL AS description FROM rw_catalog.rw_indexes \
        UNION ALL SELECT id AS objoid, NULL::integer AS classoid, 0 AS objsubid, NULL AS description FROM rw_catalog.rw_sources \
        UNION ALL SELECT id AS objoid, NULL::integer AS classoid, 0 AS objsubid, NULL AS description FROM rw_catalog.rw_system_tables\
            ".into(),
    }
});
