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

use risingwave_common::catalog::RW_CATALOG_SCHEMA_NAME;
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::{BuiltinView, SystemCatalogColumnsDef};

pub static RW_STREAMING_PARALLELISM_COLUMNS: LazyLock<Vec<SystemCatalogColumnsDef<'_>>> =
    LazyLock::new(|| {
        vec![
            (DataType::Int32, "id"),
            (DataType::Varchar, "name"),
            (DataType::Varchar, "relation_type"),
            (DataType::Varchar, "parallelism"),
        ]
    });
pub static RW_STREAMING_PARALLELISM: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "rw_streaming_parallelism",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &RW_STREAMING_PARALLELISM_COLUMNS,
    sql: "WITH all_streaming_jobs AS ( \
            SELECT id, name, 'table' as relation_type FROM rw_tables \
            UNION ALL \
            SELECT id, name, 'materialized view' as relation_type FROM rw_materialized_views \
            UNION ALL \
            SELECT id, name, 'sink' as relation_type FROM rw_sinks \
            UNION ALL \
            SELECT id, name, 'index' as relation_type FROM rw_indexes \
        ) \
        SELECT \
            job.id, \
            job.name, \
            job.relation_type, \
            tf.parallelism \
        FROM all_streaming_jobs job \
        INNER JOIN rw_table_fragments tf ON job.id = tf.table_id \
        ORDER BY job.id\
        "
    .to_string(),
});
