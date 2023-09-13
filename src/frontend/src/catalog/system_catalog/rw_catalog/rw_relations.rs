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

use crate::catalog::system_catalog::BuiltinView;

/// `rw_relations` is a view that shows all relations in the database.
pub static RW_RELATIONS: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "rw_relations",
        schema: RW_CATALOG_SCHEMA_NAME,
        columns: &[
            (DataType::Int32, "id"),
            (DataType::Varchar, "name"),
            (DataType::Varchar, "relation_type"),
            (DataType::Int32, "schema_id"),
            (DataType::Int32, "owner"),
            (DataType::Varchar, "definition"),
            (DataType::Varchar, "acl"),
        ],
        sql: "SELECT id, name, 'table' AS relation_type, schema_id, owner, definition, acl FROM rw_catalog.rw_tables \
           UNION ALL SELECT id, name, 'system table' AS relation_type, schema_id, owner, definition, acl FROM rw_catalog.rw_system_tables \
           UNION ALL SELECT id, name, 'source' AS relation_type, schema_id, owner, definition, acl FROM rw_catalog.rw_sources \
           UNION ALL SELECT id, name, 'index' AS relation_type, schema_id, owner, definition, acl FROM rw_catalog.rw_indexes \
           UNION ALL SELECT id, name, 'sink' AS relation_type, schema_id, owner, definition, acl FROM rw_catalog.rw_sinks \
           UNION ALL SELECT id, name, 'materialized view' AS relation_type, schema_id, owner, definition, acl FROM rw_catalog.rw_materialized_views \
           UNION ALL SELECT id, name, 'view' AS relation_type, schema_id, owner, definition, acl FROM rw_catalog.rw_views \
           ".to_string(),
    }
});
