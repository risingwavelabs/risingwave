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
use crate::catalog::{CatalogReader, SourceId, ViewId};

const PG_TABLE_WRITE_MASK: i32 = 4 | 8 | 16;

#[function("pg_relation_is_updatable(int4, boolean) -> int4")]
fn pg_relation_is_updatable(oid: i32, _include_triggers: bool) -> Result<i32> {
    pg_relation_is_updatable_impl_captured(oid)
}

#[capture_context(CATALOG_READER, DB_NAME)]
fn pg_relation_is_updatable_impl(catalog: &CatalogReader, db_name: &str, oid: i32) -> Result<i32> {
    let catalog = catalog.read_guard();

    if let Ok(table) = catalog.get_created_table_by_id_with_db(db_name, oid as u32) {
        return Ok(if table.is_user_table() {
            PG_TABLE_WRITE_MASK
        } else {
            0
        });
    }

    if let Ok(database) = catalog.get_database_by_name(db_name) {
        for schema in database.iter_schemas() {
            if schema.get_view_by_id(ViewId::from(oid as u32)).is_some()
                || schema
                    .get_source_by_id(SourceId::from(oid as u32))
                    .is_some()
            {
                return Ok(0);
            }
        }
    }

    Ok(0)
}
