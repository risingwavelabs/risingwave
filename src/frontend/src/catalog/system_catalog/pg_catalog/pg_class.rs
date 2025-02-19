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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::catalog::OwnedByUserCatalog;
use crate::error::Result;

/// The catalog `pg_class` catalogs tables and most everything else that has columns or is otherwise
/// similar to a table. Ref: [`https://www.postgresql.org/docs/current/catalog-pg-class.html`]
#[derive(Fields)]
struct PgClass {
    #[primary_key]
    oid: i32,
    relname: String,
    relnamespace: i32,
    relowner: i32,
    // p = permanent table, u = unlogged table, t = temporary table
    relpersistence: String,
    // r = ordinary table, i = index, S = sequence, t = TOAST table, v = view, m = materialized view,
    // c = composite type, f = foreign table, p = partitioned table, I = partitioned index
    relkind: String,
    relpages: i16,
    relam: i32,
    reltablespace: i32,
    reloptions: Vec<String>,
    relispartition: bool,
    // PG uses pg_node_tree type but RW doesn't support it
    relpartbound: Option<String>,
}

#[system_catalog(table, "pg_catalog.pg_class")]
fn read_pg_class_info(reader: &SysCatalogReaderImpl) -> Result<Vec<PgClass>> {
    let catalog_reader = reader.catalog_reader.read_guard();
    let schemas = catalog_reader.iter_schemas(&reader.auth_context.database)?;

    Ok(schemas
        .flat_map(|schema| {
            schema
                .iter_user_table()
                .map(|table| PgClass {
                    oid: table.id.table_id as i32,
                    relname: table.name.clone(),
                    relnamespace: table.schema_id as i32,
                    relowner: table.owner as i32,
                    relpersistence: "p".to_owned(),
                    relkind: "r".to_owned(),
                    relpages: 0,
                    relam: 0,
                    reltablespace: 0,
                    reloptions: vec![],
                    relispartition: false,
                    relpartbound: None,
                })
                .chain(schema.iter_all_mvs().map(|mview| PgClass {
                    oid: mview.id.table_id as i32,
                    relname: mview.name.clone(),
                    relnamespace: mview.schema_id as i32,
                    relowner: mview.owner as i32,
                    relpersistence: "p".to_owned(),
                    relkind: "m".to_owned(),
                    relpages: 0,
                    relam: 0,
                    reltablespace: 0,
                    reloptions: vec![],
                    relispartition: false,
                    relpartbound: None,
                }))
                .chain(schema.iter_system_tables().map(|table| PgClass {
                    oid: table.id.table_id as i32,
                    relname: table.name.clone(),
                    relnamespace: schema.id() as i32,
                    relowner: table.owner as i32,
                    relpersistence: "p".to_owned(),
                    relkind: "r".to_owned(),
                    relpages: 0,
                    relam: 0,
                    reltablespace: 0,
                    reloptions: vec![],
                    relispartition: false,
                    relpartbound: None,
                }))
                .chain(schema.iter_index().map(|index| PgClass {
                    oid: index.id.index_id as i32,
                    relname: index.name.clone(),
                    relnamespace: schema.id() as i32,
                    relowner: index.owner() as i32,
                    relpersistence: "p".to_owned(),
                    relkind: "i".to_owned(),
                    relpages: 0,
                    relam: 0,
                    reltablespace: 0,
                    reloptions: vec![],
                    relispartition: false,
                    relpartbound: None,
                }))
                .chain(schema.iter_view().map(|view| PgClass {
                    oid: view.id as i32,
                    relname: view.name.clone(),
                    relnamespace: schema.id() as i32,
                    relowner: view.owner as i32,
                    relpersistence: "p".to_owned(),
                    relkind: "v".to_owned(),
                    relpages: 0,
                    relam: 0,
                    reltablespace: 0,
                    reloptions: vec![],
                    relispartition: false,
                    relpartbound: None,
                }))
        })
        .collect())
}
