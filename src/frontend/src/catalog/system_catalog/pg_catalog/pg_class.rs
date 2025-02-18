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
    let tables = reader.read_all_tables::<PgClass, _>(
        |_, _| true,
        |_, table| PgClass {
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
        },
    )?;

    let mviews = reader.read_all_mviews::<PgClass, _>(
        |_, _| true,
        |_, mview| PgClass {
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
        },
    )?;

    let system_tables = reader.read_all_system_tables::<PgClass, _>(
        |_, _| true,
        |schema, table| PgClass {
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
        },
    )?;

    let indexes = reader.read_all_indexes::<PgClass, _>(
        |_, _| true,
        |schema, index| PgClass {
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
        },
    )?;

    let views = reader.read_all_views::<PgClass, _>(
        |_, _| true,
        |schema, view| PgClass {
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
        },
    )?;

    Ok(tables
        .into_iter()
        .chain(mviews)
        .chain(indexes)
        .chain(views)
        .chain(system_tables)
        .collect())
}
