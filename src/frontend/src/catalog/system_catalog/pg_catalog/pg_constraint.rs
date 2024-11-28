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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::system_catalog::{SysCatalogReaderImpl, SystemTableCatalog};
use crate::error::Result;
use crate::TableCatalog;

/// The catalog `pg_constraint` records information about table and index inheritance hierarchies.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-constraint.html`]
/// This is introduced only for pg compatibility and is not used in our system.
#[derive(Fields)]
struct PgConstraint {
    #[primary_key]
    oid: i32,
    conname: String,
    connamespace: i32,
    contype: String,
    condeferrable: bool,
    convalidated: bool,
    conrelid: i32,
    contypid: i32,
    conindid: i32,
    conparentid: i32,
    confrelid: i32,
    confupdtype: String,
    confdeltype: String,
    confmatchtype: String,
    conislocal: bool,
    coninhcount: i32,
    connoinherit: bool,
    conkey: Option<Vec<i16>>,
    confkey: Option<Vec<i16>>,
    conpfeqop: Option<Vec<i32>>,
    conppeqop: Option<Vec<i32>>,
    conffeqop: Option<Vec<i32>>,
    confdelsetcols: Option<Vec<i16>>,
    conexclop: Option<Vec<i32>>,
    conbin: Option<String>,
}

impl PgConstraint {
    fn from_system_table(schema: &SchemaCatalog, table: &SystemTableCatalog) -> PgConstraint {
        // List of the constrained columns. First column starts from 1.
        let conkey: Vec<_> = table.pk.iter().map(|i| (*i + 1) as i16).collect();
        PgConstraint {
            oid: table.id.table_id() as i32, // Use table_id as a mock oid of constraint here.
            conname: format!("{}_pkey", &table.name),
            connamespace: schema.id() as i32,
            contype: "p".to_owned(), // p = primary key constraint
            condeferrable: false,
            convalidated: true,
            conrelid: table.id.table_id() as i32,
            contypid: 0,
            // Use table_id as a mock index oid of constraint here.
            conindid: table.id.table_id() as i32,
            conparentid: 0,
            confrelid: 0,
            confupdtype: " ".to_owned(),
            confdeltype: " ".to_owned(),
            confmatchtype: " ".to_owned(),
            conislocal: true,
            coninhcount: 0,
            connoinherit: true,
            conkey: Some(conkey),
            confkey: None,
            conpfeqop: None,
            conppeqop: None,
            conffeqop: None,
            confdelsetcols: None,
            conexclop: None,
            conbin: None,
        }
    }

    fn from_table(schema: &SchemaCatalog, table: &TableCatalog) -> PgConstraint {
        // List of the constrained columns. First column starts from 1.
        let conkey: Vec<_> = table
            .pk
            .iter()
            .map(|i| (i.column_index + 1) as i16)
            .collect();
        PgConstraint {
            oid: table.id.table_id() as i32, // Use table_id as a mock oid of constraint here.
            conname: format!("{}_pkey", &table.name),
            connamespace: schema.id() as i32,
            contype: "p".to_owned(), // p = primary key constraint
            condeferrable: false,
            convalidated: true,
            conrelid: table.id.table_id() as i32,
            contypid: 0,
            // Use table_id as a mock index oid of constraint here.
            conindid: table.id.table_id() as i32,
            conparentid: 0,
            confrelid: 0,
            confupdtype: " ".to_owned(),
            confdeltype: " ".to_owned(),
            confmatchtype: " ".to_owned(),
            conislocal: true,
            coninhcount: 0,
            connoinherit: true,
            conkey: Some(conkey),
            confkey: None,
            conpfeqop: None,
            conppeqop: None,
            conffeqop: None,
            confdelsetcols: None,
            conexclop: None,
            conbin: None,
        }
    }
}

#[system_catalog(table, "pg_catalog.pg_constraint")]
fn read_pg_constraint(reader: &SysCatalogReaderImpl) -> Result<Vec<PgConstraint>> {
    let catalog_reader = reader.catalog_reader.read_guard();
    let schemas = catalog_reader.iter_schemas(&reader.auth_context.database)?;

    Ok(schemas.flat_map(read_pg_constraint_in_schema).collect())
}

fn read_pg_constraint_in_schema(schema: &SchemaCatalog) -> Vec<PgConstraint> {
    // Note: We only support primary key constraints now.
    let system_table_rows = schema
        .iter_system_tables()
        .map(|table| PgConstraint::from_system_table(schema, table.as_ref()));

    let table_rows = schema
        .iter_table_mv_indices()
        .map(|table| PgConstraint::from_table(schema, table.as_ref()));

    system_table_rows.chain(table_rows).collect()
}
