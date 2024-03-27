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

use risingwave_common::types::fields::{constant, null, ConstBool, ConstI32, Null};
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
    condeferrable: ConstBool<false>,
    convalidated: ConstBool<true>,
    conrelid: i32,
    contypid: ConstI32<0>,
    conindid: i32,
    conparentid: ConstI32<0>,
    confrelid: ConstI32<0>,
    confupdtype: String,
    confdeltype: String,
    confmatchtype: String,
    conislocal: ConstBool<true>,
    coninhcount: ConstI32<0>,
    connoinherit: ConstBool<true>,
    conkey: Vec<i16>,
    confkey: Null<Vec<i16>>,
    conpfeqop: Null<Vec<i32>>,
    conppeqop: Null<Vec<i32>>,
    conffeqop: Null<Vec<i32>>,
    confdelsetcols: Null<Vec<i16>>,
    conexclop: Null<Vec<i32>>,
    conbin: Null<String>,
}

impl PgConstraint {
    fn new(
        schema_id: i32,
        table_id: i32,
        table_name: &str,
        pk_indices: impl IntoIterator<Item = i16>,
    ) -> PgConstraint {
        // List of the constrained columns. First column starts from 1.
        let conkey = pk_indices.into_iter().map(|i| i + 1).collect();

        PgConstraint {
            oid: table_id, // Use table_id as a mock oid of constraint here.
            conname: format!("{}_pkey", table_name),
            connamespace: schema_id,
            contype: "p".to_owned(), // p = primary key constraint
            condeferrable: constant(),
            convalidated: constant(),
            conrelid: table_id,
            contypid: constant(),
            // Use table_id as a mock index oid of constraint here.
            conindid: table_id,
            conparentid: constant(),
            confrelid: constant(),
            confupdtype: " ".to_owned(),
            confdeltype: " ".to_owned(),
            confmatchtype: " ".to_owned(),
            conislocal: constant(),
            coninhcount: constant(),
            connoinherit: constant(),
            conkey,
            confkey: null(),
            conpfeqop: null(),
            conppeqop: null(),
            conffeqop: null(),
            confdelsetcols: null(),
            conexclop: null(),
            conbin: null(),
        }
    }

    fn from_system_table(schema: &SchemaCatalog, table: &SystemTableCatalog) -> PgConstraint {
        PgConstraint::new(
            schema.id() as i32,
            table.id.table_id() as i32,
            &table.name,
            table.pk.iter().map(|i| *i as _),
        )
    }

    fn from_table(schema: &SchemaCatalog, table: &TableCatalog) -> PgConstraint {
        PgConstraint::new(
            schema.id() as i32,
            table.id.table_id() as i32,
            &table.name,
            table.pk.iter().map(|i| i.column_index as _),
        )
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
        .iter_valid_table()
        .map(|table| PgConstraint::from_table(schema, table.as_ref()));

    system_table_rows.chain(table_rows).collect()
}
