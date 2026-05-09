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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::id::{ObjectId, RelationId, SchemaId};
use risingwave_sqlparser::ast::{
    ColumnOption, ObjectName, ReferentialAction, Statement, TableConstraint,
};

use crate::TableCatalog;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::system_catalog::{SysCatalogReaderImpl, SystemTableCatalog};
use crate::catalog::table_catalog::TableType;
use crate::error::Result;

/// The catalog `pg_constraint` records information about table and index inheritance hierarchies.
/// Ref: `https://www.postgresql.org/docs/current/catalog-pg-constraint.html`
/// This is introduced only for pg compatibility and is not used in our system.
#[derive(Fields)]
struct PgConstraint {
    #[primary_key]
    oid: ObjectId,
    conname: String,
    connamespace: SchemaId,
    contype: String,
    condeferrable: bool,
    convalidated: bool,
    conrelid: RelationId,
    contypid: i32,
    conindid: RelationId,
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
            oid: table.id.as_object_id(), // Use table_id as a mock oid of constraint here.
            conname: format!("{}_pkey", &table.name),
            connamespace: schema.id(),
            contype: "p".to_owned(), // p = primary key constraint
            condeferrable: false,
            convalidated: true,
            conrelid: table.id.as_relation_id(),
            contypid: 0,
            // Use table_id as a mock index oid of constraint here.
            conindid: table.id.as_relation_id(),
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
            oid: table.id.as_object_id(), // Use table_id as a mock oid of constraint here.
            conname: format!("{}_pkey", &table.name),
            connamespace: schema.id(),
            contype: "p".to_owned(), // p = primary key constraint
            condeferrable: false,
            convalidated: true,
            conrelid: table.id.as_relation_id(),
            contypid: 0,
            // Use table_id as a mock index oid of constraint here.
            conindid: table.id.as_relation_id(),
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

    Ok(schemas
        .flat_map(|schema| {
            read_pg_constraint_in_schema(&catalog_reader, &reader.auth_context.database, schema)
        })
        .collect())
}

fn read_pg_constraint_in_schema(
    catalog_reader: &crate::catalog::root_catalog::Catalog,
    database: &str,
    schema: &SchemaCatalog,
) -> Vec<PgConstraint> {
    // We expose primary-key and foreign-key metadata for PostgreSQL compatibility.
    let system_table_rows = schema
        .iter_system_tables()
        .map(|table| PgConstraint::from_system_table(schema, table.as_ref()));

    let table_rows = schema
        .iter_table_mv_indices()
        .map(|table| PgConstraint::from_table(schema, table.as_ref()));

    let foreign_key_rows = schema.iter_table_mv_indices().flat_map(|table| {
        foreign_key_constraints_from_definition(catalog_reader, database, schema, table.as_ref())
    });

    system_table_rows
        .chain(table_rows)
        .chain(foreign_key_rows)
        .collect()
}

fn foreign_key_constraints_from_definition(
    catalog_reader: &crate::catalog::root_catalog::Catalog,
    database: &str,
    current_schema: &SchemaCatalog,
    table: &TableCatalog,
) -> Vec<PgConstraint> {
    if !matches!(table.table_type(), TableType::Table) {
        return vec![];
    }

    let Ok(Statement::CreateTable {
        columns,
        constraints,
        ..
    }) = table.create_sql_ast()
    else {
        return vec![];
    };

    let mut rows = Vec::new();
    let mut ordinal = 0u32;

    for constraint in constraints {
        if let TableConstraint::ForeignKey {
            name,
            columns,
            foreign_table,
            referred_columns,
            on_delete,
            on_update,
        } = constraint
            && let Some(row) = build_foreign_key_constraint(
                catalog_reader,
                database,
                current_schema,
                table,
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ordinal,
            )
        {
            rows.push(row);
            ordinal += 1;
        }
    }

    for column in columns {
        for option in column.options {
            if let ColumnOption::ForeignKey {
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
            } = option.option
                && let Some(row) = build_foreign_key_constraint(
                    catalog_reader,
                    database,
                    current_schema,
                    table,
                    option.name,
                    vec![column.name.clone()],
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    ordinal,
                )
            {
                rows.push(row);
                ordinal += 1;
            }
        }
    }

    rows
}

#[allow(clippy::too_many_arguments)]
fn build_foreign_key_constraint(
    catalog_reader: &crate::catalog::root_catalog::Catalog,
    database: &str,
    current_schema: &SchemaCatalog,
    table: &TableCatalog,
    name: Option<risingwave_sqlparser::ast::Ident>,
    columns: Vec<risingwave_sqlparser::ast::Ident>,
    foreign_table: ObjectName,
    referred_columns: Vec<risingwave_sqlparser::ast::Ident>,
    on_delete: Option<ReferentialAction>,
    on_update: Option<ReferentialAction>,
    ordinal: u32,
) -> Option<PgConstraint> {
    let foreign_table =
        resolve_referenced_table(catalog_reader, database, current_schema, &foreign_table)?;
    let conkey = column_positions(table, &columns)?;
    let confkey = if referred_columns.is_empty() {
        Some(
            foreign_table
                .pk
                .iter()
                .map(|order| (order.column_index + 1) as i16)
                .collect(),
        )
    } else {
        column_positions(foreign_table, &referred_columns)
    }?;

    let constraint_name = name
        .map(|ident| ident.real_value())
        .unwrap_or_else(|| default_foreign_key_name(&table.name, &columns));

    Some(make_foreign_key_constraint_row(
        current_schema.id(),
        table,
        foreign_table,
        constraint_name,
        conkey,
        confkey,
        on_delete,
        on_update,
        ordinal,
    ))
}

fn resolve_referenced_table<'a>(
    catalog_reader: &'a crate::catalog::root_catalog::Catalog,
    database: &str,
    current_schema: &'a SchemaCatalog,
    object_name: &ObjectName,
) -> Option<&'a std::sync::Arc<TableCatalog>> {
    match object_name.0.as_slice() {
        [table] => current_schema.get_created_table_by_name(&table.real_value()),
        [schema, table] => catalog_reader
            .get_schema_by_name(database, &schema.real_value())
            .ok()?
            .get_created_table_by_name(&table.real_value()),
        [db, schema, table] if db.real_value() == database => catalog_reader
            .get_schema_by_name(database, &schema.real_value())
            .ok()?
            .get_created_table_by_name(&table.real_value()),
        _ => None,
    }
}

fn column_positions(
    table: &TableCatalog,
    columns: &[risingwave_sqlparser::ast::Ident],
) -> Option<Vec<i16>> {
    columns
        .iter()
        .map(|ident| {
            table
                .columns
                .iter()
                .position(|column| column.name() == ident.real_value())
                .map(|index| (index + 1) as i16)
        })
        .collect()
}

fn default_foreign_key_name(
    table_name: &str,
    columns: &[risingwave_sqlparser::ast::Ident],
) -> String {
    let joined = columns
        .iter()
        .map(|ident| ident.real_value())
        .collect::<Vec<_>>()
        .join("_");
    format!("{table_name}_{joined}_fkey")
}

fn foreign_key_action_code(action: Option<ReferentialAction>) -> &'static str {
    match action.unwrap_or(ReferentialAction::NoAction) {
        ReferentialAction::NoAction => "a",
        ReferentialAction::Restrict => "r",
        ReferentialAction::Cascade => "c",
        ReferentialAction::SetNull => "n",
        ReferentialAction::SetDefault => "d",
    }
}

fn synthetic_constraint_oid(table: &TableCatalog, ordinal: u32) -> ObjectId {
    let raw = table.id.as_object_id().as_raw_id();
    ObjectId::new(raw.wrapping_mul(131).wrapping_add(ordinal + 1))
}

fn make_foreign_key_constraint_row(
    schema_id: SchemaId,
    table: &TableCatalog,
    foreign_table: &TableCatalog,
    constraint_name: String,
    conkey: Vec<i16>,
    confkey: Vec<i16>,
    on_delete: Option<ReferentialAction>,
    on_update: Option<ReferentialAction>,
    ordinal: u32,
) -> PgConstraint {
    PgConstraint {
        oid: synthetic_constraint_oid(table, ordinal),
        conname: constraint_name,
        connamespace: schema_id,
        contype: "f".to_owned(),
        condeferrable: false,
        convalidated: true,
        conrelid: table.id.as_relation_id(),
        contypid: 0,
        conindid: RelationId::new(0),
        conparentid: 0,
        confrelid: foreign_table.id.as_relation_id().as_raw_id() as i32,
        confupdtype: foreign_key_action_code(on_update).to_owned(),
        confdeltype: foreign_key_action_code(on_delete).to_owned(),
        confmatchtype: "s".to_owned(),
        conislocal: true,
        coninhcount: 0,
        connoinherit: true,
        conkey: Some(conkey),
        confkey: Some(confkey),
        conpfeqop: None,
        conppeqop: None,
        conffeqop: None,
        confdelsetcols: None,
        conexclop: None,
        conbin: None,
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{
        ColumnCatalog, ColumnDesc, ColumnId, DatabaseId, SchemaId, StreamJobStatus, TableId,
    };
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

    use super::*;

    fn visible_column(name: &str, id: i32) -> ColumnCatalog {
        ColumnCatalog::visible(ColumnDesc::named(
            name,
            ColumnId::new(id),
            risingwave_common::types::DataType::Int32,
        ))
    }

    fn test_table(
        id: u32,
        name: &str,
        definition: &str,
        columns: Vec<ColumnCatalog>,
        pk_indices: Vec<usize>,
    ) -> TableCatalog {
        TableCatalog {
            id: TableId::new(id),
            schema_id: SchemaId::new(1),
            database_id: DatabaseId::new(1),
            name: name.to_owned(),
            columns,
            pk: pk_indices
                .into_iter()
                .map(|column_index| ColumnOrder::new(column_index, OrderType::ascending()))
                .collect(),
            table_type: TableType::Table,
            stream_job_status: StreamJobStatus::Created,
            definition: definition.to_owned(),
            ..Default::default()
        }
    }

    #[test]
    fn test_make_foreign_key_constraint_row() {
        let parent = test_table(
            10,
            "parent",
            "create table parent (id int primary key);",
            vec![visible_column("id", 1)],
            vec![0],
        );
        let child = test_table(
            20,
            "child",
            "create table child (id int primary key, parent_id int references parent(id));",
            vec![visible_column("id", 1), visible_column("parent_id", 2)],
            vec![0],
        );

        let row = make_foreign_key_constraint_row(
            SchemaId::new(1),
            &child,
            &parent,
            "child_parent_id_fkey".to_owned(),
            vec![2],
            vec![1],
            Some(ReferentialAction::Cascade),
            Some(ReferentialAction::SetNull),
            0,
        );

        assert_eq!(row.contype, "f");
        assert_eq!(row.conname, "child_parent_id_fkey");
        assert_eq!(row.conrelid, child.id.as_relation_id());
        assert_eq!(row.confrelid, parent.id.as_relation_id().as_raw_id() as i32);
        assert_eq!(row.conkey, Some(vec![2]));
        assert_eq!(row.confkey, Some(vec![1]));
        assert_eq!(row.confdeltype, "c");
        assert_eq!(row.confupdtype, "n");
    }

    #[test]
    fn test_default_foreign_key_name() {
        let name = default_foreign_key_name(
            "child",
            &[
                risingwave_sqlparser::ast::Ident::from("parent_id"),
                risingwave_sqlparser::ast::Ident::from("tenant_id"),
            ],
        );
        assert_eq!(name, "child_parent_id_tenant_id_fkey");
    }
}
